package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil{
		return &kvrpcpb.RawGetResponse{}, err
	}
	defer storageReader.Close()
	var rawGet = &kvrpcpb.RawGetResponse{}
	rawGet.Value, err = storageReader.GetCF(req.GetCf(), req.Key)
	if rawGet.Value == nil{
		rawGet.NotFound = true
	}
	return rawGet, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	reqPut := storage.Put{
		Value: req.Value,
		Key: req.Key,
		Cf: req.Cf,
	}
	batch := storage.Modify{
		Data: reqPut,
	}
	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err!= nil{
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	reqDelete := storage.Delete{
		Cf: req.Cf,
		Key: req.Key,
	}
	batch := storage.Modify{
		Data: reqDelete,
	}
	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err !=nil{
		return nil, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	storageReader,err :=server.storage.Reader(req.Context)
	if err != nil{
		return &kvrpcpb.RawScanResponse{}, err
	}
	kvPairs := []*kvrpcpb.KvPair{}
	dbIter := storageReader.IterCF(req.Cf)
	dbIter.Seek(req.StartKey)
	numLimit := req.Limit
	for dbIter.Valid() != false{
		var kvPair = &kvrpcpb.KvPair{}
		kvPair.Key = dbIter.Item().Key()
		kvPair.Value, _ = dbIter.Item().Value()
		kvPairs = append(kvPairs, kvPair)
		numLimit--
		if numLimit == 0{
			break
		}
		dbIter.Next()
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvPairs,
	}, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	var response=&kvrpcpb.GetResponse{}
	storageReader,err:=server.storage.Reader(req.Context)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	defer storageReader.Close()
	txn:=mvcc.NewMvccTxn(storageReader,req.Version)
	lockGet,err:=txn.GetLock(req.Key)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	if lockGet!=nil&&lockGet.Ts<req.Version{
		response.Error=&kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lockGet.Primary,
				LockVersion: lockGet.Ts,
				Key: req.Key,
				LockTtl: lockGet.Ttl,
			},
		}
		return response,nil
	}
	valueGet,err:=txn.GetValue(req.Key)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	response.Value=valueGet
	if valueGet==nil{
		response.NotFound=true
	}
	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	var response *kvrpcpb.PrewriteResponse=&kvrpcpb.PrewriteResponse{}
	storageReader,err:=server.storage.Reader(req.Context)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	defer storageReader.Close()
	if len(req.Mutations)==0{
		return response,nil
	}
	txn:=mvcc.NewMvccTxn(storageReader,req.StartVersion)
	keyErr:=make([]*kvrpcpb.KeyError,0)
	var lockGet *mvcc.Lock
	for _,mutation:=range req.Mutations{
		write,timeStamp,err:=txn.MostRecentWrite(mutation.Key)
		if err!=nil {
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		if write!=nil&&timeStamp>=req.StartVersion{
			keyErr=append(keyErr,&kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs: req.StartVersion,
					ConflictTs: timeStamp,
					Key: mutation.Key,
					Primary: req.PrimaryLock,
				},
			})
			continue
		}
		lockGet,err=txn.GetLock(mutation.Key)
		if err!=nil{
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		if lockGet!=nil&&lockGet.Ts!=req.StartVersion{
			keyErr=append(keyErr,&kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lockGet.Primary,
					LockVersion: lockGet.Ts,
					Key: mutation.Key,
					LockTtl: lockGet.Ttl,
				},
			})
			continue
		}
		var kind mvcc.WriteKind
		switch mutation.Op{
		case kvrpcpb.Op_Put:
			kind=mvcc.WriteKindPut
			txn.PutValue(mutation.Key,mutation.Value)
		case kvrpcpb.Op_Del:
			kind=mvcc.WriteKindDelete
			txn.DeleteValue(mutation.Key)
		}
		txn.PutLock(mutation.Key,&mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts: req.StartVersion,
			Ttl: req.LockTtl,
			Kind: kind,
		})
	}
	if len(keyErr)>0{
		response.Errors=keyErr
		return response,nil
	}
	err=server.storage.Write(req.Context,txn.Writes())
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	var response *kvrpcpb.CommitResponse=&kvrpcpb.CommitResponse{}
	if len(req.Keys)==0{
		return response,nil
	}
	storageReader,err:=server.storage.Reader(req.Context)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	defer storageReader.Close()
	txn:=mvcc.NewMvccTxn(storageReader,req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	var lockGet *mvcc.Lock
	for _,key:=range req.Keys{
		lockGet,err=txn.GetLock(key)
		if err!=nil{
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		if lockGet==nil{
			return response,nil
		}
		if lockGet.Ts!=req.StartVersion{
			response.Error=&kvrpcpb.KeyError{Retryable: "ok"}
			return response,nil
		}
		txn.PutWrite(key,req.CommitVersion,&mvcc.Write{
			StartTS: req.StartVersion,
			Kind: lockGet.Kind,
		})
		txn.DeleteLock(key)
	}
	err=server.storage.Write(req.Context,txn.Writes())
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil, err
	}
	server.Latches.ReleaseLatches(req.Keys)
	return response, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	var response *kvrpcpb.ScanResponse=&kvrpcpb.ScanResponse{}
	storageReader,err:=server.storage.Reader(req.Context)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	defer storageReader.Close()
	txn:=mvcc.NewMvccTxn(storageReader,req.Version)
	scan:=mvcc.NewScanner(req.StartKey,txn)
	defer scan.Close()
	var kvPairs []*kvrpcpb.KvPair
	limit:=req.GetLimit()
	for i:=uint32(0);i<limit;i++{
		key,value,err:=scan.Next()
		if err!=nil{
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		if key==nil{
			break
		}
		lockGet,err:=txn.GetLock(key)
		if err!=nil{
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		if lockGet!=nil&&lockGet.Ts<req.Version{
			kvPairs=append(kvPairs,&kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						LockTtl: lockGet.Ttl,
						LockVersion: lockGet.Ts,
						Key: key,
						PrimaryLock: lockGet.Primary,
					},
				},
				Key: key,
			})
			continue
		}
		if value!=nil{
			kvPairs=append(kvPairs,&kvrpcpb.KvPair{
				Key: key,
				Value: value,
			})
		}
	}
	response.Pairs=kvPairs
	return response, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	response:=&kvrpcpb.CheckTxnStatusResponse{}
	storeReader,err:=server.storage.Reader(req.Context)
	if err != nil {
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	defer storeReader.Close()
	txn := mvcc.NewMvccTxn(storeReader, req.LockTs)
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			response.CommitVersion = ts
		}
		return response, nil
	}
	lockGet,err:=txn.GetLock(req.PrimaryKey)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	if lockGet==nil{
		txn.PutWrite(req.PrimaryKey,req.LockTs,&mvcc.Write{
			StartTS: req.LockTs,
			Kind: mvcc.WriteKindRollback,
		})
		err=server.storage.Write(req.Context,txn.Writes())
		if err!=nil{
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		response.Action=kvrpcpb.Action_LockNotExistRollback
		return response,nil
	}
	if mvcc.PhysicalTime(lockGet.Ts)+lockGet.Ttl<=mvcc.PhysicalTime(req.CurrentTs){
		txn.DeleteLock(lockGet.Primary)
		txn.DeleteValue(lockGet.Primary)
		txn.PutWrite(req.PrimaryKey,req.LockTs,&mvcc.Write{
			StartTS: req.LockTs,
			Kind: mvcc.WriteKindRollback,
		})
		err=server.storage.Write(req.Context,txn.Writes())
		if err!=nil{
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		response.Action=kvrpcpb.Action_TTLExpireRollback
	}
	return response, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	var response *kvrpcpb.BatchRollbackResponse=&kvrpcpb.BatchRollbackResponse{}
	storageReader,err:=server.storage.Reader(req.Context)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	defer storageReader.Close()
	txn:=mvcc.NewMvccTxn(storageReader,req.StartVersion)
	var lockGet *mvcc.Lock=&mvcc.Lock{}
	for _,key:=range req.Keys{
		write,_,err:=txn.CurrentWrite(key)
		if err!=nil{
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		if write!=nil{
			if write.Kind==mvcc.WriteKindRollback {
				continue
			} else{
				response.Error=&kvrpcpb.KeyError{
					Abort: "ok",
				}
				return response,nil
			}
		}
		lockGet,err=txn.GetLock(key)
		if err!=nil{
			regionErr,match:=err.(*raft_storage.RegionError)
			if match{
				response.RegionError=regionErr.RequestErr
				return response,nil
			}
			return nil,err
		}
		if lockGet!=nil&&lockGet.Ts==req.StartVersion{
			txn.DeleteValue(key)
			txn.DeleteLock(key)
		}
		txn.PutWrite(key,req.StartVersion,&mvcc.Write{
			Kind: mvcc.WriteKindRollback,
			StartTS: req.StartVersion,
		})
	}
	err=server.storage.Write(req.Context,txn.Writes())
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	return response, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	var response *kvrpcpb.ResolveLockResponse=&kvrpcpb.ResolveLockResponse{}
	if req.StartVersion==0{
		return response,nil
	}
	storageReader,err:=server.storage.Reader(req.Context)
	if err!=nil{
		regionErr,match:=err.(*raft_storage.RegionError)
		if match{
			response.RegionError=regionErr.RequestErr
			return response,nil
		}
		return nil,err
	}
	defer storageReader.Close()
	iter:=storageReader.IterCF(engine_util.CfLock)
	defer iter.Close()
	var keys [][]byte
	for ;iter.Valid();iter.Next(){
		item:=iter.Item()
		value,err:=item.ValueCopy(nil)
		if err!=nil {
			return response,err
		}
		lock,err:=mvcc.ParseLock(value)
		if err!=nil {
			return response,err
		}
		if lock.Ts==req.StartVersion {
			key:=item.KeyCopy(nil)
			keys=append(keys,key)
		}
	}
	if len(keys)==0 {
		return response,nil
	}
	if req.CommitVersion==0 {
		resp1,err:=server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context: req.Context,
			StartVersion: req.StartVersion,
			Keys: keys,
		})
		response.Error=resp1.Error
		response.RegionError=resp1.RegionError
		return response,err
	} else {
		resp1,err:=server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context: req.Context,
			StartVersion: req.StartVersion,
			Keys: keys,
			CommitVersion: req.CommitVersion,
		})
		response.Error=resp1.Error
		response.RegionError=resp1.RegionError
		return response,err
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
