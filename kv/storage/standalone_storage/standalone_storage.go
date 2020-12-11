package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

//creat a new StandAlongStore with the information of config
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := filepath.Join(conf.DBPath, "kv")
	kvDB := engine_util.CreateDB(kvPath, false)
	raftPath := filepath.Join(conf.DBPath, "raft")
	raftDB := engine_util.CreateDB(raftPath, false)

	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

//creat a Badger transaction to handle  a series of operations facing storage
type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneReader(kvTxn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		kvTxn: kvTxn,
	}
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneReader(s.engine.Kv.NewTransaction(false)), nil
}


func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			err := engine_util.PutCF(s.engine.Kv, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			del := b.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engine.Kv, del.Cf, del.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}


func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.kvTxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.kvTxn)
}

func (s *StandAloneReader) Close() {
	s.kvTxn.Discard()
}
