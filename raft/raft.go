// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType


	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// let electionTime randomized
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
		return nil
	}
	// Your Code Here (2A).
	newRaft := &Raft{
		id: c.ID,
		RaftLog: newLog(c.Storage),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		msgs: nil,
		votes: make(map[uint64]bool),
		Prs: make(map[uint64]*Progress),
	}
	hardState, confState, _ := newRaft.RaftLog.storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	for _,v:=range c.peers{
		newRaft.Prs[v]=&Progress{Next: newRaft.RaftLog.LastIndex()+1}
		if v==c.ID{
			newRaft.Prs[v].Match=newRaft.RaftLog.LastIndex()
		}
	}
	newRaft.becomeFollower(0,None)
	newRaft.Term, newRaft.Vote, newRaft.RaftLog.committed = hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit()
	if c.Applied > 0 {
		newRaft.RaftLog.applied = c.Applied
	}
	return newRaft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	logIndex:=r.Prs[to].Next-1
	logTerm,err:=r.RaftLog.Term(logIndex)
	if err != nil {
		if err==ErrCompacted{
			// send snapshot append
			snapshot,err:=r.RaftLog.storage.Snapshot()
			if err!=nil{
				return false
			}
			msg:=pb.Message{
				Term: r.Term,
				From: r.id,
				To: to,
				MsgType: pb.MessageType_MsgSnapshot,
				Snapshot: &snapshot,
			}
			r.msgs=append(r.msgs,msg)
			r.Prs[to].Next=snapshot.Metadata.Index+1
			return true
		}
		panic(err)
	}
	msg:=pb.Message{
		Term: r.Term,
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: r.id,
		Index: logIndex,
		Commit: r.RaftLog.committed,
		LogTerm: logTerm,
		Entries: []*pb.Entry{},
	}
	i:=int(r.Prs[to].Next-r.RaftLog.FirstIndex)
	for ;i<len(r.RaftLog.entries);i++{
		msg.Entries=append(msg.Entries,&r.RaftLog.entries[i])
	}
	r.msgs=append(r.msgs,msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg:=pb.Message{
		Term: r.Term,
		From: r.id,
		To: to,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs=append(r.msgs,msg)
}

//sendRequestVotes sends RequestVote to the given peer
func (r *Raft) sendRequestVote(to uint64){
	//call for votes
	msg:=pb.Message{
		Term: r.Term,
		From: r.id,
		To: to,
		Index: r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
		MsgType: pb.MessageType_MsgRequestVote,
	}
	r.msgs=append(r.msgs,msg)
}
// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower,StateCandidate:
		r.electionElapsed++
		if r.electionElapsed>=r.randomElectionTimeout{
			r.electionElapsed=0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				Term: r.Term,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed>=r.heartbeatTimeout{
			r.heartbeatElapsed=0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				Term: r.Term,
			})
		}
	}
}

//reStart helps to restart the time
func (r *Raft) reStart(){
	r.electionElapsed=0
	r.randomElectionTimeout=r.electionTimeout+rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reStart()
	r.State=StateFollower
	r.Term=term
	r.Vote=None
	r.Lead=lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// Leader can't become to candidate directly
	r.reStart()
	if r.State==StateLeader{
		return
	}
	r.Term++
	r.State=StateCandidate
	r.votes = make(map[uint64]bool)
	r.votes[r.id]=true
	r.Vote=r.id
	r.Lead=None
	if len(r.Prs)==1{
		r.becomeLeader()
	}
	return
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State==StateLeader{
		return
	}
	r.reStart()
	r.heartbeatElapsed=0
	r.State=StateLeader
	r.Lead=r.id
	r.Vote=None
	for peer,_ := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = r.RaftLog.LastIndex() + 2
			r.Prs[peer].Match = r.RaftLog.LastIndex() + 1
		} else {
			r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
		}
	}
	r.RaftLog.entries=append(r.RaftLog.entries,pb.Entry{
		Term: r.Term,
		Index: r.RaftLog.LastIndex()+1,
	})
	for peer,_:=range r.Prs {
		if peer==r.id{
			continue
		}
		r.sendAppend(peer)
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
	return
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term>r.Term {
		r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.heartbeatElapsed=0
			for peer,_:=range r.Prs{
				if peer!=r.id {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.heartbeatElapsed=0
			for peer,_:=range r.Prs{
				if peer!=r.id {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgAppend:
			if m.Term==r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			if m.Term == r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for peer,_:=range r.Prs{
				if peer!=r.id{
					r.sendHeartbeat(peer)
				}
			}
		case pb.MessageType_MsgPropose:
			r.appendEntries(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.sendAppend(m.From)
		}
	}
	return nil
}

//appendEntries receive data from client and add to leader's entry
func (r *Raft) appendEntries(m pb.Message){
	entries:=m.Entries
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Index = lastIndex + uint64(i) + 1
		entry.Term = r.Term
		//if entry.EntryType == pb.EntryType_EntryConfChange {
		//	if r.PendingConfIndex != None {
		//		continue
		//	}
		//	r.PendingConfIndex = entry.Index
		//}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	for peer,_:=range r.Prs{
		if peer!=r.id{
			r.sendAppend(peer)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}


// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg:=pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
	}
	if m.Term!=None&&m.Term<r.Term {
		msg.Reject=true
		r.msgs=append(r.msgs,msg)
		return
	}
	r.electionElapsed=0
	r.randomElectionTimeout=r.electionTimeout+rand.Intn(r.electionTimeout)
	r.Lead=m.From
	lastIndex:=r.RaftLog.LastIndex()
	if m.Index>lastIndex{
		msg.Reject=true
		msg.Index=lastIndex+1
		r.msgs=append(r.msgs,msg)
		return
	}
	if m.Index>=r.RaftLog.FirstIndex{
		logTerm,err:=r.RaftLog.Term(m.Index)
		if err!=nil{
			panic(err)
		}
		if logTerm!=m.LogTerm{
			msg.Reject=true
			msg.Index=m.Index
			msg.LogTerm=logTerm
			r.msgs=append(r.msgs,msg)
			return
		}
	}
	for i,entry:=range m.Entries{
		if entry.Index<r.RaftLog.FirstIndex{
			continue
		}
		if entry.Index<=r.RaftLog.LastIndex(){
			logTerm,err:=r.RaftLog.Term(entry.Index)
			if err!=nil{
				panic(err)
			}
			if logTerm!=entry.Term{
				entryIndex:=entry.Index-r.RaftLog.FirstIndex
				r.RaftLog.entries[entryIndex]=*entry
				r.RaftLog.entries=r.RaftLog.entries[:entryIndex+1]
				r.RaftLog.stabled=min(r.RaftLog.stabled,entry.Index-1)
			}
		} else {
			for j:=i;j<len(m.Entries);j++{
				r.RaftLog.entries=append(r.RaftLog.entries,*m.Entries[j])
			}
			break
		}
	}
	if m.Commit>r.RaftLog.committed{
		r.RaftLog.committed=min(m.Commit,m.Index+uint64(len(m.Entries)))
	}
	msg.Reject=false
	msg.Index=r.RaftLog.LastIndex()
	r.msgs=append(r.msgs,msg)
}
// handleVoteResponse handle the vote request from each candidate
func (r *Raft) handleRequestVote(m pb.Message){
	msg:=pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	}
	msg.Reject=true
	if r.Term>m.Term&&m.Term!=None{
		r.msgs=append(r.msgs,msg)
		return
	}
	if r.Vote!=None&&r.Vote!=m.From{
		r.msgs=append(r.msgs,msg)
		return
	}
	if r.RaftLog.LastTerm()>m.LogTerm||r.RaftLog.LastTerm()==m.LogTerm&&r.RaftLog.LastIndex()>m.Index {
		r.msgs=append(r.msgs,msg)
		return
	}
	r.Vote=m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	msg.Reject=false
	r.msgs=append(r.msgs,msg)
}

// handleEntriesResponse handle the appendEntries response from each peer
func (r *Raft) handleAppendEntriesResponse(m pb.Message){
	if m.Term!=None&&m.Term<r.Term{
		return
	}
	if m.Reject==true{
		if m.Index==None{
			return
		}
		r.Prs[m.From].Next=m.Index
		r.sendAppend(m.From)
		return
	}
	if m.Index>r.Prs[m.From].Match{
		r.Prs[m.From].Match=m.Index
		r.Prs[m.From].Next=m.Index+1
		//reset committed
		matches:=make(uint64Slice,len(r.Prs))
		i:=0
		for _,val:=range r.Prs{
			matches[i]=val.Match
			i++
		}
		sort.Sort(matches)
		committed:=matches[(len(r.Prs)-1)/2]
		if committed>r.RaftLog.committed{
			logTerm,err:=r.RaftLog.Term(committed)
			if err!=nil{
				panic(err)
			}
			if logTerm==r.Term {
				r.RaftLog.committed=committed
				for peer,_:=range r.Prs {
					if peer!=r.id{
						r.sendAppend(peer)
					}
				}
			}
		}
	}
	return
}


// handleVoteResponse handle the vote response from each peer
func (r *Raft) handleVoteResponse(m pb.Message){
	num:=0
	if m.Term!=None&&m.Term<r.Term{
		return
	}
	if m.Reject==false{
		r.votes[m.From]=true
	} else {
		r.votes[m.From]=false
	}
	for _,vote:=range r.votes{
		if vote==true{
			num++
		}
	}
	if num>len(r.Prs)/2{
		r.becomeLeader()
	} else if len(r.votes)-num>len(r.Prs)/2{
		r.becomeFollower(r.Term,None)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg:=pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
	}
	if m.Term!=None&&m.Term<r.Term{
		msg.Reject=true
		r.msgs = append(r.msgs, msg)
		return
	}
	r.Lead=m.From
	r.becomeFollower(m.Term,m.From)
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	msg:=pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
		Reject: false,
	}
	// Snapshot has already committed
	if m.Snapshot.Metadata.Index<r.RaftLog.committed{
		msg.Index=r.RaftLog.committed
		r.msgs=append(r.msgs,msg)
		return
	}
	r.becomeFollower(m.Term,m.From)
	r.RaftLog.entries=nil
	r.RaftLog.LastIndex()
	r.RaftLog.FirstIndex=m.Snapshot.Metadata.Index+1
	r.RaftLog.committed=m.Snapshot.Metadata.Index
	r.RaftLog.stabled=m.Snapshot.Metadata.Index
	r.RaftLog.applied=m.Snapshot.Metadata.Index
	r.RaftLog.pendingSnapshot=m.Snapshot
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	msg.Index=r.RaftLog.LastIndex()
	r.msgs=append(r.msgs,msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}