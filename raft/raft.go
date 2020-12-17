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
		Term: uint64(0),
		State: StateFollower,
		RaftLog: newLog(c.Storage),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
		msgs: make([]pb.Message,0),
		votes: make(map[uint64]bool),
		Prs: make(map[uint64]*Progress),
	}
	for _,v:=range c.peers{
		newRaft.votes[v]=false
		newRaft.Prs[v]=&Progress{}
	}
	return newRaft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msg:=pb.Message{
		Term: r.Term,
		MsgType: pb.MessageType_MsgPropose,
		To: to,
		From: r.id,
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
				From: r.id,
				Term: r.Term,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed>=r.heartbeatTimeout{
			r.heartbeatElapsed=0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
		To: r.id,
				Term: r.Term,
			})
		}
	}
}

//reStart helps to restart the time
func (r *Raft) reStart(){
	r.electionElapsed=0
	r.heartbeatElapsed=0
	r.randomElectionTimeout=r.electionTimeout+rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reStart()
	if r.Term <= term{
		r.State=StateFollower
		r.Term=term
		r.Vote=None
		r.Lead=lead
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// Leader can't become to candidate indirectly
	r.reStart()
	if r.State==StateLeader{
		return
	}
	r.Term++
	r.State=StateCandidate
	//reload the votes situation
	for peer,_:=range r.votes{
		r.votes[peer]=false
	}
	r.votes[r.id]=true
	r.Vote=r.id
	r.msgs=[]pb.Message{}
	if len(r.votes)==1{
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
	r.State=StateLeader
	r.Lead=r.id
	return
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term,m.From)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for peer,_:=range r.votes{
				if peer!=r.id {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for peer,_:=range r.votes{
				if peer!=r.id {
					r.sendRequestVote(peer)
				}
			}
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term,m.From)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for peer,_:=range r.votes{
				if peer!=r.id{
					r.sendHeartbeat(peer)
				}
			}
		case pb.MessageType_MsgPropose:
			for peer,_:=range r.votes{
				if peer!=r.id{
					r.sendAppend(peer)
				}
			}
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term,m.From)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		}
	}
	return nil
}


// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
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
	if r.State==StateLeader || r.State==StateCandidate{
		r.msgs=append(r.msgs,msg)
		return
	}
	if (r.Vote==None || r.Vote==m.From) && (r.RaftLog.LastTerm()<m.LogTerm ||
		r.RaftLog.LastTerm()==m.LogTerm && r.RaftLog.LastIndex()<=m.Index) {
		msg.Reject = false
		r.Vote = m.From
	}
	r.msgs=append(r.msgs,msg)
}

// handleVoteResponse handle the vote response from each peer
func (r *Raft) handleVoteResponse(m pb.Message){
	num:=0
	if m.Reject==false{
		r.votes[m.From]=true
	}
	for _,vote:=range r.votes{
		if vote==true{
			num++
		}
	}
	if num>len(r.votes)/2{
		r.becomeLeader()
		r.Step(pb.Message{
			Term: r.Term,
			MsgType: pb.MessageType_MsgBeat,
			From: r.id,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.becomeFollower(m.Term,m.From)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
