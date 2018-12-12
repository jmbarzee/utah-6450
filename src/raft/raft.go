package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"

	"labrpc"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Term        int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Vote   int
	Term   int
	Leader int

	CommitIndex int

	Entries    []Entry
	PeerStates []PeerState

	recentHeartbeat bool

	killRoutines context.CancelFunc
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

type PeerState struct {
	NextIndex  int
	MatchIndex int

	sync.Mutex // Lock to protect shared access to this peer's state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	//debugLocksf("%v.dumpState - waiting\n", rf.me)
	rf.mu.Lock()
	{
		//debugStatef("%v.GetState - { T:%v L:%v V:%v }\n", rf.me, rf.Term, rf.Leader, rf.Vote)
		term = rf.Term
		isleader = rf.me == rf.Leader
	}
	rf.mu.Unlock()
	//debugLocksf("%v.dumpState - freed\n", rf.me)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := bytes.NewBuffer([]byte{})
	e := gob.NewEncoder(w)
	e.Encode(rf.Term)
	e.Encode(rf.Leader)
	e.Encode(rf.Vote)
	e.Encode(rf.Entries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data != nil && len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.Term)
		d.Decode(&rf.Leader)
		d.Decode(&rf.Vote)
		d.Decode(&rf.Entries)
	} else {
		// bootstrap without any state?
		rf.Term = 0
		rf.Leader = -1
		rf.Vote = -1

		rf.Entries = []Entry{
			Entry{
				Index:   0,
				Command: 0,
				Term:    0,
			},
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//rf.debugf(Routine, "Start\n")
	amLeader := false
	term := 0
	index := 0

	rf.debugf(Locks, "Start - lock\n")
	rf.mu.Lock()
	{
		amLeader = (rf.Leader == rf.me)
		if !amLeader {
			goto Unlock
		}

		rf.debugf(Dump, "Start - command:%v\n", command)
		term = rf.Term
		index = len(rf.Entries)

		newEntry := Entry{
			Index:   index,
			Command: command,
			Term:    term,
		}
		rf.Entries = append(rf.Entries, newEntry)
		rf.persist()

		for peer := range rf.peers {
			if peer == rf.me {
				// Don't send heartbeat to myself
				continue
			}
			go rf.sendApplyMsg(peer)
		}
	}
Unlock:
	rf.mu.Unlock()
	rf.debugf(Locks, "Start - unlock\n", rf.me)

	return index, term, amLeader
}

func (rf *Raft) Kill() {
	//rf.debugf(Locks, "Kill - lock\n")
	rf.mu.Lock()
	{
		rf.debugf(Dump, "Kill\n")
	}
	rf.mu.Unlock()
	//rf.debugf(Locks, "Kill - unlock\n", rf.me)

	rf.killRoutines()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.debugf(Locks, "Make - lock\n")
	rf.mu.Lock()
	{
		rf.peers = peers
		rf.persister = persister
		rf.me = me

		// Your initialization code here (2A, 2B, 2C).

		// initialize from state persisted before a crash
		rf.readPersist(rf.persister.ReadRaftState())

		if rf.Leader == rf.me {
			rf.PeerStates = make([]PeerState, len(peers))

		} else {
			rf.PeerStates = make([]PeerState, 0)
		}

		rf.recentHeartbeat = false

		var ctx context.Context
		ctx, rf.killRoutines = context.WithCancel(context.Background())
		go rf.sendHeartbeats(ctx)
		go rf.watchTimeout(ctx)
		go rf.reportLogs(ctx, applyCh)

		rf.debugf(Dump, "Make - me:%v\n", rf.me)
	}
	rf.mu.Unlock()
	rf.debugf(Locks, "Make - Unlock\n")

	return rf
}
