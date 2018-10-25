package raft

import (
	"fmt"
	"log"
)

// WARNING! ChangeState assumes that caller holds the lock
func (rf *Raft) ChangePeerStates(peer int, peerState PeerState) {
	rf.debugf(PeerStates, "ChangePeerStates\n %v.{%v %v} -> {%v %v}", peer,
		rf.PeerStates[peer].NextIndex, rf.PeerStates[peer].MatchIndex,
		peerState.NextIndex, peerState.MatchIndex)

	// TODO

}

// Debugging
const (
	Dump         = "Dump"
	Routine      = "Routine"
	State        = "State"
	PeerStates   = "PeerStates"
	Locks        = "Locks"
	Message      = "Message"
	Unclassified = "Unclassified"
)

// WARNING! catagory="Dump" assumes that caller holds the lock
func (rf *Raft) debugf(catagory, format string, a ...interface{}) {
	const (
		debug = true

		debugDump         = true
		debugRoutine      = true
		debugState        = true
		debugPeerStates   = true
		debugLocks        = false
		debugMessage      = true
		debugUnclassified = true
	)

	const dumpMessage = `		Term:%v Lead:%v Vote:%v Commit:%v
		Entries: %v
		PeerStates: %v
`

	if !debug {
		return
	}

	switch catagory {
	case Dump:
		if debugDump {
			entries := entriesToString(rf.Entries)
			states := statesToString(rf.PeerStates)
			rf.logf(format+dumpMessage, append(a, rf.Term, rf.Leader, rf.Vote, rf.CommitIndex, entries, states)...)
		}
	case Routine:
		if debugRoutine {
			rf.logf(format, a...)
		}
	case State:
		if debugState {
			rf.logf(format, a...)
		}
	case PeerStates:
		if debugPeerStates {
			rf.logf(format, a...)
		}
	case Locks:
		if debugLocks {
			rf.logf(format, a...)
		}
	case Message:
		if debugMessage {
			rf.logf(format, a...)
		}
	case Unclassified:
		if debugUnclassified {
			rf.logf(format, a...)
		}
	}
}

func (rf *Raft) logf(format string, a ...interface{}) {
	args := make([]interface{}, 1)
	args[0] = rf.me
	args = append(args, a...)
	log.Printf("%v."+format, args...)
}
func (rf *Raft) fatalf(format string, a ...interface{}) {
	args := make([]interface{}, 1)
	args[0] = rf.me
	args = append(args, a...)
	err := fmt.Errorf("%v."+format, args...)
	log.Printf(err.Error())
	panic(err)
}

func entriesToString(entries []Entry) string {
	entryString := "{ "
	for _, entry := range entries {
		entryString += fmt.Sprintf("{%v %v %v} ", entry.Index, entry.Term, entry.Command)
	}
	entryString += "}"
	return entryString
}
func statesToString(states []PeerState) string {
	statesString := "{ "
	for _, state := range states {
		statesString += fmt.Sprintf("{Nxt:%v Mtc:%v} ", state.NextIndex, state.MatchIndex)
	}
	statesString += "}"
	return statesString
}
