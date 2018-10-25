package raft

import (
	"fmt"
	"log"
)

// Debugging
const (
	Dump         = "Dump"
	Routine      = "Routine"
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
