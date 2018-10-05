package raft

import "log"

// Debugging
const (
	debug      = false
	debugState = true
	debugLocks = false
)

func debugf(format string, a ...interface{}) {
	if debug {
		log.Printf(format, a...)
	}
}

func debugStatef(format string, a ...interface{}) {
	if debug && debugState {
		log.Printf(format, a...)
	}
}

func dumpState(context string, rf *Raft) {
	//debugLocksf("%v.dumpState - waiting\n", rf.me)
	rf.mu.Lock()
	{
		debugStatef("%v.%v - { T:%v L:%v V:%v }\n", rf.me, context, rf.Term, rf.Leader, rf.Vote)
	}
	rf.mu.Unlock()
	//debugLocksf("%v.dumpState - freed\n", rf.me)
}

func debugLocksf(format string, a ...interface{}) {
	if debug && debugLocks {
		log.Printf(format, a...)
	}
}
