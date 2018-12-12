package raftkv

import (
	"context"
	"encoding/gob"
	"log"
	"sync"

	"cs6450.utah.systems/u1177988/labs/src/labrpc"
	"cs6450.utah.systems/u1177988/labs/src/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Method string

const (
	MethodGet    Method = "Get"
	MethodPut    Method = "Put"
	MethodAppend Method = "Append"
)

type Op struct {
	// Your definitions here.
	Method Method
	Key    string
	Value  string
	ID     int64
}

type RaftKV struct {
	sync.Mutex

	me int
	rf *raft.Raft

	cancelAllContext context.CancelFunc

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KVstore  map[string]string
	Commited map[int64]Op
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	kv.cancelAllContext()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.Commited = make(map[int64]Op)
	kv.KVstore = make(map[string]string)

	applyCh := make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, applyCh)

	var ctx context.Context
	ctx, kv.cancelAllContext = context.WithCancel(context.Background())
	go kv.watchCommits(ctx, applyCh)

	return kv
}
