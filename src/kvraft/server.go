package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"sync"
	"time"

	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Method string
	Key    string
	Value  string
	ID     int64
	Index  int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// My Additions
	KVstore     map[string]string
	snapshot    bool
	requests    map[int]chan raft.ApplyMsg
	ResendTable map[int64]int64
}

func (kv *RaftKV) wait4Agreement(index int, term int, op Op) bool {
	kv.Lock(true)
	waitChan := make(chan raft.ApplyMsg, 1)
	kv.requests[index] = waitChan
	kv.Lock(false)

	select {
	case message := <-waitChan:
		if index == message.Index && term == message.Term {
			return true
		} else {
			return false
		}
	case <-time.After(500 * time.Millisecond):
		kv.Lock(true)
		delete(kv.requests, index)
		kv.Lock(false)
		return false
	}
}

func (kv *RaftKV) Lock(lock bool) {
	if lock {
		kv.mu.Lock()
	} else {
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.Lock(true)
	_, isLeader := kv.rf.GetState()
	reply.Err = ""
	reply.WrongLeader = false

	if !isLeader {
		kv.Lock(false)
		reply.WrongLeader = true
		return
	}

	op := Op{"Get", args.Key, "", args.ClientID, args.Index}

	kv.Lock(false)

	DPrintf("Starting get with key: %s\n", op.Key)
	ind, term, isLeader := kv.rf.Start(op)

	if isLeader {
		if kv.wait4Agreement(ind, term, op) {
			kv.Lock(true)
			if val, ok := kv.KVstore[args.Key]; ok {
				reply.Value = val
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			kv.Lock(false)
		} else {
			reply.WrongLeader = true
		}
	} else {
		reply.WrongLeader = true
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Lock(true)
	_, isLeader := kv.rf.GetState()
	reply.Err = ""
	reply.WrongLeader = false

	if !isLeader {
		kv.Lock(false)
		reply.WrongLeader = true
		return
	}
	op := Op{args.Op, args.Key, args.Value, args.ClientID, args.Index}

	DPrintf("ClientID %d and index %d\n", args.ClientID, args.Index)
	if dup, ok := kv.ResendTable[args.ClientID]; ok {
		if args.Index == dup {
			DPrintf("found dup\n")
			kv.Lock(false)
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	kv.Lock(false)
	DPrintf("Starting put/append with val: %s\n", op.Value)
	ind, term, isLeader := kv.rf.Start(op)

	if isLeader {
		if kv.wait4Agreement(ind, term, op) {
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}

	} else {
		reply.WrongLeader = true
	}

}

func (kv *RaftKV) checkForSnapshot(index int) {
	if kv.snapshot && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		kv.rf.TrimLog(index)
		kv.save()
	}
}

// Worker thread for Server
func (kv *RaftKV) watchApplyChan() {
	for {

		select {
		case ap := <-kv.applyCh:
			kv.Lock(true)

			if ap.UseSnapshot {
				kv.load(ap.Snapshot)
				kv.Lock(true)
				continue
			}

			cmd := ap.Command.(Op)

			if dup, ok := kv.ResendTable[cmd.ID]; !ok || dup != cmd.Index {
				switch cmd.Method {
				case "Put":
					kv.KVstore[cmd.Key] = cmd.Value
					kv.ResendTable[cmd.ID] = cmd.Index
				case "Append":
					kv.KVstore[cmd.Key] += cmd.Value
					kv.ResendTable[cmd.ID] = cmd.Index
				}

			}

			kv.checkForSnapshot(ap.Index)

			if c, ok := kv.requests[ap.Index]; ok {
				delete(kv.requests, ap.Index)
				c <- ap
			}
			kv.Lock(false)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *RaftKV) save() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.KVstore)
	snapshot := w.Bytes()
	kv.rf.PersistSnap(snapshot)
}

func (kv *RaftKV) load(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.KVstore)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
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
	kv.snapshot = (maxraftstate != -1) //if maxraftstate is -1 you don't need to snapshot.

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	kvData := persister.ReadSnapshot()

	if kv.snapshot && kvData != nil && len(kvData) > 0 {
		kv.load(kvData)
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.KVstore = make(map[string]string)
	kv.requests = make(map[int]chan raft.ApplyMsg)
	kv.ResendTable = make(map[int64]int64)
	go kv.watchApplyChan()

	return kv
}
