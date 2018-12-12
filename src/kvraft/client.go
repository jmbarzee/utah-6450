package raftkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"cs6450.utah.systems/u1177988/labs/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	// for keeping track of the most recent leader (probably an unnecessary optimization)
	Term   int
	Leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		ID:  nrand(),
	}
	leader := ck.Leader

Loop:
	for {
		reply := GetReply{}
		okChan := ck.SendGet(leader, &args, &reply)
		select {
		case ok := <-okChan:
			if !ok {
				leader = (leader + 1) % len(ck.servers)
			} else if reply.Status == StatusNotLead {
				leader = (leader + 1) % len(ck.servers)
			} else if reply.Status == StatusPending {
				if reply.Term >= ck.Term {
					ck.Term = reply.Term
					ck.Leader = leader
				}
			} else if reply.Status == StatusSuccess {
				if reply.Term >= ck.Term {
					ck.Term = reply.Term
					ck.Leader = leader
				}
				return reply.Value
			}
		case <-time.After(time.Millisecond * 100):
			leader = (leader + 1) % len(ck.servers)
			continue Loop
		}
	}
}

func (ck *Clerk) SendGet(id int, args *GetArgs, reply *GetReply) chan bool {
	okChan := make(chan bool)
	go func() {
		okChan <- ck.servers[id].Call("RaftKV.Get", args, reply)
		close(okChan)
	}()
	return okChan
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, method Method) {
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		ID:     nrand(),
		Method: method,
	}
	leader := ck.Leader

Loop:
	for {
		reply := PutAppendReply{}
		okChan := ck.SendPutAppend(leader, &args, &reply)
		select {
		case ok := <-okChan:
			if !ok {
				leader = (leader + 1) % len(ck.servers)
			} else if reply.Status == StatusNotLead {
				leader = (leader + 1) % len(ck.servers)
			} else if reply.Status == StatusPending {
				if reply.Term >= ck.Term {
					ck.Term = reply.Term
					ck.Leader = leader
				}
			} else if reply.Status == StatusSuccess {
				if reply.Term >= ck.Term {
					ck.Term = reply.Term
					ck.Leader = leader
				}
				return
			}
		case <-time.After(time.Millisecond * 100):
			leader = (leader + 1) % len(ck.servers)
			continue Loop
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, MethodPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, MethodAppend)
}

func (ck *Clerk) SendPutAppend(id int, args *PutAppendArgs, reply *PutAppendReply) chan bool {
	okChan := make(chan bool)
	go func() {
		okChan <- ck.servers[id].Call("RaftKV.PutAppend", args, reply)
		close(okChan)
	}()
	return okChan
}
