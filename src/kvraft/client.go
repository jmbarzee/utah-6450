package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID    int64
	Index       int64
	LastLeadder int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.servers = servers
	// You'll have to add code here.
	ck.LastLeadder = 0
	bigx, _ := rand.Int(rand.Reader, big.NewInt(int64(1)<<62))
	ck.ClientID = bigx.Int64()
	ck.Index = 0

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

	// You will have to modify this function.
	size := len(ck.servers)

	for {
		args := GetArgs{key, ck.ClientID, ck.Index}
		reply := GetReply{}

		ck.LastLeadder %= size
		done := make(chan bool, 1)
		go func() {
			ok := ck.servers[ck.LastLeadder].Call("RaftKV.Get", &args, &reply)
			done <- ok
		}()

		select {

		case ok := <-done:
			if ok && !reply.WrongLeader {
				if reply.Err == OK {
					return reply.Value
				}
				return ""
			}
			ck.LastLeadder++

		case <-time.After(150 * time.Millisecond):
			ck.LastLeadder++
			continue
		}
	}
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
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	size := len(ck.servers)
	for {
		args := PutAppendArgs{key, value, op, ck.ClientID, ck.Index}
		reply := GetReply{}

		ck.LastLeadder %= size
		done := make(chan bool, 1)
		go func() {
			ok := ck.servers[ck.LastLeadder].Call("RaftKV.PutAppend", &args, &reply)
			done <- ok
		}()

		select {
		case ok := <-done:
			if ok && !reply.WrongLeader && reply.Err == OK {
				bigx, _ := rand.Int(rand.Reader, big.NewInt(int64(1)<<62))
				ck.Index = bigx.Int64()
				return
			}
			ck.LastLeadder++

		case <-time.After(300 * time.Millisecond):
			ck.LastLeadder++
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
