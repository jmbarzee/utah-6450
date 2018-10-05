package raft

type RequestVoteArgs struct {
	Term     int
	Canidate int
}

type RequestVoteReply struct {
	Term   int
	Leader int
	Vote   int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	dumpState("RequestVote", rf)
	debugLocksf("%v.RequestVote - lock\n", rf.me)
	rf.mu.Lock()
	{
		if args.Term > rf.Term {
			// Vote for canidate
			rf.Term = args.Term
			rf.Leader = -1
			rf.Vote = args.Canidate
			rf.recentHeartbeat = true
		} else {
			// Canidate's term is not new
		}
		reply.Term = rf.Term
		reply.Leader = rf.Leader
		reply.Vote = rf.Vote
	}
	rf.mu.Unlock()
	debugLocksf("%v.RequestVote - unlock\n", rf.me)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, responseChan chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		responseChan <- reply
	}
}

func emptyResponseChan(responseChan chan *RequestVoteReply, remaining int) {
	for i := 0; i < remaining; i++ {
		<-responseChan
	}
	close(responseChan)
}

type ApplyMsgArgs struct {
	Term   int
	Leader int
	// TODO add msg
}

type ApplyMsgReply struct {
	Term   int
	Leader int
}

func (rf *Raft) ApplyMsg(args *ApplyMsgArgs, reply *ApplyMsgReply) {
	dumpState("ApplyMsg", rf)
	debugLocksf("%v.ApplyMsg - lock\n", rf.me)
	rf.mu.Lock()
	{
		if args.Term > rf.Term {
			// Leader is in newer term
			rf.Term = args.Term
			rf.Leader = args.Leader
			rf.Vote = -1
			rf.recentHeartbeat = true

		} else if args.Term == rf.Term {
			// Leader won election
			rf.Leader = args.Leader
			rf.recentHeartbeat = true

		} else {
			// Leader is in old term
		}
		reply.Term = rf.Term
		reply.Leader = rf.Leader
	}
	rf.mu.Unlock()
	debugLocksf("%v.ApplyMsg - unlock\n", rf.me)
}

func (rf *Raft) sendApplyMsg(server int, args *ApplyMsgArgs) {
	reply := &ApplyMsgReply{}
	ok := rf.peers[server].Call("Raft.ApplyMsg", args, reply)

	if !ok {
		return
	}

	dumpState("sendApplyMsg", rf)
	debugLocksf("%v.sendApplyMsg - lock\n", rf.me)
	rf.mu.Lock()
	{
		if reply.Term > rf.Term {
			// Peer is in newer term
			rf.Term = args.Term
			rf.Leader = args.Leader
			rf.Vote = -1
		} else {
			// peer is up to date
		}
	}
	rf.mu.Unlock()
	debugLocksf("%v.sendApplyMsg - unlock\n", rf.me)
}
