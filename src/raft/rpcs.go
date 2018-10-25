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
	// rf.debugf(Routine, "RequestVote\n")
	rf.debugf(Locks, "RequestVote - lock\n")
	rf.mu.Lock()
	{
		rf.debugf(Dump, "RequestVote\n")
		if args.Term > rf.Term {
			// Vote for canidate
			rf.Term = args.Term
			rf.Leader = -1
			rf.Vote = args.Canidate
			rf.recentHeartbeat = true
		} else {
			// Canidate's term is NOT new
		}
		reply.Term = rf.Term
		reply.Leader = rf.Leader
		reply.Vote = rf.Vote
	}
	rf.mu.Unlock()
	rf.debugf(Locks, "RequestVote - unlock\n")
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, responseChan chan *RequestVoteReply) {
	rf.debugf(Routine, "sendRequestVote -> %v\n", server)
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
	//rf.debugf(Routine, "ApplyMsg\n")
	rf.debugf(Locks, "ApplyMsg - lock\n")
	rf.mu.Lock()
	{
		rf.debugf(Dump, "ApplyMsg\n")

		if args.Term > rf.Term {
			// Leader is in newer term (and has a new message?)
			rf.Term = args.Term
			rf.Leader = args.Leader
			rf.Vote = -1

			rf.recentHeartbeat = true

		} else if args.Term == rf.Term {
			// Leader won election
			rf.Leader = args.Leader
			rf.recentHeartbeat = true

		} else {
			// Requester is old leader
		}
		reply.Term = rf.Term
		reply.Leader = rf.Leader
	}
	rf.mu.Unlock()
	rf.debugf(Locks, "ApplyMsg - lock\n")
}

func (rf *Raft) sendApplyMsg(server int, args *ApplyMsgArgs) {
	reply := &ApplyMsgReply{}
	ok := rf.peers[server].Call("Raft.ApplyMsg", args, reply)

	if !ok {
	// rf.debugf(Routine, "sendApplyMsg -> %v\n", server)
		return
	}

	rf.mu.Lock()
	rf.debugf(Locks, "sendApplyMsg - peer.%v.lock\n", server)
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
}
