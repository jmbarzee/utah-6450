package raft

import (
	"time"
)

type RequestVoteArgs struct {
	Term     int
	Canidate int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// rf.debugf(Routine, "RequestVote\n")
	rf.debugf(Locks, "RequestVote - lock\n")
	rf.mu.Lock()
	{
		rf.debugf(Dump, "RequestVote\n")

		if args.Term > rf.Term {
			// Peer's Term is newer
			if args.LastLogTerm > rf.Entries[len(rf.Entries)-1].Term {
				// Peer's LastLogTerm is Newer. Vote
				rf.debugf(State, "RequestVote - State Change 1\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
					rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
					args.Term, -1, args.Canidate, rf.CommitIndex)
				rf.Term = args.Term
				rf.Leader = -1
				rf.Vote = args.Canidate
				rf.recentHeartbeat = true
				reply.VoteGranted = true

			} else if args.LastLogTerm == rf.Entries[len(rf.Entries)-1].Term {
				// Peer's LastLogTerm is Same. Check LastLogIndex
				if args.LastLogIndex >= rf.Entries[len(rf.Entries)-1].Index {
					// Peer's LastLogIndex is Newer or same. Vote
					rf.debugf(State, "RequestVote - State Change 2\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
						rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
						args.Term, -1, args.Canidate, rf.CommitIndex)
					rf.Term = args.Term
					rf.Leader = -1
					rf.Vote = args.Canidate
					rf.recentHeartbeat = true
					reply.VoteGranted = true

				} else {
					// Peer's LastLogIndex is older. Don't vote
					rf.debugf(State, "RequestVote - State Change 3\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
						rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
						args.Term, -1, -1, rf.CommitIndex)
					rf.Term = args.Term
					rf.Leader = -1
					rf.Vote = -1
					reply.VoteGranted = false
				}

			} else {
				// Peer's LastLogTerm is older. Don't vote
				rf.debugf(State, "RequestVote - State Change 4\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
					rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
					args.Term, -1, -1, rf.CommitIndex)
				rf.Term = args.Term
				rf.Leader = -1
				rf.Vote = -1
				reply.VoteGranted = false
			}

		} else if args.Term == rf.Term {
			// Peers Term matches
			if rf.Leader == -1 {
				// Don't have a leader
				if rf.Vote == -1 {
					// haven't voted
					if args.LastLogTerm > rf.Entries[len(rf.Entries)-1].Term {
						// Peer's LastLogTerm is Newer. Vote
						rf.debugf(State, "RequestVote - State Change 5\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
							rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
							args.Term, -1, args.Canidate, rf.CommitIndex)
						rf.Vote = args.Canidate
						rf.recentHeartbeat = true
						reply.VoteGranted = true

					} else if args.LastLogTerm == rf.Entries[len(rf.Entries)-1].Term {
						// Peer's LastLogTerm is Same. Check LastLogIndex
						if args.LastLogIndex >= rf.Entries[len(rf.Entries)-1].Index {
							// Peer's LastLogIndex is Newer or same. Vote
							rf.debugf(State, "RequestVote - State Change 6\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
								rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
								args.Term, -1, args.Canidate, rf.CommitIndex)
							rf.Vote = args.Canidate
							rf.recentHeartbeat = true
							reply.VoteGranted = true

						} else {
							// Peer's LastLogIndex is older. Do nothing
							reply.VoteGranted = false

						}
					} else {
						// Peer's LastLogTerm is older. Do nothing
						reply.VoteGranted = false

					}
				} else if rf.Vote == args.Canidate {
					// Voted for peer already. Vote (again)
					rf.recentHeartbeat = true
					reply.VoteGranted = true

				} else {
					// Voted for someone else. Do nothing
					reply.VoteGranted = false
				}

			} else {
				// Have a leader. Do Nothing
				reply.VoteGranted = false
			}
		} else {
			// Old Term. Do nothing
			reply.VoteGranted = false
		}

		reply.Term = rf.Term
	}
	rf.mu.Unlock()
	rf.debugf(Locks, "RequestVote - unlock\n")

	rf.debugf(Message, "RequestVote  Reply:\n\tTerm:%v VoteGranted:%v\n",
		reply.Term, reply.VoteGranted)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, responseChan chan *RequestVoteReply) {
	//rf.debugf(Routine, "sendRequestVote -> %v\n", server)
	reply := &RequestVoteReply{}
	rf.debugf(Message, "sendRequestVote -> %v  Args:\n\tTerm:%v Cand:%v LastIndex:%v LastTerm:%v \n", server,
		args.Term, args.Canidate, args.LastLogIndex, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		responseChan <- reply
	} else {
		rf.debugf(Locks, "sendRequestVote - lock\n")
		rf.mu.Lock()
		{
			if args.Term == rf.Term && rf.Leader == -1 {
				go rf.sendRequestVote(server, args, responseChan)
			}
		}
		rf.mu.Unlock()
		rf.debugf(Locks, "sendRequestVote - Unlock\n")
	}
}

type ApplyMsgArgs struct {
	Term   int
	Leader int

	PrevLogIndex int
	PrevLogTerm  int

	Entries     []Entry
	CommitIndex int
}

type ApplyMsgReply struct {
	Term    int
	Success bool
}

func (rf *Raft) ApplyMsg(args *ApplyMsgArgs, reply *ApplyMsgReply) {
	//rf.debugf(Routine, "ApplyMsg\n")
	rf.debugf(Locks, "ApplyMsg - lock\n")
	rf.mu.Lock()
	{
		rf.debugf(Dump, "ApplyMsg\n")

		if args.Term > rf.Term {
			rf.debugf(State, "ApplyMsg - State Change 1\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
				rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
				args.Term, args.Leader, -1, rf.CommitIndex)
			rf.Term = args.Term
			rf.Leader = args.Leader
			rf.Vote = -1
			rf.recentHeartbeat = true

		} else if args.Term == rf.Term {
			if rf.Leader == args.Leader || rf.Leader == -1 {
				rf.debugf(State, "ApplyMsg - State Change 2\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
					rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
					args.Term, args.Leader, rf.Vote, rf.CommitIndex)
				rf.Leader = args.Leader
				rf.recentHeartbeat = true

			} else {
				// Discovered two leaders. Fail!
				rf.fatalf("ApplyMsg - Two Leaders in same Term!!! mine:%v other:%v", rf.Leader, args.Leader)

			}
		} else {
			// Old Term, Do nothing
			reply.Success = false
		}
		reply.Term = rf.Term

		if args.Term >= rf.Term {
			// Check for new log/message
			if len(rf.Entries)-1 < args.PrevLogIndex {
				// Need previous logs
				rf.debugf(Message, "ApplyMsg - Need previous logs self:{%v %v} args:{%v %v}\n",
					len(rf.Entries)-1,
					"?",
					args.PrevLogIndex,
					args.PrevLogTerm)
				rf.recentHeartbeat = true
				reply.Success = false

			} else if args.PrevLogTerm != rf.Entries[args.PrevLogIndex].Term {
				// Old logs don't match terms, backup
				rf.debugf(Message, "ApplyMsg - Old logs don't match terms self:{%v %v} args:{%v %v}\n",
					len(rf.Entries)-1,
					rf.Entries[args.PrevLogIndex].Term,
					args.PrevLogIndex,
					args.PrevLogTerm)
				rf.recentHeartbeat = true
				reply.Success = false

			} else {
				// Choke down new Logs
				rf.debugf(Dump, "ApplyMsg - accepting new logs self:{%v %v} args:{%v %v} \n\t%v\n",
					len(rf.Entries)-1,
					rf.Entries[args.PrevLogIndex].Term,
					args.PrevLogIndex,
					args.PrevLogTerm,
					entriesToString(args.Entries))
				rf.CommitIndex = args.CommitIndex
				rf.Entries = append(rf.Entries[0:args.PrevLogIndex+1], args.Entries...)
				rf.recentHeartbeat = true
				reply.Success = true
			}
		}
	}
	rf.mu.Unlock()
	rf.debugf(Locks, "ApplyMsg - Unlock\n")

	rf.debugf(Message, "ApplyMsg  Reply:\n\tTerm:%v Success:%v\n",
		reply.Term, reply.Success)
}

func (rf *Raft) sendApplyMessage(server int, args *ApplyMsgArgs, responseChan chan *ApplyMsgReply) {
	//rf.debugf(Routine, "sendApplyMessage -> %v\n", server)
	reply := &ApplyMsgReply{}
	rf.debugf(Message, "sendApplyMsg -> %v  Args:\n\tTerm:%v Lead:%v PrevIndex:%v PrevTerm:%v CommitIndex:%v \n\t%v\n", server,
		args.Term, args.Leader, args.PrevLogIndex, args.PrevLogTerm, args.CommitIndex,
		entriesToString(args.Entries))
	ok := rf.peers[server].Call("Raft.ApplyMsg", args, reply)
	if ok {
		responseChan <- reply
	}
}

func (rf *Raft) doApplyMsg(server int) {
	// rf.debugf(Routine, "sendApplyMsg -> %v\n", server)

	args := &ApplyMsgArgs{}

	rf.debugf(PeerLocks, "sendApplyMsg - peer.%v.lock\n", server)
	peerState := &rf.PeerStates[server]
	peerState.Lock()
	{
		rf.debugf(Locks, "sendApplyMsg - lock\n")
		rf.mu.Lock() // DIRTY!!!
		{
			if rf.Leader != rf.me {
				rf.mu.Unlock() // DIRTY!!!
				rf.debugf(Locks, "sendApplyMsg - unlock 2\n")
				goto Unlock
			}

			// rf.debugf(Dump, "sendApplyMsg -> %v\n", server)
			nextIndex := rf.PeerStates[server].NextIndex
			if nextIndex < 1 {
				rf.debugf(Message, "sendApplyMsg -> %v  NextIndex out of bounds!!! %v < 0 \n", server, nextIndex)
				nextIndex = 1
			}
			if nextIndex > len(rf.Entries) {
				// nextIndex > len(rf.Entries) - 1  is a heartbeat and should be handled normally
				rf.debugf(Message, "sendApplyMsg -> %v  NextIndex out of bounds!!! %v > %v\n", server, nextIndex, len(rf.Entries))
				nextIndex = len(rf.Entries)
			}

			args.Term = rf.Term
			args.Leader = rf.Leader

			args.PrevLogIndex = nextIndex - 1
			args.PrevLogTerm = rf.Entries[nextIndex-1].Term
			args.CommitIndex = rf.CommitIndex

			args.Entries = rf.Entries[nextIndex:]
		}
		rf.mu.Unlock()
		rf.debugf(Locks, "sendApplyMsg - unlock\n")

		responseChan := make(chan *ApplyMsgReply)
		timer := time.NewTimer(time.Millisecond * 100)

		go rf.sendApplyMessage(server, args, responseChan)

		select {
		case reply := <-responseChan:
			rf.debugf(Locks, "sendApplyMsg - lock 2\n")
			rf.mu.Lock()
			{
				if reply.Success {
					// Peer added new log
					if reply.Term == rf.Term {
						// Peer has matching term
						if len(args.Entries) > 0 {
							rf.PeerStates[server].NextIndex = args.Entries[len(args.Entries)-1].Index + 1
						} else {
							// NextIndex did not advance
						}
						rf.PeerStates[server].MatchIndex = rf.PeerStates[server].NextIndex - 1

						couldAdvCommitIndex := true
						for couldAdvCommitIndex {
							confirmed := 1
							couldAdvCommitIndex = false
							for peer, state := range rf.PeerStates {
								if peer == rf.me {
									continue
								}
								if state.MatchIndex > rf.CommitIndex {
									confirmed++
								}
							}
							if confirmed > len(rf.peers)/2 {
								rf.CommitIndex++
								couldAdvCommitIndex = true
							}
						}

					} else {
						// Terms Don't Match. Could we have advanced ours?
						rf.debugf(State, "sendApplyMsg -> %v  Success with unmatching terms? mine:%v other:%v\n", server, rf.Term, reply.Term)
					}

				} else {
					if reply.Term > rf.Term {
						rf.debugf(State, "sendApplyMsg - State Change\n\t{%v %v %v %v} -> {%v %v %v %v}\n",
							rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
							reply.Term, -1, -1, rf.CommitIndex)
						rf.Term = reply.Term
						rf.Leader = -1
						rf.Vote = -1

					} else if reply.Term == rf.Term {
						// Peer needs previous
						rf.PeerStates[server].NextIndex--
						rf.debugf(Message, "sendApplyMsg -> %v failed, NextIndex:%v\n", server, rf.PeerStates[server].NextIndex)
						go rf.doApplyMsg(server)
					} else {
						// Reply term is behind? I got updated since request was sent.
						// Try Again?
						rf.debugf(State, "sendApplyMsg -> %v  Failure with unmatching terms? mine:%v other:%v\n", server, rf.Term, reply.Term)
						go rf.doApplyMsg(server)
					}

				}
			}
			rf.mu.Unlock() // DIRTY!!!
			rf.debugf(Locks, "sendApplyMsg - unlock 2\n")
		case <-timer.C:
			// do noting, quit
		}

	}
Unlock:
	peerState.Unlock()
	rf.debugf(PeerLocks, "sendApplyMsg - peer.%v.Unlock\n", server)
}
