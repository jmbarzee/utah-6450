package raft

import (
	"context"
	"math/rand"
	"time"
)

func (rf *Raft) sendHeartbeats(ctx context.Context) {
	rf.debugf(Routine, "sendHeartbeats\n")
	milliseconds := time.Duration(rand.Int63n(100) + 100)
	ticker := time.NewTicker(milliseconds * time.Millisecond)

Loop:
	for {
		select {
		case <-ticker.C:
			rf.debugf(Locks, "sendHeartbeats - lock\n")
			rf.mu.Lock()
			{
				if rf.Leader != rf.me {
					// I'm not leading
					goto Unlock
				}

				for peer := range rf.peers {

					if peer == rf.me {
						// Don't send heartbeat to myself
						continue
					}
					go rf.sendApplyMsg(peer)
				}
			}
		Unlock:
			rf.mu.Unlock()
			rf.debugf(Locks, "sendHeartbeats - unlock\n")

		case <-ctx.Done():
			break Loop
		}
	}
}

func (rf *Raft) watchTimeout(ctx context.Context) {
	rf.debugf(Routine, "watchTimeout\n")
	milliseconds := time.Duration(rand.Int63n(300) + 300)
	ticker := time.NewTicker(milliseconds * time.Millisecond)

Loop:
	for {
		select {
		case <-ticker.C:
			rf.debugf(Locks, "watchTimeout - lock\n")
			rf.mu.Lock()
			{
				if rf.Leader == rf.me {
					// I'm leading
					goto Unlock
				}

				if rf.recentHeartbeat {
					// I've had a heartbeat
					rf.recentHeartbeat = false
					goto Unlock
				}

				// TODO add correct context
				go rf.seekElection(ctx)

			}
		Unlock:
			rf.mu.Unlock()
			rf.debugf(Locks, "watchTimeout - unlock\n")

		case <-ctx.Done():
			break Loop
		}
	}
}

func (rf *Raft) seekElection(ctx context.Context) {
	// rf.debugf(Routine, "seekElection\n")
	responseChan := make(chan *RequestVoteReply)
	var electionTerm int
	votes := 0
	replies := 0

	rf.debugf(Locks, "seekElection - lock 1\n")
	rf.mu.Lock()
	{
		rf.debugf(Dump, "seekElection\n")
		// Prep for vote
		rf.debugf(State, "seekElection - State Change \n\t{%v %v %v %v} -> {%v %v %v %v}\n",
			rf.Term, rf.Leader, rf.Vote, rf.CommitIndex,
			rf.Term+1, -1, rf.me, rf.CommitIndex)
		rf.Term++
		rf.Leader = -1
		rf.Vote = rf.me
		electionTerm = rf.Term
		votes++
		args := &RequestVoteArgs{
			Term:         rf.Term,
			Canidate:     rf.me,
			LastLogIndex: len(rf.Entries) - 1,
			LastLogTerm:  rf.Entries[len(rf.Entries)-1].Term,
		}
		rf.persist()

		// Ask for votes
		for peer := range rf.peers {

			if peer == rf.me {
				// Don't ask for vote from myself
				continue
			}
			go rf.sendRequestVote(peer, args, responseChan)
		}
	}
	rf.mu.Unlock()
	rf.debugf(Locks, "seekElection - unlock 1\n")

Loop:
	for {
		select {
		case reply := <-responseChan:
			replies++

			rf.debugf(Locks, "seekElection - lock 2\n")
			rf.mu.Lock() // DIRTY!!!
			{
				rf.debugf(Unclassified, "seekElection - got response: { T:%v V:%v }\n", reply.Term, reply.VoteGranted)
				if rf.Term > electionTerm {
					// Term advanced and Election ended
					rf.mu.Unlock() // DIRTY!!!
					rf.debugf(Locks, "seekElection - unlock 2\n")
					break Loop
				}

				if reply.Term > electionTerm {
					// Peer has newer term
					rf.Term = reply.Term
					rf.Vote = -1
					rf.persist()

					rf.mu.Unlock() // DIRTY!!!
					rf.debugf(Locks, "seekElection - unlock 2\n")
					break Loop

				} else if reply.Term == electionTerm {
					if reply.VoteGranted {
						votes++
						if votes > len(rf.peers)/2 {
							// I won
							rf.PeerStates = make([]PeerState, len(rf.peers))
							for peer := range rf.PeerStates {
								rf.PeerStates[peer] = PeerState{
									NextIndex:  len(rf.Entries),
									MatchIndex: 0,
								}
							}
							rf.Leader = rf.me
							rf.persist()

							rf.debugf(Dump, "seekElection - elected!\n")

							rf.mu.Unlock()
							rf.debugf(Locks, "seekElection - unlock 2\n")
							break Loop
						}

					} else if replies-votes > len(rf.peers)/2 {
						// I can't win the vote
						rf.mu.Unlock() // DIRTY!!!
						rf.debugf(Locks, "seekElection - unlock 2\n")
						break Loop

					}
				} else {
					// Peer has older term (my term might have advanced)
				}
			}
			rf.mu.Unlock() // DIRTY!!!
			rf.debugf(Locks, "seekElection - unlock 2\n")

		case <-ctx.Done():
			break Loop
		}
	}
}

func (rf *Raft) reportLogs(ctx context.Context, applyCh chan ApplyMsg) {
	rf.debugf(Routine, "reportLogs\n")
	milliseconds := time.Duration(200)
	ticker := time.NewTicker(milliseconds * time.Millisecond)
	lastReported := -1

Loop:
	for {
		select {
		case <-ticker.C:
			rf.debugf(Locks, "reportLogs - lock\n")
			rf.mu.Lock()
			{
				bound := rf.CommitIndex
				if bound > len(rf.Entries)-1 {
					bound = len(rf.Entries) - 1
				}
				for i := lastReported + 1; i <= bound; i++ {
					lastReported++
					message := ApplyMsg{
						Index:   rf.Entries[i].Index,
						Command: rf.Entries[i].Command,
					}

					rf.debugf(Dump, "reportLogs - reporting {Index:%v}\n", message.Index)
					applyCh <- message
				}
			}
			rf.mu.Unlock()
			rf.debugf(Locks, "reportLogs - unlock\n")
		case <-ctx.Done():
			close(applyCh) // TODO ensure we should be doing this
			break Loop
		}
	}
}
