package raft

import (
	"context"
	"math/rand"
	"time"
)

func (rf *Raft) sendHeartbeats(ctx context.Context) {
	dumpState("sendHeartbeats", rf)
	milliseconds := time.Duration(rand.Int63n(100) + 100)
	ticker := time.NewTicker(milliseconds * time.Millisecond)

Loop:
	for {
		select {
		case <-ticker.C:
			debugLocksf("%v.sendHeartbeats - lock\n", rf.me)
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

					args := &ApplyMsgArgs{
						Term:   rf.Term,
						Leader: rf.Leader,
					}
					go rf.sendApplyMsg(peer, args)
				}
			}
		Unlock:
			rf.mu.Unlock()
			debugLocksf("%v.sendHeartbeats - unlock\n", rf.me)

		case <-ctx.Done():
			break Loop
		}
	}
}

func (rf *Raft) watchTimeout(ctx context.Context) {
	dumpState("watchTimeout", rf)
	milliseconds := time.Duration(rand.Int63n(200) + 200)
	ticker := time.NewTicker(milliseconds * time.Millisecond)

Loop:
	for {
		select {
		case <-ticker.C:
			debugLocksf("%v.watchTimeout - lock\n", rf.me)
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
			debugLocksf("%v.watchTimeout - unlock\n", rf.me)

		case <-ctx.Done():
			break Loop
		}
	}
}

func (rf *Raft) seekElection(ctx context.Context) {
	dumpState("seekElection", rf)
	responseChan := make(chan *RequestVoteReply)
	var electionTerm int
	votes := 0
	replies := 0

	debugLocksf("%v.seekElection - lock 1\n", rf.me)
	rf.mu.Lock()
	{
		// Prep for vote
		rf.Term++
		electionTerm = rf.Term
		rf.Vote = rf.me
		votes++
		args := &RequestVoteArgs{
			Term:     rf.Term,
			Canidate: rf.me,
		}

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
	debugLocksf("%v.seekElection - unlock 1\n", rf.me)

Loop:
	for {
		select {
		case reply := <-responseChan:
			replies++

			debugLocksf("%v.seekElection - lock 2\n", rf.me)
			rf.mu.Lock()
			{
				debugStatef("\t%v.seekElection - got vote: { T:%v L:%v V:%v }\n", rf.me, reply.Term, reply.Leader, reply.Vote)
				if rf.Term > electionTerm {
					// Term advanced and Election ended
					rf.mu.Unlock()
					debugLocksf("%v.seekElection - unlock 2\n", rf.me)
					break Loop
				}

				if reply.Term > electionTerm {
					// Peer has newer term
					rf.Term = reply.Term
					rf.Leader = reply.Leader
					rf.Vote = -1

					rf.mu.Unlock()
					debugLocksf("%v.seekElection - unlock 2\n", rf.me)
					break Loop

				} else if reply.Term == electionTerm {
					if reply.Vote == rf.me {
						votes++
						if votes > len(rf.peers)/2 {
							// I won
							debugStatef("\t%v.seekElection - elected!\n", rf.me)
							rf.Leader = rf.me
							rf.mu.Unlock()
							debugLocksf("%v.seekElection - unlock 2\n", rf.me)
							break Loop
						}

					} else if replies-votes > len(rf.peers)/2 {
						// I can't win the vote
						rf.mu.Unlock()
						debugLocksf("%v.seekElection - unlock 2\n", rf.me)
						break Loop

					}
				} else {
					// Peer has older term (my term might have advanced)
				}
			}
			rf.mu.Unlock() // Warning, super dirty
			debugLocksf("%v.seekElection - unlock 2\n", rf.me)

		case <-ctx.Done():
			break Loop
		}
	}

	go emptyResponseChan(responseChan, len(rf.peers)-replies) // T
}
