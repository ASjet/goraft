package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"goraft/src/labrpc"
	"goraft/src/util"
)

var (
	_ State = (*CandidateState)(nil)
)

type CandidateState struct {
	BaseState
	wg     sync.WaitGroup
	closed atomic.Bool
	timer  *util.Timer

	votesMu sync.Mutex
	votes   []int
}

func Candidate(term int, oldState State) *CandidateState {
	// Candidate can come from any state
	cs := &CandidateState{
		BaseState: oldState.Base(),
	}
	// A candidate always vote for itself
	cs.Follow(term, cs.Me())
	cs.votes = make([]int, 1, cs.Peers())
	cs.votes[0] = cs.Me()
	Info("%s new candidate", cs)

	cs.timer = util.NewTimer(context.TODO(), electionTimeout, cs.electionTimeout).Start()
	cs.wg.Add(1)
	go cs.startElection()
	return cs
}

func (s *CandidateState) RequestVote(term, candidate int) (granted bool) {
	return false
}

func (s *CandidateState) AppendEntries(term, leader int) (success bool) {
	return false
}

func (s *CandidateState) Close() bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	s.timer.Stop()
	s.wg.Wait()
	return true
}

func (s *CandidateState) String() string {
	return fmt.Sprintf("%s%d:%03d", s.Role(), s.Me(), s.Term())
}

func (s *CandidateState) Role() string {
	return RoleCandidate
}

func (s *CandidateState) electionTimeout() {
	if s.closed.CompareAndSwap(false, true) {
		Info("%s election timeout, start another election", s)
		s.MigrateTo(Candidate(s.Term()+1, s))
	}
}

func (s *CandidateState) requestVote(peerID int, peerRPC *labrpc.ClientEnd) {
	args := &RequestVoteArgs{
		Term:      s.Term(),
		Candidate: s.Me(),
	}
	reply := new(RequestVoteReply)
	Debug("%s calling peers[%d].RequestVote(%d, %d)", s, peerID,
		args.Term, args.Candidate)
	if !peerRPC.Call("Raft.RequestVote", args, reply) || s.closed.Load() {
		if !s.closed.Load() {
			Error("%s peers[%d].RequestVote(%d, %d) failed", s, peerID,
				args.Term, args.Candidate)
		}
		return
	}
	Debug("%s peers[%d].RequestVote(%d, %d) => (%d, %v)", s, peerID,
		args.Term, args.Candidate, reply.Term, reply.Granted)

	if reply.Granted {
		s.votesMu.Lock()
		s.votes = append(s.votes, peerID)
		votes := len(s.votes)
		s.votesMu.Unlock()

		Info("%s got vote from %d(%d/%d)", s, peerID, votes, s.Peers())

		if votes >= s.Majority() && s.Close() {
			// Got majority votes, become leader
			Info("%s got majority votes(%d/%d), migrate to leader", s, votes, s.Peers())
			s.MigrateTo(Leader(s))
		}
	} else {
		if curTerm := s.Term(); reply.Term > curTerm && s.Close() {
			Info("%s got higher term %d (current %d), migrate to follower", s, reply.Term, curTerm)
			s.MigrateTo(Follower(reply.Term, NoVote, s))
		}
	}
}

func (s *CandidateState) startElection() {
	defer s.wg.Done()
	Info("%s start election", s)
	for !s.closed.Load() {
		Info("%s sending vote requests", s)
		s.PollPeers(s.requestVote)
		// We won't wait all peers to respond here

		if s.closed.Load() {
			return
		}

		// We will use sleep here to avoid handling cleanup of time.Ticker
		Info("%s waiting for another round of requests", s)
		time.Sleep(RequestVoteInterval)
	}
}
