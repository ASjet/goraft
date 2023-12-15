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
	votes   map[int]bool
}

func Candidate(term int, from State) *CandidateState {
	// Candidate can come from any state
	cs := &CandidateState{
		// A candidate always vote for itself
		BaseState: from.Base(term, from.Me()),
	}
	Info("%s new candidate", cs)

	cs.votes = make(map[int]bool, cs.Peers())
	cs.votes[cs.Me()] = true
	cs.timer = util.NewTimer(context.TODO(), genElectionTimeout(), cs.electionTimeout).Start()

	cs.wg.Add(1)
	go cs.startElection()

	return cs
}

func (s *CandidateState) RequestVote(args *RequestVoteArgs) (granted bool) {
	// A candidate always rejects vote request from other candidates
	return false
}

func (s *CandidateState) AppendEntries(args *AppendEntriesArgs) (success bool) {
	// A candidate only accept valid AppendEntries from current leader
	if !s.ValidEntries() {
		return false
	}
	s.To(Follower(args.Term, args.Leader, s))
	return true
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
	return fmt.Sprintf("%d:%s%03d", s.Me(), s.Role(), s.Term())
}

func (s *CandidateState) Role() string {
	return RoleCandidate
}

func (s *CandidateState) electionTimeout() {
	if s.closed.CompareAndSwap(false, true) {
		s.Lock()
		Info("%s election timeout, start another election", s)
		s.To(Candidate(s.Term()+1, s))
		s.Unlock()
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
		s.votes[peerID] = true
		votes := len(s.votes)
		s.votesMu.Unlock()

		Info("%s got vote from %d(%d/%d)", s, peerID, votes, s.Peers())

		if votes >= s.Majority() && s.Close() {
			// Got majority votes, become leader
			s.Lock()
			Info("%s got majority votes(%d/%d), migrate to leader", s, votes, s.Peers())
			s.To(Leader(s))
			s.Unlock()
		}
	} else {
		s.Lock()
		if curTerm := s.Term(); reply.Term > curTerm && s.Close() {
			Info("%s got higher term %d (current %d), migrate to follower", s, reply.Term, curTerm)
			s.To(Follower(reply.Term, NoVote, s))
		}
		s.Unlock()
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
