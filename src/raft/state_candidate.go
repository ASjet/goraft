package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"goraft/src/labrpc"
	"goraft/src/util"
	"goraft/src/util/log"
)

var (
	_ State = (*CandidateState)(nil)
)

type CandidateState struct {
	*BaseState
	wg    sync.WaitGroup
	timer *util.Timer

	votesMu sync.Mutex
	votes   map[int]bool
}

func Candidate(term Term, from State) *CandidateState {
	cs := &CandidateState{
		// A candidate always vote for itself
		BaseState: from.Base(term, from.Me()),
	}
	log.Info("%s new candidate", cs)

	cs.votes = make(map[int]bool, cs.Peers())
	cs.votes[cs.Me()] = true
	cs.timer = util.NewTimer(context.TODO(), genElectionTimeout(), cs.electionTimeout).Start()

	cs.wg.Add(1)
	go cs.startElection()

	return cs
}

func (s *CandidateState) RequestVote(args *RequestVoteArgs) (granted bool) {
	// A candidate always rejects vote request from other candidates in the same term
	return false
}

func (s *CandidateState) AppendEntries(args *AppendEntriesArgs) (success bool) {
	// If this RPC is received, it means the leader of this term is already elected
	if s.Close("peer %d won this election, revert to follower", args.Leader) {
		s.To(Follower(args.Term, args.Leader, s))
	}
	return true
}

func (s *CandidateState) InstallSnapshot(args *InstallSnapshotArgs) (success bool) {
	return false
}

func (s *CandidateState) Close(msg string, args ...interface{}) bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	log.Info("%s closing: %s", s, fmt.Sprintf(msg, args...))
	s.timer.Stop()
	s.wg.Wait()
	log.Info("%s closed", s)
	return true
}

func (s *CandidateState) String() string {
	return logPrefix(s)
}

func (s *CandidateState) Role() string {
	return RoleCandidate
}

func (s *CandidateState) electionTimeout() {
	if s.closed.CompareAndSwap(false, true) {
		s.Lock()
		log.Info("%s election timeout, start another election", s)
		s.To(Candidate(s.Term()+1, s))
		s.Unlock()
	}
}

func (s *CandidateState) requestVote(peerID int, peerRPC *labrpc.ClientEnd) {
	s.RLockLog()
	lastIndex, lastLog := s.GetLog(-1)
	s.RUnlockLog()

	args := &RequestVoteArgs{
		Term:         s.Term(),
		Candidate:    s.Me(),
		LastLogIndex: lastIndex,
		LastLogTerm:  lastLog.Term,
	}
	reply := new(RequestVoteReply)
	log.Debug("%s calling peers[%d].RequestVote(%s)", s, peerID, args)
	if !peerRPC.Call("Raft.RequestVote", args, reply) || s.closed.Load() {
		if !s.closed.Load() {
			log.Error("%s peers[%d].RequestVote(%s) failed", s, peerID, args)
		}
		return
	}
	log.Debug("%s peers[%d].RequestVote(%s) => (%s)", s, peerID, args, reply)

	if reply.Granted {
		s.votesMu.Lock()
		s.votes[peerID] = true
		votes := len(s.votes)
		s.votesMu.Unlock()

		log.Info("%s got vote from %d(%d/%d)", s, peerID, votes, s.Peers())

		if votes >= s.Majority() && s.Close("got majority votes(%d/%d), transition to leader", votes, s.Peers()) {
			// Got majority votes, become leader
			s.Lock()
			s.To(Leader(s))
			s.Unlock()
		}
	} else {
		s.Lock()
		if curTerm := s.Term(); reply.Term > curTerm && s.Close("got higher term %d (current %d), transition to follower", reply.Term, curTerm) {
			s.To(Follower(reply.Term, NoVote, s))
		}
		s.Unlock()
	}
}

func (s *CandidateState) startElection() {
	defer s.wg.Done()
	defer log.Info("%s election loop exited", s)
	log.Info("%s start election", s)
	for !s.closed.Load() {
		log.Info("%s sending vote requests", s)
		s.PollPeers(s.requestVote)
		// We won't wait all peers to respond here

		if s.closed.Load() {
			return
		}

		// We will use sleep here to avoid handling cleanup of time.Ticker
		log.Info("%s waiting for another round of requests", s)
		time.Sleep(RequestVoteInterval)
	}
}
