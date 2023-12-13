package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"goraft/src/labrpc"
)

var (
	_ State = (*LeaderState)(nil)
)

type LeaderState struct {
	BaseState
	wg     sync.WaitGroup
	closed atomic.Bool
}

func Leader(oldState State) *LeaderState {
	// Leader can only come from candidate
	if oldState.Role() != RoleCandidate {
		Error("%s can not migrate to leader since it's not a candidate", oldState)
		panic("invalid state transition")
	}

	ls := &LeaderState{
		BaseState: oldState.Base(),
	}
	Info("%s new leader", ls)

	ls.wg.Add(1)
	go ls.sendHeartbeats()
	return ls
}

func (s *LeaderState) RequestVote(term, candidate int) (granted bool) {
	return false
}

func (s *LeaderState) AppendEntries(term, leader int) (success bool) {
	return false
}

func (s *LeaderState) Close() bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	s.wg.Wait()
	return true
}

func (s *LeaderState) String() string {
	return fmt.Sprintf("%s%d:%03d", s.Role(), s.Me(), s.Term())
}

func (s *LeaderState) Role() string {
	return RoleLeader
}

func (s *LeaderState) sendHeartbeat(peerID int, peerRPC *labrpc.ClientEnd) {
	args := &AppendEntriesArgs{
		Term:   s.Term(),
		Leader: s.Me(),
	}
	reply := new(AppendEntriesReply)
	Debug("%s calling peers[%d].AppendEntries(%d, %d)", s, peerID,
		args.Term, args.Leader)
	if !peerRPC.Call("Raft.AppendEntries", args, reply) || s.closed.Load() {
		if !s.closed.Load() {
			Error("%s peers[%d].AppendEntries(%d, %d) failed", s, peerID,
				args.Term, args.Leader)
		}
		return
	}
	Debug("%s peers[%d].AppendEntries(%d, %d) => (%d, %v)", s, peerID,
		args.Term, args.Leader, reply.Term, reply.Success)

	if !reply.Success {
		if curTerm := s.Term(); reply.Term > curTerm && s.Close() {
			Info("%s got higher term %d (current %d), migrate to follower", s, reply.Term, curTerm)
			s.MigrateTo(Follower(reply.Term, NoVote, s))
		}
	}
}

func (s *LeaderState) sendHeartbeats() {
	defer s.wg.Done()
	Info("%s start send heartbeats", s)

	for !s.closed.Load() {
		Info("%s sending heartbeats", s)
		s.PollPeers(s.sendHeartbeat)
		// We won't wait all peers to respond here

		if s.closed.Load() {
			return
		}

		// We will use sleep here to avoid handling cleanup of time.Ticker
		Info("%s waiting for next heartbeats", s)
		time.Sleep(HeartBeatInterval)
	}
}