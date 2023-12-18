package raft

import (
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

	nextIndex  []int // The next index -1 is guaranteed existed
	matchIndex []int
}

func Leader(from State) *LeaderState {
	// Leader can only come from candidate
	if from.Role() != RoleCandidate {
		Error("%s can not transition to leader since it's not a candidate", from)
		panic("invalid state transition: only candidate can transition to leader")
	}

	ls := &LeaderState{
		BaseState:  from.Base(from.Term(), from.Me()),
		nextIndex:  make([]int, from.Peers()),
		matchIndex: make([]int, from.Peers()),
	}
	nextIndex := ls.LastLogIndex() + 1
	for i := range ls.nextIndex {
		ls.nextIndex[i] = nextIndex
	}
	Info("%s new leader", ls)

	ls.wg.Add(1)
	go ls.sendHeartbeats()

	return ls
}

func (s *LeaderState) RequestVote(args *RequestVoteArgs) (granted bool) {
	// A leader always rejects vote request from other candidates in the same term
	return false
}

func (s *LeaderState) AppendEntries(args *AppendEntriesArgs) (success bool) {
	// If this happened, it means there are multiple leaders in the same term
	Fatal("%s multiple leaders at same term: %d and %d", s, s.Me(), args.Leader)
	return false
}

func (s *LeaderState) AppendCommand(command interface{}) (index int, term int) {
	term = s.Term()
	s.LockLog()
	index = s.AppendLogs(Log{
		Term: term,
		Data: command,
	})
	s.UnlockLog()
	s.syncEntries()
	return
}

func (s *LeaderState) Close() bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	s.wg.Wait()
	return true
}

func (s *LeaderState) String() string {
	return logPrefix(s)
}

func (s *LeaderState) Role() string {
	return RoleLeader
}

func (s *LeaderState) callAppendEntries(args *AppendEntriesArgs, peerID int,
	peerRPC *labrpc.ClientEnd) (reply *AppendEntriesReply, ok bool) {
	reply = new(AppendEntriesReply)
	Debug("%s calling peers[%d].AppendEntries(%s)", s, peerID, args)
	if !peerRPC.Call("Raft.AppendEntries", args, reply) || s.closed.Load() {
		if !s.closed.Load() {
			Error("%s peers[%d].AppendEntries(%s) failed", s, peerID, args)
		}
		return nil, false
	}
	Debug("%s peers[%d].AppendEntries(%s) => (%s)", s, peerID, args, reply)
	ok = true
	if !reply.Success {
		s.Lock()
		if curTerm := s.Term(); reply.Term > curTerm {
			ok = false
			if s.Close() {
				Info("%s got higher term %d (current %d), revert to follower", s, reply.Term, curTerm)
				s.To(Follower(reply.Term, NoVote, s))
			}
		}
		s.Unlock()
	}
	return reply, ok
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

func (s *LeaderState) sendHeartbeat(peerID int, peerRPC *labrpc.ClientEnd) {
	s.RLockLog()
	args := &AppendEntriesArgs{
		Term:         s.Term(),
		Leader:       s.Me(),
		LeaderCommit: s.Committed(),
	}
	s.RUnlockLog()
	s.callAppendEntries(args, peerID, peerRPC)
}

func (s *LeaderState) syncEntries() {
	Info("%s sync entries to peers", s)
	s.PollPeers(s.syncPeerEntries)
}

func (s *LeaderState) syncPeerEntries(peerID int, peerRPC *labrpc.ClientEnd) {
	nextIndex := s.nextIndex[peerID]
	s.RLockLog()
	prevIndex, prevLog := s.GetLog(nextIndex - 1)
	args := &AppendEntriesArgs{
		Term:         s.Term(),
		Leader:       s.Me(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevLog.Term,
		Entries:      s.GetLogSince(nextIndex),
		LeaderCommit: s.Committed(),
	}
	s.RUnlockLog()

	reply, ok := s.callAppendEntries(args, peerID, peerRPC)
	if !ok {
		return
	}

	if reply.Success {
		s.Lock()
		s.nextIndex[peerID] = reply.LastLogIndex + 1
		s.matchIndex[peerID] = reply.LastLogIndex
		s.Unlock()
		return
	}

	// TODO: handle conflict

}
