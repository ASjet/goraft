package raft

import (
	"context"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"goraft/src/labrpc"
)

var (
	_ State = (*LeaderState)(nil)
)

type matchRecord struct {
	peer  int
	index int
}

type LeaderState struct {
	BaseState
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool

	nextIndexes  []atomic.Int64
	matchIndexes []atomic.Int64
	matchCh      chan matchRecord
}

func Leader(from State) *LeaderState {
	// Leader can only come from candidate
	if from.Role() != RoleCandidate {
		Error("%s can not transition to leader since it's not a candidate", from)
		panic("invalid state transition: only candidate can transition to leader")
	}

	ctx, cancel := context.WithCancel(context.Background())

	ls := &LeaderState{
		BaseState:    from.Base(from.Term(), from.Me()),
		ctx:          ctx,
		cancel:       cancel,
		nextIndexes:  make([]atomic.Int64, from.Peers()),
		matchIndexes: make([]atomic.Int64, from.Peers()),
		matchCh:      make(chan matchRecord, from.Peers()),
	}
	nextIndex := int64(ls.Committed() + 1)
	for i := range ls.nextIndexes {
		ls.nextIndexes[i].Store(nextIndex)
	}
	Info("%s new leader", ls)

	ls.wg.Add(2)
	go ls.sendHeartbeats()
	go ls.commitMatch(from.Peers())
	ls.PollPeers(ls.syncPeerEntries)

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
	Info("%s append new command %v to index %d, signal syncing to peers", s, command, index)
	s.BroadcastLog()
	return
}

func (s *LeaderState) Close() bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	Info("%s closing", s)
	s.cancel()
	s.BroadcastLog()
	s.wg.Wait()
	close(s.matchCh)
	Info("%s closed", s)
	return true
}

func (s *LeaderState) String() string {
	return logPrefix(s)
}

func (s *LeaderState) Role() string {
	return RoleLeader
}

// This should be called concurrently rather than one-by-one
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
	defer Info("%s sendHeartbeats exited", s)
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
	nextIndex := int(s.nextIndexes[peerID].Load())
	prevIndex, prevLog := s.GetLog(nextIndex - 1)
	args := &AppendEntriesArgs{
		Term:         s.Term(),
		Leader:       s.Me(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevLog.Term,
		LeaderCommit: s.Committed(),
	}
	s.RUnlockLog()

	if s.closed.Load() {
		return
	}

	Debug("%s sending heartbeat to peer %d", s, peerID)
	reply, ok := s.callAppendEntries(args, peerID, peerRPC)
	if !ok || s.closed.Load() {
		return
	}

	if reply.Success {
		newMatch := int64(reply.LastLogIndex)
		if oldMatch := s.matchIndexes[peerID].Swap(newMatch); oldMatch < newMatch {
			s.matchCh <- matchRecord{peerID, int(newMatch)}
			Info("%s peer %d match log at index %d", s, peerID, newMatch)
		}
	}

	peerNextIndex := reply.LastLogIndex + 1
	s.nextIndexes[peerID].Store(int64(peerNextIndex))
	Debug("%s update peer %d next index %d => %d", s,
		peerID, nextIndex, reply.LastLogIndex+1)

	s.RLockLog()
	lastLogIndex := s.LastLogIndex()
	s.RUnlockLog()
	if peerNextIndex <= lastLogIndex {
		Info("%s peer %d is lagging behind, signal to sync", s, peerID)
		s.BroadcastLog()
	}
}

func (s *LeaderState) syncPeerEntries(peerID int, peerRPC *labrpc.ClientEnd) {
	defer Info("%s syncPeerEntries to %d exited", s, peerID)
	Info("%s start syncPeerEntries to %d", s, peerID)
	for !s.closed.Load() {
		s.RLockLog()
		nextIndex := int(s.nextIndexes[peerID].Load())
		matchIndex := int(s.matchIndexes[peerID].Load())
		if nextIndex == matchIndex+1 && nextIndex > s.LastLogIndex() {
			// There is no new entry to sync, wait on a condition variable
			Info("%s waiting for new entries for peer %d", s, peerID)
			s.WaitLog()
			s.RUnlockLog()
			continue
		}
		s.RUnlockLog()

		if s.closed.Load() {
			return
		}

		// NOTE: The test may make this function not return,
		// so we cannot use wg to wait this goroutine
		s.sendEntries(peerID, peerRPC)
	}
}

func (s *LeaderState) sendEntries(peerID int, peerRPC *labrpc.ClientEnd) {
	s.RLockLog()
	nextIndex := int(s.nextIndexes[peerID].Load())
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

	if s.closed.Load() {
		return
	}

	Info("%s appending log[%d:%d] to peer %d", s,
		nextIndex, nextIndex+len(args.Entries), peerID)
	reply, ok := s.callAppendEntries(args, peerID, peerRPC)
	if !ok || s.closed.Load() {
		return
	}

	if reply.Success {
		newMatch := int64(reply.LastLogIndex)
		if oldMatch := s.matchIndexes[peerID].Swap(newMatch); oldMatch < newMatch {
			Debug("%s append log[%d:%d] to peer %d successfully",
				s, nextIndex, nextIndex+len(args.Entries), peerID)
			// FIXME: send on closed channel
			s.matchCh <- matchRecord{peerID, int(newMatch)}
			Info("%s peer %d match log at index %d", s, peerID, newMatch)
		}
	} else {
		Debug("%s append log[%d:%d] to peer %d failed",
			s, nextIndex, nextIndex+len(args.Entries), peerID)
	}

	s.nextIndexes[peerID].Store(int64(reply.LastLogIndex + 1))
	Debug("%s update peer %d next index %d => %d", s,
		peerID, nextIndex, reply.LastLogIndex+1)
}

func (s *LeaderState) commitMatch(nPeers int) {
	defer s.wg.Done()
	defer Info("%s commitMatch exited", s)
	Info("%s start commit match indexes", s)
	matches := make([]int, nPeers)
	for {
		select {
		case <-s.ctx.Done():
			return
		case match := <-s.matchCh:
			s.RLockLog()
			committed := s.Committed()
			matches[s.Me()] = s.LastLogIndex()
			s.RUnlockLog()

			matches[match.peer] = match.index
			majorMatch := s.majorMatch(matches)
			if majorMatch <= committed {
				continue
			}

			if s.CommitLog(majorMatch) {
				Info("%s major match at log[:%d] committed", s, s.Committed())
			}
		}
	}
}

func (s *LeaderState) majorMatch(match []int) int {
	if len(match) == 0 {
		return 0
	}
	matchSlice := slices.Clone(match)
	sort.Sort(sort.Reverse(sort.IntSlice(matchSlice)))
	return matchSlice[len(matchSlice)/2]
}
