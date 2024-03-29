package state

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"goraft/src/labrpc"
	"goraft/src/models"
	"goraft/src/util/log"
)

var (
	_ State = (*LeaderState)(nil)
)

type matchRecord struct {
	peer  int
	index int
}

type LeaderState struct {
	*BaseState
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	nextIndexes  []atomic.Int64
	matchIndexes []atomic.Int64
	matchCh      chan *matchRecord
}

// Leader can only come from candidate
func Leader(from *CandidateState) *LeaderState {
	ctx, cancel := context.WithCancel(context.Background())

	ls := &LeaderState{
		BaseState:    Base(from.Context(), from.Term()),
		ctx:          ctx,
		cancel:       cancel,
		nextIndexes:  make([]atomic.Int64, from.Peers()),
		matchIndexes: make([]atomic.Int64, from.Peers()),
		matchCh:      make(chan *matchRecord, from.Peers()),
	}
	nextIndex := int64(ls.LastLogIndex() + 1)
	for i := range ls.nextIndexes {
		ls.nextIndexes[i].Store(nextIndex)
	}
	log.Info("%s new leader", ls)

	ls.wg.Add(2)
	go ls.sendHeartbeats()
	go ls.commitMatch(from.Peers())
	ls.PollPeers(ls.syncPeerEntries)

	return ls
}

func (s *LeaderState) Role() string {
	return RoleLeader
}

func (s *LeaderState) String() string {
	return logPrefix(s)
}

func (s *LeaderState) Voted() int {
	return s.Me()
}

func (s *LeaderState) Close(msg string, args ...interface{}) bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	log.Info("%s closing: %s", s, fmt.Sprintf(msg, args...))
	s.cancel()
	s.BroadcastLog()
	s.wg.Wait()
	close(s.matchCh)
	log.Info("%s closed", s)
	return true
}

func (s *LeaderState) AppendCommand(command interface{}) (index int, term models.Term) {
	term = s.Term()
	s.LockLog()
	index = s.AppendLogs(models.Log{
		Term: term,
		Data: command,
	})
	s.UnlockLog()
	log.Info("%s append new command %v to index %d, signal syncing to peers", s, command, index)
	s.BroadcastLog()
	return
}

func (s *LeaderState) RequestVote(args *models.RequestVoteArgs) (granted bool) {
	// A leader always rejects vote request from other candidates in the same term
	return false
}

func (s *LeaderState) AppendEntries(args *models.AppendEntriesArgs) (success bool) {
	// If this happened, it means there are multiple leaders in the same term
	log.Fatal("%s multiple leaders at same term: %d and %d", s, s.Me(), args.Leader)
	return false
}

func (s *LeaderState) InstallSnapshot(args *models.InstallSnapshotArgs) (success bool) {
	// If this happened, it means there are multiple leaders in the same term
	log.Fatal("%s multiple leaders at same term: %d and %d", s, s.Me(), args.Leader)
	return false
}

// This should be called concurrently rather than one-by-one
func (s *LeaderState) callAppendEntries(args *models.AppendEntriesArgs, peerID int,
	peerRPC *labrpc.ClientEnd) (reply *models.AppendEntriesReply, ok bool) {
	reply = new(models.AppendEntriesReply)
	log.Debug("%s calling peers[%d].AppendEntries(%s)", s, peerID, args)
	if !peerRPC.Call("Raft.AppendEntries", args, reply) || s.closed.Load() {
		if !s.closed.Load() {
			log.Error("%s peers[%d].AppendEntries(%s) failed", s, peerID, args)
		}
		return nil, false
	}
	log.Debug("%s peers[%d].AppendEntries(%s) => (%s)", s, peerID, args, reply)
	ok = true
	if !reply.Success {
		s.LockState()
		if curTerm := s.Term(); reply.Term > curTerm {
			ok = false
			if s.Close("got higher term %d (current %d), revert to follower", reply.Term, curTerm) {
				s.To(Follower(reply.Term, NoVote, s.Context()))
			}
		}
		s.UnlockState()
	}
	return reply, ok
}

// This should be called concurrently rather than one-by-one
func (s *LeaderState) callInstallSnapshot(args *models.InstallSnapshotArgs, peerID int,
	peerRPC *labrpc.ClientEnd) (reply *models.InstallSnapshotReply, ok bool) {
	reply = new(models.InstallSnapshotReply)
	log.Debug("%s calling peers[%d].InstallSnapshot(%s)", s, peerID, args)
	if !peerRPC.Call("Raft.InstallSnapshot", args, reply) || s.closed.Load() {
		if !s.closed.Load() {
			log.Error("%s peers[%d].InstallSnapshot(%s) failed", s, peerID, args)
		}
		return nil, false
	}
	log.Debug("%s peers[%d].InstallSnapshot(%s) => (%s)", s, peerID, args, reply)
	ok = true
	s.LockState()
	if curTerm := s.Term(); reply.Term > curTerm {
		ok = false
		if s.Close("got higher term %d (current %d), revert to follower", reply.Term, curTerm) {
			s.To(Follower(reply.Term, NoVote, s.Context()))
		}
	}
	s.UnlockState()
	return reply, ok
}

func (s *LeaderState) sendHeartbeats() {
	defer s.wg.Done()
	defer log.Info("%s sendHeartbeats exited", s)
	log.Info("%s start send heartbeats", s)

	for !s.closed.Load() {
		log.Info("%s sending heartbeats", s)
		s.PollPeers(s.sendHeartbeat)
		// We won't wait all peers to respond here

		if s.closed.Load() {
			return
		}

		// We will use sleep here to avoid handling cleanup of time.Ticker
		log.Info("%s waiting for next heartbeats", s)
		time.Sleep(HeartBeatInterval)
	}
}

func (s *LeaderState) sendHeartbeat(peerID int, peerRPC *labrpc.ClientEnd) {
	s.RLockLog()
	prevIndex, prevLog := s.GetLog(s.Committed())
	prevTerm := s.Term()
	if prevLog != nil {
		prevTerm = prevLog.Term
	}
	args := &models.AppendEntriesArgs{
		Term:         s.Term(),
		Leader:       s.Me(),
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      nil,
		LeaderCommit: s.Committed(),
	}
	s.RUnlockLog()

	if s.closed.Load() {
		return
	}

	log.Debug("%s sending heartbeat to peer %d", s, peerID)
	reply, ok := s.callAppendEntries(args, peerID, peerRPC)
	if !ok || s.closed.Load() {
		return
	}

	newMatch := int64(reply.LastLogIndex)
	oldMatch := s.matchIndexes[peerID].Swap(newMatch)
	if reply.Success && oldMatch < newMatch {
		s.matchCh <- &matchRecord{peerID, reply.LastLogIndex}
		log.Info("%s peer %d match log at index %d", s, peerID, newMatch)
	}

	peerNextIndex := s.updateNext(peerID, args.PrevLogIndex,
		reply.LastLogIndex, reply.LastLogTerm)

	s.RLockLog()
	lastLogIndex := s.LastLogIndex()
	s.RUnlockLog()
	if peerNextIndex <= lastLogIndex {
		log.Info("%s peer %d is lagging behind, signal to sync", s, peerID)
		s.BroadcastLog()
	}
}

func (s *LeaderState) syncPeerEntries(peerID int, peerRPC *labrpc.ClientEnd) {
	defer log.Info("%s syncPeerEntries to %d exited", s, peerID)
	log.Info("%s start syncPeerEntries to %d", s, peerID)
	for !s.closed.Load() {
		s.RLockLog()
		nextIndex := int(s.nextIndexes[peerID].Load())
		if nextIndex > s.LastLogIndex() {
			// There is no new entry to sync, wait on a condition variable
			log.Info("%s waiting for new entries for peer %d", s, peerID)
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
	if nextIndex <= s.SnapshotIndex() {
		s.RUnlockLog()
		// Send a snapshot instead of entries
		log.Info("%s peer %d is lagging behind, send snapshot", s, peerID)
		s.sendSnapshot(peerID, peerRPC)
		return
	}
	prevIndex, prevLog := s.GetLog(nextIndex - 1)
	if prevLog == nil {
		log.Fatal("%s no log at index %d for peer %d", s, nextIndex-1, peerID)
	}
	args := &models.AppendEntriesArgs{
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

	if len(args.Entries) == 0 {
		log.Fatal("%s sending no entry to peer %d", s, peerID)
	}

	log.Info("%s appending log[%d:%d] to peer %d", s,
		nextIndex, nextIndex+len(args.Entries), peerID)
	reply, ok := s.callAppendEntries(args, peerID, peerRPC)
	if !ok || s.closed.Load() {
		return
	}

	newMatch := int64(reply.LastLogIndex)
	oldMatch := s.matchIndexes[peerID].Swap(newMatch)
	if reply.Success && oldMatch < newMatch {
		log.Debug("%s append log[%d:%d] to peer %d successfully",
			s, nextIndex, nextIndex+len(args.Entries), peerID)
		// FIXME: send on closed channel
		s.matchCh <- &matchRecord{peerID, reply.LastLogIndex}
		log.Info("%s peer %d match log at index %d", s, peerID, newMatch)
	} else {
		log.Debug("%s append log[%d:%d] to peer %d failed",
			s, nextIndex, nextIndex+len(args.Entries), peerID)
	}

	s.updateNext(peerID, args.PrevLogIndex, reply.LastLogIndex, reply.LastLogTerm)
}

func (s *LeaderState) sendSnapshot(peerID int, peerRPC *labrpc.ClientEnd) {
	s.RLockLog()
	lastLogIndex, lastLog := s.GetLog(s.SnapshotIndex())
	args := &models.InstallSnapshotArgs{
		Term:         s.Term(),
		Leader:       s.Me(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLog.Term,
		Snapshot:     s.GetSnapshot(),
	}
	s.RUnlockLog()

	if s.closed.Load() {
		return
	}

	log.Info("%s send snapshot at index %d term %d to perr %d",
		s, args.LastLogIndex, args.LastLogTerm, peerID)
	s.callInstallSnapshot(args, peerID, peerRPC)
}

func (s *LeaderState) commitMatch(nPeers int) {
	defer s.wg.Done()
	defer log.Info("%s commitMatch exited", s)
	log.Info("%s start commit match indexes", s)
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

			if s.Context().CommitLog(majorMatch) {
				log.Info("%s major match at log[:%d] committed", s, s.Committed())
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

func (s *LeaderState) updateNext(peerID, prevIndex, matchIndex int, matchTerm models.Term) int {
	nextIndex := matchIndex + 1
	oldNext := s.nextIndexes[peerID].Swap(int64(nextIndex))
	log.Debug("%s update peer %d next index %d => %d", s, peerID, oldNext, nextIndex)
	return nextIndex
}
