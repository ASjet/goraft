package raft

import (
	"context"
	"fmt"

	"goraft/src/util"
)

var (
	_ State = (*FollowerState)(nil)
)

type FollowerState struct {
	*BaseState
	timer *util.Timer
}

func Follower(term, follow int, from State) *FollowerState {
	// Follower can come from any state
	fs := &FollowerState{
		BaseState: from.Base(term, follow),
	}
	if follow == NoVote {
		Info("%s new follower without leader", fs)
	} else {
		Info("%s new follower with leader %d", fs, follow)
	}

	fs.timer = util.NewTimer(context.TODO(), genHeartbeatTimeout(), fs.heartbeatTimeout).Start()

	return fs
}

// If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (s *FollowerState) RequestVote(args *RequestVoteArgs) (granted bool) {
	switch s.Voted() {
	case NoVote:
		if !s.validRequestVote(args.LastLogIndex, args.LastLogTerm) {
			Info("%s reject vote request from %d: candidate is not more updated",
				s, args.Candidate)
			return false
		}
		if s.Close("start following %d", args.Candidate) {
			s.To(Follower(args.Term, args.Candidate, s))
		}
		return true
	case args.Candidate:
		s.timer.Restart()
		return true
	default:
		Info("%s reject vote request from %d: already voted other candidate",
			s, args.Candidate)
		return false
	}
}

func (s *FollowerState) AppendEntries(args *AppendEntriesArgs) (success bool) {
	switch s.Voted() {
	case NoVote:
		if s.Close("start following %d", args.Leader) {
			ns := s.To(Follower(args.Term, args.Leader, s))
			return ns.AppendEntries(args)
		}
		return false
	case args.Leader:
		s.timer.Restart()
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if s.handleEntries(args.Leader, args.PrevLogIndex, args.PrevLogTerm, args.Entries) {
			s.tryCommit(args.LeaderCommit)
			return true
		}
		return false
	default:
		return false
	}
}

func (s *FollowerState) InstallSnapshot(args *InstallSnapshotArgs) (success bool) {
	switch s.Voted() {
	case NoVote:
		if s.Close("start following %d", args.Leader) {
			ns := s.To(Follower(args.Term, args.Leader, s))
			return ns.InstallSnapshot(args)
		}
		return false
	case args.Leader:
		s.timer.Restart()
		return s.ApplySnapshot(args.LastLogIndex, args.LastLogTerm, args.Snapshot)
	default:
		return false
	}
}

func (s *FollowerState) Close(msg string, args ...interface{}) bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	Info("%s closing: %s", s, fmt.Sprintf(msg, args...))
	s.timer.Stop()
	Info("%s closed", s)
	return true
}

func (s *FollowerState) String() string {
	return logPrefix(s)
}

func (s *FollowerState) Role() string {
	return RoleFollower
}

func (s *FollowerState) heartbeatTimeout() {
	if s.closed.CompareAndSwap(false, true) {
		s.Lock()
		Info("%s heartbeat timeout, transition to candidate", s)
		s.To(Candidate(s.Term()+1, s))
		s.Unlock()
	}
}

func (s *FollowerState) tryCommit(index int) {
	s.RLockLog()
	commitIndex := s.Committed()
	s.RUnlockLog()

	if index > commitIndex {
		Info("%s receive higher commit index %d(current %d)",
			s, index, commitIndex)
		if s.CommitLog(index) {
			Info("%s log[:%d] committed", s, s.Committed()+1)
		}
	}
}

// Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
func (s *FollowerState) validRequestVote(lastLogIndex, lastLogTerm int) bool {
	curLastIndex := len(s.r.logs) - 1
	if curLastIndex < 0 {
		// There is no log entry yet, so any entry is valid
		return true
	}
	curLastTerm := s.r.logs[curLastIndex].Term

	// From 5.4.1:
	// If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date.
	if lastLogTerm < curLastTerm {
		return false
	}
	if lastLogTerm > curLastTerm {
		return true
	}

	// From 5.4.1:
	// If the logs end with the same term, then
	// whichever log is longer is more up-to-date.
	return lastLogIndex >= curLastIndex
}

func (s *FollowerState) handleEntries(leader, prevIndex, prevTerm int, entries []Log) bool {
	s.LockLog()
	defer s.UnlockLog()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if prevIndex > s.LastLogIndex() {
		Info("%s reject logs: prev index %d is larger than last log index %d",
			s, prevIndex, s.LastLogIndex())
		return false
	}

	_, prevLog := s.GetLog(prevIndex)
	if prevLog == nil {
		Info("%s reject logs: prev index %d is already trimmed", s, prevIndex)
		return false
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	if prevLog.Term != prevTerm {
		Info("%s reject logs: prev term %d at index %d is conflict with %d", s,
			prevTerm, prevIndex, prevLog.Term)
		// Fallback the whole term once a time
		termIndex, _ := s.FirstLogAtTerm(prevLog.Term)
		s.DeleteLogSince(termIndex)
		Info("%s delete log since index %d", s, termIndex)
		return false
	}

	s.DeleteLogSince(prevIndex + 1)

	if len(entries) > 0 {
		// Append any new entries not already in the log
		s.AppendLogs(entries...)
		Info("%s append new log[%d:%d]", s, prevIndex+1, prevIndex+1+len(entries))
	}

	return true
}
