package raft

import (
	"context"
	"sync/atomic"

	"goraft/src/util"
)

var (
	_ State = (*FollowerState)(nil)
)

type FollowerState struct {
	BaseState
	timer  *util.Timer
	closed atomic.Bool
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
			return false
		}
		if s.Close() {
			Info("%s start following %d", s, args.Candidate)
			s.To(Follower(args.Term, args.Candidate, s))
		}
		return true
	case args.Candidate:
		s.timer.Restart()
		return true
	default:
		return false
	}
}

func (s *FollowerState) AppendEntries(args *AppendEntriesArgs) (success bool) {
	switch s.Voted() {
	case NoVote:
		if s.Close() {
			Info("%s start following %d", s, args.Leader)
			ns := s.To(Follower(args.Term, args.Leader, s))
			return ns.AppendEntries(args)
		}
		return false
	case args.Leader:
		s.timer.Restart()
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		defer s.CommitLog(args.LeaderCommit)
		return s.handleEntries(args.Leader, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	default:
		return false
	}
}

func (s *FollowerState) Close() bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	s.timer.Stop()
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
	if len(entries) == 0 {
		Debug("%s receive heartbeat from %d", s, leader)
		return true
	}

	s.LockLog()
	defer s.UnlockLog()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if prevIndex > s.LastLogIndex() {
		return false
	}

	_, prevLog := s.GetLog(prevIndex)
	if prevLog == nil {
		// The previous log entry is already trimmed
		return false
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	if prevLog.Term != prevTerm {
		s.DeleteLogSince(prevIndex)
		return false
	}

	// Append any new entries not already in the log
	s.AppendLogs(entries...)

	return true
}
