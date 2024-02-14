package state

import (
	"context"
	"fmt"

	"goraft/src/models"
	"goraft/src/util"
	"goraft/src/util/log"
)

var (
	_ State = (*FollowerState)(nil)
)

type FollowerState struct {
	*BaseState
	voted int
	timer *util.Timer
}

func Follower(term models.Term, follow int, ctx Context) *FollowerState {
	// Follower can come from any state
	fs := &FollowerState{
		BaseState: Base(ctx, term),
		voted:     follow,
	}
	if follow == NoVote {
		log.Info("%s new follower without leader", fs)
	} else {
		log.Info("%s new follower with leader %d", fs, follow)
	}

	fs.timer = util.NewTimer(context.TODO(), genHeartbeatTimeout(), fs.heartbeatTimeout).Start()

	return fs
}

func (s *FollowerState) Role() string {
	return RoleFollower
}

func (s *FollowerState) String() string {
	return logPrefix(s)
}

func (s *FollowerState) Voted() int {
	return s.voted
}

func (s *FollowerState) Close(msg string, args ...interface{}) bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	log.Info("%s closing: %s", s, fmt.Sprintf(msg, args...))
	s.timer.Stop()
	log.Info("%s closed", s)
	return true
}

func (s *FollowerState) AppendCommand(command interface{}) (index int, term models.Term) {
	log.Fatal("%s AppendCommand: not a leader", s)
	return 0, 0
}

// If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (s *FollowerState) RequestVote(args *models.RequestVoteArgs) (granted bool) {
	switch s.Voted() {
	case NoVote:
		if !s.validRequestVote(args.LastLogIndex, args.LastLogTerm) {
			log.Info("%s reject vote request from %d: candidate is not more updated",
				s, args.Candidate)
			return false
		}
		s.follow(args.Candidate)
		return true
	case args.Candidate:
		s.timer.Restart()
		return true
	default:
		log.Info("%s reject vote request from %d: already voted other candidate",
			s, args.Candidate)
		return false
	}
}

func (s *FollowerState) AppendEntries(args *models.AppendEntriesArgs) (success bool) {
	switch s.Voted() {
	case NoVote:
		s.follow(args.Leader)
		fallthrough
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

func (s *FollowerState) InstallSnapshot(args *models.InstallSnapshotArgs) (success bool) {
	switch s.Voted() {
	case NoVote:
		s.follow(args.Leader)
		fallthrough
	case args.Leader:
		s.timer.Restart()
		return s.Context().ApplySnapshot(args.LastLogIndex, args.LastLogTerm, args.Snapshot)
	default:
		return false
	}
}

func (s *FollowerState) heartbeatTimeout() {
	if s.closed.CompareAndSwap(false, true) {
		s.LockState()
		log.Info("%s heartbeat timeout, transition to candidate", s)
		s.To(Candidate(s.Term()+1, s))
		s.UnlockState()
	}
}

func (s *FollowerState) tryCommit(index int) {
	s.RLockLog()
	commitIndex := s.Committed()
	s.RUnlockLog()

	if index > commitIndex {
		log.Info("%s receive higher commit index %d(current %d)",
			s, index, commitIndex)
		if s.Context().CommitLog(index) {
			log.Info("%s log[:%d] committed", s, s.Committed()+1)
		}
	}
}

// Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
func (s *FollowerState) validRequestVote(lastLogIndex int, lastLogTerm models.Term) bool {
	lastIndex, lastLog := s.GetLog(-1)
	if lastLog == nil {
		return true
	}
	curLastTerm := lastLog.Term

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
	return lastLogIndex >= lastIndex
}

func (s *FollowerState) handleEntries(leader, prevIndex int, prevTerm models.Term, entries []models.Log) bool {
	s.LockLog()
	defer s.UnlockLog()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if prevIndex > s.LastLogIndex() {
		log.Info("%s reject logs: prev index %d is larger than last log index %d",
			s, prevIndex, s.LastLogIndex())
		return false
	}

	_, prevLog := s.GetLog(prevIndex)
	if prevLog == nil {
		log.Info("%s reject logs: prev index %d is already trimmed", s, prevIndex)
		return false
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	if prevLog.Term != prevTerm {
		log.Info("%s reject logs: prev term %d at index %d is conflict with %d", s,
			prevTerm, prevIndex, prevLog.Term)
		// Fallback the whole term once a time
		termIndex, _ := s.FirstLogAtTerm(prevLog.Term)
		s.DeleteLogSince(termIndex)
		log.Info("%s delete log since index %d", s, termIndex)
		return false
	}

	s.DeleteLogSince(prevIndex + 1)

	if len(entries) > 0 {
		// Append any new entries not already in the log
		s.AppendLogs(entries...)
		log.Info("%s append new log[%d:%d]", s, prevIndex+1, prevIndex+1+len(entries))
	}

	return true
}

func (s *FollowerState) follow(peer int) {
	log.Info("%s starting following %d", s, peer)
	s.voted = peer
}
