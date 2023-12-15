package raft

import (
	"context"
	"fmt"
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
	Info("%s new follower with leader %d", fs, follow)

	fs.timer = util.NewTimer(context.TODO(), heartbeatTimeout, fs.heartbeatTimeout).Start()

	return fs
}

// If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (s *FollowerState) RequestVote(args *RequestVoteArgs) (granted bool) {
	switch s.Voted() {
	case NoVote:
		if !s.ValidEntries() {
			return false
		}

		if s.Close() {
			Info("%s start following %d at term %d", s, args.Candidate, args.Term)
			s.SyncTo(Follower(args.Term, args.Candidate, s))
		}

		fallthrough
	case args.Candidate:
		return true
	default:
		return false
	}
}

func (s *FollowerState) AppendEntries(args *AppendEntriesArgs) (success bool) {
	if s.Voted() != args.Leader {
		return false
	}

	if !s.ValidEntries() {
		return false
	}

	Debug("%s receive heartbeat from %d", s, args.Leader)
	s.timer.Restart()

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)

	// Append any new entries not already in the log

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	// TODO: handle log entries

	return true
}

func (s *FollowerState) Close() bool {
	if !s.closed.CompareAndSwap(false, true) {
		return false
	}
	s.timer.Stop()
	return true
}

func (s *FollowerState) String() string {
	return fmt.Sprintf("%d:%s%03d", s.Me(), s.Role(), s.Term())
}

func (s *FollowerState) Role() string {
	return RoleFollower
}

func (s *FollowerState) heartbeatTimeout() {
	if s.closed.CompareAndSwap(false, true) {
		Info("%s heartbeat timeout, migrate to candidate", s)
		s.SyncTo(Candidate(s.Term()+1, s))
	}
}
