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
		return s.follow(args.Term, args.Candidate)
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
		return s.follow(args.Term, args.Leader)
	case args.Leader:
		s.timer.Restart()
		return s.handleEntries(args.Entries)
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

func (s *FollowerState) follow(term, peer int) bool {
	if !s.ValidEntries() {
		return false
	}

	if s.Close() {
		Info("%s start following %d", s, peer)
		s.To(Follower(term, peer, s))
	}
	return true
}

func (s *FollowerState) handleEntries(entries []interface{}) bool {
	if len(entries) == 0 {
		Debug("%s receive heartbeat from %d", s, s.Voted())
		return true
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)

	// Append any new entries not already in the log

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	// TODO: handle log entries

	return true

}
