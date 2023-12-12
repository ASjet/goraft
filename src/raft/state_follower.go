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

func Follower(term, follow int, oldState State) *FollowerState {
	// Follower can come from any state
	fs := &FollowerState{
		BaseState: oldState.Base(),
	}
	fs.term = term
	fs.follow = follow
	Info("%s new follower", fs)

	fs.timer = util.NewTimer(context.TODO(), heartbeatTimeout, fs.heartbeatTimeout).Start()
	return fs
}

func (s *FollowerState) RequestVote(term, candidate int) (granted bool) {
	if s.follow == NoVote {
		Info("%s start following %d at term %d", s, candidate, term)
		s.term = term
		s.follow = candidate
		return true
	}
	// TODO: valid log entries
	return false
}

func (s *FollowerState) AppendEntries(term, leader int) (success bool) {
	if s.follow != NoVote && s.follow != leader {
		return false
	}
	Debug("%s receive heartbeat from %d", s, leader)
	s.timer.Restart()
	// TODO: valid log entries
	return true
}

func (s *FollowerState) Close() bool {
	if isFirst := s.closed.CompareAndSwap(false, true); !isFirst {
		return false
	}
	s.timer.Stop()
	return true
}

func (s *FollowerState) String() string {
	return fmt.Sprintf("%s%d:%03d", s.Role(), s.self, s.term)
}

func (s *FollowerState) Role() string {
	return RoleFollower
}

func (s *FollowerState) heartbeatTimeout() {
	if s.closed.CompareAndSwap(false, true) {
		Info("%s heartbeat timeout, migrate to candidate", s)
		s.MigrateTo(Candidate(s.term+1, s))
	}
}
