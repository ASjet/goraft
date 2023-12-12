package state

import (
	"context"
	"time"
)

var (
	_ State = (*FollowerState)(nil)
)

type FollowerState struct {
	BaseState
	ctx         context.Context
	cancel      context.CancelFunc
	heartbeatCh chan struct{}
}

func Follower(term, self, follow int, migrateFn MigrateFunc) *FollowerState {
	ctx, cancel := context.WithCancel(context.TODO())
	fs := &FollowerState{
		BaseState: BaseState{
			name:    "F",
			term:    term,
			self:    self,
			voteFor: follow,
			migrate: migrateFn,
		},
		ctx:         ctx,
		cancel:      cancel,
		heartbeatCh: make(chan struct{}),
	}
	go fs.heartbeatTimer()
	return fs
}

func (s *FollowerState) RequestVote(term, candidate int) (granted bool) {
	if s.voteFor == NoVote || s.term < term {
		s.voteFor = candidate
		return true
	}
	return false
}

func (s *FollowerState) AppendEntries(term, leader int) (success bool) {
	if s.term < term {
		s.term = term
		s.voteFor = leader
		return true
	}
	return false
}

func (s *FollowerState) Close() {
	s.cancel()
}

func (s *FollowerState) heartbeatTimer() {
	timer := time.NewTimer(heartbeatTimeout())
	for {
		timeout := false
		select {
		case <-s.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			close(s.heartbeatCh)
			return
		case <-timer.C:
			timeout = true
		case <-s.heartbeatCh:
			if !timer.Stop() {
				<-timer.C
				timeout = true
			}
		}
		if timeout {
			s.MigrateTo(Candidate(s.term+1, s.self, s.migrate))
			return
		}
		timer.Reset(heartbeatTimeout())
	}
}
