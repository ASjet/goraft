package state

import (
	"sync/atomic"

	"goraft/src/labrpc"
	"goraft/src/models"
)

// Immutable basic raft states
type BaseState struct {
	ctx      Context
	me       int
	peers    int
	majority int

	term   models.Term
	closed atomic.Bool
}

func Base(ctx Context, term models.Term) *BaseState {
	nPeers := len(ctx.Peers())
	return &BaseState{
		ctx:      ctx,
		me:       ctx.Me(),
		peers:    nPeers,
		majority: nPeers/2 + 1,

		term: term,
	}
}

// Getters

func (s *BaseState) Context() Context {
	return s.ctx
}

func (s *BaseState) Term() models.Term {
	return s.term
}

func (s *BaseState) Me() int {
	return s.me
}

func (s *BaseState) Peers() int {
	return s.peers
}

func (s *BaseState) Majority() int {
	return s.majority
}

func (s *BaseState) Committed() int {
	return s.Context().CommitIndex()
}

func (s *BaseState) SnapshotIndex() int {
	return s.Context().SnapshotIndex()
}

func (s *BaseState) LastLogIndex() int {
	return len(s.Context().Logs()) + s.SnapshotIndex() - 1 // -1 for dummy entry
}

// Call with holding LogRWLock
func (s *BaseState) GetLog(index int) (int, *models.Log) {
	index = s.LogIndexWithOffset(index)
	if index < 0 || index >= len(s.Context().Logs()) {
		return s.SnapshotIndex() + index, nil
	}
	return s.SnapshotIndex() + index, &s.Context().Logs()[index]
}

func (s *BaseState) GetLogSince(index int) []models.Log {
	index = s.LogIndexWithOffset(index)
	if index < 0 || index >= len(s.Context().Logs()) {
		return nil
	}
	return s.Context().Logs()[index:]
}

func (s *BaseState) FirstLogAtTerm(term models.Term) (int, *models.Log) {
	for i := 0; i < len(s.Context().Logs()); i++ {
		if s.Context().Logs()[i].Term == term {
			return s.SnapshotIndex() + i, &s.Context().Logs()[i]
		}
	}
	return 0, nil
}

func (s *BaseState) GetSnapshot() []byte {
	return s.Context().GetSnapshot()
}

// Iterators

func (s *BaseState) PollPeers(f func(peerID int, peerRPC *labrpc.ClientEnd)) {
	for i, peer := range s.Context().Peers() {
		if i != s.Me() {
			go f(i, peer)
		}
	}
}

// Setters

func (s *BaseState) To(state State) State {
	defer s.Context().Persist()
	s.Context().SetState(state)
	return state
}

func (s *BaseState) AppendLogs(logs ...models.Log) (index int) {
	lastIndex := s.Context().AppendLogs(logs...)
	if len(logs) > 0 {
		s.Context().Persist()
	}
	return lastIndex
}

func (s *BaseState) DeleteLogSince(index int) (n int) {
	index = s.LogIndexWithOffset(index)
	if index < 0 || index >= len(s.Context().Logs()) {
		return 0
	}
	deleted := s.Context().Logs()[index:]
	s.Context().SetLogs(s.Context().Logs()[:index])
	if len(deleted) > 0 {
		s.Context().Persist()
	}
	return len(deleted)
}

func (s *BaseState) LogIndexWithOffset(index int) int {
	if index < 0 {
		return len(s.Context().Logs()) + index
	} else {
		return index - s.SnapshotIndex()
	}
}

func (s *BaseState) Lock() {
	s.Context().LockState()
}

func (s *BaseState) Unlock() {
	s.Context().UnlockState()
}

func (s *BaseState) RLockLog() {
	s.Context().RLockLog()
}

func (s *BaseState) RUnlockLog() {
	s.Context().RUnlockLog()
}

func (s *BaseState) LockLog() {
	s.Context().LockLog()
}

func (s *BaseState) UnlockLog() {
	s.Context().UnlockLog()
}

func (s *BaseState) WaitLog() {
	s.Context().WaitLog()
}

func (s *BaseState) BroadcastLog() {
	s.Context().BroadcastLog()
}
