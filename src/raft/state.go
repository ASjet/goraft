package raft

import (
	"fmt"
	"sync/atomic"

	"goraft/src/labrpc"
	"goraft/src/models"
)

const (
	NoVote int = -1

	RoleFollower  = "F"
	RoleCandidate = "C"
	RoleLeader    = "L"
)

var (
	_ State = (*BaseState)(nil)
)

type State interface {
	Term() models.Term
	Voted() int
	Me() int
	Peers() int
	Role() string
	Base(term models.Term, follow int) *BaseState
	String() string
	SnapshotIndex() int
	LastLogIndex() int
	Committed() int
	GetLog(index int) (int, *models.Log)
	LogIndexWithOffset(index int) int

	To(state State) (newState State)
	Close(msg string, args ...interface{}) (success bool)

	AppendCommand(command interface{}) (index int, term models.Term)

	Lock()
	Unlock()
	LockLog()
	UnlockLog()

	// Call only term == curTerm
	RequestVote(args *models.RequestVoteArgs) (granted bool)
	AppendEntries(args *models.AppendEntriesArgs) (success bool)
	InstallSnapshot(args *models.InstallSnapshotArgs) (success bool)
}

// logPrefix always access the state's immutable fields
func logPrefix(s State) string {
	vote := s.Voted()
	switch vote {
	case NoVote:
		return fmt.Sprintf("%d>N:%s%03d:%02d/%02d/%02d",
			s.Me(), s.Role(), s.Term(), s.SnapshotIndex(), s.Committed(), s.LastLogIndex())
	case s.Me():
		return fmt.Sprintf("%d>S:%s%03d:%02d/%02d/%02d",
			s.Me(), s.Role(), s.Term(), s.SnapshotIndex(), s.Committed(), s.LastLogIndex())
	default:
		return fmt.Sprintf("%d>%d:%s%03d:%02d/%02d/%02d",
			s.Me(), s.Voted(), s.Role(), s.Term(), s.SnapshotIndex(), s.Committed(), s.LastLogIndex())
	}
}

// Immutable basic raft states
type BaseState struct {
	r      *Raft
	term   models.Term
	follow int
	closed atomic.Bool

	lockTraceID    atomic.Int64
	logLockTraceID atomic.Int64
}

func Base(r *Raft) *BaseState {
	return &BaseState{
		term:   0,
		follow: NoVote,
		r:      r,
	}
}

// Getters

func (s *BaseState) Base(term models.Term, follow int) *BaseState {
	return &BaseState{
		term:   term,
		follow: follow,
		r:      s.r,
	}
}

func (s *BaseState) Term() models.Term {
	return s.term
}

func (s *BaseState) Voted() int {
	return s.follow
}

func (s *BaseState) Me() int {
	return s.r.me
}

func (s *BaseState) Peers() int {
	return len(s.r.peers)
}

func (s *BaseState) Majority() int {
	return s.Peers()/2 + 1
}

func (s *BaseState) PollPeers(f func(peerID int, peerRPC *labrpc.ClientEnd)) {
	for i, peer := range s.r.peers {
		if i != s.Me() {
			go f(i, peer)
		}
	}
}

func (s *BaseState) SnapshotIndex() int {
	return s.r.snapshotIndex
}

func (s *BaseState) LastLogIndex() int {
	return len(s.r.logs) + s.SnapshotIndex() - 1 // -1 for dummy entry
}

// Call with holding LogRWLock
func (s *BaseState) GetLog(index int) (int, *models.Log) {
	index = s.logIndexWithOffset(index)
	if index < 0 || index >= len(s.r.logs) {
		return s.SnapshotIndex() + index, nil
	}
	return s.SnapshotIndex() + index, &s.r.logs[index]
}

func (s *BaseState) GetLogSince(index int) []models.Log {
	index = s.logIndexWithOffset(index)
	if index < 0 || index >= len(s.r.logs) {
		return nil
	}
	return s.r.logs[index:]
}

func (s *BaseState) FirstLogAtTerm(term models.Term) (int, *models.Log) {
	for i := 0; i < len(s.r.logs); i++ {
		if s.r.logs[i].Term == term {
			return s.SnapshotIndex() + i, &s.r.logs[i]
		}
	}
	return 0, nil
}

func (s *BaseState) Committed() int {
	return s.r.commitIndex
}

func (s *BaseState) GetSnapshot() []byte {
	return s.r.snapshot
}

// Setters

func (s *BaseState) To(state State) State {
	defer s.r.persistState()
	s.r.state = state
	return state
}

func (s *BaseState) Lock() {
	s.r.LockState()
}

func (s *BaseState) Unlock() {
	s.r.UnlockState()
}

func (s *BaseState) RLockLog() {
	s.r.RLockLog()
}

func (s *BaseState) RUnlockLog() {
	s.r.RUnlockLog()
}

func (s *BaseState) LockLog() {
	s.r.LockLog()
}

func (s *BaseState) UnlockLog() {
	s.r.UnlockLog()
}

func (s *BaseState) WaitLog() {
	s.r.WaitLog()
}

func (s *BaseState) BroadcastLog() {
	s.r.BroadcastLog()
}

func (s *BaseState) AppendLogs(logs ...models.Log) (index int) {
	s.r.logs = append(s.r.logs, logs...)
	if len(logs) > 0 {
		s.r.persistState()
	}
	return s.LastLogIndex()
}

func (s *BaseState) DeleteLogSince(index int) (n int) {
	index = s.logIndexWithOffset(index)
	if index < 0 || index >= len(s.r.logs) {
		return 0
	}
	deleted := s.r.logs[index:]
	s.r.logs = s.r.logs[:index]
	if len(deleted) > 0 {
		s.r.persistState()
	}
	return len(deleted)
}

// This will acquire log lock, use go routine to avoid deadlock
func (s *BaseState) CommitLog(index int) (advance bool) {
	return s.r.CommitLog(index)
}

func (s *BaseState) ApplySnapshot(index int, term models.Term, snapshot []byte) (applied bool) {
	return s.r.ApplySnapshot(index, term, snapshot)
}

func (s *BaseState) RequestVote(args *models.RequestVoteArgs) (granted bool) {
	panic("RequestVote not implemented")
}

func (s *BaseState) AppendEntries(args *models.AppendEntriesArgs) (success bool) {
	panic("AppendEntries not implemented")
}

func (s *BaseState) AppendCommand(command interface{}) (index int, term models.Term) {
	panic("AppendCommand not implemented")
}

func (s *BaseState) InstallSnapshot(args *models.InstallSnapshotArgs) (success bool) {
	panic("InstallSnapshot not implemented")
}

func (s *BaseState) String() string {
	return logPrefix(s)
}

func (s *BaseState) Role() string {
	panic("Role not implemented")
}

func (s *BaseState) Close(msg string, args ...interface{}) bool {
	panic("Close not implemented")
}

func (s *BaseState) logIndexWithOffset(index int) int {
	if index < 0 {
		return len(s.r.logs) + index
	} else {
		return index - s.SnapshotIndex()
	}
}

func (s *BaseState) LogIndexWithOffset(index int) int {
	return s.logIndexWithOffset(index)
}
