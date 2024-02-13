package raft

import (
	"fmt"
	"sync/atomic"

	"goraft/src/labrpc"
	"goraft/src/util/log"
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
	Term() Term
	Voted() int
	Me() int
	Peers() int
	Role() string
	Base(term Term, follow int) *BaseState
	String() string
	SnapshotIndex() int
	LastLogIndex() int
	Committed() int
	GetLog(index int) (int, *Log)

	To(state State) (newState State)
	Close(msg string, args ...interface{}) (success bool)

	AppendCommand(command interface{}) (index int, term Term)

	Lock()
	Unlock()
	LockLog()
	UnlockLog()

	// Call only term == curTerm
	RequestVote(args *RequestVoteArgs) (granted bool)
	AppendEntries(args *AppendEntriesArgs) (success bool)
	InstallSnapshot(args *InstallSnapshotArgs) (success bool)
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
	term   Term
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

func (s *BaseState) Base(term Term, follow int) *BaseState {
	return &BaseState{
		term:   term,
		follow: follow,
		r:      s.r,
	}
}

func (s *BaseState) Term() Term {
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
func (s *BaseState) GetLog(index int) (int, *Log) {
	index = s.logIndexWithOffset(index)
	if index < 0 || index >= len(s.r.logs) {
		return s.SnapshotIndex() + index, nil
	}
	return s.SnapshotIndex() + index, &s.r.logs[index]
}

func (s *BaseState) GetLogSince(index int) []Log {
	index = s.logIndexWithOffset(index)
	if index < 0 || index >= len(s.r.logs) {
		return nil
	}
	return s.r.logs[index:]
}

func (s *BaseState) FirstLogAtTerm(term Term) (int, *Log) {
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
	s.r.stateMu.Lock()
}

func (s *BaseState) Unlock() {
	s.r.stateMu.Unlock()
}

func (s *BaseState) RLockLog() {
	s.r.logCond.L.Lock()
}

func (s *BaseState) RUnlockLog() {
	s.r.logCond.L.Unlock()
}

func (s *BaseState) LockLog() {
	s.r.logCond.L.Lock()
}

func (s *BaseState) UnlockLog() {
	s.r.logCond.L.Unlock()
}

func (s *BaseState) WaitLog() {
	s.r.logCond.Wait()
}

func (s *BaseState) BroadcastLog() {
	s.r.logCond.Broadcast()
}

func (s *BaseState) AppendLogs(logs ...Log) (index int) {
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
	s.LockLog()
	defer s.UnlockLog()

	if s.r.commitIndex < s.SnapshotIndex() {
		s.r.commitIndex = s.SnapshotIndex()
	}

	advance = false
	for s.r.commitIndex < index {
		i, lastLog := s.GetLog(s.r.commitIndex + 1)
		if lastLog == nil {
			return
		}
		s.r.commitIndex++
		advance = true
		if i != s.r.commitIndex {
			log.Fatal("%d commit index %d not match with log offset %d", s.Me(),
				s.r.commitIndex, s.SnapshotIndex())
		}
		s.UnlockLog()
		s.r.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      lastLog.Data,
			CommandIndex: i,
		}
		s.LockLog()
	}

	if advance {
		s.r.persistState()
	}

	return advance
}

func (s *BaseState) ApplySnapshot(index int, term Term, snapshot []byte) (applied bool) {
	s.LockLog()

	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	if s.SnapshotIndex() >= index {
		// Already applied (an newer) snapshot
		log.Info("%s reject to apply old snapshot at index %d (current is %d), term %d",
			s, index, s.SnapshotIndex(), term)
		s.UnlockLog()
		return false
	}

	if s.LastLogIndex() >= index {
		// 6. If existing log entry has same index and term as snapshot’s last
		//    included entry, retain log entries following it and reply
		log.Info("%s drop logs in snapshot at index %d", s, index)
		s.r.logs = s.r.logs[s.logIndexWithOffset(index):]
	} else {
		// 7. Discard the entire log
		log.Info("%s drop all logs with a full-covered snapshot at index %d, term %d",
			s, index, term)
		s.r.logs = []Log{{Term: term}}
	}

	s.r.snapshot = snapshot
	s.r.snapshotIndex = index
	s.r.commitIndex = index
	s.r.persistSnapshot(snapshot)
	s.UnlockLog()

	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	log.Info("%s apply snapshot at index %d, term %d", s, index, term)
	s.r.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  term,
		SnapshotIndex: index,
	}
	return true
}

func (s *BaseState) RequestVote(args *RequestVoteArgs) (granted bool) {
	panic("RequestVote not implemented")
}

func (s *BaseState) AppendEntries(args *AppendEntriesArgs) (success bool) {
	panic("AppendEntries not implemented")
}

func (s *BaseState) AppendCommand(command interface{}) (index int, term Term) {
	panic("AppendCommand not implemented")
}

func (s *BaseState) InstallSnapshot(args *InstallSnapshotArgs) (success bool) {
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
