package raft

import (
	"fmt"

	"goraft/src/labrpc"
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
	Term() int
	Voted() int
	Me() int
	Peers() int
	Role() string
	Base(term, follow int) BaseState
	String() string
	LastLogIndex() int
	Committed() int
	GetLog(index int) (int, *Log)

	To(state State) (newState State)
	Close() (success bool)

	// Call only term == curTerm
	RequestVote(args *RequestVoteArgs) (granted bool)
	// Call only term == curTerm
	AppendEntries(args *AppendEntriesArgs) (success bool)

	AppendCommand(command interface{}) (index int, term int)
}

// logPrefix always access the state's immutable fields
func logPrefix(s State) string {
	vote := s.Voted()
	switch vote {
	case NoVote:
		return fmt.Sprintf("%d>N:%s%03d:%02d/%02d", s.Me(), s.Role(), s.Term(), s.Committed(), s.LastLogIndex())
	case s.Me():
		return fmt.Sprintf("%d>S:%s%03d:%02d/%02d", s.Me(), s.Role(), s.Term(), s.Committed(), s.LastLogIndex())
	default:
		return fmt.Sprintf("%d>%d:%s%03d:%02d/%02d", s.Me(), s.Voted(), s.Role(), s.Term(), s.Committed(), s.LastLogIndex())
	}
}

// Immutable basic raft states
type BaseState struct {
	r      *Raft
	term   int
	follow int
}

func Base(r *Raft) *BaseState {
	return &BaseState{
		term:   0,
		follow: NoVote,
		r:      r,
	}
}

// Getters

func (s *BaseState) Base(term, follow int) BaseState {
	return BaseState{
		term:   term,
		follow: follow,
		r:      s.r,
	}
}

func (s *BaseState) Term() int {
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

func (s *BaseState) LastLogIndex() int {
	return len(s.r.logs) + s.r.logIndexOffset - 1 // -1 for dummy entry
}

// Call with holding LogRWLock
func (s *BaseState) GetLog(index int) (int, *Log) {
	index = s.logIndexWithOffset(index)
	if index < 0 || index >= len(s.r.logs) {
		return s.r.logIndexOffset + index, nil
	}
	return s.r.logIndexOffset + index, &s.r.logs[index]
}

func (s *BaseState) GetLogSince(index int) []Log {
	index = s.logIndexWithOffset(index)
	if index < 0 || index >= len(s.r.logs) {
		return nil
	}
	return s.r.logs[index:]
}

func (s *BaseState) FirstLogAtTerm(term int) (int, *Log) {
	for i := 0; i < len(s.r.logs); i++ {
		if s.r.logs[i].Term == term {
			return s.r.logIndexOffset + i, &s.r.logs[i]
		}
	}
	return 0, nil
}

func (s *BaseState) Committed() int {
	return s.r.commitIndex
}

func (s *BaseState) LogOffset() int {
	return s.r.logIndexOffset
}

// Setters

func (s *BaseState) To(state State) State {
	defer s.r.persist()
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
		s.r.persist()
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
		s.r.persist()
	}
	return len(deleted)
}

// This will acquire log lock, use go routine to avoid deadlock
func (s *BaseState) CommitLog(index int) (advance bool) {
	s.LockLog()
	defer s.UnlockLog()

	advance = false
	for s.r.commitIndex < index {
		i, log := s.GetLog(s.r.commitIndex + 1)
		if log == nil {
			return
		}
		s.r.commitIndex++
		advance = true
		if i != s.r.commitIndex {
			Fatal("%d commit index %d not match with log offset %d", s.Me(),
				s.r.commitIndex, s.r.logIndexOffset)
		}
		s.r.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      log.Data,
			CommandIndex: i,
		}
	}

	if advance {
		s.r.persist()
	}

	return advance
}

func (s *BaseState) RequestVote(args *RequestVoteArgs) (granted bool) {
	panic("not implemented")
}

func (s *BaseState) AppendEntries(args *AppendEntriesArgs) (success bool) {
	panic("not implemented")
}

func (s *BaseState) AppendCommand(command interface{}) (index int, term int) {
	panic("not implemented")
}

func (s *BaseState) String() string {
	return logPrefix(s)
}

func (s *BaseState) Role() string {
	panic("not implemented")
}

func (s *BaseState) Close() bool {
	panic("not implemented")
}

func (s *BaseState) logIndexWithOffset(index int) int {
	if index < 0 {
		return len(s.r.logs) + index
	} else {
		return index - s.r.logIndexOffset
	}
}
