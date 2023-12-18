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
	Role() string
	Base(term, follow int) BaseState
	String() string

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
	if vote == NoVote {
		return fmt.Sprintf("%d>n:%s%03d", s.Me(), s.Role(), s.Term())
	}
	return fmt.Sprintf("%d>%d:%s%03d", s.Me(), s.Voted(), s.Role(), s.Term())
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

// Setters

func (s *BaseState) To(state State) State {
	s.r.state = state
	return state
}

func (s *BaseState) Lock() {
	s.r.stateMu.Lock()
}

func (s *BaseState) Unlock() {
	s.r.stateMu.Unlock()
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
