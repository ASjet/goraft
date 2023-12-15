package raft

import (
	"fmt"

	"goraft/src/labrpc"
)

type MigrateFunc func(state State)

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
	Base(term, follow int) BaseState
	String() string
	Role() string

	To(state State)
	Close() (success bool)

	// Call only term == curTerm
	RequestVote(args *RequestVoteArgs) (granted bool)
	// Call only term == curTerm
	AppendEntries(args *AppendEntriesArgs) (success bool)

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	ValidEntries() bool
}

func logPrefix(s State) string {
	vote := s.Voted()
	if vote == NoVote {
		return fmt.Sprintf("%d>n:%s%03d", s.Me(), s.Role(), s.Term())
	}
	return fmt.Sprintf("%d>%d:%s%03d", s.Me(), s.Voted(), s.Role(), s.Term())
}

// Basic raft states
type BaseState struct {
	// Internal mutable states
	r *Raft

	// Immutable states
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

func (s *BaseState) ValidEntries() bool {
	// TODO: valid log entries
	return true
}

// Setters

func (s *BaseState) To(state State) {
	s.r.state = state
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

func (s *BaseState) String() string {
	return logPrefix(s)
	panic("not implemented")
}

func (s *BaseState) Role() string {
	panic("not implemented")
}

func (s *BaseState) Close() bool {
	panic("not implemented")
}
