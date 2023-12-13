package raft

import "goraft/src/labrpc"

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
	To(state State)
	Voted() int
	Term() int
	Base() BaseState

	// Call only term == curTerm
	RequestVote(term, candidate int) (granted bool)
	AppendEntries(term, leader int) (success bool)
	String() string
	Role() string
	Close() (success bool)
}

// Basic raft states
type BaseState struct {
	// Mutable states
	term   int
	follow int

	// Immutable states
	r *Raft
}

func Base(r *Raft) *BaseState {
	return &BaseState{
		term:   0,
		follow: NoVote,
		r:      r,
	}
}

// Getters

func (s *BaseState) Base() BaseState {
	return *s
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

func (s *BaseState) Follow(term, peer int) (changed bool) {
	changed = (s.follow != peer) || (s.term != term)
	s.follow = peer
	s.term = term
	return changed
}

func (s *BaseState) To(state State) {
	s.r.state = state
}

func (s *BaseState) RequestVote(term, candidate int) (granted bool) {
	panic("not implemented")
}

func (s *BaseState) AppendEntries(term, leader int) (success bool) {
	panic("not implemented")
}

func (s *BaseState) String() string {
	panic("not implemented")
}

func (s *BaseState) Role() string {
	panic("not implemented")
}

func (s *BaseState) Close() bool {
	panic("not implemented")
}
