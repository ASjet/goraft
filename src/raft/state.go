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
	MigrateTo(state State)
	VoteFor() int
	Term() int
	Base() BaseState

	// Call only term == curTerm
	RequestVote(term, candidate int) (granted bool)
	AppendEntries(term, leader int) (success bool)
	String() string
	Role() string
	Close() (firstClose bool)
}

// Basic raft states
type BaseState struct {
	// Mutable states
	term   int
	follow int

	// Immutable states
	self    int
	peers   []*labrpc.ClientEnd
	migrate MigrateFunc
}

func Base(self int, peers []*labrpc.ClientEnd, migrateFn MigrateFunc) *BaseState {
	return &BaseState{
		term:   0,
		follow: NoVote,

		self:    self,
		peers:   peers,
		migrate: migrateFn,
	}
}

func (s *BaseState) MigrateTo(state State) {
	s.migrate(state)
}

func (s *BaseState) VoteFor() int {
	return s.follow
}

func (s *BaseState) Term() int {
	return s.term
}

func (s *BaseState) Base() BaseState {
	return *s
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
