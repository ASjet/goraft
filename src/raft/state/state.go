package state

const NoVote int = -1

type MigrateFunc func(state State)

type State interface {
	MigrateTo(state State)
	VoteFor() int
	Term() int
	String() string
	Close()
}

type BaseState struct {
	name    string
	term    int
	self    int
	voteFor int
	migrate MigrateFunc
}

func (s *BaseState) MigrateTo(state State) {
	s.migrate(state)
}

func (s *BaseState) Term() int {
	return s.term
}

func (s *BaseState) VoteFor() int {
	return s.voteFor
}

func (s *BaseState) String() string {
	return s.name
}
