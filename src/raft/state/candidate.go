package state

import "context"

type CandidateState struct {
	BaseState
	ctx    context.Context
	cancel context.CancelFunc
	voteCh chan int
}

func Candidate(term, self int, migrateFn MigrateFunc) *CandidateState {
	ctx, cancel := context.WithCancel(context.TODO())
	cs := &CandidateState{
		BaseState: BaseState{
			name:    "F",
			term:    term,
			self:    self,
			voteFor: self,
			migrate: migrateFn,
		},
		ctx:    ctx,
		cancel: cancel,
		voteCh: make(chan int, 128),
	}
	go cs.startElection()
	return cs
}

func (s *CandidateState) RequestVote(term, candidate int) (granted bool) {
	if s.term < term {
		s.term = term
		s.voteFor = candidate
		return true
	}
	return false
}

func (s *CandidateState) AppendEntries(term, leader int) (success bool) {
	if s.term < term {
		s.term = term
		s.voteFor = leader
		return true
	}
	return false
}

func (s *CandidateState) Close() {
	s.cancel()
}

func (s *CandidateState) startElection() {
	// TODO: implement election
	panic("implement me")
}
