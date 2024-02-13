package raft

import (
	"fmt"

	"goraft/src/models"
)

const (
	NoVote int = -1

	RoleFollower  = "F"
	RoleCandidate = "C"
	RoleLeader    = "L"
)

type State interface {
	// Immutable states
	Term() models.Term
	Role() string
	String() string

	// Mutable states
	Voted() int

	// Immutable context
	Context() Context
	Me() int
	Peers() int

	// Mutable context
	SnapshotIndex() int
	LastLogIndex() int
	Committed() int
	GetLog(index int) (int, *models.Log)
	LogIndexWithOffset(index int) int

	// Operations
	To(state State) (newState State)
	Close(msg string, args ...interface{}) (success bool)
	AppendCommand(command interface{}) (index int, term models.Term)

	// Called only term == curTerm
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
