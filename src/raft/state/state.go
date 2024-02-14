package state

import (
	"fmt"
	"math/rand"
	"time"

	"goraft/src/labrpc"
	"goraft/src/models"
)

const (
	NoVote int = -1

	RoleFollower  = "F"
	RoleCandidate = "C"
	RoleLeader    = "L"

	HeartBeatInterval     = time.Millisecond * 250
	RequestVoteInterval   = time.Millisecond * 200
	ElectionTimeout       = time.Millisecond * 450
	ElectionTimeoutDelta  = time.Millisecond * 100
	HeartBeatTimeout      = time.Millisecond * 450
	HeartBeatTimeoutDelta = time.Millisecond * 100
)

type Context interface {
	Me() int
	Peers() []*labrpc.ClientEnd

	SetState(state State)
	LockState()
	UnlockState()

	Logs() []models.Log
	CommitIndex() int
	SetCommitIndex(index int)
	SetLogs(logs []models.Log)
	AppendLogs(logs ...models.Log) (lastIndex int)
	RLockLog()
	RUnlockLog()
	LockLog()
	UnlockLog()
	WaitLog()
	BroadcastLog()

	GetSnapshot() []byte
	SnapshotIndex() int
	SetSnapshot(snapshot []byte, index int)
	Persist()
	PersistWithSnapshot(snapshot []byte)

	CommitLog(index int) (advance bool)
	ApplySnapshot(index int, term models.Term, snapshot []byte) (applied bool)
}

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

func genElectionTimeout() func() time.Duration {
	return func() time.Duration {
		return ElectionTimeout - ElectionTimeoutDelta/2 + time.Duration(rand.Int63n(int64(ElectionTimeoutDelta)))
	}
}

func genHeartbeatTimeout() func() time.Duration {
	return func() time.Duration {
		return ElectionTimeout - ElectionTimeoutDelta/2 + time.Duration(rand.Int63n(int64(ElectionTimeoutDelta)))
	}
}
