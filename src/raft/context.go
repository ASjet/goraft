package raft

import (
	"goraft/src/labrpc"
	"goraft/src/models"
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
