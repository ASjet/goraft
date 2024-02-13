package log

import (
	"sync"
	"sync/atomic"
	"time"
)

var (
	_ sync.Locker = (*TraceLocker)(nil)
)

type TraceLocker struct {
	lockID int64
	locker sync.Locker

	traceID atomic.Int64
}

func LockerWithTrace(lockID int64, locker sync.Locker) *TraceLocker {
	return &TraceLocker{lockID: lockID, locker: locker}
}

func (tl *TraceLocker) traceLock() int64 {
	if BacktraceLock {
		n, f, l := Call()
		traceID := time.Now().UnixNano()
		Info("[%d]%s:%d: %s Lock(%d)", tl.lockID, f, l, n, traceID)
		return traceID
	}
	return 0
}

func (tl *TraceLocker) traceUnlock() {
	if BacktraceLock {
		n, f, l := Call()
		Info("[%d]%s:%d: %s Unlock(%d)", tl.lockID, f, l, n, tl.traceID.Load())
	}
}

func (tl *TraceLocker) Lock() {
	defer tl.traceID.Store(tl.traceLock())
	tl.locker.Lock()
}

func (tl *TraceLocker) Unlock() {
	tl.traceUnlock()
	tl.locker.Unlock()
}
