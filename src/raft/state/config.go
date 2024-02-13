package state

import (
	"math/rand"
	"time"
)

const (
	HeartBeatInterval     = time.Millisecond * 250
	RequestVoteInterval   = time.Millisecond * 200
	ElectionTimeout       = time.Millisecond * 450
	ElectionTimeoutDelta  = time.Millisecond * 100
	HeartBeatTimeout      = time.Millisecond * 450
	HeartBeatTimeoutDelta = time.Millisecond * 100
)

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
