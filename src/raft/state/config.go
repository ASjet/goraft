package state

import (
	"math/rand"
	"time"
)

const (
	HeartBeatInterval     = time.Millisecond * 150
	ElectionTimeout       = time.Millisecond * 500
	ElectionTimeoutDelta  = time.Millisecond * 100
	HeartBeatTimeout      = time.Millisecond * 300
	HeartBeatTimeoutDelta = time.Millisecond * 100
)

func electionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Int63n(int64(ElectionTimeoutDelta)))
}

func heartbeatTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Int63n(int64(ElectionTimeoutDelta)))
}
