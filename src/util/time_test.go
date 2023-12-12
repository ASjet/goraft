package util

import (
	"context"
	"testing"
	"time"
)

func TestTimer(t *testing.T) {
	dura := func() time.Duration {
		return time.Millisecond * 20
	}

	fireCh := make(chan struct{}, 1)
	onFire := func() {
		fireCh <- struct{}{}
	}

	timer := NewTimer(context.TODO(), dura, onFire)
	t.Cleanup(func() {
		timer.Stop()
		close(fireCh)
	})

	timer.Start()

	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 10)
		if timer.Fired() {
			t.FailNow()
		}
		timer.Restart()
	}

	if timer.Fired() {
		t.FailNow()
	}

	time.Sleep(time.Millisecond * 20)
	if !timer.Fired() {
		t.FailNow()
	}
}
