package util

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Timer struct {
	ctx    context.Context
	cancel context.CancelFunc
	dura   func() time.Duration
	fire   func()

	wg      sync.WaitGroup
	resetCh chan struct{}

	started atomic.Bool
	stopped atomic.Bool
	fired   atomic.Bool
}

func NewTimer(ctx context.Context, dura func() time.Duration, onFire func()) *Timer {
	ctx, cancel := context.WithCancel(ctx)
	t := &Timer{
		ctx:     ctx,
		cancel:  cancel,
		dura:    dura,
		fire:    onFire,
		resetCh: make(chan struct{}),
	}
	return t
}

// Start the timer, and it will only fire once
func (t *Timer) Start() *Timer {
	// Make sure the timer is only started once
	if !t.started.CompareAndSwap(false, true) {
		return t
	}
	t.wg.Add(1)
	go t.start()
	return t
}

// Restart the timer
func (t *Timer) Restart() {
	if !t.started.Load() {
		panic("timer not started")
	}
	if t.stopped.Load() {
		return
	}

	t.resetCh <- struct{}{}
}

func (t *Timer) Stop() {
	if !t.stopped.CompareAndSwap(false, true) {
		return
	}
	t.cancel()
	t.wg.Wait()
}

func (t *Timer) Fired() bool {
	return t.fired.Load()
}

func (t *Timer) start() {
	defer t.wg.Done()
	timer := time.NewTimer(t.dura())

_TIMER_LOOP:
	for {
		select {
		case <-t.ctx.Done():
			break _TIMER_LOOP
		case <-timer.C:
			// The timer fired before receiving reset
			if !t.stopped.Load() { // In case timer fired while canceling context
				t.fired.Store(true)
				t.fire()
			}
			break _TIMER_LOOP
		case <-t.resetCh:
			if !timer.Stop() {
				// If timer fired now, it won't be regarded as timeout
				<-timer.C
			}
			timer.Reset(t.dura())
		}
	}

	// The timer is stopped, so we need to do some cleanup
	if !timer.Stop() {
		// In case the timer is already drained
		select {
		case <-timer.C:
		default:
		}
	}

	// Set the flag to indicate the resetCh is going to be closed
	t.stopped.Store(true)
	close(t.resetCh)
}
