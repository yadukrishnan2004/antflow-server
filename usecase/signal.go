package usecase

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// signalKey uniquely identifies a buffered signal slot.
type signalKey struct {
	executionID string
	name        string
}

// signalWaiter is created when PollSignal arrives before SendSignal.
// The channel is buffered(1) so SendSignal never blocks even when the
// waiter's goroutine is momentarily off-CPU.
type signalWaiter struct {
	ch chan []byte
}

// SignalStore is the in-process broker between SendSignal (which arrives
// from an external caller over gRPC) and PollSignal (which is called by
// a step function inside a worker that wants to pause until a signal
// arrives or a timeout fires).
//
// Two delivery orderings are handled:
//
//  1. Waiter arrives first (step is already blocked):
//     SendSignal finds the waiting channel and pushes the payload.
//     PollSignal unblocks immediately.
//
//  2. Signal arrives first (SendSignal before any waiter):
//     The payload is stored in a pending buffer keyed by (executionID, name).
//     When PollSignal arrives it drains the buffer instantly.
//
// Only one payload per (executionID, name) is buffered at a time. A second
// SendSignal before the first is consumed overwrites the buffer — this keeps
// the implementation simple and avoids unbounded memory growth for signals
// that are never consumed (e.g. due to a cancelled workflow).
type SignalStore struct {
	mu      sync.Mutex
	waiters map[signalKey]*signalWaiter
	pending map[signalKey][]byte // buffered payloads for early-arriving signals
}

// NewSignalStore creates an empty SignalStore.
func NewSignalStore() *SignalStore {
	return &SignalStore{
		waiters: make(map[signalKey]*signalWaiter),
		pending: make(map[signalKey][]byte),
	}
}

// Send delivers payload to the step currently waiting for (executionID, name).
// If no step is waiting yet the payload is buffered. Returns true when a
// live waiter was notified, false when buffered.
func (s *SignalStore) Send(executionID, name string, payload []byte) bool {
	key := signalKey{executionID, name}

	s.mu.Lock()
	defer s.mu.Unlock()

	if w, ok := s.waiters[key]; ok {
		// Non-blocking send: the channel is buffered(1) so this never blocks.
		// If the channel is somehow already full (shouldn't happen — one waiter
		// per key) we fall through and overwrite the pending buffer instead.
		select {
		case w.ch <- payload:
			delete(s.waiters, key)
			return true
		default:
		}
	}

	// Buffer for later consumption.
	s.pending[key] = payload
	return false
}

// Wait blocks until a signal named (executionID, name) arrives or ctx/timeout
// fires. The caller supplies a timeout duration; zero means wait indefinitely
// (bounded only by ctx cancellation).
//
// On success the signal payload is returned.
// On timeout context.DeadlineExceeded is returned.
// On ctx cancellation ctx.Err() is returned.
func (s *SignalStore) Wait(ctx context.Context, executionID, name string, timeout time.Duration) ([]byte, error) {
	key := signalKey{executionID, name}

	s.mu.Lock()

	// Fast path: a signal was already sent before we started waiting.
	if payload, ok := s.pending[key]; ok {
		delete(s.pending, key)
		s.mu.Unlock()
		return payload, nil
	}

	// Slow path: register a waiter channel and release the lock so Send can
	// find us.
	w := &signalWaiter{ch: make(chan []byte, 1)}
	s.waiters[key] = w
	s.mu.Unlock()

	// Build the timeout case. A zero duration means no timeout — we use a
	// channel that never fires so the select behaves as a two-way select.
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(timeout)
	}

	select {
	case payload, ok := <-w.ch:
		if !ok {
			return nil, context.Canceled
		}
		return payload, nil

	case <-timeoutCh:
		// Clean up the waiter so it doesn't leak.
		s.mu.Lock()
		delete(s.waiters, key)
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: signal %q for execution %q", ErrSignalTimeout, name, executionID)

	case <-ctx.Done():
		// Client cancelled or server is shutting down.
		s.mu.Lock()
		delete(s.waiters, key)
		s.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Drain removes and discards all buffered signals and waiters for the given
// execution. Call this when a workflow reaches a terminal state so GC can
// reclaim the memory.
func (s *SignalStore) Drain(executionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key := range s.pending {
		if key.executionID == executionID {
			delete(s.pending, key)
		}
	}
	for key, w := range s.waiters {
		if key.executionID == executionID {
			// Close the channel so any blocked Wait call returns with an error
			// rather than leaking forever.
			close(w.ch)
			delete(s.waiters, key)
		}
	}
}