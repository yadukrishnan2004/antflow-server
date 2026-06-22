package usecase

import "errors"

// ErrSignalTimeout is returned by SignalStore.Wait when the caller-supplied
// timeout elapses before any signal arrives.
var ErrSignalTimeout = errors.New("signal timeout")