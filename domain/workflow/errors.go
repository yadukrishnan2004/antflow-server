package workflow

import "errors"

var (
	ErrNotFound               = errors.New("not found")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrConflict               = errors.New("state conflict: execution is already in a terminal state or state transition is invalid")
)