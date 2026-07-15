package workflow

import "errors"

var (
	ErrNotFound = errors.New("not found")
	ErrWorkflowAlreadyExists = errors.New("workflow already exists")
	ErrInvalidStateTransition = errors.New("invalid state transition")
	ErrStepMismatch = errors.New("step list differs from active workflow definition")
	ErrCompensationFailed = errors.New("saga compensation failed")
	ErrConflict = errors.New("state conflict: execution is already in a terminal state or state transition is invalid")
)