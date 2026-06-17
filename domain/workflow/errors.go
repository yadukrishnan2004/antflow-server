package workflow

import "errors"

var (
	ErrNotFound               = errors.New("not found")
	ErrWorkflowAlreadyExists  = errors.New("workflow already exists")
	ErrFailedToCreate         = errors.New("failed to create")
	ErrInvalidStateTransition = errors.New("invalid state transition")
)
