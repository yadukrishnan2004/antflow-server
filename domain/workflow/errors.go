package workflow

import "errors"

var (
	ErrNotFound             = errors.New("not found")
	ErrWorkflowAlreadyExists = errors.New("workflow already exists")
)
