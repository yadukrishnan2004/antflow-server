package grpc

import (
	"context"
	"errors"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *WorkflowHandler) StartWorkflow(ctx context.Context, req *pb.StartWorkflowRequest) (*pb.StartWorkflowResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow id is required")
	}

	id,err:=h.service.GetWorkflowIdForExecution(ctx,req.Namespace)
	if err != nil {
		if errors.Is(err,workflow.ErrNotFound){
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to start workflow: %v", err)
	}

	taskQueue := req.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}
	exec, err := h.service.StartWorkflow(ctx, id, taskQueue, req.Input, int(req.TimeoutSeconds))
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to start workflow: %v", err)
	}

	return &pb.StartWorkflowResponse{
		Id:         exec.ID,
		WorkflowId: exec.WorkflowName,
		Name:       exec.WorkflowName,
		State:      string(exec.State),
	}, nil
}

// GetWorkflowResult returns the current state and result of an execution.
//
// Previously this checked `exec == nil` after the error check to detect a
// missing execution and map it to codes.NotFound. That branch was dead code:
// WorkflowExecutionRepository.GetByID returns (nil, workflow.ErrNotFound) on
// a miss, never (nil, nil), so a not-found execution actually fell into the
// `err != nil` branch above it and was reported as codes.Internal — telling
// the caller "something broke on the server" when the real answer was
// "no such workflow." Checking errors.Is(err, workflow.ErrNotFound) fixes
// that; the dead nil-check is removed since it can no longer be reached.
func (h *WorkflowHandler) GetWorkflowResult(ctx context.Context, req *pb.GetWorkflowResultRequest) (*pb.GetWorkflowResultResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow id is required")
	}

	exec, err := h.service.GetWorkflowResult(ctx, req.WorkflowId)
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workflow %q not found", req.WorkflowId)
		}
		return nil, status.Errorf(codes.Internal, "failed to get workflow result: %v", err)
	}

	return &pb.GetWorkflowResultResponse{
		State:  string(exec.State),
		Result: exec.Result,
		Error:  exec.Error,
	}, nil
}

// CancelWorkflow stops a running execution.
//
// Since usecase.CancelWorkflow started validating state transitions
// (workflow.ValidateTransition), cancelling an execution that's already
// COMPLETED, FAILED, or CANCELLED returns a wrapped
// workflow.ErrInvalidStateTransition instead of silently succeeding.
// That's a client error, not a server fault — the caller asked for
// something that's no longer valid given the execution's current state,
// and retrying the exact same call will never succeed. codes.Internal
// would tell the caller the opposite (try again later), so it's mapped to
// codes.FailedPrecondition instead. A missing execution still maps to
// codes.NotFound, matching GetWorkflowResult above.
func (h *WorkflowHandler) CancelWorkflow(ctx context.Context, req *pb.CancelWorkflowRequest) (*pb.CancelWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow id is required")
	}

	err := h.service.CancelWorkflow(ctx, req.WorkflowId)
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workflow %q not found", req.WorkflowId)
		}
		if errors.Is(err, workflow.ErrInvalidStateTransition) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to cancel workflow: %v", err)
	}

	return &pb.CancelWorkflowResponse{Success: true}, nil
}