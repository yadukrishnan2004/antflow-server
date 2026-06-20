package grpc

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RegisterWorkflow creates (or re-registers) a workflow definition.
//
// This used to special-case errors.Is(err, workflow.ErrWorkflowAlreadyExists)
// and map it to codes.AlreadyExists. Since the Phase 3 rewrite of
// usecase.RegisterWorkflow, that error is never returned: registering an
// unchanged definition is idempotent (returns the existing active
// definition), and registering a changed one deactivates the old definition
// and creates a new version — neither path errors on "already exists"
// anymore. The branch was unreachable dead code, so it's removed rather than
// left in place implying a behavior that no longer exists.
func (h *WorkflowHandler) RegisterWorkflow(ctx context.Context, req *pb.RegisterWorkflowRequest) (*pb.RegisterWorkflowResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow name is required")
	}

	wf, err := h.service.RegisterWorkflow(ctx, req.Name, req.WorkflowType, req.Steps)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register workflow: %v", err)
	}

	return &pb.RegisterWorkflowResponse{
		Id:        wf.ID,
		Name:      wf.Name,
		CreatedAt: timestamppb.New(wf.CreatedAt),
	}, nil
}