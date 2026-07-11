package grpc

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)


func (h *WorkflowHandler) RegisterWorkflow(ctx context.Context, req *pb.RegisterWorkflowRequest) (*pb.RegisterWorkflowResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow name is required")
	}

	wf, err := h.service.RegisterWorkflow(ctx, req.Namespace, req.WorkflowType, req.Steps, req.CompensationSteps, int(req.DefaultTimeoutSeconds))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register workflow: %v", err)
	}

	return &pb.RegisterWorkflowResponse{
		Id:        wf.ID,
		Name:      wf.Name,
		CreatedAt: timestamppb.New(wf.CreatedAt),
	}, nil
}