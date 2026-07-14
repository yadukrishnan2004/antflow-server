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
	if len(req.Steps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "workflow must have at least one step")
	}
	// Validate WorkflowType early at request parsing stage
	if req.WorkflowType != "CHAIN" && 
	   req.WorkflowType != "INDEPENDENT" && 
	   req.WorkflowType != "SAGA" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid workflow type %q: must be CHAIN, INDEPENDENT, or SAGA", req.WorkflowType)
	}

		// Convert gRPC []int32 to Go standard []int
	var stepMaxAttempts []int
	for _, val := range req.StepMaxAttempts {
		stepMaxAttempts = append(stepMaxAttempts, int(val))
	}

	// Pass the new slice as the last argument
	wf, err := h.service.RegisterWorkflow(
		ctx, 
		req.Namespace, 
		req.WorkflowType, 
		req.Steps, 
		req.CompensationSteps, 
		int(req.DefaultTimeoutSeconds), 
		stepMaxAttempts,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register workflow: %v", err)
	}
	return &pb.RegisterWorkflowResponse{
		Id:        wf.ID,
		Name:      wf.Name,
		CreatedAt: timestamppb.New(wf.CreatedAt),
	}, nil
}