package grpc

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *WorkflowHandler) StartWorkflow(ctx context.Context, req *pb.StartWorkflowRequest) (*pb.StartWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow id is required")
	}

	taskQueue := req.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}
	exec, err := h.service.StartWorkflow(ctx, req.WorkflowId, taskQueue, req.Input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start workflow: %v", err)
	}

	return &pb.StartWorkflowResponse{
		Id:         exec.ID,
		WorkflowId: exec.WorkflowName,
		Name:       exec.WorkflowName,
		State:      string(exec.State),
	}, nil
}

func (h *WorkflowHandler) GetWorkflowResult(ctx context.Context, req *pb.GetWorkflowResultRequest) (*pb.GetWorkflowResultResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow id is required")
	}

	exec, err := h.service.GetWorkflowResult(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get workflow result: %v", err)
	}
	if exec == nil {
		return nil, status.Error(codes.NotFound, "workflow task not found")
	}

	return &pb.GetWorkflowResultResponse{
		State:  string(exec.State),
		Result: exec.Result,
		Error:  exec.Error,
	}, nil
}

func (h *WorkflowHandler) CancelWorkflow(ctx context.Context, req *pb.CancelWorkflowRequest) (*pb.CancelWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow id is required")
	}

	err := h.service.CancelWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to cancel workflow: %v", err)
	}

	return &pb.CancelWorkflowResponse{Success: true}, nil
}
