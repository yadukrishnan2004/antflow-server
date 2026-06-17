package grpc

import (
	"context"
	"errors"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (h *WorkflowHandler) RegisterNameSpace(ctx context.Context, req *pb.RegisterNameSpaceRequest) (*pb.RegisterNameSpaceResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace name is required")
	}

	id, err := h.service.RegisterNameSpace(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register namespace: %v", err)
	}

	return &pb.RegisterNameSpaceResponse{
		Id: id,
	}, nil
}

func (h *WorkflowHandler) RegisterWorkflow(ctx context.Context, req *pb.RegisterWorkflowRequest) (*pb.RegisterWorkflowResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow name is required")
	}

	wf, err := h.service.RegisterWorkflow(ctx, req.Name, req.WorkflowType, req.Steps)
	if err != nil {
		if errors.Is(err, workflow.ErrWorkflowAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to register workflow: %v", err)
	}

	return &pb.RegisterWorkflowResponse{
		Id:        wf.ID,
		Name:      wf.Name,
		CreatedAt: timestamppb.New(wf.CreatedAt),
	}, nil
}
