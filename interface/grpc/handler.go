package grpc

import (
	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/usecase"
)

type WorkflowHandler struct {
	pb.UnimplementedWorkflowServiceServer
	service usecase.WorkflowService
}

func NewWorkflowHandler(service usecase.WorkflowService) *WorkflowHandler {
	return &WorkflowHandler{
		service: service,
	}
}
