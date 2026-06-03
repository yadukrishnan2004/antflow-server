package grpc

import (
	"context"
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/usecase"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func (h *WorkflowHandler) RegisterWorkflow(ctx context.Context, req *pb.RegisterWorkflowRequest) (*pb.RegisterWorkflowResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow name is required")
	}

	wf, err := h.service.RegisterWorkflow(req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register workflow: %v", err)
	}

	return &pb.RegisterWorkflowResponse{
		Id:        wf.ID,
		Name:      wf.Name,
		CreatedAt: timestamppb.New(wf.CreatedAt),
	}, nil
}

func (h *WorkflowHandler) StartWorkflow(ctx context.Context, req *pb.StartWorkflowRequest) (*pb.StartWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow id is required")
	}

	// For simplicity, hardcode a default task queue, or you could add it to StartWorkflowRequest
	// We'll use a default "default" queue if not provided in the proto.
	task, err := h.service.StartWorkflow(req.WorkflowId, "default", req.Input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start workflow: %v", err)
	}

	return &pb.StartWorkflowResponse{
		Id:         task.ID,
		WorkflowId: task.WorkflowID,
		Name:       task.Name,
		State:      string(task.State),
	}, nil
}

func (h *WorkflowHandler) StreamTasks(req *pb.StreamTasksRequest, stream pb.WorkflowService_StreamTasksServer) error {
	taskQueue := req.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			task, err := h.service.PollTask(stream.Context(), taskQueue)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to poll task: %v", err)
			}

			if task != nil {
				// We found a task, send it to the worker
				err = stream.Send(&pb.StreamTaskResponse{
					TaskId:     task.ID,
					WorkflowId: task.WorkflowID,
					Name:       task.Name,
					Input:      task.Input,
				})
				if err != nil {
					return err
				}
			} else {
				// No task found, long poll delay
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (h *WorkflowHandler) CompleteTask(ctx context.Context, req *pb.CompleteTaskRequest) (*pb.CompleteTaskResponse, error) {
	if req.TaskId == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}

	err := h.service.CompleteTask(ctx, req.TaskId, req.Result, req.Error)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to complete task: %v", err)
	}

	return &pb.CompleteTaskResponse{Success: true}, nil
}
