package grpc

import (
	"context"
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *WorkflowHandler) StreamTasks(req *pb.StreamTasksRequest, stream pb.WorkflowService_StreamTasksServer) error {
	taskQueue := req.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}

	ch, unsubscribe := h.service.SubscribeToQueue(taskQueue)
	defer unsubscribe()

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
				workflowName, err := h.service.GetWorkflowNameForExecution(stream.Context(), task.WorkflowExecutionID)
				if err != nil {
					return status.Errorf(codes.Internal, "failed to resolve workflow name: %v", err)
				}
				// We found a task, send it to the worker
				err = stream.Send(&pb.StreamTaskResponse{
					TaskId:     task.ID,
					WorkflowId: task.WorkflowExecutionID,
					Name:       workflowName,
					StepName:   task.StepName,
					StepIndex:  int32(task.StepIndex),
					Input:      task.Input,
				})
				if err != nil {
					return err
				}
			} else {
				select {
				case <-stream.Context().Done():
					return stream.Context().Err()
				case <-ch:
					// woken up by new task notification
				case <-time.After(1 * time.Second):
					// periodically poll to check for scheduled tasks/retries
				}
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

func (h *WorkflowHandler) StreamCompensationTasks(req *pb.StreamTasksRequest, stream pb.WorkflowService_StreamCompensationTasksServer) error {
	taskQueue := req.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}

	ch, unsubscribe := h.service.SubscribeToQueue(taskQueue)
	defer unsubscribe()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			task, err := h.service.PollCompensationTask(stream.Context(), taskQueue)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to poll compensation task: %v", err)
			}

			if task != nil {
				workflowName, err := h.service.GetWorkflowNameForExecution(stream.Context(), task.WorkflowExecutionID)
				if err != nil {
					return status.Errorf(codes.Internal, "failed to resolve workflow name: %v", err)
				}
				err = stream.Send(&pb.CompensationTaskResponse{
					TaskId:               task.ID,
					WorkflowId:           task.WorkflowExecutionID,
					Name:                 workflowName,
					StepName:             task.StepName,
					CompensationStepName: task.CompensationStepName,
					StepIndex:            int32(task.StepIndex),
					Input:                task.Input,
				})
				if err != nil {
					return err
				}
			} else {
				select {
				case <-stream.Context().Done():
					return stream.Context().Err()
				case <-ch:
				case <-time.After(1 * time.Second):
				}
			}
		}
	}
}

func (h *WorkflowHandler) CompleteCompensationTask(ctx context.Context, req *pb.CompleteCompensationTaskRequest) (*pb.CompleteCompensationTaskResponse, error) {
	if req.TaskId == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}

	err := h.service.CompleteCompensationTask(ctx, req.TaskId, req.Result, req.Error)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to complete compensation task: %v", err)
	}

	return &pb.CompleteCompensationTaskResponse{Success: true}, nil
}



func (h *WorkflowHandler) HeartbeatTask(ctx context.Context, req *pb.HeartbeatTaskRequest) (*pb.HeartbeatTaskResponse, error) {
	if req.TaskId == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}

	err := h.service.HeartbeatTask(ctx, req.TaskId)
	if err != nil {
		// ErrNotFound means the task was cancelled or already expired.
		// Tell the worker to stop processing immediately.
		return &pb.HeartbeatTaskResponse{Accepted: false}, nil
	}

	return &pb.HeartbeatTaskResponse{Accepted: true}, nil
}

func (h *WorkflowHandler) HeartbeatCompensationTask(ctx context.Context, req *pb.HeartbeatTaskRequest) (*pb.HeartbeatTaskResponse, error) {
	if req.TaskId == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}

	err := h.service.HeartbeatCompensationTask(ctx, req.TaskId)
	if err != nil {
		return &pb.HeartbeatTaskResponse{Accepted: false}, nil
	}

	return &pb.HeartbeatTaskResponse{Accepted: true}, nil
}