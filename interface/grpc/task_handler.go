package grpc

import (
	"context"

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
