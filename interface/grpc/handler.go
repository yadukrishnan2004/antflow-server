package grpc

import (
	"context"
	"strings"
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

// ====================================================================================================================================================
func (h *WorkflowHandler) RegisterWorkflow(ctx context.Context, req *pb.RegisterWorkflowRequest) (*pb.RegisterWorkflowResponse, error) {
    if req.Name == "" {
        return nil, status.Error(codes.InvalidArgument, "workflow name is required")
    }


    wf, err := h.service.RegisterWorkflow(req.Name, req.WorkflowType, req.Steps)
    if err != nil {
        // Step mismatch is a caller error, not an internal server error
        if strings.Contains(err.Error(), "already registered") ||
            strings.Contains(err.Error(), "mismatch") {
            return nil, status.Errorf(codes.AlreadyExists, "%v", err)
        }
        return nil, status.Errorf(codes.Internal, "failed to register workflow: %v", err)
    }

    return &pb.RegisterWorkflowResponse{
        Id:        wf.Name,
        Name:      wf.Name,
        CreatedAt: timestamppb.New(wf.CreatedAt),
    }, nil
}
// =====================================================================================================================================================


// =====================================================================================================================================================
func (h *WorkflowHandler) StartWorkflow(ctx context.Context, req *pb.StartWorkflowRequest) (*pb.StartWorkflowResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow id is required")
	}

	taskQueue := req.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}
	task, err := h.service.StartWorkflow(req.WorkflowId, taskQueue, req.Input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to start workflow: %v", err)
	}

	return &pb.StartWorkflowResponse{
		Id:         task.ID,
		WorkflowId: task.WorkflowExecutionID,
		Name:       task.StepName,
		State:      string(task.State),
	}, nil
}

// =====================================================================================================================================================
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
					WorkflowId: task.WorkflowExecutionID,
					Name:       task.StepName,
					StepName:   task.StepName,
					StepIndex:  int32(task.StepIndex),
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

// =====================================================================================================================================================
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

// =====================================================================================================================================================
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

// =====================================================================================================================================================
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

// =====================================================================================================================================================
func (h *WorkflowHandler) StreamWorkflowHistory(req *pb.StreamWorkflowHistoryRequest, stream pb.WorkflowService_StreamWorkflowHistoryServer) error {
	if req.WorkflowId == "" {
		return status.Error(codes.InvalidArgument, "workflow id is required")
	}

	lastSentEventID := int64(-1)

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			events, err := h.service.GetHistory(stream.Context(), req.WorkflowId)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to get history: %v", err)
			}

			terminalReached := false
			for _, event := range events {
				var actName string
				if event.StepName != nil {
					actName = *event.StepName
				}
				if event.EventID > lastSentEventID {
					err = stream.Send(&pb.HistoryEvent{
						EventId:      event.EventID,
						EventType:    event.EventType,
						ActivityName: actName,
						Result:       event.Payload,
					})
					if err != nil {
						return err
					}
					lastSentEventID = event.EventID

					if event.EventType == "WorkflowExecutionCompleted" || event.EventType == "WorkflowExecutionFailed" || event.EventType == "WorkflowExecutionCancelled" {
						terminalReached = true
					}
				}
			}

			if terminalReached {
				return nil // Close stream gracefully
			}

			time.Sleep(1 * time.Second)
		}
	}
}
//=============================================================================================================================================================================