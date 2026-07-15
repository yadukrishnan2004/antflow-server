package grpc

import (
	"errors"
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)


func (h *WorkflowHandler) StreamWorkflowHistory(req *pb.StreamWorkflowHistoryRequest, stream pb.WorkflowService_StreamWorkflowHistoryServer) error {
	if req.WorkflowId == "" {
		return status.Error(codes.InvalidArgument, "workflow id is required")
	}
	// NEW: Validate that the workflow execution actually exists before polling
	_, err := h.service.GetWorkflowResult(stream.Context(), req.WorkflowId)
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return status.Errorf(codes.NotFound, "workflow execution %q not found", req.WorkflowId)
		}
		return status.Errorf(codes.Internal, "failed to verify workflow existence: %v", err)
	}
	lastSentEventID := int64(-1)
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			events, err := h.service.GetHistoryAfter(stream.Context(), req.WorkflowId, lastSentEventID)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to get history: %v", err)
			}

			terminalReached := false
			for _, event := range events {
				var actName string
				if event.StepName != nil {
					actName = *event.StepName
				}

				if err := stream.Send(&pb.HistoryEvent{
					EventId:      event.ID,
					EventType:    event.EventType,
					ActivityName: actName,
					Result:       event.Payload,
				}); err != nil {
					return err
				}
				lastSentEventID = event.ID

				switch event.EventType {
				case workflow.EventWorkflowCompleted, workflow.EventWorkflowFailed, workflow.EventWorkflowCancelled:
					terminalReached = true
				}
			}

			if terminalReached {
				return nil // Close stream gracefully
			}

			time.Sleep(1 * time.Second)
		}
	}
}