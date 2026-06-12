package grpc

import (
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
