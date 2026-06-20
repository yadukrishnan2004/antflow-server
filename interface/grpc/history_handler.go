package grpc

import (
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StreamWorkflowHistory polls for new history events and streams them to the
// client as they appear, closing the stream once a terminal workflow event
// is sent.
//
// Uses GetHistoryAfter (HistoryEventRepository.GetByExecutionAfter under the
// hood) instead of GetHistory. The old version called GetHistory — a full
// "every event for this execution" read — once per second for the entire
// lifetime of the stream, re-filtering client-side with lastSentEventID on
// every tick. For a long-running workflow with hundreds of history events,
// that's an O(n) DB read repeated O(seconds-until-completion) times. Asking
// the DB for "only events after this ID" makes each tick O(new events)
// instead, and removes the need for the handler to do its own filtering.
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