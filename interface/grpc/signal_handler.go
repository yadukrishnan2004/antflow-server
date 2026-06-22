package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/usecase"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SendSignal delivers a named payload to a running workflow execution.
// It returns immediately — the payload is delivered to a waiting step or
// buffered for the next WaitForSignal call.
func (h *WorkflowHandler) SendSignal(ctx context.Context, req *pb.SendSignalRequest) (*pb.SendSignalResponse, error) {
	if req.ExecutionId == "" {
		return nil, status.Error(codes.InvalidArgument, "execution_id is required")
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "signal name is required")
	}

	delivered := h.service.SendSignal(req.ExecutionId, req.Name, req.Payload)
	return &pb.SendSignalResponse{Delivered: delivered}, nil
}

// PollSignal is a server-streaming RPC. It sends exactly one SignalEvent and
// closes (success), or closes with a timeout event when TimeoutMs elapses, or
// returns Canceled if the client disconnects.
//
// The implementation uses a Go channel (inside SignalStore.Wait) paired with
// a select block:
//
//	select {
//	case payload := <-signalCh:   // signal arrived → send + close
//	case <-time.After(timeout):   // timeout → send timed_out=true + close
//	case <-ctx.Done():            // client cancelled → return Canceled
//	}
func (h *WorkflowHandler) PollSignal(req *pb.PollSignalRequest, stream pb.WorkflowService_PollSignalServer) error {
	if req.ExecutionId == "" {
		return status.Error(codes.InvalidArgument, "execution_id is required")
	}
	if req.Name == "" {
		return status.Error(codes.InvalidArgument, "signal name is required")
	}

	var timeout time.Duration
	if req.TimeoutMs > 0 {
		timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}

	payload, err := h.service.WaitForSignal(stream.Context(), req.ExecutionId, req.Name, timeout)
	if err != nil {
		if errors.Is(err, usecase.ErrSignalTimeout) {
			// Send the timeout marker so the SDK can distinguish this from a
			// network error, then close the stream cleanly with nil.
			_ = stream.Send(&pb.SignalEvent{
				Name:     req.Name,
				TimedOut: true,
			})
			return nil
		}
		// ctx cancelled or server shutting down.
		return status.Errorf(codes.Canceled, "signal wait cancelled: %v", err)
	}

	if err := stream.Send(&pb.SignalEvent{
		Name:    req.Name,
		Payload: payload,
	}); err != nil {
		return err
	}
	return nil
}