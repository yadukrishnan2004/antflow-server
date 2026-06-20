package usecase

import (
	"context"

	"github.com/yadukrishnan2004/antflow-server/domain/workflow"
)

func (i *workflowInteractor) GetHistory(ctx context.Context, workflowID string) ([]workflow.HistoryEvent, error) {
	return i.historyRepo.GetByExecution(ctx, workflowID)
}

// GetHistoryAfter returns only events with ID > afterID for the given
// execution, ordered ascending. Thin pass-through to
// HistoryEventRepository.GetByExecutionAfter — kept as a usecase method
// (rather than having gRPC call the repo directly) so the interface boundary
// between interface/grpc and the data layer stays consistent with every
// other read in this service.
func (i *workflowInteractor) GetHistoryAfter(ctx context.Context, workflowID string, afterID int64) ([]workflow.HistoryEvent, error) {
	return i.historyRepo.GetByExecutionAfter(ctx, workflowID, afterID)
}