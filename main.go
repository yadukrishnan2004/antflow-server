package main

import (
	"log"
	"net"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/infrastructure/persistence"
	appgrpc "github.com/yadukrishnan2004/antflow-server/interface/grpc"
	"github.com/yadukrishnan2004/antflow-server/usecase"
	"google.golang.org/grpc"
)

func main() {
	dsn := "postgres://postgres:1234@localhost:5432/postgres?sslmode=disable"
	storage, err := persistence.New(dsn)
	if err != nil {
		log.Fatalf("failed to initialize persistence layer: %v", err)
	}

	// Run schema migrations in dependency order.
	for _, m := range []struct {
		name string
		fn   func() error
	}{
		{"Namespace", storage.Namespace.Migrate},
		{"WorkflowDefinition", storage.WorkflowDefinition.Migrate},
		{"WorkflowDefinitionStep", storage.WorkflowDefinitionStep.Migrate},
		{"WorkflowExecution", storage.WorkflowExecution.Migrate},
		{"Task", storage.Task.Migrate},
		{"HistoryEvent", storage.HistoryEvent.Migrate},
		{"Checkpoint", storage.Checkpoint.Migrate},
		{"CompensationTask", storage.CompensationTask.Migrate},
	} {
		if err := m.fn(); err != nil {
			log.Fatalf("%s migration failed: %v", m.name, err)
		}
	}

	workflowService := usecase.New(
		storage.Namespace,
		storage.WorkflowDefinition,
		storage.WorkflowDefinitionStep,
		storage.WorkflowExecution,
		storage.Task,
		storage.CompensationTask,
		storage.HistoryEvent,
		storage.Checkpoint,
	)

	workflowHandler := appgrpc.NewWorkflowHandler(workflowService)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Register the core WorkflowService RPCs.
	pb.RegisterWorkflowServiceServer(grpcServer, workflowHandler)

	log.Println("AntFlow server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve gRPC server: %v", err)
	}
}