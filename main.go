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
	// Connect to PostgreSQL database
	dsn := "postgres://postgres:1234@localhost:5432/postgres?sslmode=disable"
	storage, err := persistence.New(dsn)
	if err != nil {
		log.Fatalf("failed to initialize persistence layer: %v", err)
	}

	// Migrate database schemas sequentially
	if err := storage.Namespace.Migrate(); err != nil {
		log.Fatalf("Namespace migration failed: %v", err)
	}
	if err := storage.WorkflowDefinition.Migrate(); err != nil {
		log.Fatalf("WorkflowDefinition migration failed: %v", err)
	}
	if err := storage.WorkflowDefinitionStep.Migrate(); err != nil {
		log.Fatalf("WorkflowDefinitionStep migration failed: %v", err)
	}
	if err := storage.WorkflowExecution.Migrate(); err != nil {
		log.Fatalf("WorkflowExecution migration failed: %v", err)
	}
	if err := storage.Task.Migrate(); err != nil {
		log.Fatalf("Task migration failed: %v", err)
	}
	if err := storage.HistoryEvent.Migrate(); err != nil {
		log.Fatalf("HistoryEvent migration failed: %v", err)
	}
	if err := storage.Checkpoint.Migrate(); err != nil {
		log.Fatalf("Checkpoint migration failed: %v", err)
	}

	// Initialize the Usecase Service
	workflowService := usecase.New(
		storage.Namespace,
		storage.WorkflowDefinition,
		storage.WorkflowDefinitionStep,
		storage.WorkflowExecution,
		storage.Task,
		storage.HistoryEvent,
		storage.Checkpoint,
	)

	// Initialize the gRPC Handler
	workflowHandler := appgrpc.NewWorkflowHandler(workflowService)

	// Create and Start the gRPC Server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkflowServiceServer(grpcServer, workflowHandler)

	log.Println("AntFlow server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve gRPC server: %v", err)
	}
}
