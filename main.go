package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/infrastructure/persistence"
	appgrpc "github.com/yadukrishnan2004/antflow-server/interface/grpc"
	"github.com/yadukrishnan2004/antflow-server/usecase"
	"google.golang.org/grpc"

	"os"
	"os/signal"
	"syscall"
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
		storage.TxManager,
	)
	
	// Recover orphaned executions on startup boot
	if err := workflowService.RecoverWorkflows(context.Background()); err != nil {
		log.Printf("warn: startup workflow recovery encountered error: %v", err)
	}


	// NEW: Run periodic workflow-deadline enforcement.
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := workflowService.ExpireOverdueWorkflows(context.Background()); err != nil {
				log.Printf("error: failed to expire overdue workflows: %v", err)
			}
		}
	}()
	workflowHandler := appgrpc.NewWorkflowHandler(workflowService)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Register the core WorkflowService RPCs.
	pb.RegisterWorkflowServiceServer(grpcServer, workflowHandler)

	// 1. Channel to listen for OS signals (SIGINT, SIGTERM)
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	// 2. Run the gRPC server in a separate background goroutine
	go func() {
		log.Println("AntFlow server listening on :50051")
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("failed to serve gRPC server: %v", err)
		}
	}()
	// 3. Block main thread until we receive a termination signal
	sig := <-stopChan
	log.Printf("info: received signal %v, initiating graceful shutdown...", sig)
	// 4. Stop accepting new RPCs and drain in-flight streams
	grpcServer.GracefulStop()
	log.Println("info: gRPC server stopped gracefully")
	// 5. Clean up database connections
	if err := storage.Close(); err != nil {
		log.Printf("error: failed to close storage pool: %v", err)
	} else {
		log.Println("info: database connection pool closed")
	}
	log.Println("info: shutdown complete. Exiting.")
}
