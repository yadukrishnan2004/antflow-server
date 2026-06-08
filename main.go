package main

import (
	"log"
	"net"
	"time"

	"github.com/yadukrishnan2004/antflow-server/api/grpc/pb"
	"github.com/yadukrishnan2004/antflow-server/infrastructure/persistence"
	appgrpc "github.com/yadukrishnan2004/antflow-server/interface/grpc"
	"github.com/yadukrishnan2004/antflow-server/usecase"
	"google.golang.org/grpc"
)

func main() {
	// Connect to PostgreSQL database
	dsn := "postgres://postgres:1234@localhost:5432/postgres?sslmode=disable"
	wRepo, tRepo, _, hRepo, err := persistence.New(dsn)
	if err != nil {
		log.Fatalf("failed to initialize persistence layer: %v", err)
	}

	// Initialize the Usecase
	workflowService := usecase.NewWorkflowService(wRepo, tRepo, hRepo)

	// Initialize the gRPC Handler
	workflowHandler := appgrpc.NewWorkflowHandler(workflowService)

	// Start background task to reset timed-out tasks
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			if err := tRepo.ResetTimedOutTasks(); err != nil {
				log.Printf("Failed to reset timed out tasks: %v", err)
			}
		}
	}()

	// Create and Start the gRPC Server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkflowServiceServer(grpcServer, workflowHandler)

	log.Println("Starting gRPC server on port 50051...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve gRPC server: %v", err)
	}
}
