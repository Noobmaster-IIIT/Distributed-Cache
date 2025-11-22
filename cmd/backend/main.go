package main

import (
	"fmt"
	"log"
	"net"
	"os"

	pb "distributed-cache-go/gen/protos"

	"distributed-cache-go/internal/backend"
	"distributed-cache-go/internal/pkg/config"
	"distributed-cache-go/internal/pkg/consul"
	"distributed-cache-go/internal/pkg/graceful"

	"google.golang.org/grpc"
)

var backendPort = config.GetEnv("BACKEND_PORT", ":8080")

const (
	consulService = "backend-server"
)

func main() {
	log.Println("Starting Backend Server...")

	lis, err := net.Listen("tcp", backendPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create a Consul client.
	consulClient, err := consul.NewClient()
	if err != nil {
		log.Fatalf("Failed to create consul client: %v", err)
	}

	// Create a new Backend server instance.
	backendServer, err := backend.NewServer()
	if err != nil {
		log.Fatalf("Failed to initialize backend server: %v", err)
	}
	defer backendServer.Close() // Ensure connection pool is closed on exit

	// Create gRPC server and register both services.
	grpcServer := grpc.NewServer()
	pb.RegisterBackendServiceServer(grpcServer, backendServer)
	pb.RegisterHeartbeatServiceServer(grpcServer, backendServer)

	// Create a unique ID for this service instance.
	serviceID := fmt.Sprintf("%s-%s", consulService, backendPort)

	// Register this service with Consul using the unique ID.
	if err := consulClient.Register(serviceID, consulService, backendPort); err != nil {
		log.Fatalf("Failed to register with consul: %v", err)
	}
	log.Printf("Backend server registered with Consul as '%s'", serviceID)

	// Set up graceful shutdown.
	go func() {
		graceful.Shutdown(func() {
			log.Println("Shutting down backend server...")
			// Deregister using the unique ID.
			if err := consulClient.Deregister(serviceID); err != nil {
				log.Printf("Failed to deregister from consul: %v", err)
			}
			grpcServer.GracefulStop()
			log.Println("Backend server stopped.")
			os.Exit(0)
		})
	}()

	log.Printf("Backend server listening on %s", backendPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
