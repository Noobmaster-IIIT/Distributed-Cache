package main

import (
	"fmt"
	"log"
	"net"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/loadbalancer"
	"distributed-cache-go/internal/pkg/consul"
	"distributed-cache-go/internal/pkg/graceful"

	"google.golang.org/grpc"
)

const (
	lbPort        = ":8084"
	consulService = "load-balancer"
)

func main() {
	log.Println("Starting Load Balancer Server...")

	lis, err := net.Listen("tcp", lbPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create a new Load Balancer server
	lbServer := loadbalancer.NewServer()

	// Create gRPC server and register both services
	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServiceServer(grpcServer, lbServer)
	pb.RegisterLoadBalancerCacheServiceServer(grpcServer, lbServer)

	// Create a Consul client.
	consulClient, err := consul.NewClient()
	if err != nil {
		log.Fatalf("Failed to create consul client: %v", err)
	}

	// Create a unique ID for this service instance.
	serviceID := fmt.Sprintf("%s-%s", consulService, lbPort)

	// Register this service with Consul using the unique ID.
	if err := consulClient.Register(serviceID, consulService, lbPort); err != nil {
		log.Fatalf("Failed to register with consul: %v", err)
	}
	log.Printf("Load Balancer registered with Consul as '%s'", serviceID)

	// Handle graceful shutdown
	go func() {
		graceful.Shutdown(func() {
			log.Println("Shutting down load balancer...")
			// Deregister using the unique ID.
			if err := consulClient.Deregister(serviceID); err != nil {
				log.Printf("Failed to deregister from consul: %v", err)
			}
			grpcServer.GracefulStop()
			log.Println("Load Balancer stopped.")
		})
	}()

	log.Printf("Load Balancer listening on %s", lbPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
