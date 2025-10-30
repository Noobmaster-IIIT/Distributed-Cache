package main

import (
	"fmt"
	"log"
	"net"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/pkg/consul"
	"distributed-cache-go/internal/pkg/graceful"
	"distributed-cache-go/internal/sentinel"

	"google.golang.org/grpc"
)

const (
	sentinelPort  = ":8081"
	consulService = "sentinel"
)

func main() {
	log.Println("Starting Sentinel Server...")

	lis, err := net.Listen("tcp", sentinelPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	consulClient, err := consul.NewClient()
	if err != nil {
		log.Fatalf("Failed to create consul client: %v", err)
	}

	sentinelServer := sentinel.NewServer(consulClient)
	monitor := sentinel.NewMonitor(sentinelServer)

	go monitor.Run()

	grpcServer := grpc.NewServer()
	pb.RegisterSentinelServiceServer(grpcServer, sentinelServer)

	// Create a unique ID for this service instance.
	serviceID := fmt.Sprintf("%s-%s", consulService, sentinelPort)

	// Register this service with Consul using the unique ID.
	if err := consulClient.Register(serviceID, consulService, sentinelPort); err != nil {
		log.Fatalf("Failed to register with consul: %v", err)
	}
	log.Printf("Sentinel registered with Consul as '%s'", serviceID)

	go func() {
		graceful.Shutdown(func() {
			log.Println("Shutting down sentinel...")
			monitor.Stop()
			// Deregister using the unique ID.
			if err := consulClient.Deregister(serviceID); err != nil {
				log.Printf("Failed to deregister from consul: %v", err)
			}
			grpcServer.GracefulStop()
			log.Println("Sentinel stopped.")
		})
	}()

	log.Printf("Sentinel listening on %s", sentinelPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
