package main

import (
	"fmt"
	"log"
	"net"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/pkg/config"
	"distributed-cache-go/internal/pkg/consul"
	"distributed-cache-go/internal/pkg/graceful"
	"distributed-cache-go/internal/sentinel"

	"google.golang.org/grpc"
)

var sentinelPort = config.GetEnv("SENTINEL_PORT", ":8081")

const (
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

	// Create the Sentinel server (no longer needs a monitor)
	sentinelServer := sentinel.NewServer(consulClient)

	grpcServer := grpc.NewServer()
	pb.RegisterSentinelServiceServer(grpcServer, sentinelServer)

	serviceID := fmt.Sprintf("%s-%s", consulService, sentinelPort)
	if err := consulClient.Register(serviceID, consulService, sentinelPort); err != nil {
		log.Fatalf("Failed to register with consul: %v", err)
	}
	log.Printf("Sentinel registered with Consul as '%s'", serviceID)

	go func() {
		graceful.Shutdown(func() {
			log.Println("Shutting down sentinel...")
			// No monitor to stop
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
