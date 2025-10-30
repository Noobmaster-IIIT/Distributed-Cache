package main

import (
	"fmt"
	"log"
	"net"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/gateway"
	"distributed-cache-go/internal/pkg/consul"
	"distributed-cache-go/internal/pkg/graceful"

	"google.golang.org/grpc"
)

const (
	gatewayPort   = ":8082"
	consulService = "gateway-server"
)

func main() {
	log.Println("Starting Gateway Server...")

	// Create a Consul client which is needed by the gateway server for discovery.
	consulClient, err := consul.NewClient()
	if err != nil {
		log.Fatalf("Failed to create consul client: %v", err)
	}

	// Create a new Gateway server instance, passing the Consul client.
	gatewayServer, err := gateway.NewServer(consulClient)
	if err != nil {
		log.Fatalf("Failed to initialize gateway server: %v", err)
	}

	// Create the gRPC server.
	grpcServer := grpc.NewServer()
	pb.RegisterGatewayServiceServer(grpcServer, gatewayServer)

	// Start listening on the specified port.
	lis, err := net.Listen("tcp", gatewayPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Register this service with Consul.
	// The service ID needs to be unique for deregistration.
	serviceID := fmt.Sprintf("%s-%s", consulService, gatewayPort)
	if err := consulClient.Register(serviceID, consulService, gatewayPort); err != nil {
		log.Fatalf("Failed to register with consul: %v", err)
	}
	log.Printf("Gateway server registered with Consul as '%s'", serviceID)

	// Set up graceful shutdown.
	go func() {
		graceful.Shutdown(func() {
			log.Println("Shutting down gateway server...")
			if err := consulClient.Deregister(serviceID); err != nil {
				log.Printf("Failed to deregister from consul: %v", err)
			}
			grpcServer.GracefulStop()
			log.Println("Gateway server stopped.")
		})
	}()

	log.Printf("Gateway server listening on %s", gatewayPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
