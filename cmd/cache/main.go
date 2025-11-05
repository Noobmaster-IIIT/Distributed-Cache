package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/cache"
	"distributed-cache-go/internal/pkg/config" // Make sure this import is here
	"distributed-cache-go/internal/pkg/consul" // Make sure this import is here
	"distributed-cache-go/internal/pkg/graceful"

	"google.golang.org/grpc"
)

var (
	serverID = flag.Int("id", 0, "Unique ID for this cache server")
	port     = flag.Int("port", 0, "Starting port (overrides default calculation)")
)

func main() {
	flag.Parse()

	// Get the base port from environment, default to 9000
	basePortStr := config.GetEnv("CACHE_BASE_PORT", "9000")
	basePort, err := strconv.Atoi(basePortStr)
	if err != nil {
		log.Printf("Warning: Invalid CACHE_BASE_PORT, defaulting to 9000.")
		basePort = 9000
	}

	// Read ACK_POLICY from environment
	ackPolicyStr := config.GetEnv("ACK_POLICY", "0") // Default to 0 (async)
	ackPolicy, err := strconv.Atoi(ackPolicyStr)
	if err != nil || ackPolicy < 0 {
		log.Printf("Warning: Invalid ACK_POLICY, defaulting to 0 (async).")
		ackPolicy = 0
	}

	// Use the --port flag if provided, otherwise calculate the default
	startPort := *port
	if startPort == 0 {
		startPort = basePort + *serverID
	}

	log.Printf("Starting Cache Server %d (Cluster %d), trying ports from %d...", *serverID, *serverID/4, startPort)

	// --- Port finding loop ---
	var lis net.Listener
	var actualPort int
	const maxRetries = 100 // Prevent an infinite loop

	for i := 0; i < maxRetries; i++ {
		currentPort := startPort + i
		addr := fmt.Sprintf(":%d", currentPort)
		lis, err = net.Listen("tcp", addr)

		if err == nil {
			actualPort = currentPort // Save the successful port number
			break                    // Exit the loop
		}

		if strings.Contains(err.Error(), "address already in use") {
			log.Printf("Port %d is already in use, trying next port...", currentPort)
			continue
		}

		// If it's any other error, it's fatal.
		log.Fatalf("Failed to listen with an unexpected error: %v", err)
	}

	if lis == nil {
		log.Fatalf("Could not find an open port after %d attempts, starting from %d", maxRetries, startPort)
	}
	// --- Port finding loop ends ---

	clusterID := *serverID / 4

	// --- THIS IS THE FIX ---
	// Create the Consul client (needed for NewServer)
	consulClient, err := consul.NewClient()
	if err != nil {
		log.Fatalf("Failed to create consul client: %v", err)
	}

	// Create a new Cache Server instance, passing all 5 arguments
	cacheServer, err := cache.NewServer(int32(*serverID), int32(actualPort), int32(clusterID), consulClient, ackPolicy)
	if err != nil {
		log.Fatalf("Failed to create cache server: %v", err)
	}
	// --- END FIX ---

	grpcServer := grpc.NewServer()
	pb.RegisterCacheServiceServer(grpcServer, cacheServer)

	// Create a unique ID for this service instance for Consul
	serviceID := fmt.Sprintf("%s-%d", "cache-server", *serverID)
	actualPortStr := fmt.Sprintf(":%d", actualPort)

	// Register with Consul using the unique ID
	if err := consulClient.Register(serviceID, "cache-server", actualPortStr); err != nil {
		log.Fatalf("Failed to register with consul: %v", err)
	}
	log.Printf("Cache server %d registered with Consul as '%s'", *serverID, serviceID)

	// Handle graceful shutdown
	go func() {
		graceful.Shutdown(func() {
			log.Printf("Shutting down cache server %d...", *serverID)
			// Deregister from Consul
			if err := consulClient.Deregister(serviceID); err != nil {
				log.Printf("Failed to deregister from consul: %v", err)
			}
			cacheServer.Shutdown() // Persist data
			grpcServer.GracefulStop()
			log.Printf("Cache server %d stopped.", *serverID)
		})
	}()

	// Start the registration and role-finding logic
	go func() {
		<-time.After(500 * time.Millisecond) // Give gRPC server a moment to start
		err := cacheServer.RegisterAndDetermineRole()
		if err != nil {
			log.Printf("CacheServer %d: Critical error during registration: %v. Shutting down.", *serverID, err)
			os.Exit(1)
		}
	}()

	log.Printf("Cache Server %d listening on :%d with ACK_POLICY=%d", *serverID, actualPort, ackPolicy)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %V", err)
	}
}
