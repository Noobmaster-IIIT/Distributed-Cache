package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/cache"
	"distributed-cache-go/internal/pkg/graceful"

	"google.golang.org/grpc"
)

var (
	serverID = flag.Int("id", 0, "Unique ID for this cache server")
	// This flag now defines the STARTING port, not the final one.
	port = flag.Int("port", 0, "Starting port for this cache server (defaults to 9000 + id)")
)

func main() {
	flag.Parse()

	// Use the flag as the starting point for our port search.
	startPort := *port
	if startPort == 0 {
		startPort = 9000 + *serverID
	}

	log.Printf("Starting Cache Server %d (Cluster %d), trying ports from %d...", *serverID, *serverID/4, startPort)

	// --- Port finding loop starts here ---
	var lis net.Listener
	var err error
	var actualPort int
	const maxRetries = 100 // Prevent an infinite loop

	for i := 0; i < maxRetries; i++ {
		currentPort := startPort + i
		addr := fmt.Sprintf(":%d", currentPort)
		lis, err = net.Listen("tcp", addr)

		// If there's no error, we found a free port!
		if err == nil {
			actualPort = currentPort // Save the successful port number
			break                    // Exit the loop
		}

		// If the error is "address already in use", log it and continue to the next port.
		if strings.Contains(err.Error(), "address already in use") {
			log.Printf("Port %d is already in use, trying next port...", currentPort)
			continue
		}

		// If it's any other error, it's fatal.
		log.Fatalf("Failed to listen with an unexpected error: %v", err)
	}

	// If the loop finished without finding a port, exit.
	if lis == nil {
		log.Fatalf("Could not find an open port after %d attempts, starting from %d", maxRetries, startPort)
	}
	// --- Port finding loop ends here ---

	clusterID := *serverID / 4

	// Create a new Cache Server instance using the ACTUAL port we are listening on.
	cacheServer, err := cache.NewServer(int32(*serverID), int32(actualPort), int32(clusterID))
	if err != nil {
		log.Fatalf("Failed to create cache server: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterCacheServiceServer(grpcServer, cacheServer)

	// Handle graceful shutdown
	go func() {
		graceful.Shutdown(func() {
			log.Printf("Shutting down cache server %d...", *serverID)
			cacheServer.Shutdown()
			grpcServer.GracefulStop()
			log.Printf("Cache server %d stopped.", *serverID)
		})
	}()

	// Initial registration and role determination
	go func() {
		<-time.After(500 * time.Millisecond)
		err := cacheServer.RegisterAndDetermineRole()
		if err != nil {
			log.Printf("CacheServer %d: Critical error during registration: %v. Shutting down.", *serverID, err)
			os.Exit(1)
		}
	}()

	// Log the actual port the server is using.
	log.Printf("Cache Server %d listening on :%d", *serverID, actualPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
