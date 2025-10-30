package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/pkg/consul"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 3 {
		printUsage()
		os.Exit(1)
	}

	entity := os.Args[1]
	operation := os.Args[2]
	var data map[string]interface{}

	if len(os.Args) >= 4 {
		if err := json.Unmarshal([]byte(os.Args[3]), &data); err != nil {
			log.Fatalf("Error: Unable to parse data as JSON: %v", err)
		}
	}

	// Discover gateway via Consul
	consulClient, err := consul.NewClient()
	if err != nil {
		log.Fatalf("Failed to create consul client: %v", err)
	}

	gatewayAddr, err := consulClient.Discover("gateway-server")
	if err != nil {
		log.Fatalf("Failed to discover gateway server: %v", err)
	}
	log.Printf("Discovered gateway server at %s", gatewayAddr)

	// Connect to gateway
	conn, err := grpc.Dial(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gateway: %v", err)
	}
	defer conn.Close()

	client := pb.NewGatewayServiceClient(conn)

	// Build query
	query := map[string]interface{}{
		"entity":    entity,
		"operation": operation,
		"data":      data,
	}
	jsonQuery, _ := json.Marshal(query)

	log.Printf("Sending query: %s", string(jsonQuery))

	// Send request
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ProcessQuery(ctx, &pb.GatewayRequest{JsonQuery: string(jsonQuery)})
	if err != nil {
		log.Fatalf("gRPC request failed: %v", err)
	}

	// Pretty-print response
	var prettyJSON map[string]interface{}
	if err := json.Unmarshal([]byte(resp.Response), &prettyJSON); err == nil {
		out, _ := json.MarshalIndent(prettyJSON, "", "  ")
		fmt.Println("\nResponse from gateway server:")
		fmt.Println(string(out))
	} else {
		fmt.Println("\nResponse from gateway server:", resp.Response)
	}
}

func printUsage() {
	fmt.Println("Database Client Usage:")
	fmt.Println("\ngo run ./cmd/client <entity> <operation> [data]")
	fmt.Println("\nExample:")
	fmt.Println("  go run ./cmd/client student create '{\"first_name\":\"Jane\",\"last_name\":\"Doe\",\"program\":\"Physics\"}'")
	fmt.Println("  go run ./cmd/client student read '{\"id\":1}'")
}
