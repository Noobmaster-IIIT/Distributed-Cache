package loadbalancer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"sync"

	pb "distributed-cache-go/gen/protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type primaryInfo struct {
	serverID int32
	port     int32
	stub     pb.CacheServiceClient
	conn     *grpc.ClientConn
}

// Server implements the load balancer and metadata gRPC services.
type Server struct {
	pb.UnimplementedLoadBalancerCacheServiceServer
	pb.UnimplementedMetadataServiceServer

	mu         sync.RWMutex
	primaries  map[int32]primaryInfo // map[clusterID]primaryInfo
	queryMap   map[string]int32      // map[queryHash]serverID
	queryCount map[int32]int         // map[serverID]count
}

// NewServer creates a new Load Balancer server instance.
func NewServer() *Server {
	return &Server{
		primaries:  make(map[int32]primaryInfo),
		queryMap:   make(map[string]int32),
		queryCount: make(map[int32]int),
	}
}

func hashQuery(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// --- Implementation of LoadBalancerCacheService (for Gateway) ---

// GetCachedData handles requests from the Gateway to retrieve cached data.
func (s *Server) GetCachedData(ctx context.Context, req *pb.LoadBalancerCacheRequest) (*pb.LoadBalancerCacheResponse, error) {
	key := req.Key
	queryHash := hashQuery(key) // Although the gateway already hashed, we might re-hash for consistency.
	log.Printf("LoadBalancer: Looking for cached data with key: %s, hash: %s", key, queryHash)

	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.primaries) == 0 {
		log.Println("LoadBalancer: No primary cache servers available")
		return &pb.LoadBalancerCacheResponse{Found: false}, nil
	}

	serverID, found := s.queryMap[queryHash]
	if !found {
		log.Println("LoadBalancer: Cache miss (query hash not found in map)")
		return &pb.LoadBalancerCacheResponse{Found: false}, nil
	}

	// Find the stub for the serverID. We need to iterate as primaries are keyed by clusterID.
	var targetPrimary primaryInfo
	primaryFound := false
	for _, p := range s.primaries {
		if p.serverID == serverID {
			targetPrimary = p
			primaryFound = true
			break
		}
	}

	if !primaryFound {
		log.Printf("LoadBalancer: Stub for CacheServer %d not found (server may be down)", serverID)
		return &pb.LoadBalancerCacheResponse{Found: false}, nil
	}

	log.Printf("LoadBalancer: Cache hit for hash '%s' on Primary CacheServer %d", queryHash, serverID)
	resp, err := targetPrimary.stub.GetResult(ctx, &pb.CacheRequest{QueryHash: queryHash})
	if err != nil {
		log.Printf("LoadBalancer: Failed to contact Primary CacheServer %d: %v", serverID, err)
		return &pb.LoadBalancerCacheResponse{Found: false}, nil
	}

	if resp.Found {
		s.queryCount[serverID]++
		return &pb.LoadBalancerCacheResponse{Found: true, Value: resp.Result}, nil
	}

	return &pb.LoadBalancerCacheResponse{Found: false}, nil
}

// SetCachedData handles requests from the Gateway to store data in the cache.
func (s *Server) SetCachedData(ctx context.Context, req *pb.LoadBalancerCacheSetRequest) (*pb.LoadBalancerCacheSetResponse, error) {
	key := req.Key
	queryHash := hashQuery(key)
	log.Printf("LoadBalancer: Setting cached data for key: %s, hash: %s, operation: %s", key, queryHash, req.Operation)

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.primaries) == 0 {
		log.Println("LoadBalancer: No primary cache servers available")
		return &pb.LoadBalancerCacheSetResponse{Success: false}, nil
	}

	// Find the least loaded primary server
	var leastLoadedID int32 = -1
	minLoad := -1
	var targetStub pb.CacheServiceClient

	for _, p := range s.primaries {
		// In a real system, we would query the load from the cache server.
		// For this implementation, we use the simple query count as a proxy for load.
		load := s.queryCount[p.serverID]
		if leastLoadedID == -1 || load < minLoad {
			minLoad = load
			leastLoadedID = p.serverID
			targetStub = p.stub
		}
	}

	if leastLoadedID == -1 {
		log.Println("LoadBalancer: Could not determine least loaded server")
		return &pb.LoadBalancerCacheSetResponse{Success: false}, nil
	}

	// Send request to the chosen cache server
	_, err := targetStub.SetResult(ctx, &pb.CacheSetRequest{
		QueryHash: queryHash,
		Result:    req.Value,
		Entity:    req.Entity,
		Operation: req.Operation,
	})
	if err != nil {
		log.Printf("LoadBalancer: Failed to communicate with Primary CacheServer %d: %v", leastLoadedID, err)
		return &pb.LoadBalancerCacheSetResponse{Success: false}, nil
	}

	// Only map read operations for future cache hits
	if req.Operation == "read" {
		s.queryMap[queryHash] = leastLoadedID
		s.queryCount[leastLoadedID]++
	}

	log.Printf("LoadBalancer: Processed '%s' operation on Primary CacheServer %d", req.Operation, leastLoadedID)
	return &pb.LoadBalancerCacheSetResponse{Success: true}, nil
}

// --- Implementation of MetadataService (for Sentinel/Cache) ---

// NotifyPromotion is called by the Sentinel when a replica is promoted to primary.
func (s *Server) NotifyPromotion(ctx context.Context, req *pb.NotifyPromotionRequest) (*pb.NotifyPromotionResponse, error) {
	serverID := req.ServerId
	port := req.Port
	clusterID := req.ClusterId
	log.Printf("LoadBalancer: Received promotion notification for CacheServer %d (cluster %d) on port %d", serverID, clusterID, port)

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Remove the old primary for this cluster, if it exists
	if oldPrimary, ok := s.primaries[clusterID]; ok {
		log.Printf("LoadBalancer: Removing old PRIMARY %d for cluster %d", oldPrimary.serverID, clusterID)
		oldPrimary.conn.Close() // Close the connection to the old primary
		delete(s.queryCount, oldPrimary.serverID)
		// Remap queries from old primary to new one
		remapped := 0
		for qHash, sID := range s.queryMap {
			if sID == oldPrimary.serverID {
				s.queryMap[qHash] = serverID
				remapped++
			}
		}
		log.Printf("LoadBalancer: Remapped %d queries to new primary %d", remapped, serverID)
	}

	// 2. Add the new primary
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("LoadBalancer: Failed to connect to new primary %d: %v", serverID, err)
		return &pb.NotifyPromotionResponse{Success: false}, nil
	}

	stub := pb.NewCacheServiceClient(conn)
	s.primaries[clusterID] = primaryInfo{
		serverID: serverID,
		port:     port,
		stub:     stub,
		conn:     conn,
	}
	s.queryCount[serverID] = 0 // Reset query count for the new primary
	log.Printf("LoadBalancer: Set CacheServer %d as PRIMARY for cluster %d", serverID, clusterID)

	return &pb.NotifyPromotionResponse{Success: true}, nil
}
