package cache

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "distributed-cache-go/gen/protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server implements the cache gRPC service.
type Server struct {
	pb.UnimplementedCacheServiceServer

	serverID    int32
	port        int32
	clusterID   int32
	isPrimary   bool
	mu          sync.RWMutex
	cache       *Cache
	replication *ReplicationManager

	sentinelAddress     string
	loadBalancerAddress string
}

// NewServer creates a new Cache server instance.
func NewServer(serverID, port, clusterID int32) (*Server, error) {
	cache, err := NewCache(serverID)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cache: %w", err)
	}

	s := &Server{
		serverID:            serverID,
		port:                port,
		clusterID:           clusterID,
		cache:               cache,
		sentinelAddress:     "localhost:8081",
		loadBalancerAddress: "localhost:8083",
	}
	s.replication = NewReplicationManager(s)
	return s, nil
}

// Shutdown handles graceful shutdown tasks like data persistence.
func (s *Server) Shutdown() {
	s.cache.Persist()
}

// RegisterAndDetermineRole contacts the Sentinel to register and find its role.
func (s *Server) RegisterAndDetermineRole() error {
	conn, err := grpc.Dial(s.sentinelAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to sentinel: %w", err)
	}
	defer conn.Close()

	client := pb.NewSentinelServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.RegisterServer(ctx, &pb.CacheServerRegistrationRequest{
		ClusterId: s.clusterID,
		ServerId:  s.serverID,
		Port:      s.port,
	})
	if err != nil {
		return fmt.Errorf("failed to register with sentinel: %w", err)
	}

	s.mu.Lock()
	s.isPrimary = resp.IsPrimary
	s.mu.Unlock()

	if s.isPrimary {
		log.Printf("CacheServer %d: Assigned as PRIMARY for cluster %d.", s.serverID, s.clusterID)
		go s.registerWithLoadBalancer()
		go s.discoverAndManageReplicas()
	} else {
		log.Printf("CacheServer %d: Assigned as REPLICA. Primary is at port %d.", s.serverID, resp.PrimaryPort)
		if resp.PrimaryPort == 0 {
			return fmt.Errorf("sentinel assigned replica role but provided no primary port")
		}
		go s.replication.SyncWithPrimary(resp.PrimaryPort)
	}
	return nil
}

// --- gRPC Service Method Implementations ---

// GetResult retrieves a value from the cache.
func (s *Server) GetResult(ctx context.Context, req *pb.CacheRequest) (*pb.CacheResponse, error) {
	val, found := s.cache.Get(req.QueryHash)
	return &pb.CacheResponse{Result: val, Found: found}, nil
}

// SetResult sets a value in the cache and handles replication if primary.
func (s *Server) SetResult(ctx context.Context, req *pb.CacheSetRequest) (*pb.CacheSetResponse, error) {
	log.Printf("CacheServer %d: Processing '%s' for hash %s", s.serverID, req.Operation, req.QueryHash)

	s.mu.RLock()
	isPrimary := s.isPrimary
	s.mu.RUnlock()

	if req.Operation == "read" {
		s.cache.Set(req.QueryHash, req.Result, req.Entity)
	} else {
		// For write operations, we invalidate the entity.
		s.cache.InvalidateEntity(req.Entity)
	}

	// If this server is a primary, replicate the operation to replicas.
	if isPrimary {
		s.replication.Replicate(req)
	}

	return &pb.CacheSetResponse{Success: true}, nil
}

// GetLoad returns the current number of items in the cache.
func (s *Server) GetLoad(ctx context.Context, req *pb.EmptyRequest) (*pb.LoadResponse, error) {
	return &pb.LoadResponse{Load: int32(s.cache.Len())}, nil
}

// GetOffset returns the current replication offset.
func (s *Server) GetOffset(ctx context.Context, req *pb.EmptyRequest) (*pb.OffsetResponse, error) {
	return &pb.OffsetResponse{Offset: int32(s.cache.Offset())}, nil
}

// GetRole returns whether the server is a primary or a replica.
func (s *Server) GetRole(ctx context.Context, req *pb.EmptyRequest) (*pb.RoleResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &pb.RoleResponse{IsPrimary: s.isPrimary}, nil
}

// PromoteToPrimary handles a promotion notification from the Sentinel.
func (s *Server) PromoteToPrimary(ctx context.Context, req *pb.EmptyRequest) (*pb.PromotionResponse, error) {
	s.mu.Lock()
	wasPrimary := s.isPrimary
	s.isPrimary = true
	s.mu.Unlock()

	if !wasPrimary {
		log.Printf("CacheServer %d: PROMOTED to PRIMARY for cluster %d.", s.serverID, s.clusterID)
		// Stop trying to sync with an old primary
		s.replication.StopSyncing()
		// Start acting as a primary
		go s.registerWithLoadBalancer()
		go s.discoverAndManageReplicas()
	}
	return &pb.PromotionResponse{Success: true}, nil
}

// InvalidateEntityCache is called by a primary to invalidate a replica's cache.
func (s *Server) InvalidateEntityCache(ctx context.Context, req *pb.InvalidateEntityRequest) (*pb.InvalidateEntityResponse, error) {
	s.mu.RLock()
	isPrimary := s.isPrimary
	s.mu.RUnlock()
	if isPrimary {
		return &pb.InvalidateEntityResponse{Success: false}, fmt.Errorf("invalidation command sent to a primary")
	}
	s.cache.InvalidateEntity(req.Entity)
	return &pb.InvalidateEntityResponse{Success: true}, nil
}

// --- Helper methods for primary duties ---

func (s *Server) registerWithLoadBalancer() {
	log.Printf("CacheServer %d: Registering as PRIMARY with Load Balancer.", s.serverID)
	// This uses NotifyPromotion to inform the LB of the current primary.
	conn, err := grpc.Dial(s.loadBalancerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("CacheServer %d: Failed to connect to LB: %v", s.serverID, err)
		return
	}
	defer conn.Close()

	client := pb.NewMetadataServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.NotifyPromotion(ctx, &pb.NotifyPromotionRequest{
		ServerId:  s.serverID,
		Port:      s.port,
		ClusterId: s.clusterID,
	})
	if err != nil {
		log.Printf("CacheServer %d: Failed to register with LB: %v", s.serverID, err)
	}
}

func (s *Server) discoverAndManageReplicas() {
	log.Printf("CacheServer %d: Discovering replicas for cluster %d.", s.serverID, s.clusterID)
	conn, err := grpc.Dial(s.sentinelAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("CacheServer %d: Failed to connect to Sentinel for replica discovery: %v", s.serverID, err)
		return
	}
	defer conn.Close()

	client := pb.NewSentinelServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.GetClusterReplicas(ctx, &pb.GetClusterReplicasRequest{
		ClusterId: s.clusterID,
		ServerId:  s.serverID, // Exclude self
	})
	if err != nil {
		log.Printf("CacheServer %d: Failed to get replicas from Sentinel: %v", s.serverID, err)
		return
	}

	s.replication.UpdateReplicas(resp.Replicas)
}
