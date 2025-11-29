package cache

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/pkg/consul"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server implements the cache gRPC service.
type Server struct {
	pb.UnimplementedCacheServiceServer

	serverID     int32
	port         int32
	clusterID    int32
	isPrimary    bool
	mu           sync.RWMutex
	cache        *Cache
	replication  *ReplicationManager
	consulClient *consul.Client
	ackPolicy    int

	// ctx is used to signal shutdown to long-running goroutines like HeartbeatStream
	ctx    context.Context
	cancel context.CancelFunc
}

// NewServer creates a new Cache server instance.
func NewServer(serverID, port, clusterID int32, client *consul.Client, ackPolicy int) (*Server, error) {
	cache, err := NewCache(serverID)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize cache: %w", err)
	}

	// Create a context that can be cancelled. This is CRITICAL for the HeartbeatStream.
	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		serverID:     serverID,
		port:         port,
		clusterID:    clusterID,
		cache:        cache,
		consulClient: client,
		ackPolicy:    ackPolicy,
		ctx:          ctx,
		cancel:       cancel,
	}
	s.replication = NewReplicationManager(s)
	return s, nil
}

// Shutdown handles graceful shutdown tasks like data persistence and context cancellation.
func (s *Server) Shutdown() {
	s.cancel() // Signal all goroutines (like HeartbeatStream) to stop
	s.cache.Persist()
}

// RegisterAndDetermineRole contacts the Sentinel to register and find its role.
func (s *Server) RegisterAndDetermineRole() error {
	sentinelAddress, err := s.consulClient.Discover("sentinel")
	if err != nil {
		return fmt.Errorf("failed to discover sentinel: %w", err)
	}

	conn, err := grpc.Dial(sentinelAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		// Build the full address string for the replication manager
		primaryAddr := fmt.Sprintf("localhost:%d", resp.PrimaryPort)
		go s.replication.SyncWithPrimary(primaryAddr)
	}
	return nil
}

// --- gRPC Service Method Implementations ---

func (s *Server) GetResult(ctx context.Context, req *pb.CacheRequest) (*pb.CacheResponse, error) {
	val, found := s.cache.Get(req.QueryHash)
	return &pb.CacheResponse{Result: val, Found: found}, nil
}

func (s *Server) SetResult(ctx context.Context, req *pb.CacheSetRequest) (*pb.CacheSetResponse, error) {
	log.Printf("CacheServer %d: Processing '%s' for hash %s", s.serverID, req.Operation, req.QueryHash)

	s.mu.RLock()
	isPrimary := s.isPrimary
	s.mu.RUnlock()

	if req.Operation == "read" {
		s.cache.Set(req.QueryHash, req.Result, req.Entity)
	} else {
		s.cache.InvalidateEntity(req.Entity)
	}

	if isPrimary {
		if s.ackPolicy == 0 {
			s.replication.ReplicateAsync(req)
		} else {
			log.Printf("Primary %d: Waiting for %d replica(s) to acknowledge...", s.serverID, s.ackPolicy)
			success := s.replication.ReplicateAndWait(req, s.ackPolicy)
			if !success {
				log.Printf("Primary %d: Timed out waiting for %d ACKs. Write may not be fully replicated.", s.serverID, s.ackPolicy)
			}
			log.Printf("Primary %d: Acknowledgment received.", s.serverID)
		}
	}

	return &pb.CacheSetResponse{Success: true}, nil
}

func (s *Server) GetLoad(ctx context.Context, req *pb.EmptyRequest) (*pb.LoadResponse, error) {
	return &pb.LoadResponse{Load: int32(s.cache.Len())}, nil
}

func (s *Server) GetOffset(ctx context.Context, req *pb.EmptyRequest) (*pb.OffsetResponse, error) {
	return &pb.OffsetResponse{Offset: int32(s.cache.Offset())}, nil
}

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
		s.replication.StopSyncing()
		go s.registerWithLoadBalancer()
		go s.discoverAndManageReplicas()

		// We just got promoted, but our old context might be cancelled or outdated.
		// Let's create a new one for the heartbeat stream.
		s.mu.Lock()
		s.ctx, s.cancel = context.WithCancel(context.Background())
		s.mu.Unlock()
	}
	return &pb.PromotionResponse{Success: true}, nil
}

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

// --- RPC HANDLERS FOR REPLICATION ---

func (s *Server) PingWithOffset(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	s.mu.RLock()
	isPrimary := s.isPrimary
	s.mu.RUnlock()
	if !isPrimary {
		return nil, fmt.Errorf("not a primary")
	}

	commands, upToDate := s.cache.GetCommandsSince(int64(req.Offset))
	if !upToDate {
		return &pb.PingResponse{UpToDate: false, Commands: nil}, nil
	}

	return &pb.PingResponse{UpToDate: true, Commands: commands}, nil
}

func (s *Server) GetSnapshot(ctx context.Context, req *pb.EmptyRequest) (*pb.SnapshotResponse, error) {
	s.mu.RLock()
	isPrimary := s.isPrimary
	s.mu.RUnlock()
	if !isPrimary {
		return nil, fmt.Errorf("not a primary")
	}

	data, err := s.cache.GetSnapshot()
	if err != nil {
		return &pb.SnapshotResponse{Success: false, Data: nil}, err
	}
	return &pb.SnapshotResponse{Success: true, Data: data}, nil
}

func (s *Server) SetSnapshot(ctx context.Context, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	s.mu.RLock()
	isPrimary := s.isPrimary
	s.mu.RUnlock()
	if isPrimary {
		return nil, fmt.Errorf("cannot apply snapshot to a primary")
	}

	if err := s.cache.SetSnapshot(req.Data); err != nil {
		return &pb.SnapshotResponse{Success: false}, err
	}
	return &pb.SnapshotResponse{Success: true}, nil
}

// --- NEW METHOD: HeartbeatStream  ---
// This is the server-side implementation of the stream.
func (s *Server) HeartbeatStream(req *pb.CacheHeartbeatRequest, stream pb.CacheService_HeartbeatStreamServer) error {
	log.Printf("HeartbeatStream initiated for Sentinel (Server: %d, Cluster: %d)", req.ServerId, req.ClusterId)

	// Use the server's main context
	ctx := s.ctx
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// REMOVED: The check for !isPrimary.
			// We want Replicas to send heartbeats too!

			s.mu.RLock()
			offset := int32(s.cache.Offset())
			s.mu.RUnlock()

			hb := &pb.CacheHeartbeat{
				Offset:    offset,
				Timestamp: time.Now().Unix(),
			}

			if err := stream.Send(hb); err != nil {
				log.Printf("Heartbeat: Failed to send to Sentinel: %v", err)
				return err
			}

		case <-ctx.Done():
			log.Printf("Heartbeat: Server %d shutting down. Closing stream.", s.serverID)
			return nil
		}
	}
}

// --- Helper methods for primary duties ---

func (s *Server) registerWithLoadBalancer() {
	lbAddress, err := s.consulClient.Discover("load-balancer")
	if err != nil {
		log.Printf("CacheServer %d: Failed to discover Load Balancer: %v", s.serverID, err)
		return
	}

	log.Printf("CacheServer %d: Registering as PRIMARY with Load Balancer at %s.", s.serverID, lbAddress)
	conn, err := grpc.Dial(lbAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	sentinelAddress, err := s.consulClient.Discover("sentinel")
	if err != nil {
		log.Printf("CacheServer %d: Failed to discover Sentinel for replica discovery: %v", s.serverID, err)
		return
	}

	log.Printf("CacheServer %d: Discovering replicas from Sentinel at %s.", s.serverID, sentinelAddress)
	conn, err := grpc.Dial(sentinelAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("CacheServer %d: Failed to connect to Sentinel: %v", s.serverID, err)
		return
	}
	defer conn.Close()

	client := pb.NewSentinelServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.GetClusterReplicas(ctx, &pb.GetClusterReplicasRequest{
		ClusterId: s.clusterID,
		ServerId:  s.serverID,
	})
	if err != nil {
		log.Printf("CacheServer %d: Failed to get replicas from Sentinel: %v", s.serverID, err)
		return
	}

	s.replication.UpdateReplicas(resp.Replicas)
}
