package sentinel

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

const (
	healthThreshold = 3 // Mark as down after 3 failed checks
)

// serverInfo holds details about a registered cache server.
type serverInfo struct {
	serverID int32
	port     int32
}

// clusterState holds the primary and replicas for a single cluster.
type clusterState struct {
	primary  *serverInfo
	replicas map[int32]*serverInfo // map[serverID]*serverInfo
}

// serverHealth tracks the health status of a cache server.
type serverHealth struct {
	failedChecks int
	offset       int32
}

// Server implements the sentinel gRPC service.
type Server struct {
	pb.UnimplementedSentinelServiceServer

	mu           sync.RWMutex
	clusters     map[int32]*clusterState // map[clusterID]*clusterState
	serverHealth map[int32]*serverHealth // map[serverID]*serverHealth
	consulClient *consul.Client
}

// NewServer creates a new Sentinel server instance.
func NewServer(client *consul.Client) *Server {
	return &Server{
		clusters:     make(map[int32]*clusterState),
		serverHealth: make(map[int32]*serverHealth),
		consulClient: client,
	}
}

// RegisterServer is called by cache servers to register themselves.
func (s *Server) RegisterServer(ctx context.Context, req *pb.CacheServerRegistrationRequest) (*pb.CacheServerRegistrationResponse, error) {
	clusterID := req.ClusterId
	serverID := req.ServerId
	port := req.Port
	isPrimary := false

	s.mu.Lock()
	defer s.mu.Unlock()

	// Clean up any stale entries for this server before registering.
	s.removeServer(serverID)
	log.Printf("Sentinel: Cleaned up any stale entries for server %d before registration.", serverID)

	// Initialize cluster if it doesn't exist
	if _, ok := s.clusters[clusterID]; !ok {
		s.clusters[clusterID] = &clusterState{
			replicas: make(map[int32]*serverInfo),
		}
	}
	cluster := s.clusters[clusterID]
	newServer := &serverInfo{serverID: serverID, port: port}

	// Initialize health tracking for this server
	s.serverHealth[serverID] = &serverHealth{failedChecks: 0}

	if cluster.primary == nil {
		// No primary exists, this server becomes the new primary.
		cluster.primary = newServer
		isPrimary = true
		log.Printf("Sentinel: CacheServer %d becomes PRIMARY for cluster %d", serverID, clusterID)
		go s.notifyLoadBalancer(clusterID, newServer)
	} else {
		// Primary exists, register as replica
		cluster.replicas[serverID] = newServer
		log.Printf("Sentinel: CacheServer %d registered as REPLICA for cluster %d", serverID, clusterID)
	}

	primaryPort := int32(0)
	if cluster.primary != nil {
		primaryPort = cluster.primary.port
	}

	return &pb.CacheServerRegistrationResponse{
		Success:     true,
		IsPrimary:   isPrimary,
		PrimaryPort: primaryPort,
	}, nil
}

// removeServer is a new helper function to clean up a server entry.
// This function MUST be called while holding the 's.mu' lock.
func (s *Server) removeServer(serverID int32) {
	delete(s.serverHealth, serverID)
	for _, cluster := range s.clusters {
		if cluster.primary != nil && cluster.primary.serverID == serverID {
			cluster.primary = nil
		}
		delete(cluster.replicas, serverID)
	}
}

// GetClusterReplicas provides a list of replicas in a cluster.
func (s *Server) GetClusterReplicas(ctx context.Context, req *pb.GetClusterReplicasRequest) (*pb.GetClusterReplicasResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cluster, ok := s.clusters[req.ClusterId]
	if !ok {
		return &pb.GetClusterReplicasResponse{Success: true, Replicas: []*pb.ReplicaInfo{}}, nil
	}

	var replicas []*pb.ReplicaInfo
	for _, replica := range cluster.replicas {
		if replica.serverID != req.ServerId { // Exclude the requester
			replicas = append(replicas, &pb.ReplicaInfo{ServerId: replica.serverID, Port: replica.port})
		}
	}

	return &pb.GetClusterReplicasResponse{Success: true, Replicas: replicas}, nil
}

// promoteReplica promotes the best replica to become the new primary for a cluster.
// This function is called as a goroutine and MUST lock itself.
func (s *Server) promoteReplica(clusterID int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cluster, ok := s.clusters[clusterID]
	if !ok {
		return // Cluster no longer exists
	}

	// Find the best replica to promote (healthiest and highest offset)
	var bestReplica *serverInfo
	highestOffset := int32(-1)

	for _, replica := range cluster.replicas {
		health, ok := s.serverHealth[replica.serverID]
		// We only promote healthy replicas
		if ok && health.failedChecks < healthThreshold {
			if health.offset > highestOffset {
				highestOffset = health.offset
				bestReplica = replica
			}
		}
	}

	if bestReplica == nil {
		log.Printf("Sentinel: No healthy replicas available in cluster %d to promote.", clusterID)
		cluster.primary = nil // The cluster is now without a primary
		return
	}

	log.Printf("Sentinel: Promoting replica %d to PRIMARY in cluster %d.", bestReplica.serverID, clusterID)

	// Update cluster state
	delete(cluster.replicas, bestReplica.serverID)
	if cluster.primary != nil {
		// The old primary, if it ever comes back, should be a replica
		cluster.replicas[cluster.primary.serverID] = cluster.primary
	}
	cluster.primary = bestReplica

	// Notify other services in the background
	go s.notifyServerOfPromotion(bestReplica)
	go s.notifyLoadBalancer(clusterID, bestReplica)
}

// notifyLoadBalancer sends a promotion notification to the load balancer.
// This function is called as a goroutine and does not lock.
func (s *Server) notifyLoadBalancer(clusterID int32, newPrimary *serverInfo) {
	lbAddress, err := s.consulClient.Discover("load-balancer")
	if err != nil {
		log.Printf("Sentinel: CRITICAL: Could not discover load balancer: %v", err)
		return
	}

	log.Printf("Sentinel: Notifying Load Balancer at %s about new primary %d.", lbAddress, newPrimary.serverID)
	conn, err := grpc.Dial(lbAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Sentinel: Error connecting to load balancer: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewMetadataServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = client.NotifyPromotion(ctx, &pb.NotifyPromotionRequest{
		ServerId:  newPrimary.serverID,
		Port:      newPrimary.port,
		ClusterId: clusterID,
	})

	if err != nil {
		log.Printf("Sentinel: Error notifying load balancer: %v", err)
	}
}

// notifyServerOfPromotion tells a cache server it has been promoted.
// This function is called as a goroutine and does not lock.
func (s *Server) notifyServerOfPromotion(server *serverInfo) {
	addr := fmt.Sprintf("localhost:%d", server.port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Sentinel: Failed to connect to new primary %d to notify promotion: %v", server.serverID, err)
		return
	}
	defer conn.Close()

	client := pb.NewCacheServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.PromoteToPrimary(ctx, &pb.EmptyRequest{})
	if err != nil {
		log.Printf("Sentinel: Failed to notify new primary %d of promotion: %v", server.serverID, err)
	}
}
