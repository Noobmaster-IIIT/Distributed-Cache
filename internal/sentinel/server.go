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
	healthThreshold = 3
)

type serverInfo struct {
	serverID int32
	port     int32
}

type clusterState struct {
	primary  *serverInfo
	replicas map[int32]*serverInfo
}

type serverHealth struct {
	failedChecks int
	offset       int32
}

type Server struct {
	pb.UnimplementedSentinelServiceServer
	mu           sync.RWMutex
	clusters     map[int32]*clusterState
	serverHealth map[int32]*serverHealth
	consulClient *consul.Client
}

func NewServer(client *consul.Client) *Server {
	return &Server{
		clusters:     make(map[int32]*clusterState),
		serverHealth: make(map[int32]*serverHealth),
		consulClient: client,
	}
}

func (s *Server) RegisterServer(ctx context.Context, req *pb.CacheServerRegistrationRequest) (*pb.CacheServerRegistrationResponse, error) {
	clusterID := req.ClusterId
	serverID := req.ServerId
	port := req.Port
	isPrimary := false

	s.mu.Lock()
	defer s.mu.Unlock()

	// Clean up any stale entries for this server before registering.
	s.removeServer(serverID)
	log.Printf("Sentinel: Cleaned up stale entries for server %d.", serverID)

	if _, ok := s.clusters[clusterID]; !ok {
		s.clusters[clusterID] = &clusterState{
			replicas: make(map[int32]*serverInfo),
		}
	}
	cluster := s.clusters[clusterID]
	newServer := &serverInfo{serverID: serverID, port: port}

	s.serverHealth[serverID] = &serverHealth{failedChecks: 0}

	if cluster.primary == nil {
		cluster.primary = newServer
		isPrimary = true
		log.Printf("Sentinel: CacheServer %d becomes PRIMARY for cluster %d", serverID, clusterID)
		go s.notifyLoadBalancer(clusterID, newServer)
	} else {
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

// removeServer is a helper to clean up a server entry.
func (s *Server) removeServer(serverID int32) {
	delete(s.serverHealth, serverID)
	for _, cluster := range s.clusters {
		if cluster.primary != nil && cluster.primary.serverID == serverID {
			cluster.primary = nil
		}
		delete(cluster.replicas, serverID)
	}
}

func (s *Server) GetClusterReplicas(ctx context.Context, req *pb.GetClusterReplicasRequest) (*pb.GetClusterReplicasResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cluster, ok := s.clusters[req.ClusterId]
	if !ok {
		return &pb.GetClusterReplicasResponse{Success: true, Replicas: []*pb.ReplicaInfo{}}, nil
	}

	var replicas []*pb.ReplicaInfo
	for _, replica := range cluster.replicas {
		if replica.serverID != req.ServerId {
			replicas = append(replicas, &pb.ReplicaInfo{ServerId: replica.serverID, Port: replica.port})
		}
	}

	return &pb.GetClusterReplicasResponse{Success: true, Replicas: replicas}, nil
}

func (s *Server) promoteReplica(clusterID int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cluster, ok := s.clusters[clusterID]
	if !ok {
		return
	}

	var bestReplica *serverInfo
	highestOffset := int32(-1)

	for _, replica := range cluster.replicas {
		health, ok := s.serverHealth[replica.serverID]
		if ok && health.failedChecks < healthThreshold {
			if health.offset > highestOffset {
				highestOffset = health.offset
				bestReplica = replica
			}
		}
	}

	if bestReplica == nil {
		log.Printf("Sentinel: No healthy replicas available in cluster %d to promote.", clusterID)
		cluster.primary = nil
		return
	}

	log.Printf("Sentinel: Promoting replica %d to PRIMARY in cluster %d.", bestReplica.serverID, clusterID)

	delete(cluster.replicas, bestReplica.serverID)
	if cluster.primary != nil {
		cluster.replicas[cluster.primary.serverID] = cluster.primary
	}
	cluster.primary = bestReplica

	go s.notifyServerOfPromotion(bestReplica)
	go s.notifyLoadBalancer(clusterID, bestReplica)
}

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
