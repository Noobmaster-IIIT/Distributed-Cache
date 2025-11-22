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
	// How long to wait for a primary to reconnect before declaring it dead.
	heartbeatGracePeriod = 9 * time.Second
	// How often the primary should be sending a heartbeat.
	heartbeatInterval = 3 * time.Second
	// How long to wait before retrying a failed connection.
	reconnectInterval = 2 * time.Second
)

type serverInfo struct {
	serverID int32
	port     int32
}

type clusterState struct {
	primary  *serverInfo
	replicas map[int32]*serverInfo
}

// serverHealth now just tracks the offset and last contact time.
type serverHealth struct {
	offset        int32
	lastHeartbeat time.Time
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

// rpc call from cache server RegisterServer now launches the watchPrimary goroutine
func (s *Server) RegisterServer(ctx context.Context, req *pb.CacheServerRegistrationRequest) (*pb.CacheServerRegistrationResponse, error) {
	clusterID := req.ClusterId
	serverID := req.ServerId
	port := req.Port
	isPrimary := false

	s.mu.Lock()
	defer s.mu.Unlock()

	s.removeServer(serverID)
	log.Printf("Sentinel: Cleaned up any stale entries for server %d before registration.", serverID)

	if _, ok := s.clusters[clusterID]; !ok {
		s.clusters[clusterID] = &clusterState{
			replicas: make(map[int32]*serverInfo),
		}
	}
	cluster := s.clusters[clusterID]
	newServer := &serverInfo{serverID: serverID, port: port}

	// Initialize health tracking
	s.serverHealth[serverID] = &serverHealth{lastHeartbeat: time.Now()}

	if cluster.primary == nil {
		cluster.primary = newServer
		isPrimary = true
		log.Printf("Sentinel: CacheServer %d becomes PRIMARY for cluster %d", serverID, clusterID)

		// Launch the persistent heartbeat monitor for this new primary
		go s.watchPrimary(clusterID, newServer)
		go s.notifyLoadBalancer(clusterID, newServer)
	} else {
		cluster.replicas[serverID] = newServer
		log.Printf("Sentinel: CacheServer %d registered as REPLICA for cluster %d", serverID, clusterID)
		go s.watchReplica(clusterID, newServer)

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

// removeServer is a helper function to clean up a server entry.
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
		if replica.serverID != req.ServerId {
			replicas = append(replicas, &pb.ReplicaInfo{ServerId: replica.serverID, Port: replica.port})
		}
	}

	return &pb.GetClusterReplicasResponse{Success: true, Replicas: replicas}, nil
}

// promoteReplica promotes the best replica to become the new primary for a cluster.
func (s *Server) promoteReplica(clusterID int32) {
	cluster, ok := s.clusters[clusterID]
	if !ok {
		return
	}

	var bestReplica *serverInfo
	highestOffset := int32(-1)

	// Find the best replica (most up-to-date)
	for _, replica := range cluster.replicas {
		health, ok := s.serverHealth[replica.serverID]
		if ok {
			// Check if we've heard from this replica recently
			if time.Since(health.lastHeartbeat) < (heartbeatGracePeriod * 2) {
				if health.offset > highestOffset {
					highestOffset = health.offset
					bestReplica = replica
				}
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

	// Launch a monitor for the *newly* promoted primary
	go s.watchPrimary(clusterID, bestReplica)
	go s.notifyServerOfPromotion(bestReplica)
	go s.notifyLoadBalancer(clusterID, bestReplica)
}

// watchPrimary is the new, persistent monitor for a single primary
func (s *Server) watchPrimary(clusterID int32, info *serverInfo) {
	addr := fmt.Sprintf("localhost:%d", info.port)
	log.Printf("Sentinel: Starting heartbeat watch for Primary %d at %s", info.serverID, addr)

	var lastSuccessfulHeartbeat time.Time = time.Now()

	// This outer loop handles reconnection
	for {
		// --- THIS IS THE GRACE PERIOD CHECK ---
		// First, check if the grace period has expired *before* trying to connect.
		if time.Since(lastSuccessfulHeartbeat) > heartbeatGracePeriod {
			log.Printf("Sentinel: Primary %d is officially DOWN (no heartbeat for >%v). INITIATING FAILOVER.", info.serverID, heartbeatGracePeriod)
			// Trigger the failover and *exit this goroutine*.
			s.triggerFailover(clusterID, info.serverID)
			return
		}

		// If the grace period is still active, try to connect.

		// Safety check: Is this server still the primary?
		if !s.isStillPrimary(clusterID, info.serverID) {
			log.Printf("Sentinel: Server %d is no longer primary. Stopping watch.", info.serverID)
			return
		}

		ctx, cancelStream := context.WithCancel(context.Background())

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Sentinel: Failed to connect to Primary %d for heartbeat: %v", info.serverID, err)
			cancelStream()
			time.Sleep(reconnectInterval)
			continue // Go back to the top of the loop to re-check grace period
		}

		client := pb.NewCacheServiceClient(conn)
		stream, err := client.HeartbeatStream(ctx,
			&pb.CacheHeartbeatRequest{ClusterId: clusterID, ServerId: info.serverID})
		if err != nil {
			log.Printf("Sentinel: Failed to start heartbeat stream with Primary %d: %v", info.serverID, err)
			conn.Close()
			cancelStream()
			time.Sleep(reconnectInterval)
			continue // Go back to the top of the loop to re-check grace period
		}

		log.Printf("Sentinel: Heartbeat stream established with Primary %d.", info.serverID)

		// This inner loop reads from the stream
		for {
			hb, err := stream.Recv()
			if err != nil {
				// The stream broke! This is the failure signal.
				log.Printf("Sentinel: LOST HEARTBEAT from Primary %d (Cluster %d): %v",
					info.serverID, clusterID, err)
				conn.Close()
				cancelStream()
				break // Break inner loop to go back to the reconnection loop
			}

			// We got a heartbeat! Update the status.
			readableTime := time.Unix(hb.Timestamp, 0)
			fmt.Printf("Sentinel: Heartbeat received from primary %d at %s\n", info.serverID, readableTime.Format("15:04:05"))
			lastSuccessfulHeartbeat = time.Now() // <-- THIS IS THE KEY

			if !s.isStillPrimary(clusterID, info.serverID) {
				log.Printf("Sentinel: Server %d is no longer primary. Stopping watch.", info.serverID)
				return
			}
			s.updateHeartbeat(info.serverID, hb.Offset)
		}
	}
}
func (s *Server) watchReplica(clusterID int32, info *serverInfo) {
	addr := fmt.Sprintf("localhost:%d", info.port)
	log.Printf("Sentinel: Starting heartbeat watch for Replica %d at %s", info.serverID, addr)

	// Retry loop
	for {
		// Check if we should stop watching (e.g. if it was promoted or removed)
		s.mu.RLock()
		cluster, ok := s.clusters[clusterID]
		if !ok || cluster.replicas[info.serverID] == nil {
			s.mu.RUnlock()
			return
		}
		s.mu.RUnlock()

		ctx, cancelStream := context.WithCancel(context.Background())

		// Connect to the Replica
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// If we can't connect to a replica, we just wait and retry.
			// If it's truly dead, the 'promoteReplica' logic will filter it out anyway based on time.Since().
			log.Printf("Sentinel: Cannot connect to Replica %d: %v", info.serverID, err)
			cancelStream()
			time.Sleep(reconnectInterval)
			continue
		}

		client := pb.NewCacheServiceClient(conn)
		stream, err := client.HeartbeatStream(ctx,
			&pb.CacheHeartbeatRequest{ClusterId: clusterID, ServerId: info.serverID})

		if err != nil {
			conn.Close()
			cancelStream()
			time.Sleep(reconnectInterval)
			continue
		}

		// Stream Loop
		for {
			hb, err := stream.Recv()
			if err != nil {
				log.Printf("Sentinel: Lost heartbeat from Replica %d", info.serverID)
				conn.Close()
				cancelStream()
				break // Break inner loop to retry connection
			}
			// Update the health map! This keeps the replica eligible for promotion.
			readableTime := time.Unix(hb.Timestamp, 0)
			fmt.Printf("Sentinel: Heartbeat received from replica %d at %s\n", info.serverID, readableTime.Format("15:04:05"))
			s.updateHeartbeat(info.serverID, hb.Offset)
		}
	}
}

// triggerFailover safely checks if a failover is needed and starts one
func (s *Server) triggerFailover(clusterID int32, deadServerID int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the server that died is *still* the primary
	// (It might have been replaced by a different goroutine already)
	if s.clusters[clusterID] == nil || s.clusters[clusterID].primary == nil || s.clusters[clusterID].primary.serverID != deadServerID {
		log.Printf("Sentinel: Failover for %d unnecessary (already failed over).", deadServerID)
		return
	}

	log.Printf("Sentinel: PRIMARY %d is officially DOWN. Initiating failover.", deadServerID)
	s.removeServer(deadServerID)
	s.promoteReplica(clusterID)
}

// updateHeartbeat updates the offset and timestamp for a server
func (s *Server) updateHeartbeat(serverID int32, offset int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if health, ok := s.serverHealth[serverID]; ok {
		health.lastHeartbeat = time.Now()
		health.offset = offset
		// log.Printf("Sentinel: Received heartbeat from %d. Offset: %d", serverID, offset)
	}
}

// isStillPrimary is a thread-safe check
func (s *Server) isStillPrimary(clusterID int32, serverID int32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cluster, ok := s.clusters[clusterID]
	if !ok {
		return false
	}
	return cluster.primary != nil && cluster.primary.serverID == serverID
}

// notifyLoadBalancer sends a promotion notification to the load balancer.
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
