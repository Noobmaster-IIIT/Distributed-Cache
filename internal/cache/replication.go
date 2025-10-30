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

type replicaClient struct {
	stub pb.CacheServiceClient
	conn *grpc.ClientConn
}

// ReplicationManager handles all replication tasks for a primary cache server.
type ReplicationManager struct {
	server     *Server // Reference to the parent server
	mu         sync.RWMutex
	replicas   map[int32]replicaClient
	stopSyncCh chan struct{} // Used to stop syncing with a primary
}

// NewReplicationManager creates a new replication manager.
func NewReplicationManager(s *Server) *ReplicationManager {
	return &ReplicationManager{
		server:   s,
		replicas: make(map[int32]replicaClient),
	}
}

// UpdateReplicas connects to new replicas and disconnects from old ones.
func (rm *ReplicationManager) UpdateReplicas(newReplicas []*pb.ReplicaInfo) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	newReplicaSet := make(map[int32]bool)
	for _, r := range newReplicas {
		newReplicaSet[r.ServerId] = true
		// If this is a new replica, connect to it.
		if _, ok := rm.replicas[r.ServerId]; !ok {
			addr := fmt.Sprintf("localhost:%d", r.Port)
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Primary %d: Failed to connect to new replica %d: %v", rm.server.serverID, r.ServerId, err)
				continue
			}
			rm.replicas[r.ServerId] = replicaClient{
				stub: pb.NewCacheServiceClient(conn),
				conn: conn,
			}
			log.Printf("Primary %d: Connected to replica %d", rm.server.serverID, r.ServerId)
		}
	}

	// Disconnect from replicas that are no longer in the list.
	for id, client := range rm.replicas {
		if !newReplicaSet[id] {
			client.conn.Close()
			delete(rm.replicas, id)
			log.Printf("Primary %d: Disconnected from old replica %d", rm.server.serverID, id)
		}
	}
}

// Replicate sends a write operation to all connected replicas asynchronously.
func (rm *ReplicationManager) Replicate(req *pb.CacheSetRequest) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if len(rm.replicas) == 0 {
		return
	}

	log.Printf("Primary %d: Replicating operation '%s' to %d replicas", rm.server.serverID, req.Operation, len(rm.replicas))
	for id, client := range rm.replicas {
		go func(replicaID int32, c pb.CacheServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := c.SetResult(ctx, req)
			if err != nil {
				log.Printf("Primary %d: Failed to replicate to replica %d: %v", rm.server.serverID, replicaID, err)
				// Here you could add logic to mark the replica as lagging or unhealthy.
			}
		}(id, client.stub)
	}
}

// SyncWithPrimary is called by a replica to connect and sync with its primary.
func (rm *ReplicationManager) SyncWithPrimary(primaryPort int32) {
	rm.mu.Lock()
	rm.stopSyncCh = make(chan struct{})
	rm.mu.Unlock()

	addr := fmt.Sprintf("localhost:%d", primaryPort)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Replica %d: Failed to connect to primary on port %d: %v", rm.server.serverID, primaryPort, err)
		return
	}
	defer conn.Close()

	log.Printf("Replica %d: Connected to primary on port %d for synchronization.", rm.server.serverID, primaryPort)
	// In a full implementation, this is where you'd implement snapshotting and
	// continuous replication stream logic. For this version, we just establish the connection.

	// Keep running until told to stop (e.g., when this replica gets promoted).
	<-rm.stopSyncCh
	log.Printf("Replica %d: Halting sync with old primary.", rm.server.serverID)
}

// StopSyncing tells a replica to stop its synchronization loop.
func (rm *ReplicationManager) StopSyncing() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.stopSyncCh != nil {
		close(rm.stopSyncCh)
		rm.stopSyncCh = nil
	}
}
