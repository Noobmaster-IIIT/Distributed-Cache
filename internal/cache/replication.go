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

const replicationTimeout = 2 * time.Second

type replicaClient struct {
	stub pb.CacheServiceClient
	conn *grpc.ClientConn
}

// ReplicationManager handles all replication tasks for a primary or replica.
type ReplicationManager struct {
	server     *Server
	mu         sync.RWMutex
	replicas   map[int32]replicaClient
	stopSyncCh chan struct{}
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

	for id, client := range rm.replicas {
		if !newReplicaSet[id] {
			client.conn.Close()
			delete(rm.replicas, id)
			log.Printf("Primary %d: Disconnected from old replica %d", rm.server.serverID, id)
		}
	}
}

// ReplicateAsync sends a write operation to all connected replicas asynchronously.
func (rm *ReplicationManager) ReplicateAsync(req *pb.CacheSetRequest) {
	rm.mu.RLock()
	replicas := rm.getReplicaStubs()
	rm.mu.RUnlock()

	if len(replicas) == 0 {
		return
	}

	log.Printf("Primary %d: Replicating operation '%s' asynchronously to %d replicas", rm.server.serverID, req.Operation, len(replicas))
	for id, stub := range replicas {
		go func(replicaID int32, c pb.CacheServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
			defer cancel()
			_, err := c.SetResult(ctx, req)
			if err != nil {
				log.Printf("Primary %d: Failed to replicate to replica %d: %v", rm.server.serverID, replicaID, err)
			}
		}(id, stub)
	}
}

// ReplicateAndWait sends a write and waits for a specific number of replicas to acknowledge.
func (rm *ReplicationManager) ReplicateAndWait(req *pb.CacheSetRequest, numAcks int) bool {
	rm.mu.RLock()
	replicas := rm.getReplicaStubs()
	rm.mu.RUnlock()

	if len(replicas) == 0 {
		log.Printf("Primary %d: No replicas to wait for.", rm.server.serverID)
		return true
	}

	if numAcks > len(replicas) {
		numAcks = len(replicas)
	}

	ackChannel := make(chan bool, len(replicas))
	var wg sync.WaitGroup

	for id, stub := range replicas {
		wg.Add(1)
		go func(replicaID int32, c pb.CacheServiceClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
			defer cancel()

			resp, err := c.SetResult(ctx, req)
			if err != nil {
				log.Printf("Primary %d: Failed to get ACK from replica %d: %v", rm.server.serverID, replicaID, err)
				ackChannel <- false
			} else if resp.Success {
				ackChannel <- true
			} else {
				ackChannel <- false
			}
		}(id, stub)
	}

	go func() {
		wg.Wait()
		close(ackChannel)
	}()

	ackCount := 0
	for success := range ackChannel {
		if success {
			ackCount++
		}
		if ackCount >= numAcks {
			return true
		}
	}

	return ackCount >= numAcks
}

// getReplicaStubs safely returns a map of replica IDs to their stubs.
func (rm *ReplicationManager) getReplicaStubs() map[int32]pb.CacheServiceClient {
	stubs := make(map[int32]pb.CacheServiceClient)
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	for id, client := range rm.replicas {
		stubs[id] = client.stub
	}
	return stubs
}

// SyncWithPrimary is called by a replica to connect and sync with its primary.
func (rm *ReplicationManager) SyncWithPrimary(primaryAddr string) {
	rm.mu.Lock()
	rm.stopSyncCh = make(chan struct{})
	rm.mu.Unlock()

	conn, err := grpc.Dial(primaryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Replica %d: Failed to connect to primary at %s: %v", rm.server.serverID, primaryAddr, err)
		return
	}
	defer conn.Close()

	primaryClient := pb.NewCacheServiceClient(conn)
	log.Printf("Replica %d: Connected to primary at %s for synchronization.", rm.server.serverID, primaryAddr)

	currentOffset := rm.server.cache.Offset()
	pingReq := &pb.PingRequest{Offset: int32(currentOffset), ServerId: rm.server.serverID}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	log.Printf("Replica %d: Pinging primary with offset %d.", rm.server.serverID, currentOffset)
	pingResp, err := primaryClient.PingWithOffset(ctx, pingReq)
	cancel() // Cancel context after use
	if err != nil {
		log.Printf("Replica %d: Error during PingWithOffset: %v. Falling back to full snapshot.", rm.server.serverID, err)
		rm.requestFullSnapshot(primaryClient)
		return
	}

	if pingResp.UpToDate {
		if len(pingResp.Commands) > 0 {
			log.Printf("Replica %d: Applying %d incremental commands from primary...", rm.server.serverID, len(pingResp.Commands))
			for _, cmd := range pingResp.Commands {
				if cmd.Operation == "read" {
					rm.server.cache.Set(cmd.QueryHash, cmd.Result, cmd.Entity)
				} else {
					rm.server.cache.InvalidateEntity(cmd.Entity)
				}
			}
			log.Printf("Replica %d: Incremental sync complete. New offset: %d", rm.server.serverID, rm.server.cache.Offset())
		} else {
			log.Printf("Replica %d: Already up-to-date with primary.", rm.server.serverID)
		}
	} else {
		log.Printf("Replica %d: Too far behind primary. Requesting full snapshot...", rm.server.serverID)
		rm.requestFullSnapshot(primaryClient)
	}

	// Keep running until told to stop
	<-rm.stopSyncCh
	log.Printf("Replica %d: Halting sync with old primary.", rm.server.serverID)
}

// requestFullSnapshot handles the logic for getting and applying a full snapshot.
func (rm *ReplicationManager) requestFullSnapshot(primaryClient pb.CacheServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	snapshotResp, err := primaryClient.GetSnapshot(ctx, &pb.EmptyRequest{})
	if err != nil {
		log.Printf("Replica %d: Failed to get snapshot from primary: %v", rm.server.serverID, err)
		return
	}
	if !snapshotResp.Success {
		log.Printf("Replica %d: Primary reported an error during snapshot.", rm.server.serverID)
		return
	}

	log.Printf("Replica %d: Received snapshot (%d bytes), applying...", rm.server.serverID, len(snapshotResp.Data))

	if err := rm.server.cache.SetSnapshot(snapshotResp.Data); err != nil {
		log.Printf("Replica %d: Failed to apply snapshot: %v", rm.server.serverID, err)
	}
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
