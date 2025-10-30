package sentinel

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

const checkInterval = 5 * time.Second

// Monitor handles the health checking and failover logic.
type Monitor struct {
	server *Server
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewMonitor creates a new health monitor.
func NewMonitor(server *Server) *Monitor {
	return &Monitor{
		server: server,
		stopCh: make(chan struct{}),
	}
}

// Run starts the monitoring loop.
func (m *Monitor) Run() {
	log.Println("Sentinel monitor started.")
	m.wg.Add(1)
	defer m.wg.Done()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.performChecks()
		case <-m.stopCh:
			log.Println("Sentinel monitor stopped.")
			return
		}
	}
}

// Stop gracefully stops the monitor.
func (m *Monitor) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// performChecks iterates through all registered servers and checks their health.
func (m *Monitor) performChecks() {
	m.server.mu.RLock()
	// Create a copy of servers to check to avoid holding the lock for too long.
	type serverToCheck struct {
		serverInfo
		isPrimary bool
		clusterID int32
	}
	var checks []serverToCheck

	for cid, cluster := range m.server.clusters {
		if cluster.primary != nil {
			checks = append(checks, serverToCheck{*cluster.primary, true, cid})
		}
		for _, replica := range cluster.replicas {
			checks = append(checks, serverToCheck{*replica, false, cid})
		}
	}
	m.server.mu.RUnlock()

	// Perform health checks concurrently
	var wg sync.WaitGroup
	for _, s := range checks {
		wg.Add(1)
		go func(srv serverToCheck) {
			defer wg.Done()
			m.checkServerHealth(srv.clusterID, srv.serverID, srv.port, srv.isPrimary)
		}(s)
	}
	wg.Wait()
}

// checkServerHealth pings a single server and updates its health status.
func (m *Monitor) checkServerHealth(clusterID, serverID, port int32, isPrimary bool) {
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Monitor: Failed to dial server %d: %v", serverID, err)
		m.updateHealthStatus(clusterID, serverID, false, 0, isPrimary)
		return
	}
	defer conn.Close()

	client := pb.NewCacheServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// We can use GetOffset as a health check and to get replication status
	resp, err := client.GetOffset(ctx, &pb.EmptyRequest{})
	if err != nil {
		log.Printf("Monitor: Health check failed for server %d on port %d", serverID, port)
		m.updateHealthStatus(clusterID, serverID, false, 0, isPrimary)
		return
	}

	m.updateHealthStatus(clusterID, serverID, true, resp.Offset, isPrimary)
}

// updateHealthStatus updates the server's health in the main server struct.
func (m *Monitor) updateHealthStatus(clusterID, serverID int32, isHealthy bool, offset int32, isPrimary bool) {
	m.server.mu.Lock()
	defer m.server.mu.Unlock()

	health, ok := m.server.serverHealth[serverID]
	if !ok {
		return // Server was deregistered
	}

	if isHealthy {
		health.failedChecks = 0
		health.offset = offset
	} else {
		health.failedChecks++
		log.Printf("Sentinel: Server %d has failed check %d/%d", serverID, health.failedChecks, healthThreshold)
		if isPrimary && health.failedChecks >= healthThreshold {
			log.Printf("Sentinel: PRIMARY server %d in cluster %d is DOWN. Initiating failover.", serverID, clusterID)
			// Need to run promotion in a separate goroutine to avoid deadlock
			go m.server.promoteReplica(clusterID)
		}
	}
}
