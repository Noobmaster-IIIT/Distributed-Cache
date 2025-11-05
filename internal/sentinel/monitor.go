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
const dialTimeout = 2 * time.Second // Time to wait for a connection

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
			log.Println("Sentinel Monitor: ----- Running health check cycle -----")
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

type serverToCheck struct {
	serverInfo
	isPrimary bool
	clusterID int32
}

// performChecks iterates through all registered servers and checks their health.
func (m *Monitor) performChecks() {
	m.server.mu.RLock()
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

	if len(checks) == 0 {
		log.Println("Sentinel Monitor: No servers registered to check.")
		return
	}

	log.Printf("Sentinel Monitor: Checking %d registered servers...", len(checks))

	for _, srv := range checks {
		isHealthy, offset := m.checkServerHealth(srv.serverID, srv.port)
		m.updateHealthStatus(srv.clusterID, srv.serverID, isHealthy, offset, srv.isPrimary)
	}
}

// checkServerHealth pings a single server using a blocking dial.
// This is the corrected function.
func (m *Monitor) checkServerHealth(serverID int32, port int32) (bool, int32) {
	addr := fmt.Sprintf("localhost:%d", port)

	// Create a context with a short timeout.
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	// Dial with WithBlock() to force a synchronous connection attempt.
	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // <-- Forces the dial to block and actually connect
	)

	// If DialContext fails (timeout, connection refused), the server is dead.
	if err != nil {
		log.Printf("Monitor: FAILED to dial server %d at port %d: %v", serverID, port, err)
		return false, 0
	}
	defer conn.Close()

	// If dial succeeded, the server is alive. We can now get the offset.
	client := pb.NewCacheServiceClient(conn)

	// We can reuse the same context for the RPC call
	resp, err := client.GetOffset(ctx, &pb.EmptyRequest{})
	if err != nil {
		// This is a secondary failure (e.g., server is up but RPC is broken)
		log.Printf("Monitor: FAILED to get offset from ALIVE server %d at port %d: %v", serverID, port, err)
		return false, 0
	}

	log.Printf("Monitor: SUCCESS ping for server %d at port %d (Offset: %d)", serverID, port, resp.Offset)
	return true, resp.Offset
}

// updateHealthStatus updates the server's health in the main server struct.
func (m *Monitor) updateHealthStatus(clusterID, serverID int32, isHealthy bool, offset int32, isPrimary bool) {
	m.server.mu.Lock()
	defer m.server.mu.Unlock()

	health, ok := m.server.serverHealth[serverID]
	if !ok {
		log.Printf("Monitor: updateHealthStatus called for server %d, but it is no longer registered.", serverID)
		return
	}

	if isHealthy {
		health.failedChecks = 0
		health.offset = offset
	} else {
		health.failedChecks++
		log.Printf("Sentinel: Server %d has failed check %d/%d", serverID, health.failedChecks, healthThreshold)

		if health.failedChecks >= healthThreshold {
			log.Printf("Sentinel: Server %d marked as DOWN.", serverID)

			m.server.removeServer(serverID)

			if isPrimary {
				log.Printf("Sentinel: PRIMARY server %d in cluster %d is DOWN. Initiating failover.", serverID, clusterID)
				m.server.promoteReplica(clusterID)
			}
		}
	}
}
