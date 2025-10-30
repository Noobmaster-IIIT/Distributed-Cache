package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/pkg/consul"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server implements the gateway gRPC service.
type Server struct {
	pb.UnimplementedGatewayServiceServer
	consulClient     *consul.Client
	backendIsHealthy bool
	mu               sync.RWMutex
}

// NewServer creates a new Gateway server instance.
func NewServer(consulClient *consul.Client) (*Server, error) {
	s := &Server{
		consulClient:     consulClient,
		backendIsHealthy: true,
	}
	// Start a background goroutine to periodically check backend health
	go s.checkBackendHealth()
	return s, nil
}

// ProcessQuery handles incoming queries from clients.
func (s *Server) ProcessQuery(ctx context.Context, req *pb.GatewayRequest) (*pb.GatewayResponse, error) {
	var queryData map[string]interface{}
	if err := json.Unmarshal([]byte(req.JsonQuery), &queryData); err != nil {
		return &pb.GatewayResponse{Response: `{"error": "Invalid JSON"}`}, nil
	}

	// Check backend health before proceeding
	s.mu.RLock()
	isHealthy := s.backendIsHealthy
	s.mu.RUnlock()

	// Create a consistent cache key
	cacheKey, err := createConsistentCacheKey(queryData)
	if err != nil {
		return &pb.GatewayResponse{Response: fmt.Sprintf(`{"error": "failed to create cache key: %v"}`, err)}, nil
	}
	log.Printf("Gateway: Using cache key %s", cacheKey)

	// Dynamically discover the load balancer
	lbAddress, err := s.consulClient.Discover("load-balancer")
	if err != nil {
		log.Printf("Gateway: CRITICAL: Could not discover load balancer: %v", err)
		return &pb.GatewayResponse{Response: `{"error": "Service discovery failed for load-balancer"}`}, nil
	}

	// 1. Check cache via Load Balancer
	var cacheResp *pb.LoadBalancerCacheResponse
	conn, err := grpc.Dial(lbAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		defer conn.Close()
		cacheClient := pb.NewLoadBalancerCacheServiceClient(conn)
		cacheResp, err = cacheClient.GetCachedData(ctx, &pb.LoadBalancerCacheRequest{Key: cacheKey})
	}

	if err == nil && cacheResp.Found {
		log.Println("Gateway: Cache hit.")
		return &pb.GatewayResponse{Response: cacheResp.Value}, nil
	}

	log.Println("Gateway: Cache miss.")
	if !isHealthy {
		return &pb.GatewayResponse{Response: `{"error": "Service unavailable", "details": "Backend is down and data not in cache"}`}, nil
	}

	// 2. On cache miss, query the backend
	sqlQuery, params, err := buildSQLFromQuery(queryData)
	if err != nil {
		return &pb.GatewayResponse{Response: fmt.Sprintf(`{"error": "%v"}`, err)}, nil
	}

	paramsJSON, _ := json.Marshal(map[string]interface{}{"params": params})
	backendResp, err := s.executeBackendSQL(ctx, sqlQuery, string(paramsJSON))
	if err != nil {
		// Mark backend as unhealthy
		s.updateBackendHealth(false)
		return &pb.GatewayResponse{Response: fmt.Sprintf(`{"error": "Backend service error", "details": "%v"}`, err)}, nil
	}

	// 3. Set the result in the cache via the Load Balancer
	operation, _ := queryData["operation"].(string)
	entity, _ := queryData["entity"].(string)

	if conn != nil {
		cacheClient := pb.NewLoadBalancerCacheServiceClient(conn)
		_, setErr := cacheClient.SetCachedData(ctx, &pb.LoadBalancerCacheSetRequest{
			Key:       cacheKey,
			Value:     backendResp.Result,
			Entity:    entity,
			Operation: operation,
		})
		if setErr != nil {
			log.Printf("Gateway: Warning: Failed to set cache data: %v", setErr)
		}
	}

	return &pb.GatewayResponse{Response: backendResp.Result}, nil
}

// executeBackendSQL sends a query to the backend server after discovering it.
func (s *Server) executeBackendSQL(ctx context.Context, sqlQuery, params string) (*pb.BackendResponse, error) {
	backendAddress, err := s.consulClient.Discover("backend-server")
	if err != nil {
		return nil, fmt.Errorf("failed to discover backend server: %w", err)
	}

	conn, err := grpc.Dial(backendAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	backendClient := pb.NewBackendServiceClient(conn)
	return backendClient.ExecuteSQL(ctx, &pb.BackendRequest{SqlQuery: sqlQuery, Params: params})
}

// checkBackendHealth periodically sends health checks to the backend server.
func (s *Server) checkBackendHealth() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		backendAddress, err := s.consulClient.Discover("backend-server")
		if err != nil {
			log.Printf("Gateway Health Check: Could not discover backend server: %v", err)
			s.updateBackendHealth(false)
			continue
		}

		conn, err := grpc.Dial(backendAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		healthy := false
		if err == nil {
			heartbeatClient := pb.NewHeartbeatServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := heartbeatClient.CheckHealth(ctx, &pb.HeartbeatRequest{GatewayId: "gateway-1"})
			cancel()
			conn.Close()
			if err == nil {
				healthy = true
			}
		}
		s.updateBackendHealth(healthy)
	}
}

// updateBackendHealth is a thread-safe way to update the health status.
func (s *Server) updateBackendHealth(isHealthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backendIsHealthy && !isHealthy {
		log.Println("Backend is now UNHEALTHY!")
	}
	if !s.backendIsHealthy && isHealthy {
		log.Println("Backend is now HEALTHY!")
	}
	s.backendIsHealthy = isHealthy
}

// createConsistentCacheKey generates a predictable key for caching.
func createConsistentCacheKey(queryData map[string]interface{}) (string, error) {
	entity, ok := queryData["entity"].(string)
	if !ok {
		return "", fmt.Errorf("entity is missing or not a string")
	}
	operation, ok := queryData["operation"].(string)
	if !ok {
		return "", fmt.Errorf("operation is missing or not a string")
	}

	var data map[string]interface{}
	if queryData["data"] != nil {
		data = queryData["data"].(map[string]interface{})
	}

	sortedData := make(map[string]interface{})
	var keys []string
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sortedData[k] = data[k]
	}

	dataBytes, err := json.Marshal(sortedData)
	if err != nil {
		return "", err
	}
	rawKey := fmt.Sprintf("%s:%s:%s", entity, operation, string(dataBytes))

	hash := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(hash[:]), nil
}

// buildSQLFromQuery translates the JSON query into an SQL statement.
func buildSQLFromQuery(queryData map[string]interface{}) (string, []interface{}, error) {
	entity, ok := queryData["entity"].(string)
	if !ok {
		return "", nil, fmt.Errorf("'entity' field is required")
	}
	operation, ok := queryData["operation"].(string)
	if !ok {
		return "", nil, fmt.Errorf("'operation' field is required")
	}
	var data map[string]interface{}
	if queryData["data"] != nil {
		data = queryData["data"].(map[string]interface{})
	}

	switch operation {
	case "create":
		if len(data) == 0 {
			return "", nil, fmt.Errorf("create operation requires data")
		}
		var columns []string
		var placeholders []string
		var params []interface{}
		// Sort keys for consistent query generation
		var keys []string
		for k := range data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			columns = append(columns, k)
			placeholders = append(placeholders, "?")
			params = append(params, data[k])
		}
		sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			entity, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
		return sql, params, nil

	case "read":
		selectFields, ok := queryData["select"].(string)
		if !ok {
			selectFields = "*"
		}
		if len(data) == 0 {
			sql := fmt.Sprintf("SELECT %s FROM %s", selectFields, entity)
			return sql, nil, nil
		}
		var conditions []string
		var params []interface{}
		var keys []string
		for k := range data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			conditions = append(conditions, fmt.Sprintf("%s = ?", k))
			params = append(params, data[k])
		}
		sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
			selectFields, entity, strings.Join(conditions, " AND "))
		return sql, params, nil

	case "update":
		filterValue, ok := data["id"]
		if !ok {
			return "", nil, fmt.Errorf("update operation requires an 'id' field in data for filtering")
		}
		var setClauses []string
		var params []interface{}
		var keys []string
		for k := range data {
			if k != "id" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", k))
			params = append(params, data[k])
		}
		if len(setClauses) == 0 {
			return "", nil, fmt.Errorf("update operation requires at least one field to update besides 'id'")
		}
		params = append(params, filterValue)
		sql := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?",
			entity, strings.Join(setClauses, ", "))
		return sql, params, nil

	case "delete":
		if len(data) == 0 {
			return "", nil, fmt.Errorf("delete operation requires conditions in data")
		}
		var conditions []string
		var params []interface{}
		var keys []string
		for k := range data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			conditions = append(conditions, fmt.Sprintf("%s = ?", k))
			params = append(params, data[k])
		}
		sql := fmt.Sprintf("DELETE FROM %s WHERE %s",
			entity, strings.Join(conditions, " AND "))
		return sql, params, nil

	default:
		return "", nil, fmt.Errorf("unsupported operation: %s", operation)
	}
}
