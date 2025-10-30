package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	pb "distributed-cache-go/gen/protos"

	_ "github.com/go-sql-driver/mysql"
)

const (
	dbUser     = "cacheuser"
	dbPassword = "1234"
	dbName     = "university"
)

// Server implements the backend and heartbeat gRPC services.
type Server struct {
	pb.UnimplementedBackendServiceServer
	pb.UnimplementedHeartbeatServiceServer
	db *sql.DB
}

// NewServer creates a new Backend server and initializes its database connection pool.
func NewServer() (*Server, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(127.0.0.1:3306)/%s", dbUser, dbPassword, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure the connection pool
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(10)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("Successfully connected to the MySQL database.")
	return &Server{db: db}, nil
}

// Close closes the database connection pool.
func (s *Server) Close() {
	if s.db != nil {
		s.db.Close()
	}
}

// ExecuteSQL handles SQL execution requests from the gateway.
func (s *Server) ExecuteSQL(ctx context.Context, req *pb.BackendRequest) (*pb.BackendResponse, error) {
	var paramsMap map[string][]interface{}
	if err := json.Unmarshal([]byte(req.Params), &paramsMap); err != nil {
		return &pb.BackendResponse{Result: `{"error": "invalid params format"}`}, nil
	}
	params := paramsMap["params"]

	log.Printf("Backend received SQL: %s with params: %v", req.SqlQuery, params)

	// Check if it's a SELECT query
	isSelect := strings.HasPrefix(strings.ToLower(strings.TrimSpace(req.SqlQuery)), "select")

	if isSelect {
		return s.executeSelect(ctx, req.SqlQuery, params...)
	}
	return s.executeModify(ctx, req.SqlQuery, params...)
}

// executeSelect handles SELECT queries and returns rows as JSON.
func (s *Server) executeSelect(ctx context.Context, query string, args ...interface{}) (*pb.BackendResponse, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return &pb.BackendResponse{Result: fmt.Sprintf(`{"error": "database error: %v"}`, err)}, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return &pb.BackendResponse{Result: fmt.Sprintf(`{"error": "failed to get columns: %v"}`, err)}, nil
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return &pb.BackendResponse{Result: fmt.Sprintf(`{"error": "failed to scan row: %v"}`, err)}, nil
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				entry[col] = string(b)
			} else {
				entry[col] = val
			}
		}
		results = append(results, entry)
	}

	jsonResult, err := json.Marshal(results)
	if err != nil {
		return &pb.BackendResponse{Result: fmt.Sprintf(`{"error": "failed to marshal result: %v"}`, err)}, nil
	}
	return &pb.BackendResponse{Result: string(jsonResult)}, nil
}

// executeModify handles INSERT, UPDATE, DELETE queries.
func (s *Server) executeModify(ctx context.Context, query string, args ...interface{}) (*pb.BackendResponse, error) {
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return &pb.BackendResponse{Result: fmt.Sprintf(`{"error": "database error: %v"}`, err)}, nil
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return &pb.BackendResponse{Result: fmt.Sprintf(`{"error": "failed to get affected rows: %v"}`, err)}, nil
	}

	response := map[string]int64{"affected_rows": rowsAffected}
	jsonResult, _ := json.Marshal(response)
	return &pb.BackendResponse{Result: string(jsonResult)}, nil
}

// CheckHealth handles heartbeat requests from the gateway.
func (s *Server) CheckHealth(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// Simple ACK to indicate the service is alive.
	return &pb.HeartbeatResponse{Received: true}, nil
}
