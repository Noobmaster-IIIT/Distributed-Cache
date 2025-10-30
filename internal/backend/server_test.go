package backend

import (
	"context"
	"os"
	"testing"

	pb "distributed-cache-go/gen/protos"

	"github.com/stretchr/testify/assert"
)

// Note: These tests require a running MySQL database configured as per the README.
// A more advanced setup would use a dedicated test database and test containers.

func TestServer_Connection(t *testing.T) {
	// Skip this test if running in a CI environment without a database.
	if os.Getenv("CI") != "" {
		t.Skip("Skipping database-dependent test in CI environment")
	}

	s, err := NewServer()
	assert.NoError(t, err, "Should be able to connect to the database")
	assert.NotNil(t, s)
	defer s.Close()
}

func TestServer_CheckHealth(t *testing.T) {
	s := &Server{} // No DB needed for this test
	resp, err := s.CheckHealth(context.Background(), &pb.HeartbeatRequest{})
	assert.NoError(t, err)
	assert.True(t, resp.Received)
}

// A full ExecuteSQL test would require setting up and tearing down test data
// in the database, which is beyond the scope of this example.
