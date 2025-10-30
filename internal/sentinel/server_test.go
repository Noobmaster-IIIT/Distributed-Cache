package sentinel

import (
	"context"
	"testing"

	pb "distributed-cache-go/gen/protos"

	"github.com/stretchr/testify/assert"
)

func TestRegisterServer(t *testing.T) {
	s := NewServer()
	ctx := context.Background()

	// 1. Register the first server in a new cluster (should become primary)
	req1 := &pb.CacheServerRegistrationRequest{
		ClusterId: 0,
		ServerId:  1,
		Port:      50051,
	}
	resp1, err1 := s.RegisterServer(ctx, req1)
	assert.NoError(t, err1)
	assert.True(t, resp1.Success)
	assert.True(t, resp1.IsPrimary)
	assert.Equal(t, int32(50051), resp1.PrimaryPort)

	// Verify internal state
	assert.NotNil(t, s.clusters[0].primary)
	assert.Equal(t, int32(1), s.clusters[0].primary.serverID)
	assert.NotNil(t, s.serverHealth[1])

	// 2. Register a second server in the same cluster (should become a replica)
	req2 := &pb.CacheServerRegistrationRequest{
		ClusterId: 0,
		ServerId:  2,
		Port:      50052,
	}
	resp2, err2 := s.RegisterServer(ctx, req2)
	assert.NoError(t, err2)
	assert.True(t, resp2.Success)
	assert.False(t, resp2.IsPrimary)
	assert.Equal(t, int32(50051), resp2.PrimaryPort)

	// Verify internal state
	assert.NotNil(t, s.clusters[0].replicas[2])
	assert.Equal(t, int32(2), s.clusters[0].replicas[2].serverID)
	assert.NotNil(t, s.serverHealth[2])

	// 3. Test GetClusterReplicas
	getReq := &pb.GetClusterReplicasRequest{ClusterId: 0, ServerId: 1} // Primary requesting replicas
	getResp, err := s.GetClusterReplicas(ctx, getReq)
	assert.NoError(t, err)
	assert.Len(t, getResp.Replicas, 1)
	assert.Equal(t, int32(2), getResp.Replicas[0].ServerId)
}
