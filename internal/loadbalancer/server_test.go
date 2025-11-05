package loadbalancer

import (
	"context"
	"testing"

	pb "distributed-cache-go/gen/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// MockCacheService is a mock implementation of the CacheServiceClient.
// It MUST implement all methods of the interface to be valid.
type MockCacheService struct {
	mock.Mock
}

func (m *MockCacheService) GetResult(ctx context.Context, in *pb.CacheRequest, opts ...grpc.CallOption) (*pb.CacheResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.CacheResponse), args.Error(1)
}

func (m *MockCacheService) SetResult(ctx context.Context, in *pb.CacheSetRequest, opts ...grpc.CallOption) (*pb.CacheSetResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.CacheSetResponse), args.Error(1)
}

func (m *MockCacheService) GetLoad(ctx context.Context, in *pb.EmptyRequest, opts ...grpc.CallOption) (*pb.LoadResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.LoadResponse), args.Error(1)
}

func (m *MockCacheService) GetRole(ctx context.Context, in *pb.EmptyRequest, opts ...grpc.CallOption) (*pb.RoleResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.RoleResponse), args.Error(1)
}

func (m *MockCacheService) PromoteToPrimary(ctx context.Context, in *pb.EmptyRequest, opts ...grpc.CallOption) (*pb.PromotionResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.PromotionResponse), args.Error(1)
}

func (m *MockCacheService) RegisterReplica(ctx context.Context, in *pb.ReplicaRegisterRequest, opts ...grpc.CallOption) (*pb.ReplicaRegisterResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.ReplicaRegisterResponse), args.Error(1)
}

func (m *MockCacheService) GetSnapshot(ctx context.Context, in *pb.EmptyRequest, opts ...grpc.CallOption) (*pb.SnapshotResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.SnapshotResponse), args.Error(1)
}

func (m *MockCacheService) SetSnapshot(ctx context.Context, in *pb.SnapshotRequest, opts ...grpc.CallOption) (*pb.SnapshotResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.SnapshotResponse), args.Error(1)
}

func (m *MockCacheService) GetOffset(ctx context.Context, in *pb.EmptyRequest, opts ...grpc.CallOption) (*pb.OffsetResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.OffsetResponse), args.Error(1)
}

func (m *MockCacheService) PingWithOffset(ctx context.Context, in *pb.PingRequest, opts ...grpc.CallOption) (*pb.PingResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.PingResponse), args.Error(1)
}

func (m *MockCacheService) InvalidateEntityCache(ctx context.Context, in *pb.InvalidateEntityRequest, opts ...grpc.CallOption) (*pb.InvalidateEntityResponse, error) {
	args := m.Called(ctx, in)
	return args.Get(0).(*pb.InvalidateEntityResponse), args.Error(1)
}

// --- The Actual Test ---

func TestSetAndGetCachedData(t *testing.T) {
	s := NewServer()

	// Manually add a mock primary to the server state
	mockStub := new(MockCacheService)
	serverID := int32(1)
	clusterID := int32(0)

	s.primaries[clusterID] = primaryInfo{
		serverID: serverID,
		port:     50051,
		stub:     mockStub,
		// conn is nil as we are not making real connections
	}
	s.queryCount[serverID] = 0

	// --- Test SetCachedData ---
	key := "student:read:{\"id\":1}"
	value := `[{"id": 1, "name": "John Doe"}]`
	queryHash := hashQuery(key)

	setRequest := &pb.LoadBalancerCacheSetRequest{
		Key:       key,
		Value:     value,
		Entity:    "student",
		Operation: "read",
	}

	// Setup mock expectation for SetResult
	mockStub.On("SetResult", mock.Anything, &pb.CacheSetRequest{
		QueryHash: queryHash,
		Result:    value,
		Entity:    "student",
		Operation: "read",
	}).Return(&pb.CacheSetResponse{Success: true}, nil)

	setResp, err := s.SetCachedData(context.Background(), setRequest)
	assert.NoError(t, err)
	assert.True(t, setResp.Success)
	assert.Equal(t, serverID, s.queryMap[queryHash])
	assert.Equal(t, 1, s.queryCount[serverID])
	mockStub.AssertExpectations(t)

	// --- Test GetCachedData (Cache Hit) ---
	getRequest := &pb.LoadBalancerCacheRequest{Key: key}

	// Setup mock expectation for GetResult
	mockStub.On("GetResult", mock.Anything, &pb.CacheRequest{QueryHash: queryHash}).Return(&pb.CacheResponse{
		Found:  true,
		Result: value,
	}, nil)

	getResp, err := s.GetCachedData(context.Background(), getRequest)
	assert.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, value, getResp.Value)
	assert.Equal(t, 2, s.queryCount[serverID])
	mockStub.AssertExpectations(t)

	// --- Test GetCachedData (Cache Miss) ---
	missReq := &pb.LoadBalancerCacheRequest{Key: "non-existent-key"}
	missResp, err := s.GetCachedData(context.Background(), missReq)
	assert.NoError(t, err)
	assert.False(t, missResp.Found)
}