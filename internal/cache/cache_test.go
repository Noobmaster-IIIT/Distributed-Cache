package cache

import (
	"os"
	"testing"
     "distributed-cache-go/internal/cache/lru"
	"github.com/stretchr/testify/assert"
)

func TestCacheSetAndGet(t *testing.T) {
	c, err := NewCache(999) // Use a test-specific server ID
	assert.NoError(t, err)
	defer os.Remove(c.filePath) // Clean up test file

	key := "test_key"
	value := "test_value"
	entity := "test_entity"

	// Test Set
	c.Set(key, value, entity)
	assert.Equal(t, 1, c.Len())
	assert.Equal(t, int64(1), c.Offset())

	// Test Get
	retrievedVal, found := c.Get(key)
	assert.True(t, found)
	assert.Equal(t, value, retrievedVal)
}

func TestCacheLRUEviction(t *testing.T) {
	// Create a cache with a small size for testing eviction
	smallCache := &Cache{
		lru:      lru.New(20), // Max size of 2
		filePath: "test_evict.json",
	}
	defer os.Remove(smallCache.filePath)

	smallCache.Set("key1", "val1", "")
	smallCache.Set("key2", "val2", "")
	assert.Equal(t, 2, smallCache.Len())

	// This should evict key1
	smallCache.Set("key3", "val3", "")
	assert.Equal(t, 2, smallCache.Len())

	_, found1 := smallCache.Get("key1")
	_, found2 := smallCache.Get("key2")
	_, found3 := smallCache.Get("key3")

	assert.False(t, found1, "key1 should have been evicted")
	assert.True(t, found2)
	assert.True(t, found3)
}

func TestCachePersistence(t *testing.T) {
	serverID := int32(998)
	c1, err := NewCache(serverID)
	assert.NoError(t, err)
	defer os.Remove(c1.filePath)

	c1.Set("persist_key1", "persist_val1", "persist_entity")
	c1.Set("persist_key2", "persist_val2", "persist_entity")
	c1.Persist()

	// Create a new cache instance for the same server ID, which should load the data
	c2, err := NewCache(serverID)
	assert.NoError(t, err)

	assert.Equal(t, c1.Len(), c2.Len())
	assert.Equal(t, c1.Offset(), c2.Offset())

	val, found := c2.Get("persist_key1")
	assert.True(t, found)
	assert.Equal(t, "persist_val1", val)
}

func TestInvalidateEntity(t *testing.T) {
	c, err := NewCache(997)
	assert.NoError(t, err)
	defer os.Remove(c.filePath)

	entity := "student"
	c.Set("hash1", "val1", entity)
	c.Set("hash2", "val2", entity)
	c.Set("hash3", "val3", "other_entity")

	assert.Equal(t, 3, c.Len())
	c.InvalidateEntity(entity)
	assert.Equal(t, 1, c.Len())

	_, found1 := c.Get("hash1")
	_, found2 := c.Get("hash2")
	_, found3 := c.Get("hash3")

	assert.False(t, found1)
	assert.False(t, found2)
	assert.True(t, found3) // Should still exist
}
