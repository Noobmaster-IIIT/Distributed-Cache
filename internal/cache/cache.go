package cache

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"distributed-cache-go/internal/cache/lru"
)

const (
	maxCacheEntries = 200
	persistenceDir  = "cache_data"
)

// CacheData represents the structure of the data to be persisted to disk.
type CacheData struct {
	Items       map[string]lru.Entry `json:"items"`
	EntityIndex map[string][]string  `json:"entity_index"`
	Order       []string             `json:"order"`
	Offset      int64                `json:"offset"`
}

// Cache is the main cache data structure.
type Cache struct {
	mu          sync.RWMutex
	lru         *lru.Cache
	entityIndex map[string]map[string]struct{} // map[entity]map[queryHash]struct{}
	offset      int64
	serverID    int32
	filePath    string
}

// NewCache creates and initializes a new cache, loading from disk if possible.
func NewCache(serverID int32) (*Cache, error) {
	filePath := fmt.Sprintf("%s/cache_server_%d.json", persistenceDir, serverID)
	c := &Cache{
		lru:         lru.New(maxCacheEntries),
		entityIndex: make(map[string]map[string]struct{}),
		serverID:    serverID,
		filePath:    filePath,
	}

	if err := os.MkdirAll(persistenceDir, 0755); err != nil {
		return nil, err
	}

	c.load()
	return c, nil
}

// Get retrieves a value by its key (query hash).
func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, found := c.lru.Get(key)
	if !found {
		return "", false
	}
	return val.(string), true
}

// Set adds or updates a value in the cache.
func (c *Cache) Set(key, value, entity string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lru.Set(key, value)
	c.offset++

	if entity != "" {
		if _, ok := c.entityIndex[entity]; !ok {
			c.entityIndex[entity] = make(map[string]struct{})
		}
		c.entityIndex[entity][key] = struct{}{}
	}
}

// InvalidateEntity removes all cache entries associated with a given entity.
func (c *Cache) InvalidateEntity(entity string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hashes, ok := c.entityIndex[entity]
	if !ok {
		return
	}

	log.Printf("CacheServer %d: Invalidating %d entries for entity '%s'", c.serverID, len(hashes), entity)
	for hash := range hashes {
		c.lru.Remove(hash)
	}
	delete(c.entityIndex, entity)
	c.offset++ // Invalidation is a replicated event
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lru.Len()
}

// Offset returns the current replication offset.
func (c *Cache) Offset() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.offset
}

// Persist saves the current cache state to a file.
func (c *Cache) Persist() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	items, order := c.lru.GetAll()
	entityIndex := make(map[string][]string)
	for entity, hashes := range c.entityIndex {
		for hash := range hashes {
			entityIndex[entity] = append(entityIndex[entity], hash)
		}
	}

	data := CacheData{
		Items:       items,
		Order:       order,
		EntityIndex: entityIndex,
		Offset:      c.offset,
	}

	file, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Printf("CacheServer %d: Failed to marshal cache data for persistence: %v", c.serverID, err)
		return
	}

	if err := os.WriteFile(c.filePath, file, 0644); err != nil {
		log.Printf("CacheServer %d: Failed to write persistence file: %v", c.serverID, err)
	} else {
		log.Printf("CacheServer %d: Successfully persisted cache state to %s", c.serverID, c.filePath)
	}
}

// load retrieves cache state from a file.
func (c *Cache) load() {
	c.mu.Lock()
	defer c.mu.Unlock()

	file, err := os.ReadFile(c.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("CacheServer %d: No persistence file found. Starting with empty cache.", c.serverID)
		} else {
			log.Printf("CacheServer %d: Error reading persistence file: %v", c.serverID, err)
		}
		return
	}

	var data CacheData
	if err := json.Unmarshal(file, &data); err != nil {
		log.Printf("CacheServer %d: Failed to unmarshal persistence data: %v", c.serverID, err)
		return
	}

	c.lru.Load(data.Items, data.Order)
	c.offset = data.Offset
	for entity, hashes := range data.EntityIndex {
		c.entityIndex[entity] = make(map[string]struct{})
		for _, hash := range hashes {
			c.entityIndex[entity][hash] = struct{}{}
		}
	}
	log.Printf("CacheServer %d: Successfully loaded %d items from persistence file.", c.serverID, c.lru.Len())
}
