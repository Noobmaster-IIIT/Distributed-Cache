package cache

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	pb "distributed-cache-go/gen/protos"
	"distributed-cache-go/internal/cache/lru"
)

const (
	maxCacheEntries        = 200
	persistenceDir         = "cache_data"
	replicationBacklogSize = 10
)

// ReplicationCommand holds a command for the backlog
type ReplicationCommand struct {
	Offset     int64
	SetRequest *pb.CacheSetRequest
}

// CacheData represents the structure of the data to be persisted to disk.
type CacheData struct {
	Items       map[string]lru.Entry `json:"items"`
	EntityIndex map[string][]string  `json:"entity_index"`
	Order       []string             `json:"order"`
	Offset      int64                `json:"offset"`
}

// Cache is the main cache data structure.
type Cache struct {
	mu                 sync.RWMutex
	lru                *lru.Cache
	entityIndex        map[string]map[string]struct{}
	offset             int64
	serverID           int32
	filePath           string
	replicationBacklog []ReplicationCommand // The backlog of recent commands
}

// NewCache creates and initializes a new cache, loading from disk if possible.
func NewCache(serverID int32) (*Cache, error) {
	filePath := fmt.Sprintf("%s/cache_server_%d.json", persistenceDir, serverID)
	c := &Cache{
		lru:                lru.New(maxCacheEntries),
		entityIndex:        make(map[string]map[string]struct{}),
		serverID:           serverID,
		filePath:           filePath,
		replicationBacklog: make([]ReplicationCommand, 0, replicationBacklogSize),
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

	setRequest := &pb.CacheSetRequest{
		QueryHash: key,
		Result:    value,
		Entity:    entity,
		Operation: "read",
	}
	c.addToBacklog(c.offset, setRequest)
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
	c.offset++

	setRequest := &pb.CacheSetRequest{
		Entity:    entity,
		Operation: "invalidate",
	}
	c.addToBacklog(c.offset, setRequest)
}

// addToBacklog is a helper to manage the backlog
func (c *Cache) addToBacklog(offset int64, req *pb.CacheSetRequest) {
	cmd := ReplicationCommand{
		Offset:     offset,
		SetRequest: req,
	}
	c.replicationBacklog = append(c.replicationBacklog, cmd)

	if len(c.replicationBacklog) > replicationBacklogSize {
		c.replicationBacklog = c.replicationBacklog[1:]
	}
}

// GetCommandsSince gets commands from the backlog since a given offset
func (c *Cache) GetCommandsSince(replicaOffset int64) ([]*pb.CacheSetRequest, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.replicationBacklog) == 0 || replicaOffset >= c.offset {
		return nil, true // Replica is up-to-date
	}

	firstOffsetInBacklog := int64(0)
	if len(c.replicationBacklog) > 0 {
		firstOffsetInBacklog = c.replicationBacklog[0].Offset
	}

	if replicaOffset < firstOffsetInBacklog {
		log.Printf("Replica offset %d is too old. First in backlog is %d.", replicaOffset, firstOffsetInBacklog)
		return nil, false // Replica is too far behind, needs full snapshot
	}

	var commands []*pb.CacheSetRequest
	for _, cmd := range c.replicationBacklog {
		if cmd.Offset > replicaOffset {
			commands = append(commands, cmd.SetRequest)
		}
	}
	return commands, true
}

// GetSnapshot gets a full snapshot of the cache
func (c *Cache) GetSnapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items, _ := c.lru.GetAll()
	return json.Marshal(items)
}

// SetSnapshot applies a full snapshot to the cache
func (c *Cache) SetSnapshot(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var items map[string]lru.Entry
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	order := make([]string, 0, len(items))
	for key := range items {
		order = append(order, key)
	}

	c.lru.Load(items, order)
	c.offset = int64(c.lru.Len())
	log.Printf("CacheServer %d: Applied snapshot, cache has %d items, offset is %d", c.serverID, c.lru.Len(), c.offset)
	return nil
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
	if c.lru.Len() == 0 {
		return // Don't bother writing empty files
	}
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
		log.Printf("CacheServer %d: Failed to marshal cache data: %v", c.serverID, err)
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
