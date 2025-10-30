package lru

import (
	"container/list"
	"sync"
)

// Entry holds the key and value for a cache item.
type Entry struct {
	Key   string
	Value interface{}
}

// Cache is a thread-safe, fixed-size LRU cache.
type Cache struct {
	mu       sync.RWMutex
	maxSize  int
	ll       *list.List
	cacheMap map[string]*list.Element
}

// New creates a new LRU cache with the given max size.
func New(maxSize int) *Cache {
	return &Cache{
		maxSize:  maxSize,
		ll:       list.New(),
		cacheMap: make(map[string]*list.Element),
	}
}

// Set adds or updates an entry in the cache.
func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If the key already exists, update it and move to front.
	if ee, ok := c.cacheMap[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*Entry).Value = value
		return
	}

	// Add a new entry.
	ele := c.ll.PushFront(&Entry{Key: key, Value: value})
	c.cacheMap[key] = ele

	// Evict the oldest entry if the cache is over capacity.
	if c.ll.Len() > c.maxSize {
		c.removeOldest()
	}
}

// Get retrieves an entry from the cache.
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ee, ok := c.cacheMap[key]; ok {
		c.ll.MoveToFront(ee)
		return ee.Value.(*Entry).Value, true
	}
	return nil, false
}

// Remove deletes an entry from the cache.
func (c *Cache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ee, ok := c.cacheMap[key]; ok {
		c.removeElement(ee)
	}
}

// Len returns the current number of items in the cache.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ll.Len()
}

// GetAll is used for persistence, returning all items and their order.
func (c *Cache) GetAll() (map[string]Entry, []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	items := make(map[string]Entry)
	order := make([]string, 0, c.ll.Len())
	for e := c.ll.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*Entry)
		items[entry.Key] = *entry
		order = append(order, entry.Key)
	}
	return items, order
}

// Load is used for persistence to load data into the cache.
func (c *Cache) Load(items map[string]Entry, order []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Reconstruct the cache from the persisted order and items.
	for _, key := range order {
		if entry, ok := items[key]; ok {
			ele := c.ll.PushBack(&Entry{Key: entry.Key, Value: entry.Value})
			c.cacheMap[entry.Key] = ele
		}
	}
}

// removeOldest removes the least recently used item from the cache.
func (c *Cache) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

// removeElement is a helper to remove a specific list element.
func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*Entry)
	delete(c.cacheMap, kv.Key)
}
