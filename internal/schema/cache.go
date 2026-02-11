package schema

import (
	"container/list"
	"sync"
)

// LRUCache is a thread-safe LRU cache for schemas.
type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	cache    map[Key]*list.Element
	order    *list.List
}

type cacheEntry struct {
	key    Key
	schema *Schema
}

// NewLRUCache creates a new LRU cache with the given capacity.
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[Key]*list.Element),
		order:    list.New(),
	}
}

// Get retrieves a schema from cache. Returns nil if not found.
func (c *LRUCache) Get(key Key) *Schema {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.cache[key]
	if !exists {
		return nil
	}

	// Move to front (most recently used)
	c.order.MoveToFront(elem)
	entry := elem.Value.(*cacheEntry)

	// Return a copy
	copy := *entry.schema
	return &copy
}

// Put adds a schema to the cache, evicting the least recently used if full.
func (c *LRUCache) Put(schema *Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := schema.Key()

	// If already exists, update and move to front
	if elem, exists := c.cache[key]; exists {
		c.order.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		copy := *schema
		entry.schema = &copy
		return
	}

	// Evict if at capacity
	if c.order.Len() >= c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			entry := oldest.Value.(*cacheEntry)
			delete(c.cache, entry.key)
			c.order.Remove(oldest)
		}
	}

	// Add new entry
	copy := *schema
	entry := &cacheEntry{key: key, schema: &copy}
	elem := c.order.PushFront(entry)
	c.cache[key] = elem
}

// Invalidate removes a schema from the cache.
func (c *LRUCache) Invalidate(key Key) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, exists := c.cache[key]
	if !exists {
		return
	}

	delete(c.cache, key)
	c.order.Remove(elem)
}

// Clear removes all entries from the cache.
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[Key]*list.Element)
	c.order = list.New()
}
