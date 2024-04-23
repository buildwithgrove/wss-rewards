package cache

import (
	"sync"
	"time"
)

// TODO - update cache to use badger DB
// TODO - update cache to store relays to be sent for out of session nodes

// Cache provides a simple in-memory cache
type Cache[T any] struct {
	entries map[string]entry[T]
	mu      sync.RWMutex
}

type entry[T any] struct {
	value  T
	expire time.Time
}

// NewCache returns a cache struct with a window time to check for expired entries
func NewCache[T any](cleanWindow time.Duration) *Cache[T] {
	cache := &Cache[T]{
		entries: make(map[string]entry[T]),
	}
	go cache.evictExpiredEntries(cleanWindow)
	return cache
}

// evictExpiredEntries removes entries that have exceeded their TTL
// cleanWindow: Interval between removing expired entries
func (ss *Cache[T]) evictExpiredEntries(cleanWindow time.Duration) {
	ticker := time.NewTicker(cleanWindow)
	for range ticker.C {
		ss.mu.Lock()

		entries := make(map[string]entry[T])

		for key, entry := range ss.entries {
			now := time.Now()
			if now.After(entry.expire) {
				continue
			}
			entries[key] = entry
		}

		// Golang maps always growth on memory as the memory is not completely de-allocated
		// when removing entries, to avoid memory leaks, is safer to allocate the entries in a
		// new map to force the GC to remove the memory by the previous store.
		ss.entries = entries

		ss.mu.Unlock()
	}
}

func (ss *Cache[T]) Get(key string) (T, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	entry, ok := ss.entries[key]
	return entry.value, ok
}

func (ss *Cache[T]) Set(key string, value T, ttl time.Duration) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.entries[key] = entry[T]{
		value:  value,
		expire: time.Now().Add(ttl),
	}
}
