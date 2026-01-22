package lss

import (
	"sync"
	"time"
)

// IndexEntry points to a value's location in a segment
type IndexEntry struct {
	SegmentID uint64
	Offset    int64
	Timestamp time.Time
}

// Index provides O(1) key lookups using an in-memory hash map.
// The index maps keys to lists of IndexEntry (one per version).
type Index struct {
	entries map[string][]IndexEntry
	mu      sync.RWMutex
}

// NewIndex creates a new empty index
func NewIndex() *Index {
	return &Index{
		entries: make(map[string][]IndexEntry),
	}
}

// Add adds an index entry for a key
func (idx *Index) Add(key string, entry IndexEntry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries[key] = append(idx.entries[key], entry)
}

// Set replaces all entries for a key with a single entry
func (idx *Index) Set(key string, entry IndexEntry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries[key] = []IndexEntry{entry}
}

// Get returns all index entries for a key
func (idx *Index) Get(key string) []IndexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := idx.entries[key]
	if entries == nil {
		return nil
	}

	// Return a copy
	result := make([]IndexEntry, len(entries))
	copy(result, entries)
	return result
}

// GetLatest returns the most recent index entry for a key
func (idx *Index) GetLatest(key string) *IndexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := idx.entries[key]
	if len(entries) == 0 {
		return nil
	}

	// Find entry with latest timestamp
	latest := &entries[0]
	for i := 1; i < len(entries); i++ {
		if entries[i].Timestamp.After(latest.Timestamp) {
			latest = &entries[i]
		}
	}

	// Return a copy
	result := *latest
	return &result
}

// Delete removes all entries for a key
func (idx *Index) Delete(key string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	delete(idx.entries, key)
}

// GetKeysInRange returns all keys in the range [start, end)
func (idx *Index) GetKeysInRange(start, end string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var keys []string
	for key := range idx.entries {
		if key >= start && key < end {
			keys = append(keys, key)
		}
	}
	return keys
}

// GetAllKeys returns all keys in the index
func (idx *Index) GetAllKeys() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys := make([]string, 0, len(idx.entries))
	for key := range idx.entries {
		keys = append(keys, key)
	}
	return keys
}

// Len returns the number of keys in the index
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Clear removes all entries from the index
func (idx *Index) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.entries = make(map[string][]IndexEntry)
}

// RemoveSegment removes all entries pointing to a specific segment
func (idx *Index) RemoveSegment(segmentID uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for key, entries := range idx.entries {
		var remaining []IndexEntry
		for _, e := range entries {
			if e.SegmentID != segmentID {
				remaining = append(remaining, e)
			}
		}
		if len(remaining) == 0 {
			delete(idx.entries, key)
		} else {
			idx.entries[key] = remaining
		}
	}
}
