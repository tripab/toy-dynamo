package storage

import (
	"sync"
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// TombstoneCompactor removes tombstones older than TTL from storage.
// This completes the tombstone lifecycle management:
// 1. Delete creates a tombstone with vector clock for causality
// 2. Tombstone replicates to all nodes via normal write path
// 3. After TTL, tombstone is removed during compaction
type TombstoneCompactor struct {
	storage Storage
	ttl     time.Duration
	mu      sync.Mutex
}

// NewTombstoneCompactor creates a new tombstone compactor
func NewTombstoneCompactor(storage Storage, ttl time.Duration) *TombstoneCompactor {
	return &TombstoneCompactor{
		storage: storage,
		ttl:     ttl,
	}
}

// Compact removes tombstones older than TTL from storage.
// It iterates all keys and removes entries where all versions
// are tombstones older than the TTL cutoff.
func (tc *TombstoneCompactor) Compact() (int, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	keys, err := tc.storage.GetAllKeys()
	if err != nil {
		return 0, err
	}

	now := time.Now()
	cutoff := now.Add(-tc.ttl)
	removedCount := 0

	for _, key := range keys {
		values, err := tc.storage.Get(key)
		if err != nil {
			// Key might have been deleted concurrently
			continue
		}

		if len(values) == 0 {
			continue
		}

		// Check if all values are old tombstones that can be removed
		allOldTombstones := true
		for _, v := range values {
			if !v.IsTombstone {
				allOldTombstones = false
				break
			}
			// Check tombstone age using vector clock timestamp
			if v.VectorClock != nil && v.VectorClock.Timestamp.After(cutoff) {
				// Tombstone is too recent, keep it
				allOldTombstones = false
				break
			}
		}

		if allOldTombstones {
			// All versions are old tombstones, safe to remove the key entirely
			if err := tc.storage.Delete(key); err == nil {
				removedCount++
			}
		}
	}

	return removedCount, nil
}

// CompactKey compacts a single key, removing old tombstone versions.
// Returns true if the key was fully removed (all versions were old tombstones).
func (tc *TombstoneCompactor) CompactKey(key string) (bool, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	values, err := tc.storage.Get(key)
	if err != nil {
		return false, err
	}

	if len(values) == 0 {
		return false, nil
	}

	now := time.Now()
	cutoff := now.Add(-tc.ttl)

	// Check if all values are old tombstones
	allOldTombstones := true
	for _, v := range values {
		if !v.IsTombstone {
			allOldTombstones = false
			break
		}
		if v.VectorClock != nil && v.VectorClock.Timestamp.After(cutoff) {
			allOldTombstones = false
			break
		}
	}

	if allOldTombstones {
		return true, tc.storage.Delete(key)
	}

	return false, nil
}

// IsTombstoneExpired checks if a tombstone version has exceeded the TTL
func (tc *TombstoneCompactor) IsTombstoneExpired(v versioning.VersionedValue) bool {
	if !v.IsTombstone {
		return false
	}

	now := time.Now()
	cutoff := now.Add(-tc.ttl)

	if v.VectorClock == nil {
		// No timestamp, consider it expired
		return true
	}

	return v.VectorClock.Timestamp.Before(cutoff)
}
