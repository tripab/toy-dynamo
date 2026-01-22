package tests

import (
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/storage"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

func TestTombstoneCompactor_CompactOldTombstones(t *testing.T) {
	// Create memory storage
	store := storage.NewMemoryStorage()

	// Create a tombstone with an old timestamp (simulating a delete from the past)
	oldClock := versioning.NewVectorClock()
	oldClock.Increment("node1")
	oldClock.Timestamp = time.Now().Add(-8 * 24 * time.Hour) // 8 days ago

	tombstone := versioning.VersionedValue{
		Data:        nil,
		VectorClock: oldClock,
		IsTombstone: true,
	}

	// Store the old tombstone
	err := store.Put("deleted-key", tombstone)
	if err != nil {
		t.Fatalf("Failed to put tombstone: %v", err)
	}

	// Create compactor with 7-day TTL
	compactor := storage.NewTombstoneCompactor(store, 7*24*time.Hour)

	// Run compaction
	removed, err := compactor.Compact()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Should have removed 1 key
	if removed != 1 {
		t.Errorf("Expected 1 removed key, got %d", removed)
	}

	// Verify the key no longer exists
	_, err = store.Get("deleted-key")
	if err != storage.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestTombstoneCompactor_PreserveRecentTombstones(t *testing.T) {
	// Create memory storage
	store := storage.NewMemoryStorage()

	// Create a tombstone with a recent timestamp
	recentClock := versioning.NewVectorClock()
	recentClock.Increment("node1")
	// Default timestamp is time.Now()

	tombstone := versioning.VersionedValue{
		Data:        nil,
		VectorClock: recentClock,
		IsTombstone: true,
	}

	// Store the recent tombstone
	err := store.Put("recently-deleted-key", tombstone)
	if err != nil {
		t.Fatalf("Failed to put tombstone: %v", err)
	}

	// Create compactor with 7-day TTL
	compactor := storage.NewTombstoneCompactor(store, 7*24*time.Hour)

	// Run compaction
	removed, err := compactor.Compact()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Should NOT have removed any keys (tombstone is too recent)
	if removed != 0 {
		t.Errorf("Expected 0 removed keys, got %d", removed)
	}

	// Verify the key still exists
	values, err := store.Get("recently-deleted-key")
	if err != nil {
		t.Fatalf("Key should still exist: %v", err)
	}
	if len(values) != 1 {
		t.Errorf("Expected 1 value, got %d", len(values))
	}
	if !values[0].IsTombstone {
		t.Error("Value should be a tombstone")
	}
}

func TestTombstoneCompactor_PreserveNonTombstones(t *testing.T) {
	// Create memory storage
	store := storage.NewMemoryStorage()

	// Create a regular value
	clock := versioning.NewVectorClock()
	clock.Increment("node1")

	value := versioning.VersionedValue{
		Data:        []byte("test-value"),
		VectorClock: clock,
		IsTombstone: false,
	}

	// Store the value
	err := store.Put("regular-key", value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Create compactor with very short TTL (to ensure it would remove old tombstones)
	compactor := storage.NewTombstoneCompactor(store, 1*time.Nanosecond)

	// Run compaction
	removed, err := compactor.Compact()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Should NOT have removed any keys (it's not a tombstone)
	if removed != 0 {
		t.Errorf("Expected 0 removed keys, got %d", removed)
	}

	// Verify the key still exists
	values, err := store.Get("regular-key")
	if err != nil {
		t.Fatalf("Key should still exist: %v", err)
	}
	if len(values) != 1 {
		t.Errorf("Expected 1 value, got %d", len(values))
	}
	if string(values[0].Data) != "test-value" {
		t.Errorf("Expected 'test-value', got '%s'", string(values[0].Data))
	}
}

func TestTombstoneCompactor_MixedValues(t *testing.T) {
	// Create memory storage
	store := storage.NewMemoryStorage()

	// Create an old tombstone
	oldClock := versioning.NewVectorClock()
	oldClock.Increment("node1")
	oldClock.Timestamp = time.Now().Add(-10 * 24 * time.Hour) // 10 days ago

	oldTombstone := versioning.VersionedValue{
		Data:        nil,
		VectorClock: oldClock,
		IsTombstone: true,
	}

	// Create a recent tombstone
	recentClock := versioning.NewVectorClock()
	recentClock.Increment("node1")
	// Default timestamp is time.Now()

	recentTombstone := versioning.VersionedValue{
		Data:        nil,
		VectorClock: recentClock,
		IsTombstone: true,
	}

	// Create a regular value
	valueClock := versioning.NewVectorClock()
	valueClock.Increment("node2")

	regularValue := versioning.VersionedValue{
		Data:        []byte("regular"),
		VectorClock: valueClock,
		IsTombstone: false,
	}

	// Store all values
	store.Put("old-tombstone-key", oldTombstone)
	store.Put("recent-tombstone-key", recentTombstone)
	store.Put("regular-key", regularValue)

	// Create compactor with 7-day TTL
	compactor := storage.NewTombstoneCompactor(store, 7*24*time.Hour)

	// Run compaction
	removed, err := compactor.Compact()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Should have removed only the old tombstone
	if removed != 1 {
		t.Errorf("Expected 1 removed key, got %d", removed)
	}

	// Verify old tombstone is gone
	_, err = store.Get("old-tombstone-key")
	if err != storage.ErrKeyNotFound {
		t.Errorf("Old tombstone should be removed, got err: %v", err)
	}

	// Verify recent tombstone still exists
	values, err := store.Get("recent-tombstone-key")
	if err != nil {
		t.Fatalf("Recent tombstone should exist: %v", err)
	}
	if !values[0].IsTombstone {
		t.Error("Should be a tombstone")
	}

	// Verify regular value still exists
	values, err = store.Get("regular-key")
	if err != nil {
		t.Fatalf("Regular value should exist: %v", err)
	}
	if string(values[0].Data) != "regular" {
		t.Errorf("Expected 'regular', got '%s'", string(values[0].Data))
	}
}

func TestTombstoneCompactor_IsTombstoneExpired(t *testing.T) {
	store := storage.NewMemoryStorage()
	compactor := storage.NewTombstoneCompactor(store, 7*24*time.Hour)

	// Test old tombstone
	oldClock := versioning.NewVectorClock()
	oldClock.Timestamp = time.Now().Add(-8 * 24 * time.Hour)
	oldTombstone := versioning.VersionedValue{
		Data:        nil,
		VectorClock: oldClock,
		IsTombstone: true,
	}
	if !compactor.IsTombstoneExpired(oldTombstone) {
		t.Error("Old tombstone should be expired")
	}

	// Test recent tombstone
	recentClock := versioning.NewVectorClock()
	recentTombstone := versioning.VersionedValue{
		Data:        nil,
		VectorClock: recentClock,
		IsTombstone: true,
	}
	if compactor.IsTombstoneExpired(recentTombstone) {
		t.Error("Recent tombstone should not be expired")
	}

	// Test non-tombstone (should never be "expired")
	nonTombstone := versioning.VersionedValue{
		Data:        []byte("data"),
		VectorClock: oldClock,
		IsTombstone: false,
	}
	if compactor.IsTombstoneExpired(nonTombstone) {
		t.Error("Non-tombstone should not be marked as expired")
	}
}

func TestTombstoneCompactor_CompactKey(t *testing.T) {
	store := storage.NewMemoryStorage()
	compactor := storage.NewTombstoneCompactor(store, 7*24*time.Hour)

	// Store an old tombstone
	oldClock := versioning.NewVectorClock()
	oldClock.Timestamp = time.Now().Add(-10 * 24 * time.Hour)
	store.Put("test-key", versioning.VersionedValue{
		Data:        nil,
		VectorClock: oldClock,
		IsTombstone: true,
	})

	// Compact single key
	removed, err := compactor.CompactKey("test-key")
	if err != nil {
		t.Fatalf("CompactKey failed: %v", err)
	}
	if !removed {
		t.Error("Key should have been removed")
	}

	// Verify key is gone
	_, err = store.Get("test-key")
	if err != storage.ErrKeyNotFound {
		t.Error("Key should have been removed")
	}
}

func TestTombstoneCompactor_EmptyStorage(t *testing.T) {
	store := storage.NewMemoryStorage()
	compactor := storage.NewTombstoneCompactor(store, 7*24*time.Hour)

	// Compact empty storage should work without error
	removed, err := compactor.Compact()
	if err != nil {
		t.Fatalf("Compaction of empty storage failed: %v", err)
	}
	if removed != 0 {
		t.Errorf("Expected 0 removed from empty storage, got %d", removed)
	}
}
