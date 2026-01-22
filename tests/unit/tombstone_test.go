package tests

import (
	"testing"

	"github.com/tripab/toy-dynamo/pkg/storage"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

func TestTombstoneInMemoryStorage(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Write a regular value
	vc1 := versioning.NewVectorClock()
	vc1.Increment("node1")

	err := store.Put("key1", versioning.VersionedValue{
		Data:        []byte("value1"),
		VectorClock: vc1,
		IsTombstone: false,
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Write a tombstone (delete)
	vc2 := vc1.Copy()
	vc2.Increment("node1")

	err = store.Put("key1", versioning.VersionedValue{
		Data:        nil,
		VectorClock: vc2,
		IsTombstone: true,
	})
	if err != nil {
		t.Fatalf("Put tombstone failed: %v", err)
	}

	// Get should return the tombstone (since it dominates the original value)
	values, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(values) != 1 {
		t.Fatalf("Expected 1 value (tombstone should dominate), got %d", len(values))
	}

	if !values[0].IsTombstone {
		t.Error("Expected tombstone, got regular value")
	}
}

func TestConcurrentTombstoneAndWrite(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Write a value on node1
	vc1 := versioning.NewVectorClock()
	vc1.Increment("node1")

	err := store.Put("key1", versioning.VersionedValue{
		Data:        []byte("value1"),
		VectorClock: vc1,
		IsTombstone: false,
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Concurrent: node2 writes a value (without seeing node1's write)
	vc2 := versioning.NewVectorClock()
	vc2.Increment("node2")

	err = store.Put("key1", versioning.VersionedValue{
		Data:        []byte("value2"),
		VectorClock: vc2,
		IsTombstone: false,
	})
	if err != nil {
		t.Fatalf("Put concurrent value failed: %v", err)
	}

	// Both values should exist (they are concurrent)
	values, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(values) != 2 {
		t.Fatalf("Expected 2 concurrent values, got %d", len(values))
	}

	// Now node1 deletes (based on its version)
	vc3 := vc1.Copy()
	vc3.Increment("node1")

	err = store.Put("key1", versioning.VersionedValue{
		Data:        nil,
		VectorClock: vc3,
		IsTombstone: true,
	})
	if err != nil {
		t.Fatalf("Put tombstone failed: %v", err)
	}

	// Now we should have: tombstone from node1 (dominates vc1) and value2 from node2
	values, err = store.Get("key1")
	if err != nil {
		t.Fatalf("Get after tombstone failed: %v", err)
	}

	// Two concurrent versions: tombstone and value2
	if len(values) != 2 {
		t.Fatalf("Expected 2 concurrent values, got %d", len(values))
	}

	hasTombstone := false
	hasValue2 := false
	for _, v := range values {
		if v.IsTombstone {
			hasTombstone = true
		}
		if string(v.Data) == "value2" {
			hasValue2 = true
		}
	}

	if !hasTombstone {
		t.Error("Expected tombstone in concurrent values")
	}
	if !hasValue2 {
		t.Error("Expected value2 in concurrent values")
	}
}

func TestVersionedValueEqualsWithTombstone(t *testing.T) {
	vc := versioning.NewVectorClock()
	vc.Increment("node1")

	v1 := versioning.VersionedValue{
		Data:        nil,
		VectorClock: vc,
		IsTombstone: true,
	}

	v2 := versioning.VersionedValue{
		Data:        nil,
		VectorClock: vc.Copy(),
		IsTombstone: true,
	}

	if !v1.Equals(&v2) {
		t.Error("Identical tombstones should be equal")
	}

	v3 := versioning.VersionedValue{
		Data:        nil,
		VectorClock: vc.Copy(),
		IsTombstone: false,
	}

	if v1.Equals(&v3) {
		t.Error("Tombstone and non-tombstone should not be equal")
	}
}
