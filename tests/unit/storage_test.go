package tests

import (
	"testing"

	"github.com/tripab/toy-dynamo/pkg/storage"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

func TestMemoryStoragePutGet(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	vc := versioning.NewVectorClock()
	vc.Increment("node1")

	value := versioning.VersionedValue{
		Data:        []byte("test data"),
		VectorClock: vc,
	}

	err := store.Put("key1", value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	values, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(values) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(values))
	}

	if string(values[0].Data) != "test data" {
		t.Errorf("Expected 'test data', got '%s'", string(values[0].Data))
	}
}

func TestMemoryStorageVersionReconciliation(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	// Create sequential versions - each new version dominates the previous
	// v1: node1=1
	vc1 := versioning.NewVectorClock()
	vc1.Versions["node1"] = 1
	value1 := versioning.VersionedValue{
		Data:        []byte("version1"),
		VectorClock: vc1,
	}

	// v2: node1=2 (dominates v1)
	vc2 := versioning.NewVectorClock()
	vc2.Versions["node1"] = 2
	value2 := versioning.VersionedValue{
		Data:        []byte("version2"),
		VectorClock: vc2,
	}

	// v3: node1=3 (dominates v1 and v2)
	vc3 := versioning.NewVectorClock()
	vc3.Versions["node1"] = 3
	value3 := versioning.VersionedValue{
		Data:        []byte("version3"),
		VectorClock: vc3,
	}

	// Put all three versions
	store.Put("key", value1)
	store.Put("key", value2)
	store.Put("key", value3)

	// Should only have 1 version (v3 dominates all others)
	values, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(values) != 1 {
		t.Errorf("Expected 1 version after reconciliation, got %d", len(values))
	}

	if string(values[0].Data) != "version3" {
		t.Errorf("Expected 'version3', got '%s'", string(values[0].Data))
	}
}

func TestMemoryStorageConcurrentVersions(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	// Create concurrent versions (neither dominates the other)
	// vc1: node1=2, node2=1
	vc1 := versioning.NewVectorClock()
	vc1.Versions["node1"] = 2
	vc1.Versions["node2"] = 1
	value1 := versioning.VersionedValue{
		Data:        []byte("version-a"),
		VectorClock: vc1,
	}

	// vc2: node1=1, node2=2 (concurrent with vc1)
	vc2 := versioning.NewVectorClock()
	vc2.Versions["node1"] = 1
	vc2.Versions["node2"] = 2
	value2 := versioning.VersionedValue{
		Data:        []byte("version-b"),
		VectorClock: vc2,
	}

	store.Put("key", value1)
	store.Put("key", value2)

	// Both versions should be retained (they're concurrent)
	values, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(values) != 2 {
		t.Errorf("Expected 2 concurrent versions, got %d", len(values))
	}
}

func TestMemoryStorageSequentialUpdates(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	// Simulate 100 sequential updates - only latest should remain
	for i := 1; i <= 100; i++ {
		vc := versioning.NewVectorClock()
		vc.Versions["node1"] = uint64(i)
		value := versioning.VersionedValue{
			Data:        []byte("data"),
			VectorClock: vc,
		}
		store.Put("key", value)
	}

	values, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(values) != 1 {
		t.Errorf("Expected 1 version after 100 sequential updates, got %d", len(values))
	}

	if values[0].VectorClock.Versions["node1"] != 100 {
		t.Errorf("Expected version 100, got %d", values[0].VectorClock.Versions["node1"])
	}
}

func TestMemoryStorageDelete(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	vc := versioning.NewVectorClock()
	vc.Increment("node1")

	value := versioning.VersionedValue{
		Data:        []byte("test"),
		VectorClock: vc,
	}

	store.Put("key", value)
	store.Delete("key")

	_, err := store.Get("key")
	if err != storage.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestMemoryStorageGetRange(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	// Add keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		vc := versioning.NewVectorClock()
		vc.Increment("node1")
		value := versioning.VersionedValue{
			Data:        []byte(k + "-data"),
			VectorClock: vc,
		}
		store.Put(k, value)
	}

	// Get range [b, d)
	result, err := store.GetRange("b", "d")
	if err != nil {
		t.Fatalf("GetRange failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 keys in range [b,d), got %d", len(result))
	}

	if _, ok := result["b"]; !ok {
		t.Error("Expected key 'b' in result")
	}
	if _, ok := result["c"]; !ok {
		t.Error("Expected key 'c' in result")
	}
}

func TestMemoryStorageGetAllKeys(t *testing.T) {
	store := storage.NewMemoryStorage()
	defer store.Close()

	// Add keys
	keys := []string{"c", "a", "b"}
	for _, k := range keys {
		vc := versioning.NewVectorClock()
		vc.Increment("node1")
		value := versioning.VersionedValue{
			Data:        []byte(k),
			VectorClock: vc,
		}
		store.Put(k, value)
	}

	allKeys, err := store.GetAllKeys()
	if err != nil {
		t.Fatalf("GetAllKeys failed: %v", err)
	}

	if len(allKeys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(allKeys))
	}

	// Should be sorted
	if allKeys[0] != "a" || allKeys[1] != "b" || allKeys[2] != "c" {
		t.Errorf("Expected sorted keys [a,b,c], got %v", allKeys)
	}
}
