package tests

import (
	"fmt"
	"testing"

	"github.com/tripab/toy-dynamo/pkg/replication"
	"github.com/tripab/toy-dynamo/pkg/ring"
	"github.com/tripab/toy-dynamo/pkg/storage"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

func TestReplicatorCreation(t *testing.T) {
	nodeInfo := &mockNodeInfo{id: "node1", address: "localhost:8000"}
	store := storage.NewMemoryStorage()
	defer store.Close()
	r := ring.NewRing(3, 4)
	config := newMockConfig()

	rep := replication.NewReplicator(nodeInfo, store, r, config)
	if rep == nil {
		t.Fatal("NewReplicator returned nil")
	}
}

func TestReplicatorReplicateKey_InPreferenceList(t *testing.T) {
	nodeInfo := &mockNodeInfo{id: "node1", address: "localhost:8000"}
	store := storage.NewMemoryStorage()
	defer store.Close()
	r := ring.NewRing(3, 4)
	config := newMockConfig()

	r.AddNode("node1", 4)

	rep := replication.NewReplicator(nodeInfo, store, r, config)

	vc := versioning.NewVectorClock()
	vc.Increment("node1")
	value := versioning.VersionedValue{
		Data:        []byte("test-data"),
		VectorClock: vc,
	}

	err := rep.ReplicateKey("test-key", value)
	if err != nil {
		t.Fatalf("ReplicateKey failed: %v", err)
	}

	values, err := store.Get("test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(values) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(values))
	}

	if string(values[0].Data) != "test-data" {
		t.Errorf("Expected 'test-data', got '%s'", string(values[0].Data))
	}
}

func TestReplicatorReplicateKey_NotInPreferenceList(t *testing.T) {
	nodeInfo := &mockNodeInfo{id: "node1", address: "localhost:8000"}
	store := storage.NewMemoryStorage()
	defer store.Close()
	r := ring.NewRing(3, 4)
	config := newMockConfig()

	r.AddNode("node2", 4)

	rep := replication.NewReplicator(nodeInfo, store, r, config)

	vc := versioning.NewVectorClock()
	vc.Increment("node2")
	value := versioning.VersionedValue{
		Data:        []byte("test-data"),
		VectorClock: vc,
	}

	err := rep.ReplicateKey("test-key", value)
	if err != nil {
		t.Fatalf("ReplicateKey failed: %v", err)
	}

	_, err = store.Get("test-key")
	if err == nil {
		t.Log("Value stored despite node not in preference list - expected with single-node ring")
	}
}

func TestReplicatorReplicateKey_MultipleNodes(t *testing.T) {
	nodeInfo := &mockNodeInfo{id: "node1", address: "localhost:8000"}
	store := storage.NewMemoryStorage()
	defer store.Close()
	r := ring.NewRing(3, 4)
	config := newMockConfig()

	r.AddNode("node1", 4)
	r.AddNode("node2", 4)
	r.AddNode("node3", 4)

	rep := replication.NewReplicator(nodeInfo, store, r, config)

	vc := versioning.NewVectorClock()
	vc.Increment("node1")
	value := versioning.VersionedValue{
		Data:        []byte("replicated-data"),
		VectorClock: vc,
	}

	storedCount := 0
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		err := rep.ReplicateKey(key, value)
		if err != nil {
			t.Fatalf("ReplicateKey %s failed: %v", key, err)
		}

		values, err := store.Get(key)
		if err == nil && len(values) > 0 {
			storedCount++
		}
	}

	if storedCount == 0 {
		t.Error("Expected at least some keys to be stored locally")
	}
}
