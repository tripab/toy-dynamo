package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/replication"
	"github.com/tripab/toy-dynamo/pkg/storage"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

func newTestHintedHandoff(nodeID string) (*replication.HintedHandoff, *membership.Membership) {
	config := newMockConfig()
	mem := membership.NewMembership(nodeID, "localhost:8000", config)
	store := storage.NewMemoryStorage()
	nodeInfo := &mockNodeInfo{id: nodeID, address: "localhost:8000"}

	hh := replication.NewHintedHandoff(nodeInfo, mem, store, config)
	return hh, mem
}

func TestHintedHandoffStoreHint(t *testing.T) {
	hh, _ := newTestHintedHandoff("node1")

	hint := &replication.Hint{
		ForNode:   "node2",
		Key:       "key1",
		Value:     versioning.VersionedValue{Data: []byte("data1"), VectorClock: versioning.NewVectorClock()},
		Timestamp: time.Now(),
	}

	err := hh.StoreHint("node2", hint)
	if err != nil {
		t.Fatalf("StoreHint failed: %v", err)
	}
}

func TestHintedHandoffStoreMultipleHints(t *testing.T) {
	hh, _ := newTestHintedHandoff("node1")

	for i := 0; i < 5; i++ {
		vc := versioning.NewVectorClock()
		vc.Increment("node1")
		hint := &replication.Hint{
			ForNode:   "node2",
			Key:       fmt.Sprintf("key-%d", i),
			Value:     versioning.VersionedValue{Data: []byte("data"), VectorClock: vc},
			Timestamp: time.Now(),
		}
		err := hh.StoreHint("node2", hint)
		if err != nil {
			t.Fatalf("StoreHint %d failed: %v", i, err)
		}
	}

	for i := 0; i < 3; i++ {
		vc := versioning.NewVectorClock()
		vc.Increment("node1")
		hint := &replication.Hint{
			ForNode:   "node3",
			Key:       fmt.Sprintf("key-%d", i),
			Value:     versioning.VersionedValue{Data: []byte("data"), VectorClock: vc},
			Timestamp: time.Now(),
		}
		err := hh.StoreHint("node3", hint)
		if err != nil {
			t.Fatalf("StoreHint for node3 %d failed: %v", i, err)
		}
	}
}

func TestHintedHandoffDeliverHints_NodeDead(t *testing.T) {
	hh, mem := newTestHintedHandoff("node1")

	mem.AddMember(&membership.Member{
		NodeID:    "node2",
		Address:   "localhost:8001",
		Status:    membership.StatusDead,
		Heartbeat: 1,
		Timestamp: time.Now(),
	})

	vc := versioning.NewVectorClock()
	vc.Increment("node1")
	hint := &replication.Hint{
		ForNode:   "node2",
		Key:       "key1",
		Value:     versioning.VersionedValue{Data: []byte("data"), VectorClock: vc},
		Timestamp: time.Now(),
	}
	hh.StoreHint("node2", hint)

	// DeliverHints should skip dead nodes without panicking
	hh.DeliverHints()
}

func TestHintedHandoffDeliverHints_NodeSuspected(t *testing.T) {
	hh, mem := newTestHintedHandoff("node1")

	mem.AddMember(&membership.Member{
		NodeID:    "node2",
		Address:   "localhost:8001",
		Status:    membership.StatusSuspected,
		Heartbeat: 1,
		Timestamp: time.Now(),
	})

	vc := versioning.NewVectorClock()
	vc.Increment("node1")
	hint := &replication.Hint{
		ForNode:   "node2",
		Key:       "key1",
		Value:     versioning.VersionedValue{Data: []byte("data"), VectorClock: vc},
		Timestamp: time.Now(),
	}
	hh.StoreHint("node2", hint)

	// DeliverHints should skip suspected nodes
	hh.DeliverHints()
}

func TestHintedHandoffDeliverHints_NoRPCClient(t *testing.T) {
	hh, mem := newTestHintedHandoff("node1")

	mem.AddMember(&membership.Member{
		NodeID:    "node2",
		Address:   "localhost:8001",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Timestamp: time.Now(),
	})

	vc := versioning.NewVectorClock()
	vc.Increment("node1")
	hint := &replication.Hint{
		ForNode:   "node2",
		Key:       "key1",
		Value:     versioning.VersionedValue{Data: []byte("data"), VectorClock: vc},
		Timestamp: time.Now(),
	}
	hh.StoreHint("node2", hint)

	// Without RPC client, delivery should fail gracefully
	hh.DeliverHints()
}

func TestHintedHandoffDeliverHints_UnknownNode(t *testing.T) {
	hh, _ := newTestHintedHandoff("node1")

	vc := versioning.NewVectorClock()
	vc.Increment("node1")
	hint := &replication.Hint{
		ForNode:   "unknown-node",
		Key:       "key1",
		Value:     versioning.VersionedValue{Data: []byte("data"), VectorClock: vc},
		Timestamp: time.Now(),
	}
	hh.StoreHint("unknown-node", hint)

	// Should handle unknown nodes gracefully
	hh.DeliverHints()
}

func TestHintedHandoffSetRPCClient(t *testing.T) {
	hh, _ := newTestHintedHandoff("node1")

	// Setting nil should not panic
	hh.SetRPCClient(nil)
}

func TestHintedHandoffDeliverHints_EmptyHints(t *testing.T) {
	hh, _ := newTestHintedHandoff("node1")

	// No hints stored - should be a no-op
	hh.DeliverHints()
}
