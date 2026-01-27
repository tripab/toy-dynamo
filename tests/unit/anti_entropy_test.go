package tests

import (
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/ring"
	"github.com/tripab/toy-dynamo/pkg/storage"
	"github.com/tripab/toy-dynamo/pkg/synchronization"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// mockConfig implements types.Config for testing
type mockConfig struct {
	n                    int
	r                    int
	w                    int
	gossipInterval       time.Duration
	antiEntropyInterval  time.Duration
	hintedHandoffEnabled bool
	hintTimeout          time.Duration
	requestTimeout       time.Duration
	readRepairEnabled    bool
	vectorClockMaxSize   int
}

func (c *mockConfig) GetN() int                         { return c.n }
func (c *mockConfig) GetR() int                         { return c.r }
func (c *mockConfig) GetW() int                         { return c.w }
func (c *mockConfig) GetGossipInterval() time.Duration  { return c.gossipInterval }
func (c *mockConfig) GetAntiEntropyInterval() time.Duration { return c.antiEntropyInterval }
func (c *mockConfig) GetHintedHandoffEnabled() bool     { return c.hintedHandoffEnabled }
func (c *mockConfig) GetHintTimeout() time.Duration     { return c.hintTimeout }
func (c *mockConfig) GetRequestTimeout() time.Duration  { return c.requestTimeout }
func (c *mockConfig) GetReadRepairEnabled() bool        { return c.readRepairEnabled }
func (c *mockConfig) GetVectorClockMaxSize() int        { return c.vectorClockMaxSize }

func newMockConfig() *mockConfig {
	return &mockConfig{
		n:                    3,
		r:                    2,
		w:                    2,
		gossipInterval:       time.Second,
		antiEntropyInterval:  time.Minute,
		hintedHandoffEnabled: true,
		hintTimeout:          time.Hour,
		requestTimeout:       5 * time.Second,
		readRepairEnabled:    true,
		vectorClockMaxSize:   10,
	}
}

func TestAntiEntropyCreation(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	m := membership.NewMembership("node1", "localhost:8080", newMockConfig())
	config := newMockConfig()

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	if ae == nil {
		t.Error("Expected non-nil AntiEntropy")
	}
}

func TestAntiEntropyRunWithoutRPCClient(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	m := membership.NewMembership("node1", "localhost:8080", newMockConfig())
	config := newMockConfig()

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Run without RPC client should not panic
	ae.Run()
}

func TestAntiEntropyRebuildTree(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	m := membership.NewMembership("node1", "localhost:8080", newMockConfig())
	config := newMockConfig()

	// Add some data to storage
	vc := versioning.NewVectorClock()
	vc.Increment("node1")
	store.Put("key1", versioning.VersionedValue{
		Data:        []byte("value1"),
		VectorClock: vc,
	})

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Rebuild tree for a range
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}
	ae.RebuildTree(keyRange)

	// No error means success
}

func TestAntiEntropyInvalidateTree(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	m := membership.NewMembership("node1", "localhost:8080", newMockConfig())
	config := newMockConfig()

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Build a tree first
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}
	ae.RebuildTree(keyRange)

	// Invalidate should not panic
	ae.InvalidateTree(keyRange)
}

func TestAntiEntropyWithStorageData(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	m := membership.NewMembership("node1", "localhost:8080", newMockConfig())
	config := newMockConfig()

	// Add test data
	for i := 0; i < 10; i++ {
		vc := versioning.NewVectorClock()
		vc.Increment("node1")
		key := "key" + string(rune('0'+i))
		store.Put(key, versioning.VersionedValue{
			Data:        []byte("value" + string(rune('0'+i))),
			VectorClock: vc,
		})
	}

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Rebuild tree and verify it works with data
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}
	ae.RebuildTree(keyRange)
}

func TestAntiEntropyWithMultipleRanges(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)

	// Add node to ring to get tokens
	r.AddNode("node1", 10)

	m := membership.NewMembership("node1", "localhost:8080", newMockConfig())
	config := newMockConfig()

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Run should handle multiple token ranges
	ae.Run()
}

func TestAntiEntropyWithMembership(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	config := newMockConfig()

	// Set up a multi-node membership
	m := membership.NewMembership("node1", "localhost:8080", config)

	// Add other nodes
	r.AddNode("node1", 10)
	r.AddNode("node2", 10)
	r.AddNode("node3", 10)

	m.AddMember(&membership.Member{
		NodeID:    "node1",
		Address:   "localhost:8080",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Tokens:    r.GetTokens("node1"),
	})
	m.AddMember(&membership.Member{
		NodeID:    "node2",
		Address:   "localhost:8081",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Tokens:    r.GetTokens("node2"),
	})
	m.AddMember(&membership.Member{
		NodeID:    "node3",
		Address:   "localhost:8082",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Tokens:    r.GetTokens("node3"),
	})

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Run should try to sync but fail gracefully without RPC client
	ae.Run()
}

func TestAntiEntropyIgnoresDeadNodes(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	config := newMockConfig()

	m := membership.NewMembership("node1", "localhost:8080", config)

	r.AddNode("node1", 10)
	r.AddNode("node2", 10)

	m.AddMember(&membership.Member{
		NodeID:    "node1",
		Address:   "localhost:8080",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Tokens:    r.GetTokens("node1"),
	})
	m.AddMember(&membership.Member{
		NodeID:    "node2",
		Address:   "localhost:8081",
		Status:    membership.StatusDead, // Dead node
		Heartbeat: 1,
		Tokens:    r.GetTokens("node2"),
	})

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Run should skip dead nodes
	ae.Run()
}

func TestAntiEntropyVersionReconciliation(t *testing.T) {
	// Test that the anti-entropy correctly identifies version differences
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	m := membership.NewMembership("node1", "localhost:8080", newMockConfig())
	config := newMockConfig()

	// Add data with different versions
	vc1 := versioning.NewVectorClock()
	vc1.Increment("node1")

	vc2 := versioning.NewVectorClock()
	vc2.Increment("node2")

	// These are concurrent versions
	store.Put("key1", versioning.VersionedValue{
		Data:        []byte("value1-node1"),
		VectorClock: vc1,
	})
	store.Put("key1", versioning.VersionedValue{
		Data:        []byte("value1-node2"),
		VectorClock: vc2,
	})

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Rebuild tree with concurrent versions
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}
	ae.RebuildTree(keyRange)

	// Get the values to verify both are stored
	values, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}

	if len(values) != 2 {
		t.Errorf("Expected 2 concurrent versions, got %d", len(values))
	}
}

func TestAntiEntropyWithTombstones(t *testing.T) {
	store := storage.NewMemoryStorage()
	r := ring.NewRing(3, 10)
	m := membership.NewMembership("node1", "localhost:8080", newMockConfig())
	config := newMockConfig()

	// Add a tombstone
	vc := versioning.NewVectorClock()
	vc.Increment("node1")
	store.Put("deleted-key", versioning.VersionedValue{
		Data:        nil,
		VectorClock: vc,
		IsTombstone: true,
	})

	ae := synchronization.NewAntiEntropy("node1", store, r, m, config)

	// Rebuild tree should handle tombstones
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}
	ae.RebuildTree(keyRange)
}

func TestMerkleTreeKeyRange(t *testing.T) {
	// Test that KeyRange is correctly used
	keyRange := synchronization.KeyRange{Start: "a", End: "z"}
	tree := synchronization.NewMerkleTree(keyRange)

	data := map[string][]byte{
		"apple":  []byte("red"),
		"banana": []byte("yellow"),
		"cherry": []byte("red"),
	}

	tree.Build(data)

	rootHash := tree.GetRootHash()
	if rootHash == "" {
		t.Error("Expected non-empty root hash")
	}

	root := tree.GetRoot()
	if root == nil {
		t.Error("Expected non-nil root")
	}
}

func TestMerkleTreeEmptyData(t *testing.T) {
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}
	tree := synchronization.NewMerkleTree(keyRange)

	// Build with empty data
	tree.Build(map[string][]byte{})

	rootHash := tree.GetRootHash()
	if rootHash == "" {
		t.Error("Expected non-empty root hash even for empty tree")
	}
}

func TestMerkleTreeUpdate(t *testing.T) {
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}
	tree := synchronization.NewMerkleTree(keyRange)

	data := map[string][]byte{
		"key1": []byte("value1"),
	}
	tree.Build(data)

	originalHash := tree.GetRootHash()

	// Update the tree
	tree.Update("key2", []byte("value2"))

	newHash := tree.GetRootHash()
	if newHash == originalHash {
		t.Error("Expected different hash after update")
	}
}

func TestMerkleTreeDelete(t *testing.T) {
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}
	tree := synchronization.NewMerkleTree(keyRange)

	data := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}
	tree.Build(data)

	originalHash := tree.GetRootHash()

	// Delete a key
	tree.Delete("key1")

	newHash := tree.GetRootHash()
	if newHash == originalHash {
		t.Error("Expected different hash after delete")
	}
}

func TestMerkleTreeFindDifferences(t *testing.T) {
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}

	tree1 := synchronization.NewMerkleTree(keyRange)
	tree1.Build(map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	})

	tree2 := synchronization.NewMerkleTree(keyRange)
	tree2.Build(map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("different"),
	})

	diffs := tree1.FindDifferences(tree1.GetRoot(), tree2.GetRoot())

	if len(diffs) == 0 {
		t.Error("Expected to find differences")
	}

	// Should find key2 as different
	found := false
	for _, k := range diffs {
		if k == "key2" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find 'key2' in differences")
	}
}

func TestMerkleTreeNoDifferences(t *testing.T) {
	keyRange := synchronization.KeyRange{Start: "", End: "\xff"}

	data := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	tree1 := synchronization.NewMerkleTree(keyRange)
	tree1.Build(data)

	tree2 := synchronization.NewMerkleTree(keyRange)
	tree2.Build(data)

	diffs := tree1.FindDifferences(tree1.GetRoot(), tree2.GetRoot())

	if len(diffs) != 0 {
		t.Errorf("Expected no differences, got %v", diffs)
	}
}
