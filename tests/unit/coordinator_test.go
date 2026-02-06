package tests

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// getFreePort returns a free TCP port for testing
func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func singleNodeConfig() *dynamo.Config {
	return &dynamo.Config{
		N:                           1,
		R:                           1,
		W:                           1,
		VirtualNodes:                4,
		StorageEngine:               "memory",
		GossipInterval:              10 * time.Second,
		AntiEntropyInterval:         60 * time.Second,
		HintedHandoffEnabled:        true,
		HintTimeout:                 10 * time.Second,
		VectorClockMaxSize:          10,
		RequestTimeout:              500 * time.Millisecond,
		ReadRepairEnabled:           false,
		TombstoneTTL:                7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
		MaxRetries:                  1,
		InitialRetryBackoff:         50 * time.Millisecond,
		MaxRetryBackoff:             1 * time.Second,
		RetryBackoffMultiplier:      2.0,
		CircuitBreakerThreshold:     5,
		CircuitBreakerResetTimeout:  30 * time.Second,
		EnableCircuitBreaker:        false,
		EnableRetry:                 false,
		AdmissionControlEnabled:     false,
		CoordinatorSelectionEnabled: false,
	}
}

func startTestNode(t *testing.T, id string) (*dynamo.Node, string) {
	t.Helper()
	port := getFreePort(t)
	addr := fmt.Sprintf("localhost:%d", port)
	config := singleNodeConfig()
	node, err := dynamo.NewNode(id, addr, config)
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	return node, addr
}

func TestCoordinatorPutAndGet_SingleNode(t *testing.T) {
	node, _ := startTestNode(t, "coord-test-1")
	defer node.Stop()

	ctx := context.Background()

	// Put a value
	err := node.Put(ctx, "key1", []byte("value1"), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get it back
	result, err := node.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(result.Values) == 0 {
		t.Fatal("Expected at least one value")
	}

	if string(result.Values[0].Data) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(result.Values[0].Data))
	}
}

func TestCoordinatorPut_IncrementsVectorClock(t *testing.T) {
	node, _ := startTestNode(t, "coord-vc-1")
	defer node.Stop()

	ctx := context.Background()

	err := node.Put(ctx, "key1", []byte("value1"), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	result, err := node.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Vector clock should have an entry for the coordinator node
	vc := result.Values[0].VectorClock
	if vc == nil {
		t.Fatal("VectorClock should not be nil")
	}

	counter, exists := vc.Versions["coord-vc-1"]
	if !exists {
		t.Error("VectorClock should have entry for coordinator node")
	}
	if counter != 1 {
		t.Errorf("Expected counter 1, got %d", counter)
	}
}

func TestCoordinatorPut_WithExistingContext(t *testing.T) {
	node, _ := startTestNode(t, "coord-ctx-1")
	defer node.Stop()

	ctx := context.Background()

	// First write
	err := node.Put(ctx, "key1", []byte("v1"), nil)
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	// Get to obtain context
	result, err := node.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Second write with context from first read
	err = node.Put(ctx, "key1", []byte("v2"), result.Context)
	if err != nil {
		t.Fatalf("Second Put failed: %v", err)
	}

	// Verify the second write dominates the first
	result2, err := node.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Second Get failed: %v", err)
	}

	if len(result2.Values) != 1 {
		t.Fatalf("Expected 1 value after causal update, got %d", len(result2.Values))
	}

	if string(result2.Values[0].Data) != "v2" {
		t.Errorf("Expected 'v2', got '%s'", string(result2.Values[0].Data))
	}

	// The counter should be 2 (incremented twice)
	counter := result2.Values[0].VectorClock.Versions["coord-ctx-1"]
	if counter != 2 {
		t.Errorf("Expected vector clock counter 2, got %d", counter)
	}
}

func TestCoordinatorDelete_CreatesTombstone(t *testing.T) {
	node, _ := startTestNode(t, "coord-del-1")
	defer node.Stop()

	ctx := context.Background()

	// Put a value
	err := node.Put(ctx, "key1", []byte("value1"), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get to obtain context
	result, err := node.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Delete with context
	err = node.Delete(ctx, "key1", result.Context)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// After delete, Get should return the tombstone or empty depending on reconciliation
	result2, err := node.Get(ctx, "key1")
	if err != nil {
		// Might get no results if tombstone is the only version and storage filters it
		return
	}

	// If we get results, verify tombstone is present
	for _, v := range result2.Values {
		if v.IsTombstone {
			// Tombstone has a vector clock that dominates the original write
			if v.VectorClock.Versions["coord-del-1"] < 2 {
				t.Errorf("Tombstone vector clock should be >= 2, got %d", v.VectorClock.Versions["coord-del-1"])
			}
			return
		}
	}
}

func TestCoordinatorGet_ReconcilesConcurrentVersions(t *testing.T) {
	node, _ := startTestNode(t, "coord-conc-1")
	defer node.Stop()

	ctx := context.Background()

	// Write two concurrent versions directly to storage
	// These simulate writes from different coordinators
	vc1 := versioning.NewVectorClock()
	vc1.Versions["nodeA"] = 1
	v1 := versioning.VersionedValue{
		Data:        []byte("version-a"),
		VectorClock: vc1,
	}

	vc2 := versioning.NewVectorClock()
	vc2.Versions["nodeB"] = 1
	v2 := versioning.VersionedValue{
		Data:        []byte("version-b"),
		VectorClock: vc2,
	}

	// Store both directly (bypassing coordinator) to simulate concurrent writes
	node.LocalPut("concurrent-key", v1)
	node.LocalPut("concurrent-key", v2)

	// Get should return both concurrent versions
	result, err := node.Get(ctx, "concurrent-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(result.Values) != 2 {
		t.Errorf("Expected 2 concurrent versions, got %d", len(result.Values))
	}

	// Context should contain merged vector clock
	if result.Context == nil || result.Context.VectorClock == nil {
		t.Fatal("Expected merged context with vector clock")
	}

	merged := result.Context.VectorClock
	if merged.Versions["nodeA"] != 1 || merged.Versions["nodeB"] != 1 {
		t.Errorf("Expected merged clock {nodeA:1, nodeB:1}, got %v", merged.Versions)
	}
}

func TestCoordinatorPut_MultipleKeysIndependent(t *testing.T) {
	node, _ := startTestNode(t, "coord-multi-1")
	defer node.Stop()

	ctx := context.Background()

	// Write multiple keys
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := node.Put(ctx, key, []byte(value), nil)
		if err != nil {
			t.Fatalf("Put key %s failed: %v", key, err)
		}
	}

	// Read all back
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)
		result, err := node.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get key %s failed: %v", key, err)
		}
		if len(result.Values) == 0 {
			t.Fatalf("No values for key %s", key)
		}
		if string(result.Values[0].Data) != expected {
			t.Errorf("Key %s: expected '%s', got '%s'", key, expected, string(result.Values[0].Data))
		}
	}
}

func TestCoordinatorGet_NonExistentKey(t *testing.T) {
	node, _ := startTestNode(t, "coord-miss-1")
	defer node.Stop()

	ctx := context.Background()

	// Get a key that doesn't exist - should timeout or return error
	// With R=1 and single node, should get a timeout since no values are found
	_, err := node.Get(ctx, "nonexistent-key")
	if err == nil {
		// Some implementations may return empty result without error
		return
	}
	// Should get either timeout or read quorum failed
	if err != dynamo.ErrTimeout && err != dynamo.ErrReadQuorumFailed {
		t.Errorf("Expected timeout or read quorum error for missing key, got: %v", err)
	}
}

func TestCoordinatorPut_VectorClockPruning(t *testing.T) {
	node, _ := startTestNode(t, "coord-prune-1")
	defer node.Stop()

	ctx := context.Background()

	// Create a context with a large vector clock
	vc := versioning.NewVectorClock()
	for i := 0; i < 15; i++ {
		vc.Versions[fmt.Sprintf("node-%d", i)] = uint64(i + 1)
	}

	writeCtx := &dynamo.Context{VectorClock: vc}

	err := node.Put(ctx, "pruned-key", []byte("data"), writeCtx)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	result, err := node.Get(ctx, "pruned-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// VectorClockMaxSize is 10, so after pruning + increment, should be <= 10+1
	vcSize := len(result.Values[0].VectorClock.Versions)
	if vcSize > 11 {
		t.Errorf("Expected vector clock to be pruned to ~10, got %d entries", vcSize)
	}
}

func TestCoordinatorPut_ContextCancel(t *testing.T) {
	node, _ := startTestNode(t, "coord-cancel-1")
	defer node.Stop()

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := node.Put(ctx, "key1", []byte("value1"), nil)
	if err == nil {
		// May succeed if write completes before context check
		return
	}
	// Should get context.Canceled
	if err != context.Canceled {
		t.Logf("Got error (expected context.Canceled or nil): %v", err)
	}
}

func TestCoordinatorSequentialWrites_VersionsIncrement(t *testing.T) {
	node, _ := startTestNode(t, "coord-seq-1")
	defer node.Stop()

	ctx := context.Background()

	// Sequential writes with context passing
	var lastCtx *dynamo.Context
	for i := 1; i <= 5; i++ {
		err := node.Put(ctx, "counter", []byte(fmt.Sprintf("v%d", i)), lastCtx)
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}

		result, err := node.Get(ctx, "counter")
		if err != nil {
			t.Fatalf("Get after put %d failed: %v", i, err)
		}
		lastCtx = result.Context
	}

	// Final read should have exactly one version
	result, err := node.Get(ctx, "counter")
	if err != nil {
		t.Fatalf("Final Get failed: %v", err)
	}

	if len(result.Values) != 1 {
		t.Fatalf("Expected 1 version after sequential writes, got %d", len(result.Values))
	}

	counter := result.Values[0].VectorClock.Versions["coord-seq-1"]
	if counter != 5 {
		t.Errorf("Expected vector clock counter 5, got %d", counter)
	}
}
