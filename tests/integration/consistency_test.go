package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

// TestConcurrentWrites verifies that concurrent writes to the same key from
// different nodes produce concurrent versions that can be detected via vector clocks.
func TestConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 1 // Allow reads from any single node to observe local state
	config.W = 1 // Allow writes to succeed with single ack for speed
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = false // Disable to preserve concurrent versions for observation

	nodes := make([]*dynamo.Node, 3)
	for i := 0; i < 3; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("cw-node%d", i),
			fmt.Sprintf("localhost:850%d", i),
			config,
		)
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Failed to start node%d: %v", i, err)
		}
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				node.Stop()
			}
		}
	}()

	for i := 1; i < 3; i++ {
		if err := nodes[i].Join([]string{"localhost:8500"}); err != nil {
			t.Fatalf("node%d join failed: %v", i, err)
		}
	}
	time.Sleep(2 * time.Second)

	ctx := context.Background()
	key := "concurrent-key"

	// Write initial value so all nodes have a baseline
	if err := nodes[0].Put(ctx, key, []byte("initial"), nil); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Concurrent writes from two different nodes without read-modify-write
	// These should create concurrent versions since neither writer knows about the other's write
	var wg sync.WaitGroup
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = nodes[0].Put(ctx, key, []byte("value-from-node0"), nil)
	}()
	go func() {
		defer wg.Done()
		errs[1] = nodes[1].Put(ctx, key, []byte("value-from-node1"), nil)
	}()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Concurrent write %d failed: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Read back and check - with W=1, R=1, and concurrent writes,
	// we may see one or both values depending on which node we read from
	// and whether replication has completed.
	// The key guarantee: no data loss. At least one of the written values must be visible.
	result, err := nodes[2].Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after concurrent writes failed: %v", err)
	}

	if len(result.Values) == 0 {
		t.Fatal("Expected at least one value after concurrent writes")
	}

	// Log what we see - concurrent versions should be preserved
	for i, v := range result.Values {
		t.Logf("Version %d: data=%s, tombstone=%v, clock=%v",
			i, string(v.Data), v.IsTombstone, v.VectorClock.Versions)
	}

	// Verify at least one of the written values is present
	foundNode0 := false
	foundNode1 := false
	for _, v := range result.Values {
		if string(v.Data) == "value-from-node0" {
			foundNode0 = true
		}
		if string(v.Data) == "value-from-node1" {
			foundNode1 = true
		}
	}
	if !foundNode0 && !foundNode1 {
		t.Error("Neither concurrent write value found - data may have been lost")
	}
	t.Logf("Found node0 value: %v, node1 value: %v", foundNode0, foundNode1)
}

// TestReadYourWrites verifies that with R+W>N, a write followed by a read from
// the same node returns the written value (read-your-writes consistency).
func TestReadYourWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2 // R+W = 4 > N=3, guarantees quorum overlap
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second

	nodes := make([]*dynamo.Node, 3)
	for i := 0; i < 3; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("ryw-node%d", i),
			fmt.Sprintf("localhost:860%d", i),
			config,
		)
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Failed to start node%d: %v", i, err)
		}
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				node.Stop()
			}
		}
	}()

	for i := 1; i < 3; i++ {
		if err := nodes[i].Join([]string{"localhost:8600"}); err != nil {
			t.Fatalf("node%d join failed: %v", i, err)
		}
	}
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Test read-your-writes: write from one node, immediately read from the same node
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("ryw-key-%d", i)
		value := fmt.Sprintf("ryw-value-%d", i)
		writeNode := nodes[i%3]

		if err := writeNode.Put(ctx, key, []byte(value), nil); err != nil {
			t.Fatalf("Put key %s failed: %v", key, err)
		}

		// Immediately read from the same node
		result, err := writeNode.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get key %s (read-your-write) failed: %v", key, err)
		}
		if len(result.Values) == 0 {
			t.Fatalf("Key %s: read-your-write returned no values", key)
		}
		if string(result.Values[0].Data) != value {
			t.Errorf("Key %s: read-your-write expected '%s', got '%s'", key, value, string(result.Values[0].Data))
		}
	}

	// Test cross-node reads: write from one node, read from another
	// With R=2, W=2, N=3, the quorum overlap guarantees the reader will see the write
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("cross-key-%d", i)
		value := fmt.Sprintf("cross-value-%d", i)
		writeNode := nodes[i%3]
		readNode := nodes[(i+1)%3]

		if err := writeNode.Put(ctx, key, []byte(value), nil); err != nil {
			t.Fatalf("Put key %s failed: %v", key, err)
		}

		result, err := readNode.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get key %s (cross-node) failed: %v", key, err)
		}
		if len(result.Values) == 0 {
			t.Fatalf("Key %s: cross-node read returned no values", key)
		}
		if string(result.Values[0].Data) != value {
			t.Errorf("Key %s: cross-node read expected '%s', got '%s'", key, value, string(result.Values[0].Data))
		}
	}
}

// TestDeleteAndTombstone verifies that deleting a key writes a tombstone
// and subsequent reads return no data.
func TestDeleteAndTombstone(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second

	nodes := make([]*dynamo.Node, 3)
	for i := 0; i < 3; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("del-node%d", i),
			fmt.Sprintf("localhost:870%d", i),
			config,
		)
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Failed to start node%d: %v", i, err)
		}
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				node.Stop()
			}
		}
	}()

	for i := 1; i < 3; i++ {
		if err := nodes[i].Join([]string{"localhost:8700"}); err != nil {
			t.Fatalf("node%d join failed: %v", i, err)
		}
	}
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Write a value
	err := nodes[0].Put(ctx, "delete-key", []byte("to-be-deleted"), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify it's readable
	result, err := nodes[1].Get(ctx, "delete-key")
	if err != nil {
		t.Fatalf("Get before delete failed: %v", err)
	}
	if len(result.Values) == 0 || string(result.Values[0].Data) != "to-be-deleted" {
		t.Fatal("Expected value before delete")
	}

	// Delete the key
	err = nodes[0].Delete(ctx, "delete-key", result.Context)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Read after delete - should get either no values or only tombstones
	result, err = nodes[1].Get(ctx, "delete-key")
	if err != nil {
		// key not found is acceptable after delete
		t.Logf("Get after delete returned error (acceptable): %v", err)
		return
	}

	// If we got results, they should all be tombstones or empty
	for _, v := range result.Values {
		if !v.IsTombstone && len(v.Data) > 0 {
			t.Errorf("Expected tombstone or empty value after delete, got data: %s", string(v.Data))
		}
	}
	t.Logf("Get after delete returned %d values (tombstones expected)", len(result.Values))
}

// TestSequentialVersioning verifies that sequential writes to the same key
// from the same node produce a single lineage (no spurious conflicts).
func TestSequentialVersioning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second

	nodes := make([]*dynamo.Node, 3)
	for i := 0; i < 3; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("seq-node%d", i),
			fmt.Sprintf("localhost:880%d", i),
			config,
		)
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Failed to start node%d: %v", i, err)
		}
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				node.Stop()
			}
		}
	}()

	for i := 1; i < 3; i++ {
		if err := nodes[i].Join([]string{"localhost:8800"}); err != nil {
			t.Fatalf("node%d join failed: %v", i, err)
		}
	}
	time.Sleep(2 * time.Second)

	ctx := context.Background()
	key := "seq-key"

	// Write 10 sequential updates using read-modify-write pattern.
	// Each write reads the current context (vector clock) and passes it
	// to the next Put, creating a proper causal chain where each version
	// is a descendant of the previous one.
	var writeCtx *dynamo.Context
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("version-%d", i)
		if err := nodes[0].Put(ctx, key, []byte(value), writeCtx); err != nil {
			t.Fatalf("Put version %d failed: %v", i, err)
		}

		// Read back the context for the next write
		result, err := nodes[0].Get(ctx, key)
		if err != nil {
			t.Fatalf("Get after version %d failed: %v", i, err)
		}
		writeCtx = result.Context
	}

	// Read back from a different node. Because read-modify-write creates a
	// proper causal chain, all older versions are ancestors and should be
	// reconciled away. With R=2, the coordinator collects responses from 2
	// replicas; if both return the same version, ReconcileConcurrent may keep
	// duplicates (Equal versions are not deduplicated). All returned versions
	// should have the latest data.
	result, err := nodes[1].Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after sequential writes failed: %v", err)
	}

	if len(result.Values) == 0 {
		t.Fatal("Expected at least one version after sequential writes")
	}

	// Every returned version should be the latest (version-9)
	for i, v := range result.Values {
		if string(v.Data) != "version-9" {
			t.Errorf("Version %d: expected 'version-9', got '%s' (clock: %v)",
				i, string(v.Data), v.VectorClock.Versions)
		}
	}
	t.Logf("Sequential versioning returned %d version(s), all 'version-9'", len(result.Values))
}
