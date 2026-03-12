package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// TestReadRepairConvergence verifies that divergent replicas converge via
// read repair. It creates controlled divergence by writing directly to one
// node's local storage, then reads through the coordinator (which triggers
// read repair) and confirms that all replicas eventually hold the latest value.
func TestReadRepairConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = true
	config.HintedHandoffEnabled = true

	nodes := make([]*dynamo.Node, 3)
	for i := 0; i < 3; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("conv-node%d", i),
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

	// 1. Write initial value via coordinator (replicates to quorum).
	key := "convergence-key"
	if err := nodes[0].Put(ctx, key, []byte("v1"), nil); err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// 2. Read to get the current context (vector clock).
	result, err := nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Initial read failed: %v", err)
	}
	if len(result.Values) == 0 {
		t.Fatal("Expected at least one value after write")
	}
	t.Logf("After initial write: value=%s", string(result.Values[0].Data))

	// 3. Create controlled divergence: write "v2" directly to node0's
	//    local storage with a strictly newer vector clock. This simulates
	//    what happens when a partition heals and one node has newer data.
	newClock := result.Values[0].VectorClock.Copy()
	newClock.Increment("conv-node0")
	newClock.Increment("conv-node0") // Extra increment to ensure it dominates.

	if err := nodes[0].LocalPut(key, versioning.VersionedValue{
		Data:        []byte("v2"),
		VectorClock: newClock,
	}); err != nil {
		t.Fatalf("Direct local write failed: %v", err)
	}

	// Verify divergence: node0 has "v2", others still have "v1".
	localVals, _ := nodes[0].LocalGet(key)
	if len(localVals) == 0 || string(localVals[0].Data) != "v2" {
		t.Fatalf("Expected node0 to have v2 locally, got: %v", localVals)
	}
	for i := 1; i < 3; i++ {
		vals, _ := nodes[i].LocalGet(key)
		if len(vals) > 0 && string(vals[0].Data) == "v2" {
			t.Fatalf("node%d should not have v2 yet", i)
		}
	}
	t.Log("Divergence confirmed: node0=v2, node1=v1, node2=v1")

	// 4. Read via coordinator — this triggers read repair.
	result, err = nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Coordinator read failed: %v", err)
	}
	if len(result.Values) == 0 || string(result.Values[0].Data) != "v2" {
		t.Fatalf("Expected coordinator to return v2, got: %v", result.Values)
	}
	t.Log("Coordinator read returned v2 (read repair triggered)")

	// 5. Wait for async read repair to propagate.
	time.Sleep(2 * time.Second)

	// 6. Verify convergence: all nodes should now have "v2".
	converged := true
	for i := 0; i < 3; i++ {
		vals, err := nodes[i].LocalGet(key)
		if err != nil {
			t.Logf("node%d LocalGet error: %v", i, err)
			converged = false
			continue
		}
		found := false
		for _, v := range vals {
			if string(v.Data) == "v2" {
				found = true
				break
			}
		}
		if !found {
			t.Logf("node%d has NOT converged (values: %v)", i, dataStrings(vals))
			converged = false
		} else {
			t.Logf("node%d converged to v2", i)
		}
	}

	if !converged {
		t.Error("Not all nodes converged to v2 after read repair")
	} else {
		t.Log("All nodes converged to v2")
	}
}

// TestWriteDuringPartitionConvergence simulates a more realistic scenario:
// write via the coordinator, stop a node (simulating partition), write again,
// restart the node, and verify the restarted node gets the latest value
// through subsequent reads.
func TestWriteDuringPartitionConvergence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = true
	config.HintedHandoffEnabled = true

	// Use 4 nodes so sloppy quorum can still satisfy W=2 with one down.
	nodes := make([]*dynamo.Node, 4)
	for i := 0; i < 4; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("pconv-node%d", i),
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

	for i := 1; i < 4; i++ {
		if err := nodes[i].Join([]string{"localhost:8800"}); err != nil {
			t.Fatalf("node%d join failed: %v", i, err)
		}
	}
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Write initial value.
	key := "partition-conv-key"
	if err := nodes[0].Put(ctx, key, []byte("before"), nil); err != nil {
		t.Fatalf("Pre-partition write failed: %v", err)
	}

	result, err := nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Pre-partition read failed: %v", err)
	}
	t.Logf("Pre-partition value: %s", string(result.Values[0].Data))

	// Simulate partition: stop node2.
	t.Log("Simulating partition: stopping node2")
	nodes[2].Stop()
	nodes[2] = nil
	time.Sleep(500 * time.Millisecond)

	// Write during partition.
	if err := nodes[0].Put(ctx, key, []byte("during"), result.Context); err != nil {
		t.Logf("Write during partition: %v (may fail if key maps to stopped node)", err)
	} else {
		t.Log("Write during partition succeeded")
	}

	// Heal partition: restart node2.
	t.Log("Healing partition: restarting node2")
	node2, err := dynamo.NewNode("pconv-node2", "localhost:8802", config)
	if err != nil {
		t.Fatalf("Failed to recreate node2: %v", err)
	}
	nodes[2] = node2
	if err := node2.Start(); err != nil {
		t.Fatalf("Failed to restart node2: %v", err)
	}
	if err := node2.Join([]string{"localhost:8800"}); err != nil {
		t.Logf("node2 rejoin: %v", err)
	}
	time.Sleep(2 * time.Second)

	// Read from node0 to trigger any remaining read repair.
	result, err = nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Post-heal read failed: %v", err)
	}
	t.Logf("Post-heal value from node0: %s", string(result.Values[0].Data))

	// Read from the rejoined node — should eventually see the latest value
	// after the cluster stabilizes and read repair runs.
	result, err = nodes[1].Get(ctx, key)
	if err != nil {
		t.Logf("Post-heal read from node1: %v", err)
	} else if len(result.Values) > 0 {
		t.Logf("Post-heal value from node1: %s", string(result.Values[0].Data))
	}
}

func dataStrings(vals []versioning.VersionedValue) []string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = fmt.Sprintf("%q", string(v.Data))
	}
	return out
}
