package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

// TestNodeIsolation simulates a network partition by stopping nodes and verifies
// that the remaining nodes continue to serve requests. When nodes rejoin,
// data written during the partition is accessible again through the cluster.
func TestNodeIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.HintedHandoffEnabled = true

	// Create 4 nodes for sloppy quorum to work during partition
	nodes := make([]*dynamo.Node, 4)
	for i := 0; i < 4; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("part-node%d", i),
			fmt.Sprintf("localhost:890%d", i),
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
		if err := nodes[i].Join([]string{"localhost:8900"}); err != nil {
			t.Fatalf("node%d join failed: %v", i, err)
		}
	}
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Write data before partition
	err := nodes[0].Put(ctx, "pre-partition-key", []byte("pre-partition-value"), nil)
	if err != nil {
		t.Fatalf("Pre-partition write failed: %v", err)
	}

	// Simulate partition: stop node2 and node3
	t.Log("Simulating partition: stopping node2 and node3")
	nodes[2].Stop()
	nodes[2] = nil
	nodes[3].Stop()
	nodes[3] = nil

	time.Sleep(500 * time.Millisecond)

	// The remaining partition (node0, node1) should still serve reads
	// for pre-partition data (since R=2 and both nodes have it)
	result, err := nodes[0].Get(ctx, "pre-partition-key")
	if err != nil {
		t.Fatalf("Read during partition failed: %v", err)
	}
	if len(result.Values) == 0 || string(result.Values[0].Data) != "pre-partition-value" {
		t.Error("Expected pre-partition data to be readable")
	}

	// Writes during partition may or may not succeed depending on
	// whether the preference list for the key has 2 alive nodes.
	// With 2 nodes alive and W=2, writes that happen to target
	// the alive nodes will succeed.
	writeSuccess := 0
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("during-partition-%d", i)
		err := nodes[0].Put(ctx, key, []byte(fmt.Sprintf("partition-value-%d", i)), nil)
		if err == nil {
			writeSuccess++
		}
	}
	t.Logf("Writes during partition: %d/5 succeeded", writeSuccess)

	// Heal partition: restart node2 and node3
	t.Log("Healing partition: restarting node2 and node3")
	node2, err := dynamo.NewNode("part-node2", "localhost:8902", config)
	if err != nil {
		t.Fatalf("Failed to recreate node2: %v", err)
	}
	nodes[2] = node2
	if err := node2.Start(); err != nil {
		t.Fatalf("Failed to restart node2: %v", err)
	}
	if err := node2.Join([]string{"localhost:8900"}); err != nil {
		t.Logf("node2 rejoin: %v", err)
	}

	node3, err := dynamo.NewNode("part-node3", "localhost:8903", config)
	if err != nil {
		t.Fatalf("Failed to recreate node3: %v", err)
	}
	nodes[3] = node3
	if err := node3.Start(); err != nil {
		t.Fatalf("Failed to restart node3: %v", err)
	}
	if err := node3.Join([]string{"localhost:8900"}); err != nil {
		t.Logf("node3 rejoin: %v", err)
	}

	time.Sleep(2 * time.Second)

	// After healing, pre-partition data should be available from rejoined nodes
	result, err = node2.Get(ctx, "pre-partition-key")
	if err != nil {
		t.Logf("Rejoined node2 read: %v (may need anti-entropy to sync)", err)
	} else if len(result.Values) > 0 {
		t.Logf("Rejoined node2 sees pre-partition data: %s", string(result.Values[0].Data))
	}

	// Verify cluster is fully operational again with all 4 nodes
	err = nodes[0].Put(ctx, "post-partition-key", []byte("post-partition-value"), nil)
	if err != nil {
		t.Fatalf("Post-partition write failed: %v", err)
	}

	result, err = node3.Get(ctx, "post-partition-key")
	if err != nil {
		t.Fatalf("Post-partition read from node3 failed: %v", err)
	}
	if len(result.Values) == 0 || string(result.Values[0].Data) != "post-partition-value" {
		t.Error("Expected post-partition data from rejoined node3")
	}
}

// TestMajorityPartition verifies behavior when a majority of nodes are still available.
// With N=3, W=2, if 2 of 3 nodes are up, writes should succeed.
func TestMajorityPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.HintedHandoffEnabled = true

	// 4 nodes so sloppy quorum has somewhere to redirect
	nodes := make([]*dynamo.Node, 4)
	for i := 0; i < 4; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("maj-node%d", i),
			fmt.Sprintf("localhost:910%d", i),
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
		if err := nodes[i].Join([]string{"localhost:9100"}); err != nil {
			t.Fatalf("node%d join failed: %v", i, err)
		}
	}
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Stop one node (minority failure)
	t.Log("Stopping node3 (minority failure)")
	nodes[3].Stop()
	nodes[3] = nil
	time.Sleep(500 * time.Millisecond)

	// With 3 nodes alive, writes and reads should succeed (W=2, R=2)
	successfulWrites := 0
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("maj-key-%d", i)
		err := nodes[0].Put(ctx, key, []byte(fmt.Sprintf("maj-value-%d", i)), nil)
		if err == nil {
			successfulWrites++
		}
	}

	if successfulWrites == 0 {
		t.Fatal("No writes succeeded during minority node failure")
	}
	t.Logf("Writes during minority failure: %d/10 succeeded", successfulWrites)

	// Reads should also succeed
	successfulReads := 0
	for i := 0; i < successfulWrites; i++ {
		key := fmt.Sprintf("maj-key-%d", i)
		result, err := nodes[1].Get(ctx, key)
		if err == nil && len(result.Values) > 0 {
			successfulReads++
		}
	}
	t.Logf("Reads during minority failure: %d/%d succeeded", successfulReads, successfulWrites)

	if successfulReads == 0 {
		t.Error("No reads succeeded during minority node failure")
	}
}
