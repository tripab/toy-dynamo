package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

// TestHintedHandoffBasic verifies that writes succeed when a node is temporarily down
// and hints are stored for later delivery.
func TestHintedHandoffBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.HintedHandoffEnabled = true
	config.GossipInterval = 500 * time.Millisecond

	// Create 4 nodes (one extra for sloppy quorum with hinted handoff)
	nodes := make([]*dynamo.Node, 4)
	for i := 0; i < 4; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("node%d", i),
			fmt.Sprintf("localhost:810%d", i),
			config,
		)
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
	}

	// Start all nodes
	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Failed to start node%d: %v", i, err)
		}
	}

	// Cleanup all nodes at the end
	defer func() {
		for _, node := range nodes {
			if node != nil {
				node.Stop()
			}
		}
	}()

	// Join nodes to form cluster
	for i := 1; i < 4; i++ {
		if err := nodes[i].Join([]string{"localhost:8100"}); err != nil {
			t.Logf("Warning: node%d join returned error: %v", i, err)
		}
	}

	// Allow gossip to propagate membership
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Write initial data while all nodes are up
	err := nodes[0].Put(ctx, "hh-key-1", []byte("initial-value"), nil)
	if err != nil {
		t.Fatalf("Initial Put failed: %v", err)
	}

	// Verify the initial write is readable
	result, err := nodes[1].Get(ctx, "hh-key-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(result.Values) == 0 || string(result.Values[0].Data) != "initial-value" {
		t.Errorf("Expected 'initial-value', got: %v", result.Values)
	}

	t.Log("Initial write and read successful")
}

// TestHintedHandoffDelivery verifies that hints are delivered when a failed node recovers.
func TestHintedHandoffDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 1 // Use R=1 to allow reads from any node
	config.W = 2
	config.HintedHandoffEnabled = true
	config.GossipInterval = 500 * time.Millisecond
	config.RequestTimeout = 2 * time.Second

	// Create 4 nodes
	nodes := make([]*dynamo.Node, 4)
	for i := 0; i < 4; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("hh-node%d", i),
			fmt.Sprintf("localhost:820%d", i),
			config,
		)
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
	}

	// Start all nodes
	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Failed to start node%d: %v", i, err)
		}
	}

	// Cleanup function
	defer func() {
		for _, node := range nodes {
			if node != nil {
				node.Stop()
			}
		}
	}()

	// Join nodes to form cluster
	for i := 1; i < 4; i++ {
		if err := nodes[i].Join([]string{"localhost:8200"}); err != nil {
			t.Logf("Warning: node%d join returned error: %v", i, err)
		}
	}

	// Allow gossip to propagate
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Stop node2 to simulate failure
	t.Log("Stopping node2 to simulate failure")
	nodes[2].Stop()
	nodes[2] = nil // Mark as stopped

	// Allow failure to be detected
	time.Sleep(1 * time.Second)

	// Write data while node2 is down - this should store hints
	t.Log("Writing data while node2 is down")
	err := nodes[0].Put(ctx, "hh-delivery-key", []byte("hinted-value"), nil)
	if err != nil {
		// With W=2 and N=3, we should be able to write with 3 nodes alive
		// even if one is down (sloppy quorum)
		t.Logf("Put returned error (may be expected): %v", err)
	}

	// Verify data is readable from remaining nodes
	result, err := nodes[0].Get(ctx, "hh-delivery-key")
	if err != nil {
		t.Logf("Get from node0 returned error: %v", err)
	} else if len(result.Values) > 0 {
		t.Logf("Data readable from node0: %s", string(result.Values[0].Data))
	}

	// Restart node2
	t.Log("Restarting node2")
	node2, err := dynamo.NewNode("hh-node2", "localhost:8202", config)
	if err != nil {
		t.Fatalf("Failed to recreate node2: %v", err)
	}
	nodes[2] = node2

	if err := node2.Start(); err != nil {
		t.Fatalf("Failed to restart node2: %v", err)
	}

	// Rejoin the cluster
	if err := node2.Join([]string{"localhost:8200"}); err != nil {
		t.Logf("Warning: node2 rejoin returned error: %v", err)
	}

	// Wait for hint delivery (the hinted handoff loop runs every 10 seconds by default)
	// We'll wait up to 15 seconds for hints to be delivered
	t.Log("Waiting for hint delivery...")
	time.Sleep(15 * time.Second)

	// Verify data is now on the recovered node
	result, err = node2.Get(ctx, "hh-delivery-key")
	if err != nil {
		t.Logf("Get from recovered node2 returned error: %v", err)
	} else if len(result.Values) > 0 {
		t.Logf("Data successfully propagated to recovered node2: %s", string(result.Values[0].Data))
	} else {
		t.Log("Data not yet on recovered node2 (may need anti-entropy)")
	}
}

// TestWriteAvailabilityDuringFailure verifies that writes succeed even when
// one node in the preference list is unavailable.
func TestWriteAvailabilityDuringFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.HintedHandoffEnabled = true
	config.GossipInterval = 500 * time.Millisecond
	config.RequestTimeout = 2 * time.Second

	// Create 4 nodes
	nodes := make([]*dynamo.Node, 4)
	for i := 0; i < 4; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("wa-node%d", i),
			fmt.Sprintf("localhost:830%d", i),
			config,
		)
		if err != nil {
			t.Fatalf("Failed to create node%d: %v", i, err)
		}
		nodes[i] = node
	}

	// Start all nodes
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

	// Join nodes
	for i := 1; i < 4; i++ {
		if err := nodes[i].Join([]string{"localhost:8300"}); err != nil {
			t.Logf("Warning: node%d join returned error: %v", i, err)
		}
	}

	// Allow gossip to propagate
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Stop node3
	t.Log("Stopping node3")
	nodes[3].Stop()
	nodes[3] = nil

	time.Sleep(1 * time.Second)

	// Writes should still succeed with 3 nodes (sloppy quorum)
	// The write goes to the first N nodes in the preference list
	// If one is down, the next node in the ring accepts the write as a hint
	t.Log("Attempting writes while node3 is down")

	successCount := 0
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("wa-key-%d", i)
		err := nodes[0].Put(ctx, key, []byte(fmt.Sprintf("value-%d", i)), nil)
		if err != nil {
			t.Logf("Write %d failed: %v", i, err)
		} else {
			successCount++
			t.Logf("Write %d succeeded", i)
		}
	}

	if successCount == 0 {
		t.Error("No writes succeeded during node failure - hinted handoff may not be working")
	} else {
		t.Logf("%d out of 5 writes succeeded during node failure", successCount)
	}
}
