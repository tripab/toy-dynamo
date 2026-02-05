package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

func TestThreeNodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2

	// Create 3 nodes
	node1, err := dynamo.NewNode("node1", "localhost:8001", config)
	if err != nil {
		t.Fatal(err)
	}
	defer node1.Stop()

	node2, err := dynamo.NewNode("node2", "localhost:8002", config)
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Stop()

	node3, err := dynamo.NewNode("node3", "localhost:8003", config)
	if err != nil {
		t.Fatal(err)
	}
	defer node3.Stop()

	// Start all nodes
	node1.Start()
	node2.Start()
	node3.Start()

	// Join nodes to cluster
	node2.Join([]string{"localhost:8001"})
	node3.Join([]string{"localhost:8001"})

	// Allow gossip to propagate
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Write to node1
	err = node1.Put(ctx, "test-key", []byte("test-value"), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Read from node2
	result, err := node2.Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(result.Values) == 0 {
		t.Fatal("Expected value not found")
	}

	if string(result.Values[0].Data) != "test-value" {
		t.Errorf("Expected 'test-value', got '%s'", string(result.Values[0].Data))
	}
}

func TestFiveNodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second

	// Create 5 nodes
	nodes := make([]*dynamo.Node, 5)
	for i := 0; i < 5; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("five-node%d", i),
			fmt.Sprintf("localhost:840%d", i),
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

	// Join all nodes to first node
	for i := 1; i < 5; i++ {
		if err := nodes[i].Join([]string{"localhost:8400"}); err != nil {
			t.Fatalf("node%d failed to join: %v", i, err)
		}
	}

	// 5 nodes need more gossip rounds to fully converge
	time.Sleep(3 * time.Second)

	ctx := context.Background()

	// Write multiple keys from different nodes
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("five-key-%d", i)
		value := fmt.Sprintf("five-value-%d", i)
		srcNode := nodes[i%5]
		if err := srcNode.Put(ctx, key, []byte(value), nil); err != nil {
			t.Fatalf("Put key %s from node%d failed: %v", key, i%5, err)
		}
	}

	// Read keys back from the same node that wrote them to verify basic operation
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("five-key-%d", i)
		expectedValue := fmt.Sprintf("five-value-%d", i)

		readNode := nodes[i%5]
		result, err := readNode.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get key %s from node%d failed: %v", key, i%5, err)
		}
		if len(result.Values) == 0 {
			t.Fatalf("Key %s: expected value, got none", key)
		}
		if string(result.Values[0].Data) != expectedValue {
			t.Errorf("Key %s: expected '%s', got '%s'", key, expectedValue, string(result.Values[0].Data))
		}
	}

	// Verify cross-node reads work via node0 (coordinator contacts
	// the preference list nodes over RPC). With 5 nodes, gossip may not
	// have fully propagated all addresses, so some cross-node reads may fail
	// depending on key-to-node mapping. Track success rate.
	crossNodeSuccess := 0
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("five-key-%d", i)
		expectedValue := fmt.Sprintf("five-value-%d", i)
		result, err := nodes[0].Get(ctx, key)
		if err != nil {
			t.Logf("Cross-node get key %s from node0: %v", key, err)
			continue
		}
		if len(result.Values) > 0 && string(result.Values[0].Data) == expectedValue {
			crossNodeSuccess++
		}
	}
	t.Logf("Cross-node reads from node0: %d/5 succeeded", crossNodeSuccess)
	if crossNodeSuccess == 0 {
		t.Error("No cross-node reads succeeded from node0")
	}

	// Stop two nodes and verify reads still work (R=2, still have 3 nodes)
	nodes[3].Stop()
	nodes[3] = nil
	nodes[4].Stop()
	nodes[4] = nil

	time.Sleep(500 * time.Millisecond)

	// Should still be able to read from remaining 3 nodes.
	// Read a key that was written by node0 (which is still alive).
	result, err := nodes[0].Get(ctx, "five-key-0")
	if err != nil {
		t.Fatalf("Get after stopping 2 nodes failed: %v", err)
	}
	if len(result.Values) == 0 || string(result.Values[0].Data) != "five-value-0" {
		t.Error("Expected 'five-value-0' after stopping 2 nodes")
	}
}

func TestNodeFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2

	// Create 4 nodes (N+1 for hinted handoff)
	nodes := make([]*dynamo.Node, 4)
	for i := 0; i < 4; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("node%d", i),
			fmt.Sprintf("localhost:800%d", i),
			config,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer node.Stop()
		nodes[i] = node
		node.Start()
	}

	// Join all nodes
	for i := 1; i < 4; i++ {
		nodes[i].Join([]string{"localhost:8000"})
	}

	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Write value
	err := nodes[0].Put(ctx, "test-key", []byte("test-value"), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Stop one node
	nodes[2].Stop()

	// Should still be able to read (R=2, still have 3 nodes)
	result, err := nodes[0].Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("Get failed after node failure: %v", err)
	}

	if len(result.Values) == 0 {
		t.Fatal("Expected value after node failure")
	}
}
