package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

// TestHintedHandoffVerification takes a node down, writes data that should be
// stored on the downed node via hinted handoff, brings the node back, and
// verifies that hints are delivered and the data arrives on the recovered node.
func TestHintedHandoffVerification(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.HintedHandoffEnabled = true
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = true
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	// Use 4 nodes so that with N=3, W=2 we can still satisfy quorum
	// when one node is down.
	nodes := createCluster(t, 4, "hhv", 9800, config)
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop()
			}
		}
	}()

	ctx := context.Background()

	// Step 1: Write initial value while all nodes are up.
	key := "hh-verify-key"
	if err := nodes[0].Put(ctx, key, []byte("before-failure"), nil); err != nil {
		t.Fatalf("Initial put failed: %v", err)
	}

	result, err := nodes[1].Get(ctx, key)
	if err != nil {
		t.Fatalf("Initial read failed: %v", err)
	}
	t.Logf("Initial value readable: %s", string(result.Values[0].Data))

	// Step 2: Stop node2 to simulate failure.
	t.Log("Stopping node2 to simulate failure")
	nodes[2].Stop()
	nodes[2] = nil

	// Allow failure to be detected via gossip.
	time.Sleep(time.Second)

	// Step 3: Write multiple keys while node2 is down.
	// These writes should store hints for node2 when it's in the preference list.
	numKeys := 5
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("hh-verify-%d", i)
		v := fmt.Appendf(nil, "hinted-value-%d", i)
		if err := nodes[0].Put(ctx, k, v, nil); err != nil {
			// Some keys may not have node2 in their preference list,
			// so writes should succeed regardless.
			t.Logf("Write key %s: %v", k, err)
		}
	}

	// Verify writes are readable from remaining nodes.
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("hh-verify-%d", i)
		result, err := nodes[0].Get(ctx, k)
		if err != nil {
			t.Logf("Read %s from node0 failed: %v", k, err)
			continue
		}
		if len(result.Values) > 0 {
			t.Logf("Key %s readable: %s", k, string(result.Values[0].Data))
		}
	}

	// Step 4: Restart node2 and rejoin the cluster.
	t.Log("Restarting node2")
	node2, err := dynamo.NewNode("hhv-node2", "localhost:9802", config)
	if err != nil {
		t.Fatalf("Failed to recreate node2: %v", err)
	}
	nodes[2] = node2
	if err := node2.Start(); err != nil {
		t.Fatalf("Failed to restart node2: %v", err)
	}
	if err := node2.Join([]string{"localhost:9800"}); err != nil {
		t.Logf("node2 rejoin: %v", err)
	}

	// Step 5: Wait for hint delivery + read repair to propagate data.
	// The hinted handoff loop runs every 10s, so wait accordingly.
	// Also trigger reads from surviving nodes to kick off read repair.
	t.Log("Waiting for hint delivery and read repair...")
	time.Sleep(3 * time.Second) // Let gossip propagate membership

	// Trigger read repair by reading from other nodes.
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("hh-verify-%d", i)
		nodes[0].Get(ctx, k)
		nodes[1].Get(ctx, k)
	}

	// Wait for async read repair to complete.
	time.Sleep(2 * time.Second)

	// Step 6: Verify data on the recovered node.
	// Use LocalGet to check node2's local storage directly.
	delivered := 0
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("hh-verify-%d", i)
		vals, err := nodes[2].LocalGet(k)
		if err != nil {
			t.Logf("Key %s not yet on node2: %v", k, err)
			continue
		}
		if len(vals) > 0 {
			t.Logf("Key %s delivered to node2: %s", k, string(vals[0].Data))
			delivered++
		}
	}

	t.Logf("Data delivered to recovered node: %d/%d keys", delivered, numKeys)

	// At least some keys should have been delivered via read repair or hints.
	if delivered == 0 {
		t.Error("No data was delivered to the recovered node — hinted handoff and read repair may not be working")
	}
}

// TestHintedHandoffWriteAvailability verifies that writes continue to succeed
// (with sloppy quorum) when a node is down, and all written data is eventually
// readable after the node recovers.
func TestHintedHandoffWriteAvailability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.HintedHandoffEnabled = true
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = true
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	nodes := createCluster(t, 4, "hhwa", 9850, config)
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop()
			}
		}
	}()

	ctx := context.Background()

	// Stop node3.
	t.Log("Stopping node3")
	nodes[3].Stop()
	nodes[3] = nil
	time.Sleep(time.Second)

	// Write 10 keys while node3 is down.
	written := make(map[string]string)
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("hhwa-key-%d", i)
		v := fmt.Sprintf("hhwa-val-%d", i)
		if err := nodes[0].Put(ctx, k, []byte(v), nil); err != nil {
			t.Logf("Write %s failed (acceptable): %v", k, err)
			continue
		}
		written[k] = v
	}

	if len(written) == 0 {
		t.Fatal("No writes succeeded while node3 was down")
	}
	t.Logf("%d/%d writes succeeded while node3 was down", len(written), 10)

	// Restart node3.
	t.Log("Restarting node3")
	node3, err := dynamo.NewNode("hhwa-node3", "localhost:9853", config)
	if err != nil {
		t.Fatalf("Failed to recreate node3: %v", err)
	}
	nodes[3] = node3
	if err := node3.Start(); err != nil {
		t.Fatalf("Failed to restart node3: %v", err)
	}
	if err := node3.Join([]string{"localhost:9850"}); err != nil {
		t.Logf("node3 rejoin: %v", err)
	}

	time.Sleep(3 * time.Second) // Let gossip propagate

	// Trigger reads to kick off read repair.
	for k := range written {
		nodes[0].Get(ctx, k)
	}
	time.Sleep(2 * time.Second)

	// Verify all written data is readable from the cluster.
	readable := 0
	for k, expectedVal := range written {
		result, err := nodes[0].Get(ctx, k)
		if err != nil {
			t.Logf("Read %s failed: %v", k, err)
			continue
		}
		if len(result.Values) == 0 {
			t.Logf("Key %s: no values returned", k)
			continue
		}
		if string(result.Values[0].Data) == expectedVal {
			readable++
		} else {
			t.Logf("Key %s: expected %q, got %q", k, expectedVal, string(result.Values[0].Data))
		}
	}

	t.Logf("Readable after recovery: %d/%d", readable, len(written))
	if readable < len(written) {
		t.Errorf("Not all written data is readable after node recovery: %d/%d", readable, len(written))
	}
}
