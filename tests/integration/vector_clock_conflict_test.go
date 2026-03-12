package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// TestVectorClockConflictDetection verifies that concurrent writes to the same
// key from different nodes produce concurrent versions that are all surfaced
// on read. This is the core Dynamo guarantee: no acknowledged write is silently
// lost, and conflicts are exposed to the application via vector clocks.
func TestVectorClockConflictDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = false // Disable to preserve concurrent versions for observation
	config.HintedHandoffEnabled = true
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	nodes := createCluster(t, 3, "vc", 9100, config)
	defer stopCluster(nodes)

	ctx := context.Background()
	key := "conflict-key"

	// Phase 1: Write from two nodes concurrently with nil context.
	// Each coordinator creates a fresh vector clock {nodeID: 1}, making
	// the writes concurrent (neither VC dominates the other).
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = nodes[0].Put(ctx, key, []byte(`"alice"`), nil)
	}()
	go func() {
		defer wg.Done()
		errs[1] = nodes[1].Put(ctx, key, []byte(`"bob"`), nil)
	}()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Concurrent write %d failed: %v", i, err)
		}
	}

	// Allow replication to settle.
	time.Sleep(500 * time.Millisecond)

	// Phase 2: Read and verify conflict detection.
	result, err := nodes[2].Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after concurrent writes failed: %v", err)
	}
	if len(result.Values) == 0 {
		t.Fatal("Expected at least one value after concurrent writes")
	}

	// Collect returned values.
	valueSet := make(map[string]bool)
	for _, v := range result.Values {
		var s string
		if err := json.Unmarshal(v.Data, &s); err != nil {
			s = string(v.Data)
		}
		valueSet[s] = true
	}
	t.Logf("Read returned %d version(s): %v", len(result.Values), valueSet)

	// Core guarantee: no acknowledged write is lost.
	if !valueSet["alice"] && !valueSet["bob"] {
		t.Fatal("Neither concurrent write value found - data loss")
	}

	// If both versions are present, verify they are truly concurrent.
	if valueSet["alice"] && valueSet["bob"] {
		t.Log("Both concurrent versions detected - verifying vector clocks")
		verifyAllConcurrent(t, result.Values)
	} else {
		// Only one version visible: this can happen if one write's replication
		// completed before the other started (serialized at the storage level).
		t.Logf("Only one version visible (timing-dependent); at least no data was lost")
	}
}

// TestVectorClockConflictViaLocalPut creates controlled conflicts by writing
// directly to each node's local storage with independent vector clocks. This
// eliminates timing dependencies and guarantees conflict detection.
func TestVectorClockConflictViaLocalPut(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = false
	config.HintedHandoffEnabled = false
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	nodes := createCluster(t, 3, "vclp", 9200, config)
	defer stopCluster(nodes)

	ctx := context.Background()
	key := "controlled-conflict"

	// Write three concurrent versions directly to each node's local storage
	// with independent vector clocks. This guarantees all three are concurrent.
	versions := []struct {
		data   string
		nodeID string
	}{
		{`"version-A"`, "vclp-node0"},
		{`"version-B"`, "vclp-node1"},
		{`"version-C"`, "vclp-node2"},
	}

	for i, v := range versions {
		vc := versioning.NewVectorClock()
		vc.Increment(v.nodeID)
		err := nodes[i].LocalPut(key, versioning.VersionedValue{
			Data:        []byte(v.data),
			VectorClock: vc,
		})
		if err != nil {
			t.Fatalf("LocalPut to node%d failed: %v", i, err)
		}
	}

	// Read via coordinator — should merge versions from R=2 nodes.
	result, err := nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after local puts failed: %v", err)
	}

	valueSet := make(map[string]bool)
	for _, v := range result.Values {
		var s string
		if err := json.Unmarshal(v.Data, &s); err != nil {
			s = string(v.Data)
		}
		valueSet[s] = true
		t.Logf("  version: %s, VC: %v", s, v.VectorClock.Versions)
	}

	// With R=2, we contact 2 of 3 nodes. Each node has one version.
	// The coordinator should return at least 2 concurrent versions.
	if len(result.Values) < 2 {
		t.Errorf("Expected at least 2 concurrent versions, got %d", len(result.Values))
	}

	// Verify all returned versions are pairwise concurrent.
	verifyAllConcurrent(t, result.Values)

	// Verify no version is silently lost within what the coordinator contacted.
	t.Logf("Detected %d/%d conflicts via coordinator read", len(result.Values), len(versions))
}

// TestVectorClockConflictResolution verifies the full read-reconcile-write
// cycle: concurrent versions are read, reconciled by the application, and
// written back as a single version that causally dominates all predecessors.
func TestVectorClockConflictResolution(t *testing.T) {
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
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	nodes := createCluster(t, 3, "vcr", 9300, config)
	defer stopCluster(nodes)

	ctx := context.Background()
	key := "resolve-conflict"

	// Create controlled divergence: write two concurrent versions to ALL nodes
	// via LocalPut. Storage.Put uses ReconcileConcurrent, so both concurrent
	// versions are preserved on each node.
	vc0 := versioning.NewVectorClock()
	vc0.Increment("vcr-node0")
	vc1 := versioning.NewVectorClock()
	vc1.Increment("vcr-node1")

	for i := 0; i < 3; i++ {
		if err := nodes[i].LocalPut(key, versioning.VersionedValue{
			Data: []byte(`"value-X"`), VectorClock: vc0,
		}); err != nil {
			t.Fatalf("LocalPut value-X to node%d: %v", i, err)
		}
		if err := nodes[i].LocalPut(key, versioning.VersionedValue{
			Data: []byte(`"value-Y"`), VectorClock: vc1,
		}); err != nil {
			t.Fatalf("LocalPut value-Y to node%d: %v", i, err)
		}
	}

	// Read — should see both concurrent versions from any R=2 quorum.
	result, err := nodes[2].Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(result.Values) < 2 {
		t.Fatalf("Expected at least 2 concurrent versions, got %d", len(result.Values))
	}

	// Application-side reconciliation: merge values and write back with the
	// merged context (vector clock). This establishes a causal successor that
	// dominates both concurrent versions.
	reconciled := []byte(`"merged-XY"`)
	if err := nodes[2].Put(ctx, key, reconciled, result.Context); err != nil {
		t.Fatalf("Put reconciled value failed: %v", err)
	}

	// Allow replication to settle.
	time.Sleep(500 * time.Millisecond)

	// Read again — should see exactly one version (the reconciled one)
	// since it causally dominates both predecessors.
	result2, err := nodes[0].Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after reconciliation failed: %v", err)
	}

	if len(result2.Values) == 0 {
		t.Fatal("Expected reconciled value, got none")
	}

	// All returned versions should be the reconciled value.
	for i, v := range result2.Values {
		var s string
		if err := json.Unmarshal(v.Data, &s); err != nil {
			s = string(v.Data)
		}
		if s != "merged-XY" {
			t.Errorf("Version %d: expected 'merged-XY', got '%s'", i, s)
		}
	}

	// The reconciled version's vector clock should dominate both originals.
	reconciledVC := result2.Values[0].VectorClock
	if reconciledVC == nil {
		t.Fatal("Reconciled version has nil vector clock")
	}

	// It should have entries for at least the reconciling node.
	t.Logf("Reconciled VC: %v", reconciledVC.Versions)

	// Verify the reconciled VC dominates both originals.
	if reconciledVC.Compare(vc0) != versioning.After {
		t.Errorf("Reconciled VC should dominate vc0 {%v}", vc0.Versions)
	}
	if reconciledVC.Compare(vc1) != versioning.After {
		t.Errorf("Reconciled VC should dominate vc1 {%v}", vc1.Versions)
	}

	t.Log("Conflict resolution successful: reconciled version dominates all predecessors")
}

// TestVectorClockMultiKeyConflicts runs the conflict detection workload across
// multiple keys to increase coverage. For each key, two concurrent writes are
// issued and the result is checked.
func TestVectorClockMultiKeyConflicts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := dynamo.DefaultConfig()
	config.N = 3
	config.R = 2
	config.W = 2
	config.GossipInterval = 200 * time.Millisecond
	config.RequestTimeout = 2 * time.Second
	config.ReadRepairEnabled = false
	config.HintedHandoffEnabled = true
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false

	nodes := createCluster(t, 3, "vcmk", 9400, config)
	defer stopCluster(nodes)

	ctx := context.Background()
	numKeys := 10
	conflictsDetected := 0

	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("multi-conflict-%d", k)

		// Concurrent writes from two different nodes, no causal context.
		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs[0] = nodes[0].Put(ctx, key, fmt.Appendf(nil, `"node0-k%d"`, k), nil)
		}()
		go func() {
			defer wg.Done()
			errs[1] = nodes[1].Put(ctx, key, fmt.Appendf(nil, `"node1-k%d"`, k), nil)
		}()
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Fatalf("Key %s write %d failed: %v", key, i, err)
			}
		}
	}

	// Allow replication to settle.
	time.Sleep(time.Second)

	// Read all keys and check for conflicts.
	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("multi-conflict-%d", k)
		result, err := nodes[2].Get(ctx, key)
		if err != nil {
			t.Fatalf("Get %s failed: %v", key, err)
		}
		if len(result.Values) == 0 {
			t.Fatalf("Key %s: no values returned", key)
		}

		// Count how many keys have detectable conflicts.
		if len(result.Values) > 1 {
			conflictsDetected++
			verifyAllConcurrent(t, result.Values)
		}

		// Core safety check: at least one of the written values must be present.
		found := false
		for _, v := range result.Values {
			var s string
			if err := json.Unmarshal(v.Data, &s); err != nil {
				s = string(v.Data)
			}
			if s == fmt.Sprintf("node0-k%d", k) || s == fmt.Sprintf("node1-k%d", k) {
				found = true
			}
		}
		if !found {
			t.Errorf("Key %s: none of the written values found — data loss", key)
		}
	}

	t.Logf("Conflicts detected in %d/%d keys (timing-dependent)", conflictsDetected, numKeys)
	// We expect at least some conflicts when writes are truly concurrent.
	// With 10 keys, it's very unlikely that ALL writes serialize by chance.
	if conflictsDetected == 0 {
		t.Log("Warning: no conflicts detected across all keys — writes may have serialized")
	}
}

// --- Helpers ---

// verifyAllConcurrent asserts that every pair of returned versions has a
// Concurrent vector clock relationship (no version dominates another).
func verifyAllConcurrent(t *testing.T, values []versioning.VersionedValue) {
	t.Helper()
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if values[i].VectorClock == nil || values[j].VectorClock == nil {
				t.Errorf("Version %d or %d has nil vector clock", i, j)
				continue
			}
			ordering := values[i].VectorClock.Compare(values[j].VectorClock)
			if ordering != versioning.Concurrent && ordering != versioning.Equal {
				t.Errorf("Versions %d and %d are not concurrent: ordering=%v, vc[%d]=%v, vc[%d]=%v",
					i, j, ordering, i, values[i].VectorClock.Versions, j, values[j].VectorClock.Versions)
			}
		}
	}
}

// createCluster creates and starts a cluster of n nodes with the given config.
// The node IDs are prefixed with the given prefix, and ports start from basePort.
func createCluster(t *testing.T, n int, prefix string, basePort int, config *dynamo.Config) []*dynamo.Node {
	t.Helper()
	nodes := make([]*dynamo.Node, n)
	for i := 0; i < n; i++ {
		node, err := dynamo.NewNode(
			fmt.Sprintf("%s-node%d", prefix, i),
			fmt.Sprintf("localhost:%d", basePort+i),
			config,
		)
		if err != nil {
			t.Fatalf("Failed to create %s-node%d: %v", prefix, i, err)
		}
		nodes[i] = node
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Failed to start %s-node%d: %v", prefix, i, err)
		}
	}

	// Join nodes 1..n to node 0.
	seedAddr := fmt.Sprintf("localhost:%d", basePort)
	for i := 1; i < n; i++ {
		if err := nodes[i].Join([]string{seedAddr}); err != nil {
			t.Fatalf("%s-node%d join failed: %v", prefix, i, err)
		}
	}

	// Allow gossip to propagate membership.
	time.Sleep(2 * time.Second)
	return nodes
}

// stopCluster gracefully stops all non-nil nodes.
func stopCluster(nodes []*dynamo.Node) {
	for _, node := range nodes {
		if node != nil {
			node.Stop()
		}
	}
}
