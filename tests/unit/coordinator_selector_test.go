package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

// --- LatencyTracker tests ---

func TestLatencyTracker_RecordAndP99(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 100,
	})

	// Record 100 samples: 1ms, 2ms, ..., 100ms
	for i := 1; i <= 100; i++ {
		selector.RecordLatency("node-1", time.Duration(i)*time.Millisecond)
	}

	// P99 index = int(99 * 0.99) = 98, sorted array [1,2,...,100] → index 98 = 99ms
	p99 := selector.GetNodeP99("node-1")
	if p99 != 99*time.Millisecond {
		t.Errorf("expected p99 = 99ms, got %v", p99)
	}
}

func TestLatencyTracker_RollingWindow(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 10,
	})

	// Fill window with 10ms samples
	for i := 0; i < 10; i++ {
		selector.RecordLatency("node-1", 10*time.Millisecond)
	}

	// Now overwrite with 1ms samples
	for i := 0; i < 10; i++ {
		selector.RecordLatency("node-1", 1*time.Millisecond)
	}

	// P99 should now reflect the 1ms samples
	p99 := selector.GetNodeP99("node-1")
	if p99 != 1*time.Millisecond {
		t.Errorf("expected p99 = 1ms after overwrite, got %v", p99)
	}
}

func TestLatencyTracker_NoData(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(nil)

	p99 := selector.GetNodeP99("unknown-node")
	if p99 != 0 {
		t.Errorf("expected p99 = 0 for unknown node, got %v", p99)
	}
}

// --- SelectCoordinator tests ---

func TestSelectCoordinator_PicksFastest(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 50,
	})

	// Node A: slow (50ms)
	for i := 0; i < 20; i++ {
		selector.RecordLatency("node-A", 50*time.Millisecond)
	}
	// Node B: fast (5ms)
	for i := 0; i < 20; i++ {
		selector.RecordLatency("node-B", 5*time.Millisecond)
	}
	// Node C: medium (20ms)
	for i := 0; i < 20; i++ {
		selector.RecordLatency("node-C", 20*time.Millisecond)
	}

	result := selector.SelectCoordinator([]string{"node-A", "node-B", "node-C"})
	if result != "node-B" {
		t.Errorf("expected fastest node node-B, got %s", result)
	}
}

func TestSelectCoordinator_FallsBackToFirst(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(nil)

	// No latency data recorded
	result := selector.SelectCoordinator([]string{"node-A", "node-B", "node-C"})
	if result != "node-A" {
		t.Errorf("expected first node node-A when no data, got %s", result)
	}
}

func TestSelectCoordinator_EmptyList(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(nil)

	result := selector.SelectCoordinator([]string{})
	if result != "" {
		t.Errorf("expected empty string for empty list, got %s", result)
	}
}

func TestSelectCoordinator_SingleNode(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(nil)

	result := selector.SelectCoordinator([]string{"node-only"})
	if result != "node-only" {
		t.Errorf("expected node-only, got %s", result)
	}
}

func TestSelectCoordinator_PrefersKnownOverUnknown(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 50,
	})

	// Only record data for node-B
	for i := 0; i < 10; i++ {
		selector.RecordLatency("node-B", 50*time.Millisecond)
	}

	// node-A has no data, node-B has data. Since unknown nodes get max duration,
	// node-B (known, even though 50ms) should be picked over unknown node-A.
	result := selector.SelectCoordinator([]string{"node-A", "node-B"})
	if result != "node-B" {
		t.Errorf("expected known node node-B over unknown node-A, got %s", result)
	}
}

// --- ReorderByLatency tests ---

func TestReorderByLatency_SortsBySpeed(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 50,
	})

	// Record different latencies
	for i := 0; i < 20; i++ {
		selector.RecordLatency("slow", 100*time.Millisecond)
		selector.RecordLatency("medium", 50*time.Millisecond)
		selector.RecordLatency("fast", 10*time.Millisecond)
	}

	reordered := selector.ReorderByLatency([]string{"slow", "medium", "fast"})

	if len(reordered) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(reordered))
	}
	if reordered[0] != "fast" {
		t.Errorf("expected first = fast, got %s", reordered[0])
	}
	if reordered[1] != "medium" {
		t.Errorf("expected second = medium, got %s", reordered[1])
	}
	if reordered[2] != "slow" {
		t.Errorf("expected third = slow, got %s", reordered[2])
	}
}

func TestReorderByLatency_UnknownNodesLast(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 50,
	})

	// Only record data for node-B
	for i := 0; i < 10; i++ {
		selector.RecordLatency("node-B", 10*time.Millisecond)
	}

	reordered := selector.ReorderByLatency([]string{"node-A", "node-B", "node-C"})

	if len(reordered) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(reordered))
	}
	// node-B (with data) should come first
	if reordered[0] != "node-B" {
		t.Errorf("expected known node first, got %s", reordered[0])
	}
	// Unknown nodes should preserve original relative order
	if reordered[1] != "node-A" {
		t.Errorf("expected node-A second (original order), got %s", reordered[1])
	}
	if reordered[2] != "node-C" {
		t.Errorf("expected node-C third (original order), got %s", reordered[2])
	}
}

func TestReorderByLatency_PreservesOriginalIfNoData(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(nil)

	input := []string{"node-C", "node-A", "node-B"}
	reordered := selector.ReorderByLatency(input)

	if len(reordered) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(reordered))
	}
	for i, expected := range input {
		if reordered[i] != expected {
			t.Errorf("position %d: expected %s, got %s", i, expected, reordered[i])
		}
	}
}

func TestReorderByLatency_SingleNode(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(nil)

	reordered := selector.ReorderByLatency([]string{"only"})
	if len(reordered) != 1 || reordered[0] != "only" {
		t.Errorf("expected [only], got %v", reordered)
	}
}

func TestReorderByLatency_DoesNotMutateInput(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 50,
	})

	for i := 0; i < 10; i++ {
		selector.RecordLatency("fast", 1*time.Millisecond)
		selector.RecordLatency("slow", 100*time.Millisecond)
	}

	original := []string{"slow", "fast"}
	_ = selector.ReorderByLatency(original)

	// Original slice should not be modified
	if original[0] != "slow" || original[1] != "fast" {
		t.Errorf("input was mutated: %v", original)
	}
}

// --- Concurrent access tests ---

func TestCoordinatorSelector_ConcurrentAccess(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 100,
	})

	var wg sync.WaitGroup

	// Concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			nodeID := "node-" + string(rune('A'+n))
			for j := 0; j < 100; j++ {
				selector.RecordLatency(nodeID, time.Duration(n+1)*time.Millisecond)
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				selector.SelectCoordinator([]string{"node-A", "node-B", "node-C"})
				selector.ReorderByLatency([]string{"node-A", "node-B", "node-C"})
				selector.GetNodeP99("node-A")
			}
		}()
	}

	wg.Wait()
	// No panic = success for concurrent access safety
}

// --- GetNodeCount test ---

func TestCoordinatorSelector_GetNodeCount(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(nil)

	if selector.GetNodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", selector.GetNodeCount())
	}

	selector.RecordLatency("node-1", 1*time.Millisecond)
	if selector.GetNodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", selector.GetNodeCount())
	}

	selector.RecordLatency("node-2", 2*time.Millisecond)
	if selector.GetNodeCount() != 2 {
		t.Errorf("expected 2 nodes, got %d", selector.GetNodeCount())
	}

	// Recording again for existing node shouldn't increase count
	selector.RecordLatency("node-1", 3*time.Millisecond)
	if selector.GetNodeCount() != 2 {
		t.Errorf("expected 2 nodes after duplicate, got %d", selector.GetNodeCount())
	}
}

// --- Default config test ---

func TestCoordinatorSelector_DefaultConfig(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(nil)

	// Should work with default config
	selector.RecordLatency("node-1", 10*time.Millisecond)
	result := selector.SelectCoordinator([]string{"node-1"})
	if result != "node-1" {
		t.Errorf("expected node-1, got %s", result)
	}
}

// --- Integration test: selector changes preference over time ---

func TestCoordinatorSelector_AdaptsToLatencyChanges(t *testing.T) {
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 10,
	})

	// Phase 1: node-A is fast, node-B is slow
	for i := 0; i < 10; i++ {
		selector.RecordLatency("node-A", 5*time.Millisecond)
		selector.RecordLatency("node-B", 50*time.Millisecond)
	}

	result1 := selector.SelectCoordinator([]string{"node-A", "node-B"})
	if result1 != "node-A" {
		t.Errorf("phase 1: expected node-A, got %s", result1)
	}

	// Phase 2: node-A becomes slow, node-B becomes fast (overwrite window)
	for i := 0; i < 10; i++ {
		selector.RecordLatency("node-A", 100*time.Millisecond)
		selector.RecordLatency("node-B", 2*time.Millisecond)
	}

	result2 := selector.SelectCoordinator([]string{"node-A", "node-B"})
	if result2 != "node-B" {
		t.Errorf("phase 2: expected node-B after latency change, got %s", result2)
	}
}

// --- Integration with Node config ---

func TestCoordinatorSelector_EnabledByDefault(t *testing.T) {
	config := dynamo.DefaultConfig()
	if !config.CoordinatorSelectionEnabled {
		t.Error("expected coordinator selection to be enabled by default")
	}
	if config.CoordinatorSelectionWindowSize != 100 {
		t.Errorf("expected default window size 100, got %d", config.CoordinatorSelectionWindowSize)
	}
}

func TestCoordinatorSelector_DisabledConfig(t *testing.T) {
	config := dynamo.DefaultConfig()
	config.CoordinatorSelectionEnabled = false
	config.N = 1
	config.R = 1
	config.W = 1

	node, err := dynamo.NewNode("test-node", "localhost:0", config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Selector should be nil when disabled
	if node.GetCoordinatorSelector() != nil {
		t.Error("expected nil selector when disabled")
	}
}

func TestCoordinatorSelector_EnabledConfig(t *testing.T) {
	config := dynamo.DefaultConfig()
	config.CoordinatorSelectionEnabled = true
	config.N = 1
	config.R = 1
	config.W = 1

	node, err := dynamo.NewNode("test-node", "localhost:0", config)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if node.GetCoordinatorSelector() == nil {
		t.Error("expected non-nil selector when enabled")
	}
}

// --- Latency variance test ---

func TestCoordinatorSelector_HandlesHighVariance(t *testing.T) {
	// Use window size 10 so p99 index = int(9 * 0.99) = 8 (0-based)
	// With 10 samples, 2 outliers at the end will show up at index 8
	selector := dynamo.NewCoordinatorSelector(&dynamo.CoordinatorSelectorConfig{
		WindowSize: 10,
	})

	// Node-A: mostly fast but with spikes (high variance)
	// 8 samples at 5ms + 2 at 500ms → sorted: [5,5,5,5,5,5,5,5,500,500]
	// p99 index = int(9 * 0.99) = 8 → 500ms
	for i := 0; i < 8; i++ {
		selector.RecordLatency("node-A", 5*time.Millisecond)
	}
	selector.RecordLatency("node-A", 500*time.Millisecond)
	selector.RecordLatency("node-A", 500*time.Millisecond)

	// Node-B: consistently medium (all 30ms)
	for i := 0; i < 10; i++ {
		selector.RecordLatency("node-B", 30*time.Millisecond)
	}

	// P99 for node-A should be affected by the spikes
	p99A := selector.GetNodeP99("node-A")
	if p99A != 500*time.Millisecond {
		t.Errorf("expected node-A p99 = 500ms, got %v", p99A)
	}

	// P99 for node-B should be 30ms (consistent)
	p99B := selector.GetNodeP99("node-B")
	if p99B != 30*time.Millisecond {
		t.Errorf("expected node-B p99 = 30ms, got %v", p99B)
	}

	// Node-B should be selected because its p99 is lower despite node-A
	// having better median latency
	result := selector.SelectCoordinator([]string{"node-A", "node-B"})
	if result != "node-B" {
		t.Errorf("expected node-B (lower p99), got %s", result)
	}
}
