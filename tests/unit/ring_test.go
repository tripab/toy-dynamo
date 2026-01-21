package tests

import (
	"fmt"
	"testing"

	"github.com/tripab/toy-dynamo/pkg/ring"
)

func TestRingAddNode(t *testing.T) {
	r := ring.NewRing(3, 10)

	tokens := r.AddNode("node1", 10)

	if len(tokens) != 10 {
		t.Errorf("Expected 10 tokens, got %d", len(tokens))
	}

	if r.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", r.GetNodeCount())
	}
}

func TestRingPreferenceList(t *testing.T) {
	r := ring.NewRing(3, 10)

	r.AddNode("node1", 10)
	r.AddNode("node2", 10)
	r.AddNode("node3", 10)

	preferenceList := r.GetPreferenceList("test-key", 3)

	if len(preferenceList) != 3 {
		t.Errorf("Expected 3 nodes in preference list, got %d", len(preferenceList))
	}

	// Check uniqueness
	seen := make(map[string]bool)
	for _, nodeID := range preferenceList {
		if seen[nodeID] {
			t.Errorf("Duplicate node in preference list: %s", nodeID)
		}
		seen[nodeID] = true
	}
}

func TestRingRemoveNode(t *testing.T) {
	r := ring.NewRing(3, 10)

	r.AddNode("node1", 10)
	r.AddNode("node2", 10)

	if r.GetNodeCount() != 2 {
		t.Errorf("Expected 2 nodes")
	}

	r.RemoveNode("node1")

	if r.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node after removal")
	}

	preferenceList := r.GetPreferenceList("test-key", 3)
	if len(preferenceList) != 1 {
		t.Errorf("Expected 1 node in preference list after removal")
	}
}

func TestRingLoadDistribution(t *testing.T) {
	r := ring.NewRing(3, 256)

	// Add 10 nodes
	for i := 0; i < 10; i++ {
		r.AddNode(fmt.Sprintf("node%d", i), 256)
	}

	// Test key distribution
	keyDistribution := make(map[string]int)

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		coordinator := r.GetCoordinator(key)
		keyDistribution[coordinator]++
	}

	// Check distribution is reasonably balanced (within 30% of average)
	avgKeys := 10000 / 10
	for nodeID, count := range keyDistribution {
		deviation := float64(count-avgKeys) / float64(avgKeys)
		if deviation > 0.3 || deviation < -0.3 {
			t.Logf("Warning: Node %s has %d keys (%.1f%% deviation)",
				nodeID, count, deviation*100)
		}
	}
}
