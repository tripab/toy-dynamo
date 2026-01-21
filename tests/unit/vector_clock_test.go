package tests

import (
	"fmt"
	"testing"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

func TestVectorClockIncrement(t *testing.T) {
	vc := versioning.NewVectorClock()

	vc.Increment("node1")
	if vc.Versions["node1"] != 1 {
		t.Errorf("Expected version 1, got %d", vc.Versions["node1"])
	}

	vc.Increment("node1")
	if vc.Versions["node1"] != 2 {
		t.Errorf("Expected version 2, got %d", vc.Versions["node1"])
	}
}

func TestVectorClockCompare(t *testing.T) {
	vc1 := versioning.NewVectorClock()
	vc1.Versions["node1"] = 1
	vc1.Versions["node2"] = 2

	vc2 := versioning.NewVectorClock()
	vc2.Versions["node1"] = 1
	vc2.Versions["node2"] = 3

	ordering := vc1.Compare(vc2)
	if ordering != versioning.Before {
		t.Errorf("Expected vc1 Before vc2")
	}

	ordering = vc2.Compare(vc1)
	if ordering != versioning.After {
		t.Errorf("Expected vc2 After vc1")
	}
}

func TestVectorClockConcurrent(t *testing.T) {
	vc1 := versioning.NewVectorClock()
	vc1.Versions["node1"] = 2
	vc1.Versions["node2"] = 1

	vc2 := versioning.NewVectorClock()
	vc2.Versions["node1"] = 1
	vc2.Versions["node2"] = 2

	ordering := vc1.Compare(vc2)
	if ordering != versioning.Concurrent {
		t.Errorf("Expected Concurrent, got %v", ordering)
	}
}

func TestVectorClockMerge(t *testing.T) {
	vc1 := versioning.NewVectorClock()
	vc1.Versions["node1"] = 2
	vc1.Versions["node2"] = 1

	vc2 := versioning.NewVectorClock()
	vc2.Versions["node1"] = 1
	vc2.Versions["node2"] = 3
	vc2.Versions["node3"] = 1

	merged := vc1.Merge(vc2)

	if merged.Versions["node1"] != 2 {
		t.Errorf("Expected node1=2 in merged clock")
	}
	if merged.Versions["node2"] != 3 {
		t.Errorf("Expected node2=3 in merged clock")
	}
	if merged.Versions["node3"] != 1 {
		t.Errorf("Expected node3=1 in merged clock")
	}
}

func TestVectorClockPrune(t *testing.T) {
	vc := versioning.NewVectorClock()

	// Add many entries
	for i := 0; i < 20; i++ {
		vc.Versions[fmt.Sprintf("node%d", i)] = uint64(i)
	}

	vc.Prune(10)

	if len(vc.Versions) != 10 {
		t.Errorf("Expected 10 entries after pruning, got %d", len(vc.Versions))
	}
}
