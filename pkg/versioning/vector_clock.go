package versioning

import (
	"bytes"
	"encoding/json"
	"sort"
	"time"
)

type VectorClock struct {
	Versions  map[string]uint64 `json:"versions"`
	Timestamp time.Time         `json:"timestamp"`
}

type ClockEntry struct {
	NodeID    string
	Counter   uint64
	Timestamp time.Time
}

type Ordering int

const (
	Before Ordering = iota
	After
	Equal
	Concurrent
)

func NewVectorClock() *VectorClock {
	return &VectorClock{
		Versions:  make(map[string]uint64),
		Timestamp: time.Now(),
	}
}

// Increment increments the clock for a node
func (vc *VectorClock) Increment(nodeID string) {
	vc.Versions[nodeID]++
	vc.Timestamp = time.Now()
}

// Copy creates a deep copy of the vector clock
func (vc *VectorClock) Copy() *VectorClock {
	newVC := NewVectorClock()
	for k, v := range vc.Versions {
		newVC.Versions[k] = v
	}
	newVC.Timestamp = vc.Timestamp
	return newVC
}

// Compare determines the ordering relationship between two vector clocks
func (vc *VectorClock) Compare(other *VectorClock) Ordering {
	if vc == nil || other == nil {
		return Concurrent
	}

	vcGreater := false
	otherGreater := false

	// Get all node IDs from both clocks
	allNodes := make(map[string]bool)
	for node := range vc.Versions {
		allNodes[node] = true
	}
	for node := range other.Versions {
		allNodes[node] = true
	}

	// Compare each node's counter
	for node := range allNodes {
		vcVal := vc.Versions[node]
		otherVal := other.Versions[node]

		if vcVal > otherVal {
			vcGreater = true
		} else if otherVal > vcVal {
			otherGreater = true
		}
	}

	// Determine relationship
	if vcGreater && !otherGreater {
		return After // vc is newer
	} else if otherGreater && !vcGreater {
		return Before // vc is older
	} else if !vcGreater && !otherGreater {
		return Equal
	} else {
		return Concurrent // conflict
	}
}

// Merge creates a new vector clock that combines both clocks
func (vc *VectorClock) Merge(other *VectorClock) *VectorClock {
	merged := vc.Copy()

	for node, count := range other.Versions {
		if count > merged.Versions[node] {
			merged.Versions[node] = count
		}
	}

	if other.Timestamp.After(merged.Timestamp) {
		merged.Timestamp = other.Timestamp
	}

	return merged
}

// Prune removes the oldest entries if size exceeds maxSize
func (vc *VectorClock) Prune(maxSize int) {
	if len(vc.Versions) <= maxSize {
		return
	}

	// This is a simplified version - in production, track timestamps per entry
	// Remove entries with lowest counter values
	type entry struct {
		node  string
		count uint64
	}

	entries := make([]entry, 0, len(vc.Versions))
	for node, count := range vc.Versions {
		entries = append(entries, entry{node, count})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count < entries[j].count
	})

	// Keep only maxSize entries
	toRemove := len(entries) - maxSize
	for i := 0; i < toRemove; i++ {
		delete(vc.Versions, entries[i].node)
	}
}

// Serialize converts the vector clock to JSON
func (vc *VectorClock) Serialize() ([]byte, error) {
	return json.Marshal(vc)
}

// Deserialize creates a vector clock from JSON
func Deserialize(data []byte) (*VectorClock, error) {
	vc := NewVectorClock()
	err := json.Unmarshal(data, vc)
	return vc, err
}

// Equals checks if two vector clocks are identical
func (vc *VectorClock) Equals(other *VectorClock) bool {
	if len(vc.Versions) != len(other.Versions) {
		return false
	}

	for node, count := range vc.Versions {
		if other.Versions[node] != count {
			return false
		}
	}

	return true
}

// ReconcileConcurrent finds versions that are concurrent (conflicting)
func ReconcileConcurrent(versions []VersionedValue) []VersionedValue {
	if len(versions) <= 1 {
		return versions
	}

	// Remove dominated versions (ancestors)
	concurrent := []VersionedValue{}

	for i := range versions {
		isDominated := false

		for j := range versions {
			if i == j {
				continue
			}

			ordering := versions[i].VectorClock.Compare(versions[j].VectorClock)
			if ordering == Before {
				// versions[i] is older, skip it
				isDominated = true
				break
			}
		}

		if !isDominated {
			concurrent = append(concurrent, versions[i])
		}
	}

	return concurrent
}

type VersionedValue struct {
	Data        []byte
	VectorClock *VectorClock
	IsTombstone bool
}

func (v *VersionedValue) Equals(other *VersionedValue) bool {
	return bytes.Equal(v.Data, other.Data) &&
		v.VectorClock.Equals(other.VectorClock) &&
		v.IsTombstone == other.IsTombstone
}
