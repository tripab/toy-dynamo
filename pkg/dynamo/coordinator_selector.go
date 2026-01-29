package dynamo

import (
	"sort"
	"sync"
	"time"
)

// CoordinatorSelector implements latency-based coordinator selection as described
// in the Dynamo paper's performance optimizations. Instead of always using the
// first node in the preference list as coordinator, it tracks per-node latency
// and selects the fastest node. This reduces write latency variance and improves
// read-your-writes consistency.
type CoordinatorSelector struct {
	trackers   map[string]*LatencyTracker
	windowSize int
	mu         sync.RWMutex
}

// LatencyTracker maintains a rolling window of latency measurements for a node.
type LatencyTracker struct {
	samples    []time.Duration
	index      int
	count      int
	windowSize int
	mu         sync.Mutex
}

// CoordinatorSelectorConfig holds configuration for the coordinator selector.
type CoordinatorSelectorConfig struct {
	// WindowSize is the number of latency samples to keep per node.
	// Default: 100
	WindowSize int
}

// NewCoordinatorSelector creates a new latency-based coordinator selector.
func NewCoordinatorSelector(config *CoordinatorSelectorConfig) *CoordinatorSelector {
	windowSize := 100
	if config != nil && config.WindowSize > 0 {
		windowSize = config.WindowSize
	}

	return &CoordinatorSelector{
		trackers:   make(map[string]*LatencyTracker),
		windowSize: windowSize,
	}
}

// RecordLatency records a latency measurement for the given node.
func (cs *CoordinatorSelector) RecordLatency(nodeID string, latency time.Duration) {
	cs.mu.Lock()
	tracker, ok := cs.trackers[nodeID]
	if !ok {
		tracker = newLatencyTracker(cs.windowSize)
		cs.trackers[nodeID] = tracker
	}
	cs.mu.Unlock()

	tracker.Record(latency)
}

// SelectCoordinator returns the node from the preference list with the lowest
// p99 latency. If no latency data is available, returns the first node.
func (cs *CoordinatorSelector) SelectCoordinator(preferenceList []string) string {
	if len(preferenceList) == 0 {
		return ""
	}
	if len(preferenceList) == 1 {
		return preferenceList[0]
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	fastest := preferenceList[0]
	minLatency := cs.getNodeP99Locked(fastest)

	for _, nodeID := range preferenceList[1:] {
		latency := cs.getNodeP99Locked(nodeID)
		// Only prefer a node if we have actual latency data for it
		// and it's faster. Nodes with no data use max duration so they
		// sort to the end (we prefer known-fast nodes over unknown).
		if latency < minLatency {
			fastest = nodeID
			minLatency = latency
		}
	}

	return fastest
}

// ReorderByLatency returns a copy of the preference list reordered so that
// nodes with lower p99 latency come first. Nodes without latency data
// retain their original relative ordering at the end.
func (cs *CoordinatorSelector) ReorderByLatency(preferenceList []string) []string {
	if len(preferenceList) <= 1 {
		result := make([]string, len(preferenceList))
		copy(result, preferenceList)
		return result
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	type nodeLatency struct {
		nodeID       string
		latency      time.Duration
		hasData      bool
		originalRank int
	}

	entries := make([]nodeLatency, len(preferenceList))
	for i, nodeID := range preferenceList {
		tracker := cs.trackers[nodeID]
		if tracker != nil && tracker.Count() > 0 {
			entries[i] = nodeLatency{
				nodeID:       nodeID,
				latency:      tracker.P99(),
				hasData:      true,
				originalRank: i,
			}
		} else {
			entries[i] = nodeLatency{
				nodeID:       nodeID,
				latency:      0,
				hasData:      false,
				originalRank: i,
			}
		}
	}

	sort.SliceStable(entries, func(i, j int) bool {
		// Nodes with latency data come before nodes without
		if entries[i].hasData != entries[j].hasData {
			return entries[i].hasData
		}
		// Among nodes with data, sort by latency (ascending)
		if entries[i].hasData && entries[j].hasData {
			return entries[i].latency < entries[j].latency
		}
		// Among nodes without data, preserve original order
		return entries[i].originalRank < entries[j].originalRank
	})

	result := make([]string, len(entries))
	for i, e := range entries {
		result[i] = e.nodeID
	}
	return result
}

// GetNodeP99 returns the p99 latency for a specific node.
// Returns 0 if no data is available.
func (cs *CoordinatorSelector) GetNodeP99(nodeID string) time.Duration {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	tracker := cs.trackers[nodeID]
	if tracker == nil || tracker.Count() == 0 {
		return 0
	}
	return tracker.P99()
}

// GetNodeCount returns the number of nodes being tracked.
func (cs *CoordinatorSelector) GetNodeCount() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.trackers)
}

// getNodeP99Locked returns p99 for a node, returning max duration if no data.
// Caller must hold cs.mu read lock.
func (cs *CoordinatorSelector) getNodeP99Locked(nodeID string) time.Duration {
	tracker := cs.trackers[nodeID]
	if tracker == nil || tracker.Count() == 0 {
		return time.Duration(1<<63 - 1) // max duration - unknown nodes sort last
	}
	return tracker.P99()
}

// --- LatencyTracker ---

func newLatencyTracker(windowSize int) *LatencyTracker {
	if windowSize < 1 {
		windowSize = 100
	}
	return &LatencyTracker{
		samples:    make([]time.Duration, windowSize),
		windowSize: windowSize,
	}
}

// Record adds a latency sample to the tracker.
func (lt *LatencyTracker) Record(latency time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.samples[lt.index] = latency
	lt.index = (lt.index + 1) % lt.windowSize
	if lt.count < lt.windowSize {
		lt.count++
	}
}

// P99 returns the 99th percentile latency from the rolling window.
func (lt *LatencyTracker) P99() time.Duration {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.percentileLocked(0.99)
}

// P50 returns the 50th percentile (median) latency.
func (lt *LatencyTracker) P50() time.Duration {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.percentileLocked(0.50)
}

// Count returns the number of valid samples.
func (lt *LatencyTracker) Count() int {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.count
}

// percentileLocked calculates the given percentile. Caller must hold lt.mu.
func (lt *LatencyTracker) percentileLocked(percentile float64) time.Duration {
	if lt.count == 0 {
		return 0
	}

	// Copy valid samples for sorting
	n := lt.count
	sorted := make([]time.Duration, n)
	copy(sorted, lt.samples[:n])

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	idx := int(float64(n-1) * percentile)
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}
