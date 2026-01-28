package membership

import (
	"math"
	"sync"
	"time"
)

// StatusChangeCallback is called when a node's status changes
type StatusChangeCallback func(nodeID string, oldStatus, newStatus MemberStatus)

// HeartbeatHistory tracks heartbeat arrival times for phi accrual
type HeartbeatHistory struct {
	intervals    []time.Duration // Recent inter-arrival times
	maxSize      int             // Maximum number of intervals to keep
	lastArrival  time.Time       // Time of last heartbeat
	mu           sync.Mutex
}

// FailureDetector implements phi accrual failure detection
// Based on "The Phi Accrual Failure Detector" by Hayashibara et al.
type FailureDetector struct {
	membership *Membership
	timeout    time.Duration // Base timeout (used as fallback)

	// Phi accrual parameters
	phiThreshold     float64                      // Threshold for suspicion (default: 8)
	phiDeadThreshold float64                      // Threshold for marking dead (default: 16)
	histories        map[string]*HeartbeatHistory // Per-node heartbeat history

	// Status tracking
	suspects   map[string]time.Time
	deadNodes  map[string]time.Time

	// Callbacks for status changes
	callbacks []StatusChangeCallback

	mu sync.RWMutex
}

// NewFailureDetector creates a new phi accrual failure detector
func NewFailureDetector(membership *Membership, timeout time.Duration) *FailureDetector {
	return &FailureDetector{
		membership:       membership,
		timeout:          timeout,
		phiThreshold:     8.0,  // Suspicion threshold
		phiDeadThreshold: 16.0, // Dead threshold
		histories:        make(map[string]*HeartbeatHistory),
		suspects:         make(map[string]time.Time),
		deadNodes:        make(map[string]time.Time),
		callbacks:        make([]StatusChangeCallback, 0),
	}
}

// RegisterCallback registers a callback for status change notifications
func (fd *FailureDetector) RegisterCallback(callback StatusChangeCallback) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.callbacks = append(fd.callbacks, callback)
}

// RecordHeartbeat records a heartbeat arrival from a node
func (fd *FailureDetector) RecordHeartbeat(nodeID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	history := fd.histories[nodeID]
	if history == nil {
		history = &HeartbeatHistory{
			intervals: make([]time.Duration, 0, 100),
			maxSize:   100,
		}
		fd.histories[nodeID] = history
	}

	history.mu.Lock()
	defer history.mu.Unlock()

	now := time.Now()
	if !history.lastArrival.IsZero() {
		interval := now.Sub(history.lastArrival)
		history.intervals = append(history.intervals, interval)

		// Keep only the most recent intervals
		if len(history.intervals) > history.maxSize {
			history.intervals = history.intervals[1:]
		}
	}
	history.lastArrival = now
}

// calculatePhi calculates the phi value for a node using phi accrual algorithm
// Higher phi means more likely the node has failed
func (fd *FailureDetector) calculatePhi(nodeID string, now time.Time) float64 {
	history := fd.histories[nodeID]
	if history == nil {
		// No history, use simple timeout-based check
		return fd.calculateSimplePhi(nodeID, now)
	}

	history.mu.Lock()
	defer history.mu.Unlock()

	if len(history.intervals) < 2 {
		// Not enough data, fall back to simple timeout
		return fd.calculateSimplePhi(nodeID, now)
	}

	// Calculate mean and standard deviation of inter-arrival times
	var sum time.Duration
	for _, interval := range history.intervals {
		sum += interval
	}
	mean := float64(sum) / float64(len(history.intervals))

	var varianceSum float64
	for _, interval := range history.intervals {
		diff := float64(interval) - mean
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(len(history.intervals)))

	// Ensure minimum standard deviation to avoid division issues
	if stdDev < float64(time.Millisecond*10) {
		stdDev = float64(time.Millisecond * 10)
	}

	// Calculate time since last heartbeat
	timeSinceLast := float64(now.Sub(history.lastArrival))

	// Calculate phi using the CDF of normal distribution
	// phi = -log10(1 - CDF(timeSinceLast))
	// CDF approximation: 0.5 * (1 + erf((x - mean) / (stdDev * sqrt(2))))
	y := (timeSinceLast - mean) / (stdDev * math.Sqrt(2))
	cdf := 0.5 * (1 + erf(y))

	// Avoid log(0) by clamping cdf
	if cdf >= 0.9999 {
		cdf = 0.9999
	}

	phi := -math.Log10(1 - cdf)
	return phi
}

// calculateSimplePhi calculates phi using simple timeout-based approach (fallback)
func (fd *FailureDetector) calculateSimplePhi(nodeID string, now time.Time) float64 {
	member := fd.membership.GetMember(nodeID)
	if member == nil {
		return fd.phiDeadThreshold + 1 // Node doesn't exist, treat as dead
	}

	elapsed := now.Sub(member.Timestamp)

	// Map elapsed time to phi value
	// phi grows as time since last heartbeat increases
	ratio := float64(elapsed) / float64(fd.timeout)
	if ratio <= 1 {
		// Under timeout: phi grows linearly from 0 to phiThreshold/2
		return ratio * fd.phiThreshold / 2
	}
	// Over timeout: phi grows exponentially to ensure detection
	// At 2x timeout: phi = phiThreshold (triggers suspicion)
	// At 3x timeout: phi = phiDeadThreshold (triggers dead)
	overRatio := ratio - 1 // How much over the timeout we are
	return fd.phiThreshold/2 + overRatio*fd.phiThreshold
}

// erf is an approximation of the error function
func erf(x float64) float64 {
	// Constants for approximation
	a1 := 0.254829592
	a2 := -0.284496736
	a3 := 1.421413741
	a4 := -1.453152027
	a5 := 1.061405429
	p := 0.3275911

	sign := 1.0
	if x < 0 {
		sign = -1.0
		x = -x
	}

	t := 1.0 / (1.0 + p*x)
	y := 1.0 - (((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*math.Exp(-x*x)

	return sign * y
}

// CheckFailures checks all nodes for failures using phi accrual
func (fd *FailureDetector) CheckFailures() {
	now := time.Now()
	members := fd.membership.GetAllMembers()

	for _, member := range members {
		if member.NodeID == fd.membership.localID {
			continue
		}

		oldStatus := member.Status
		phi := fd.calculatePhi(member.NodeID, now)

		var newStatus MemberStatus
		if phi >= fd.phiDeadThreshold {
			newStatus = StatusDead
		} else if phi >= fd.phiThreshold {
			newStatus = StatusSuspected
		} else {
			newStatus = StatusAlive
		}

		// Update status if changed
		if newStatus != oldStatus {
			fd.updateStatus(member, newStatus, now)
		}
	}
}

// updateStatus updates a node's status and triggers callbacks
func (fd *FailureDetector) updateStatus(member *Member, newStatus MemberStatus, now time.Time) {
	fd.mu.Lock()
	oldStatus := member.Status
	member.Status = newStatus

	// Update tracking maps
	switch newStatus {
	case StatusAlive:
		delete(fd.suspects, member.NodeID)
		delete(fd.deadNodes, member.NodeID)
	case StatusSuspected:
		fd.suspects[member.NodeID] = now
		delete(fd.deadNodes, member.NodeID)
	case StatusDead:
		delete(fd.suspects, member.NodeID)
		fd.deadNodes[member.NodeID] = now
	}

	callbacks := make([]StatusChangeCallback, len(fd.callbacks))
	copy(callbacks, fd.callbacks)
	fd.mu.Unlock()

	// Trigger callbacks outside the lock
	for _, cb := range callbacks {
		cb(member.NodeID, oldStatus, newStatus)
	}
}

// IsSuspected returns true if the node is currently suspected
func (fd *FailureDetector) IsSuspected(nodeID string) bool {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	_, exists := fd.suspects[nodeID]
	return exists
}

// IsDead returns true if the node is marked as dead
func (fd *FailureDetector) IsDead(nodeID string) bool {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	_, exists := fd.deadNodes[nodeID]
	return exists
}

// IsAlive returns true if the node is considered alive
func (fd *FailureDetector) IsAlive(nodeID string) bool {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	_, suspected := fd.suspects[nodeID]
	_, dead := fd.deadNodes[nodeID]
	return !suspected && !dead
}

// GetSuspectedNodes returns a list of all suspected node IDs
func (fd *FailureDetector) GetSuspectedNodes() []string {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	nodes := make([]string, 0, len(fd.suspects))
	for nodeID := range fd.suspects {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// GetDeadNodes returns a list of all dead node IDs
func (fd *FailureDetector) GetDeadNodes() []string {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	nodes := make([]string, 0, len(fd.deadNodes))
	for nodeID := range fd.deadNodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// GetAliveNodes returns a list of all alive node IDs
func (fd *FailureDetector) GetAliveNodes() []string {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	members := fd.membership.GetAllMembers()
	alive := make([]string, 0, len(members))

	for _, member := range members {
		_, suspected := fd.suspects[member.NodeID]
		_, dead := fd.deadNodes[member.NodeID]
		if !suspected && !dead {
			alive = append(alive, member.NodeID)
		}
	}
	return alive
}

// GetPhiThreshold returns the current phi threshold for suspicion
func (fd *FailureDetector) GetPhiThreshold() float64 {
	return fd.phiThreshold
}

// SetPhiThreshold sets the phi threshold for suspicion
func (fd *FailureDetector) SetPhiThreshold(threshold float64) {
	fd.phiThreshold = threshold
}

// GetPhiDeadThreshold returns the current phi threshold for marking dead
func (fd *FailureDetector) GetPhiDeadThreshold() float64 {
	return fd.phiDeadThreshold
}

// SetPhiDeadThreshold sets the phi threshold for marking dead
func (fd *FailureDetector) SetPhiDeadThreshold(threshold float64) {
	fd.phiDeadThreshold = threshold
}

// GetNodePhi returns the current phi value for a node (for monitoring)
func (fd *FailureDetector) GetNodePhi(nodeID string) float64 {
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return fd.calculatePhi(nodeID, time.Now())
}
