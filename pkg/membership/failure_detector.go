package membership

import (
	"sync"
	"time"
)

type FailureDetector struct {
	membership *Membership
	timeout    time.Duration
	suspects   map[string]time.Time
	mu         sync.RWMutex
}

func NewFailureDetector(membership *Membership, timeout time.Duration) *FailureDetector {
	return &FailureDetector{
		membership: membership,
		timeout:    timeout,
		suspects:   make(map[string]time.Time),
	}
}

// CheckFailures checks for failed nodes
func (fd *FailureDetector) CheckFailures() {
	now := time.Now()
	members := fd.membership.GetAllMembers()

	for _, member := range members {
		if member.NodeID == fd.membership.localID {
			continue
		}

		elapsed := now.Sub(member.Timestamp)

		if elapsed > fd.timeout*3 {
			// Mark as dead
			member.Status = StatusDead
		} else if elapsed > fd.timeout {
			// Mark as suspected
			if member.Status == StatusAlive {
				fd.mu.Lock()
				fd.suspects[member.NodeID] = now
				fd.mu.Unlock()
				member.Status = StatusSuspected
			}
		} else {
			// Mark as alive
			if member.Status != StatusAlive {
				fd.mu.Lock()
				delete(fd.suspects, member.NodeID)
				fd.mu.Unlock()
				member.Status = StatusAlive
			}
		}
	}
}

func (fd *FailureDetector) IsSuspected(nodeID string) bool {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	_, exists := fd.suspects[nodeID]
	return exists
}
