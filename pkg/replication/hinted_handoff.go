package replication

import (
	"sync"
	"time"

	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/types"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// HintedHandoff stores writes for temporarily unavailable nodes
type HintedHandoff struct {
	nodeInfo   types.NodeInfo
	membership *membership.Membership
	storage    types.Storage
	config     types.Config
	hints      map[string][]*Hint
	mu         sync.RWMutex
}

// Hint represents a stored write for a temporarily unavailable node
type Hint struct {
	ForNode   string
	Key       string
	Value     versioning.VersionedValue
	Timestamp time.Time
}

// NewHintedHandoff creates a new HintedHandoff with typed dependencies
func NewHintedHandoff(nodeInfo types.NodeInfo, membershipMgr *membership.Membership, storage types.Storage, config types.Config) *HintedHandoff {
	return &HintedHandoff{
		nodeInfo:   nodeInfo,
		membership: membershipMgr,
		storage:    storage,
		config:     config,
		hints:      make(map[string][]*Hint),
	}
}

// StoreHint stores a hint for later delivery
func (h *HintedHandoff) StoreHint(nodeID string, hint *Hint) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.hints[nodeID] = append(h.hints[nodeID], hint)
	return nil
}

// DeliverHints attempts to deliver stored hints
func (h *HintedHandoff) DeliverHints() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for nodeID, hints := range h.hints {
		if len(hints) == 0 {
			continue
		}

		// Check if node is alive
		if !h.isNodeAlive(nodeID) {
			continue
		}

		// Deliver hints
		delivered := make([]bool, len(hints))
		for i, hint := range hints {
			if h.deliverHint(nodeID, hint) {
				delivered[i] = true
			}
		}

		// Remove delivered hints
		remaining := make([]*Hint, 0)
		for i, hint := range hints {
			if !delivered[i] {
				remaining = append(remaining, hint)
			}
		}
		h.hints[nodeID] = remaining
	}
}

func (h *HintedHandoff) isNodeAlive(nodeID string) bool {
	member := h.membership.GetMember(nodeID)
	if member == nil {
		return false
	}
	return member.Status == membership.StatusAlive
}

func (h *HintedHandoff) deliverHint(nodeID string, hint *Hint) bool {
	// In production, would send hint via RPC to target node
	// For now, this is a stub that will be implemented when RPC layer exists
	_ = nodeID
	_ = hint
	return true
}
