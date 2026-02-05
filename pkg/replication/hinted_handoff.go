package replication

import (
	"context"
	"sync"
	"time"

	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/rpc"
	"github.com/tripab/toy-dynamo/pkg/types"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// HintedHandoff stores writes for temporarily unavailable nodes
type HintedHandoff struct {
	nodeInfo   types.NodeInfo
	membership *membership.Membership
	storage    types.Storage
	config     types.Config
	rpcClient  *rpc.Client
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

// SetRPCClient sets the RPC client for hint delivery
func (h *HintedHandoff) SetRPCClient(client *rpc.Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rpcClient = client
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

// GetHintCounts returns the number of pending hints per target node.
func (h *HintedHandoff) GetHintCounts() map[string]int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	counts := make(map[string]int, len(h.hints))
	for nodeID, hints := range h.hints {
		if len(hints) > 0 {
			counts[nodeID] = len(hints)
		}
	}
	return counts
}

// GetTotalHintCount returns the total number of pending hints across all nodes.
func (h *HintedHandoff) GetTotalHintCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, hints := range h.hints {
		total += len(hints)
	}
	return total
}

func (h *HintedHandoff) deliverHint(nodeID string, hint *Hint) bool {
	// Check if RPC client is available
	if h.rpcClient == nil {
		return false
	}

	// Get the target node's address from membership
	member := h.membership.GetMember(nodeID)
	if member == nil {
		return false
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), h.config.GetRequestTimeout())
	defer cancel()

	// Deliver hint via RPC
	resp, err := h.rpcClient.DeliverHint(ctx, member.Address, h.nodeInfo.GetID(), hint.Key, hint.Value)
	if err != nil {
		// Delivery failed - hint will be retried later
		return false
	}

	return resp.Success
}
