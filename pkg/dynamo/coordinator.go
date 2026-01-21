package dynamo

import (
	"context"
	"time"

	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/replication"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// Coordinator manages request coordination with state machine approach
type Coordinator struct {
	node *Node
}

func NewCoordinator(node *Node) *Coordinator {
	return &Coordinator{
		node: node,
	}
}

// Get retrieves a value with quorum semantics
func (c *Coordinator) Get(ctx context.Context, key string) (*GetResult, error) {
	// 1. Determine preference list
	preferenceList := c.node.ring.GetPreferenceList(key, c.node.config.N)
	if len(preferenceList) == 0 {
		return nil, ErrNodeNotFound
	}

	// 2. Send read requests to all N replicas
	type response struct {
		values []versioning.VersionedValue
		nodeID string
		err    error
	}

	responses := make(chan response, len(preferenceList))

	for _, nodeID := range preferenceList {
		go func(nid string) {
			values, err := c.readFromNode(nid, key)
			responses <- response{values: values, nodeID: nid, err: err}
		}(nodeID)
	}

	// 3. Wait for R responses or timeout
	allValues := make([]versioning.VersionedValue, 0)
	successCount := 0
	timeout := time.After(c.node.config.RequestTimeout)

	for successCount < c.node.config.R {
		select {
		case resp := <-responses:
			if resp.err == nil && len(resp.values) > 0 {
				allValues = append(allValues, resp.values...)
				successCount++
			}
		case <-timeout:
			if successCount == 0 {
				return nil, ErrTimeout
			}
			return nil, ErrReadQuorumFailed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// 4. Reconcile concurrent versions
	concurrent := versioning.ReconcileConcurrent(allValues)

	result := &GetResult{
		Values:  concurrent,
		Context: c.buildContext(concurrent),
	}

	// 5. Async read repair
	if c.node.config.ReadRepairEnabled {
		go c.readRepair(key, concurrent, preferenceList)
	}

	return result, nil
}

// Put stores a value with quorum semantics
func (c *Coordinator) Put(ctx context.Context, key string, value []byte, context *Context) error {
	// 1. Determine preference list
	preferenceList := c.node.ring.GetPreferenceList(key, c.node.config.N)
	if len(preferenceList) == 0 {
		return ErrNodeNotFound
	}

	// 2. Generate new version
	var newClock *versioning.VectorClock
	if context != nil && context.VectorClock != nil {
		newClock = context.VectorClock.Copy()
	} else {
		newClock = versioning.NewVectorClock()
	}
	newClock.Increment(c.node.id)

	// Prune if necessary
	newClock.Prune(c.node.config.VectorClockMaxSize)

	versioned := versioning.VersionedValue{
		Data:        value,
		VectorClock: newClock,
	}

	// 3. Send write requests to N replicas
	type response struct {
		nodeID string
		err    error
	}

	responses := make(chan response, len(preferenceList))

	for _, nodeID := range preferenceList {
		go func(nid string) {
			err := c.writeToNode(nid, key, versioned)
			responses <- response{nodeID: nid, err: err}
		}(nodeID)
	}

	// 4. Wait for W acknowledgments or timeout
	acks := 0
	failedNodes := make([]string, 0)
	timeout := time.After(c.node.config.RequestTimeout)

	for acks < c.node.config.W && len(failedNodes) < len(preferenceList) {
		select {
		case resp := <-responses:
			if resp.err == nil {
				acks++
			} else {
				failedNodes = append(failedNodes, resp.nodeID)
			}
		case <-timeout:
			if acks == 0 {
				return ErrTimeout
			}
			return ErrWriteQuorumFailed
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// 5. Handle hinted handoff for failed nodes
	if c.node.config.HintedHandoffEnabled && len(failedNodes) > 0 {
		c.handleHintedHandoff(key, versioned, failedNodes, preferenceList)
	}

	return nil
}

// Delete removes a key (tombstone)
func (c *Coordinator) Delete(ctx context.Context, key string, context *Context) error {
	// Deletion is implemented as a write with empty value
	// In production, you'd use a tombstone marker
	return c.Put(ctx, key, nil, context)
}

func (c *Coordinator) readFromNode(nodeID string, key string) ([]versioning.VersionedValue, error) {
	if nodeID == c.node.id {
		// Local read
		return c.node.storage.Get(key)
	}

	// Remote read - would use RPC in production
	member := c.node.membership.GetMember(nodeID)
	if member == nil || member.Status != membership.StatusAlive {
		return nil, ErrNodeNotFound
	}

	// Simplified: In production, this would be an RPC call
	return c.node.storage.Get(key) // Placeholder
}

func (c *Coordinator) writeToNode(nodeID string, key string, value versioning.VersionedValue) error {
	if nodeID == c.node.id {
		// Local write
		return c.node.storage.Put(key, value)
	}

	// Remote write - would use RPC in production
	member := c.node.membership.GetMember(nodeID)
	if member == nil || member.Status != membership.StatusAlive {
		return ErrNodeNotFound
	}

	// Simplified: In production, this would be an RPC call
	return c.node.storage.Put(key, value) // Placeholder
}

func (c *Coordinator) readRepair(key string, latest []versioning.VersionedValue, preferenceList []string) {
	// Update replicas with stale data
	for _, nodeID := range preferenceList {
		if nodeID == c.node.id {
			continue
		}

		current, err := c.readFromNode(nodeID, key)
		if err != nil {
			continue
		}

		// Check if this node has stale data
		if c.hasStaleData(current, latest) {
			// Send the latest version
			for _, v := range latest {
				c.writeToNode(nodeID, key, v)
			}
		}
	}
}

func (c *Coordinator) hasStaleData(current, latest []versioning.VersionedValue) bool {
	if len(current) == 0 {
		return true
	}

	for _, l := range latest {
		found := false
		for _, c := range current {
			if c.Equals(&l) {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}

	return false
}

func (c *Coordinator) buildContext(values []versioning.VersionedValue) *Context {
	if len(values) == 0 {
		return &Context{VectorClock: versioning.NewVectorClock()}
	}

	// Merge all vector clocks
	merged := values[0].VectorClock.Copy()
	for _, v := range values[1:] {
		merged = merged.Merge(v.VectorClock)
	}

	return &Context{VectorClock: merged}
}

func (c *Coordinator) handleHintedHandoff(key string, value versioning.VersionedValue,
	failedNodes []string, preferenceList []string) {

	// Find healthy nodes to store hints
	healthyNodes := make([]string, 0)
	for _, nodeID := range preferenceList {
		isHealthy := true
		for _, failed := range failedNodes {
			if nodeID == failed {
				isHealthy = false
				break
			}
		}
		if isHealthy {
			healthyNodes = append(healthyNodes, nodeID)
		}
	}

	// Store hints on healthy nodes
	for _, failedNode := range failedNodes {
		if len(healthyNodes) > 0 {
			hintNode := healthyNodes[0]
			hint := &replication.Hint{
				ForNode:   failedNode,
				Key:       key,
				Value:     value,
				Timestamp: time.Now(),
			}
			c.node.hintedHoff.StoreHint(hintNode, hint)
			healthyNodes = healthyNodes[1:] // Rotate
		}
	}
}
