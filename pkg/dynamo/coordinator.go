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
	node     *Node
	selector *CoordinatorSelector
}

func NewCoordinator(node *Node) *Coordinator {
	return &Coordinator{
		node: node,
	}
}

// SetSelector sets the coordinator selector for latency-based routing.
func (c *Coordinator) SetSelector(selector *CoordinatorSelector) {
	c.selector = selector
}

// Get retrieves a value with quorum semantics
func (c *Coordinator) Get(ctx context.Context, key string) (*GetResult, error) {
	// 1. Determine preference list
	preferenceList := c.node.ring.GetPreferenceList(key, c.node.config.N)
	if len(preferenceList) == 0 {
		return nil, ErrNodeNotFound
	}

	// Reorder by latency if coordinator selection is enabled
	if c.selector != nil {
		preferenceList = c.selector.ReorderByLatency(preferenceList)
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

	// Reorder by latency if coordinator selection is enabled
	if c.selector != nil {
		preferenceList = c.selector.ReorderByLatency(preferenceList)
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

// Delete removes a key by writing a tombstone
func (c *Coordinator) Delete(ctx context.Context, key string, context *Context) error {
	// 1. Determine preference list
	preferenceList := c.node.ring.GetPreferenceList(key, c.node.config.N)
	if len(preferenceList) == 0 {
		return ErrNodeNotFound
	}

	// Reorder by latency if coordinator selection is enabled
	if c.selector != nil {
		preferenceList = c.selector.ReorderByLatency(preferenceList)
	}

	// 2. Generate new version for the tombstone
	var newClock *versioning.VectorClock
	if context != nil && context.VectorClock != nil {
		newClock = context.VectorClock.Copy()
	} else {
		newClock = versioning.NewVectorClock()
	}
	newClock.Increment(c.node.id)

	// Prune if necessary
	newClock.Prune(c.node.config.VectorClockMaxSize)

	// Create tombstone (empty value with IsTombstone flag)
	tombstone := versioning.VersionedValue{
		Data:        nil,
		VectorClock: newClock,
		IsTombstone: true,
	}

	// 3. Send write requests to N replicas
	type response struct {
		nodeID string
		err    error
	}

	responses := make(chan response, len(preferenceList))

	for _, nodeID := range preferenceList {
		go func(nid string) {
			err := c.writeToNode(nid, key, tombstone)
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
		c.handleHintedHandoff(key, tombstone, failedNodes, preferenceList)
	}

	return nil
}

func (c *Coordinator) readFromNode(nodeID string, key string) ([]versioning.VersionedValue, error) {
	if nodeID == c.node.id {
		// Local read
		start := time.Now()
		values, err := c.node.storage.Get(key)
		if c.selector != nil {
			c.selector.RecordLatency(nodeID, time.Since(start))
		}
		return values, err
	}

	// Remote read via RPC
	member := c.node.membership.GetMember(nodeID)
	if member == nil || member.Status != membership.StatusAlive {
		return nil, ErrNodeNotFound
	}

	// Use RPC client to read from remote node
	ctx, cancel := context.WithTimeout(context.Background(), c.node.config.RequestTimeout)
	defer cancel()

	start := time.Now()
	values, err := c.node.rpcClient.GetValues(ctx, member.Address, key)
	if c.selector != nil {
		c.selector.RecordLatency(nodeID, time.Since(start))
	}
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (c *Coordinator) writeToNode(nodeID string, key string, value versioning.VersionedValue) error {
	if nodeID == c.node.id {
		// Local write
		start := time.Now()
		err := c.node.storage.Put(key, value)
		if c.selector != nil {
			c.selector.RecordLatency(nodeID, time.Since(start))
		}
		return err
	}

	// Remote write via RPC
	member := c.node.membership.GetMember(nodeID)
	if member == nil || member.Status != membership.StatusAlive {
		return ErrNodeNotFound
	}

	// Use RPC client to write to remote node
	ctx, cancel := context.WithTimeout(context.Background(), c.node.config.RequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := c.node.rpcClient.Put(ctx, member.Address, key, value)
	if c.selector != nil {
		c.selector.RecordLatency(nodeID, time.Since(start))
	}
	if err != nil {
		return err
	}

	if !resp.Success {
		return ErrWriteFailed
	}

	return nil
}

func (c *Coordinator) readRepair(key string, latest []versioning.VersionedValue, preferenceList []string) {
	// Update replicas with stale data
	// This runs asynchronously so don't block on errors
	for _, nodeID := range preferenceList {
		if nodeID == c.node.id {
			continue
		}

		current, err := c.readFromNode(nodeID, key)
		if err != nil {
			// Node might be unreachable - skip it
			continue
		}

		// Check if this node has stale data using vector clock comparison
		if c.hasStaleData(current, latest) {
			// Send each latest version to bring the replica up to date
			// We send all latest versions because they might be concurrent
			for _, v := range latest {
				if err := c.writeToNode(nodeID, key, v); err != nil {
					// Best effort - continue with other versions
					continue
				}
			}
		}
	}
}

// hasStaleData checks if current values are stale compared to latest values
// using vector clock causality. A node has stale data if:
// 1. It has no data at all
// 2. Any of its versions are ancestors (causally before) the latest versions
// 3. It's missing any of the latest concurrent versions
func (c *Coordinator) hasStaleData(current, latest []versioning.VersionedValue) bool {
	if len(current) == 0 && len(latest) > 0 {
		return true
	}

	if len(latest) == 0 {
		return false
	}

	// For each latest version, check if the current node has it or something newer
	for _, l := range latest {
		hasLatestOrNewer := false

		for _, curr := range current {
			ordering := curr.VectorClock.Compare(l.VectorClock)
			// Current has this version or something newer/equal
			if ordering == versioning.Equal || ordering == versioning.After {
				hasLatestOrNewer = true
				break
			}
			// If concurrent, check if data and tombstone status match (same version)
			if ordering == versioning.Concurrent && curr.Equals(&l) {
				hasLatestOrNewer = true
				break
			}
		}

		if !hasLatestOrNewer {
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

// getFilteredPreferenceList returns nodes that are not known to be dead
// This implements sloppy quorum by skipping nodes that are definitely unreachable
// Suspected nodes are still included (they may be temporarily unreachable)
func (c *Coordinator) getFilteredPreferenceList(key string) []string {
	preferenceList := c.node.ring.GetPreferenceList(key, c.node.config.N)
	filtered := make([]string, 0, len(preferenceList))

	for _, nodeID := range preferenceList {
		// Always include local node
		if nodeID == c.node.id {
			filtered = append(filtered, nodeID)
			continue
		}

		// Skip nodes that are marked as dead
		member := c.node.membership.GetMember(nodeID)
		if member == nil {
			continue
		}

		// Include alive and suspected nodes (suspected may still be reachable)
		if member.Status != membership.StatusDead {
			filtered = append(filtered, nodeID)
		}
	}

	return filtered
}

// getAlivePreferenceList returns only nodes that are considered alive
// This is more strict and excludes suspected nodes
func (c *Coordinator) getAlivePreferenceList(key string) []string {
	preferenceList := c.node.ring.GetPreferenceList(key, c.node.config.N)
	filtered := make([]string, 0, len(preferenceList))

	for _, nodeID := range preferenceList {
		// Always include local node
		if nodeID == c.node.id {
			filtered = append(filtered, nodeID)
			continue
		}

		// Only include nodes that are alive
		member := c.node.membership.GetMember(nodeID)
		if member != nil && member.Status == membership.StatusAlive {
			filtered = append(filtered, nodeID)
		}
	}

	return filtered
}
