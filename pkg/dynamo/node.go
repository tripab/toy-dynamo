package dynamo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/replication"
	"github.com/tripab/toy-dynamo/pkg/ring"
	"github.com/tripab/toy-dynamo/pkg/rpc"
	"github.com/tripab/toy-dynamo/pkg/storage"
	"github.com/tripab/toy-dynamo/pkg/synchronization"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// Node represents a Dynamo storage node
type Node struct {
	id      string
	address string
	config  *Config

	ring         *ring.Ring
	storage      storage.Storage
	replicator   *replication.Replicator
	coordinator  *Coordinator
	membership   *membership.Membership
	antiEntropy  *synchronization.AntiEntropy
	hintedHoff   *replication.HintedHandoff
	failDetector *membership.FailureDetector

	// RPC components for inter-node communication
	rpcClient *rpc.Client
	rpcServer *rpc.Server

	// Tombstone compaction
	tombstoneCompactor *storage.TombstoneCompactor

	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup
	mu      sync.RWMutex
}

// NewNode creates a new Dynamo node
func NewNode(id, address string, config *Config) (*Node, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	node := &Node{
		id:      id,
		address: address,
		config:  config,
		stopCh:  make(chan struct{}),
	}

	// Initialize storage engine
	var err error
	node.storage, err = storage.NewStorage(config.StorageEngine, config.StoragePath, id)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Initialize ring with virtual nodes
	node.ring = ring.NewRing(config.N, config.VirtualNodes)

	// Initialize membership
	node.membership = membership.NewMembership(id, address, config)

	// Initialize failure detector
	node.failDetector = membership.NewFailureDetector(node.membership, config.RequestTimeout*3)

	// Initialize replication components with typed interfaces
	node.replicator = replication.NewReplicator(node, node.storage, node.ring, config)
	node.coordinator = NewCoordinator(node)
	node.hintedHoff = replication.NewHintedHandoff(node, node.membership, node.storage, config)
	node.antiEntropy = synchronization.NewAntiEntropy(id, node.storage, node.ring, node.membership, config)

	// Initialize RPC client and server
	node.rpcClient = rpc.NewClient(config.RequestTimeout)
	node.rpcServer = rpc.NewServer(address, node)

	// Set RPC client on membership for gossip
	node.membership.SetRPCClient(node.rpcClient)

	// Initialize tombstone compactor for memory storage
	// LSS storage has its own built-in compactor
	if config.StorageEngine == "memory" {
		node.tombstoneCompactor = storage.NewTombstoneCompactor(node.storage, config.TombstoneTTL)
	}

	return node, nil
}

// Start starts the node and all background processes
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Start RPC server first
	if err := n.rpcServer.Start(); err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	// Add self to ring
	member := &membership.Member{
		NodeID:    n.id,
		Address:   n.address,
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Tokens:    n.ring.AddNode(n.id, n.config.VirtualNodes),
		Timestamp: time.Now(),
	}
	n.membership.AddMember(member)

	// Start gossip protocol
	n.wg.Add(1)
	go n.gossipLoop()

	// Start failure detection
	n.wg.Add(1)
	go n.failureDetectionLoop()

	// Start anti-entropy
	n.wg.Add(1)
	go n.antiEntropyLoop()

	// Start hinted handoff delivery
	if n.config.HintedHandoffEnabled {
		n.wg.Add(1)
		go n.hintedHandoffLoop()
	}

	// Start tombstone compaction for memory storage
	if n.tombstoneCompactor != nil {
		n.wg.Add(1)
		go n.compactionLoop()
	}

	return nil
}

// Stop gracefully stops the node
func (n *Node) Stop() error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil
	}
	n.stopped = true
	close(n.stopCh)
	n.mu.Unlock()

	n.wg.Wait()

	// Stop RPC server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.rpcServer.Stop(ctx); err != nil {
		// Log but don't fail
		fmt.Printf("Warning: RPC server shutdown error: %v\n", err)
	}

	// Close RPC client
	n.rpcClient.Close()

	return n.storage.Close()
}

// Join adds this node to an existing ring using seed nodes
func (n *Node) Join(seeds []string) error {
	if len(seeds) == 0 {
		return nil // First node in cluster
	}

	// Contact seed nodes to get membership list
	for _, seed := range seeds {
		members, err := n.membership.SyncWithSeed(seed)
		if err != nil {
			continue
		}

		// Add all members to our ring
		for _, member := range members {
			if member.NodeID != n.id {
				n.ring.AddNodeWithTokens(member.NodeID, member.Tokens)
				n.membership.AddMember(member)
			}
		}
		return nil
	}

	return fmt.Errorf("failed to join cluster: no seed reachable")
}

// Get retrieves a value by key
func (n *Node) Get(ctx context.Context, key string) (*GetResult, error) {
	return n.coordinator.Get(ctx, key)
}

// Put stores a key-value pair
func (n *Node) Put(ctx context.Context, key string, value []byte, context *Context) error {
	return n.coordinator.Put(ctx, key, value, context)
}

// Delete removes a key
func (n *Node) Delete(ctx context.Context, key string, context *Context) error {
	return n.coordinator.Delete(ctx, key, context)
}

// Background loops

func (n *Node) gossipLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.membership.Gossip()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) failureDetectionLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.failDetector.CheckFailures()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) antiEntropyLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.AntiEntropyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.antiEntropy.Run()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) hintedHandoffLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.hintedHoff.DeliverHints()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) compactionLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.TombstoneCompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n.tombstoneCompactor != nil {
				n.tombstoneCompactor.Compact()
			}
		case <-n.stopCh:
			return
		}
	}
}

func validateConfig(c *Config) error {
	if c.N < 1 {
		return fmt.Errorf("N must be >= 1")
	}
	if c.R < 1 || c.R > c.N {
		return fmt.Errorf("R must be between 1 and N")
	}
	if c.W < 1 || c.W > c.N {
		return fmt.Errorf("W must be between 1 and N")
	}
	if c.VirtualNodes < 1 {
		return fmt.Errorf("VirtualNodes must be >= 1")
	}
	return nil
}

// Getter methods to implement types.NodeInfo interface

func (n *Node) GetID() string      { return n.id }
func (n *Node) GetAddress() string { return n.address }

// NodeOperations interface implementation for RPC server

// GetNodeID returns this node's ID (implements rpc.NodeOperations)
func (n *Node) GetNodeID() string {
	return n.id
}

// LocalGet retrieves values from local storage (implements rpc.NodeOperations)
func (n *Node) LocalGet(key string) ([]versioning.VersionedValue, error) {
	return n.storage.Get(key)
}

// LocalPut stores a value in local storage (implements rpc.NodeOperations)
func (n *Node) LocalPut(key string, value versioning.VersionedValue) error {
	return n.storage.Put(key, value)
}

// HandleGossip processes incoming gossip and returns local membership (implements rpc.NodeOperations)
func (n *Node) HandleGossip(members []rpc.MemberDTO) []rpc.MemberDTO {
	// Merge incoming membership information
	for _, m := range members {
		existing := n.membership.GetMember(m.NodeID)
		if existing == nil || m.Heartbeat > existing.Heartbeat {
			// Add or update member
			member := &membership.Member{
				NodeID:    m.NodeID,
				Address:   m.Address,
				Status:    membership.MemberStatus(m.Status),
				Heartbeat: m.Heartbeat,
				Tokens:    m.Tokens,
				Timestamp: m.Timestamp,
			}
			n.membership.AddMember(member)

			// Also add to ring if new
			if existing == nil && m.NodeID != n.id {
				n.ring.AddNodeWithTokens(m.NodeID, m.Tokens)
			}
		}
	}

	// Return our membership list
	localMembers := n.membership.GetAllMembers()
	result := make([]rpc.MemberDTO, len(localMembers))
	for i, m := range localMembers {
		result[i] = rpc.MemberDTO{
			NodeID:    m.NodeID,
			Address:   m.Address,
			Status:    int(m.Status),
			Heartbeat: m.Heartbeat,
			Tokens:    m.Tokens,
			Timestamp: m.Timestamp,
		}
	}
	return result
}

// HandleSync processes anti-entropy sync request (implements rpc.NodeOperations)
func (n *Node) HandleSync(req *rpc.SyncRequest) *rpc.SyncResponse {
	// Get data for the requested key range
	data, err := n.storage.GetRange(req.KeyRange.Start, req.KeyRange.End)
	if err != nil {
		return &rpc.SyncResponse{Error: err.Error()}
	}

	// Build Merkle tree for the range
	treeData := make(map[string][]byte)
	for key, values := range data {
		// Serialize values for tree
		for _, v := range values {
			treeData[key] = v.Data
		}
	}

	keyRange := synchronization.KeyRange{Start: req.KeyRange.Start, End: req.KeyRange.End}
	tree := synchronization.NewMerkleTree(keyRange)
	tree.Build(treeData)

	// Find differences if remote tree root provided
	var differences []string
	if len(req.TreeRoot) > 0 {
		// In a full implementation, we'd deserialize and compare trees
		// For now, return all keys as potential differences
		for key := range data {
			differences = append(differences, key)
		}
	}

	return &rpc.SyncResponse{
		TreeRoot:    []byte(tree.GetRootHash()),
		Differences: differences,
	}
}

// HandleHint processes a hinted handoff delivery (implements rpc.NodeOperations)
func (n *Node) HandleHint(req *rpc.HintRequest) error {
	// Store the hinted value locally
	value := req.Value.ToVersionedValue()
	return n.storage.Put(req.Key, value)
}
