package synchronization

import (
	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/types"
)

// AntiEntropy performs background replica synchronization
type AntiEntropy struct {
	nodeID     string
	storage    types.Storage
	ring       types.Ring
	membership *membership.Membership
	config     types.Config
	trees      map[KeyRange]*MerkleTree
}

// NewAntiEntropy creates a new AntiEntropy with typed dependencies
func NewAntiEntropy(nodeID string, storage types.Storage, ring types.Ring, membershipMgr *membership.Membership, config types.Config) *AntiEntropy {
	return &AntiEntropy{
		nodeID:     nodeID,
		storage:    storage,
		ring:       ring,
		membership: membershipMgr,
		config:     config,
		trees:      make(map[KeyRange]*MerkleTree),
	}
}

// Run executes one anti-entropy round
func (ae *AntiEntropy) Run() {
	// Get all key ranges this node is responsible for
	ranges := ae.getResponsibleRanges()

	for _, keyRange := range ranges {
		// Get or build Merkle tree for this range
		tree := ae.getOrBuildTree(keyRange)

		// Select random replica for this range
		replica := ae.selectReplica(keyRange)
		if replica == "" {
			continue
		}

		// Sync with replica
		ae.syncWithReplica(replica, keyRange, tree)
	}
}

func (ae *AntiEntropy) getOrBuildTree(keyRange KeyRange) *MerkleTree {
	tree, exists := ae.trees[keyRange]

	if exists {
		return tree
	}

	// Build new tree
	data := ae.getRangeData(keyRange)
	tree = NewMerkleTree(keyRange)
	tree.Build(data)

	ae.trees[keyRange] = tree

	return tree
}

func (ae *AntiEntropy) syncWithReplica(replicaID string, keyRange KeyRange, localTree *MerkleTree) {
	// In production, would make RPC call to get remote tree root
	remoteRoot := ae.getRemoteTreeRoot(replicaID, keyRange)
	if remoteRoot == nil {
		return
	}

	// Compare trees
	differences := localTree.FindDifferences(localTree.root, remoteRoot)

	// Sync differing keys
	for _, key := range differences {
		ae.syncKey(replicaID, key)
	}
}

func (ae *AntiEntropy) getResponsibleRanges() []KeyRange {
	// In production, determine ranges based on token ownership
	return []KeyRange{
		{Start: "", End: "zzz"},
	}
}

func (ae *AntiEntropy) selectReplica(keyRange KeyRange) string {
	// Select random replica responsible for this range
	return "replica1"
}

func (ae *AntiEntropy) getRangeData(keyRange KeyRange) map[string][]byte {
	// In production, query storage for keys in range
	return make(map[string][]byte)
}

func (ae *AntiEntropy) getRemoteTreeRoot(replicaID string, keyRange KeyRange) *MerkleNode {
	// In production, RPC call to get remote tree
	return nil
}

func (ae *AntiEntropy) syncKey(replicaID string, key string) {
	// In production, fetch key from replica and reconcile
}
