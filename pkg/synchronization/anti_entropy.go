package synchronization

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"math/rand"
	"sort"
	"sync"

	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/rpc"
	"github.com/tripab/toy-dynamo/pkg/types"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// AntiEntropy performs background replica synchronization using Merkle trees
// to efficiently detect and repair inconsistencies between replicas.
type AntiEntropy struct {
	nodeID     string
	storage    types.Storage
	ring       types.Ring
	membership *membership.Membership
	config     types.Config
	rpcClient  *rpc.Client
	trees      map[KeyRange]*MerkleTree
	mu         sync.RWMutex
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

// SetRPCClient sets the RPC client for anti-entropy communication
func (ae *AntiEntropy) SetRPCClient(client *rpc.Client) {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	ae.rpcClient = client
}

// Run executes one anti-entropy round
func (ae *AntiEntropy) Run() {
	ae.mu.RLock()
	if ae.rpcClient == nil {
		ae.mu.RUnlock()
		return // Cannot sync without RPC client
	}
	ae.mu.RUnlock()

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

// RebuildTree forces a rebuild of the Merkle tree for a key range
func (ae *AntiEntropy) RebuildTree(keyRange KeyRange) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	data := ae.getRangeData(keyRange)
	tree := NewMerkleTree(keyRange)
	tree.Build(data)
	ae.trees[keyRange] = tree
}

// InvalidateTree removes a cached tree, forcing rebuild on next sync
func (ae *AntiEntropy) InvalidateTree(keyRange KeyRange) {
	ae.mu.Lock()
	defer ae.mu.Unlock()
	delete(ae.trees, keyRange)
}

func (ae *AntiEntropy) getOrBuildTree(keyRange KeyRange) *MerkleTree {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	tree, exists := ae.trees[keyRange]
	if exists {
		return tree
	}

	// Build new tree from storage data
	data := ae.getRangeData(keyRange)
	tree = NewMerkleTree(keyRange)
	tree.Build(data)
	ae.trees[keyRange] = tree

	return tree
}

func (ae *AntiEntropy) syncWithReplica(replicaID string, keyRange KeyRange, localTree *MerkleTree) {
	// Get replica address
	member := ae.membership.GetMember(replicaID)
	if member == nil {
		return
	}

	// Skip if replica is not alive
	if member.Status != membership.StatusAlive {
		return
	}

	ae.mu.RLock()
	client := ae.rpcClient
	ae.mu.RUnlock()

	if client == nil {
		return
	}

	// Make RPC call to get remote sync response
	ctx, cancel := context.WithTimeout(context.Background(), ae.config.GetRequestTimeout())
	defer cancel()

	syncReq := &rpc.SyncRequest{
		KeyRange: rpc.KeyRange{
			Start: keyRange.Start,
			End:   keyRange.End,
		},
		TreeRoot: []byte(localTree.GetRootHash()),
	}

	resp, err := client.Sync(ctx, member.Address, syncReq)
	if err != nil {
		// Sync failed - will retry on next round
		return
	}

	// If root hashes match, replicas are in sync
	if string(resp.TreeRoot) == localTree.GetRootHash() {
		return
	}

	// Sync differing keys reported by the remote
	for _, key := range resp.Differences {
		ae.syncKey(replicaID, key)
	}

	// Also check locally for keys that might need syncing
	localDiffs := ae.findLocalDifferences(keyRange, resp.TreeRoot)
	for _, key := range localDiffs {
		ae.pushKeyToReplica(replicaID, key)
	}
}

// findLocalDifferences finds keys in the local tree that differ from remote
func (ae *AntiEntropy) findLocalDifferences(keyRange KeyRange, remoteRootHash []byte) []string {
	ae.mu.RLock()
	tree := ae.trees[keyRange]
	ae.mu.RUnlock()

	if tree == nil {
		return nil
	}

	// If root hashes are the same, no differences
	if tree.GetRootHash() == string(remoteRootHash) {
		return nil
	}

	// Return all local keys as potential differences
	// In a more sophisticated implementation, we would do a proper tree comparison
	data := ae.getRangeData(keyRange)
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	return keys
}

// getResponsibleRanges returns the key ranges this node is responsible for
// based on its position in the consistent hash ring
func (ae *AntiEntropy) getResponsibleRanges() []KeyRange {
	tokens := ae.ring.GetTokens(ae.nodeID)
	if len(tokens) == 0 {
		// Fallback to full range if no tokens assigned
		return []KeyRange{{Start: "", End: "\xff"}}
	}

	// Sort tokens to determine ranges
	sortedTokens := make([]uint32, len(tokens))
	copy(sortedTokens, tokens)
	sort.Slice(sortedTokens, func(i, j int) bool {
		return sortedTokens[i] < sortedTokens[j]
	})

	// Create ranges for each token
	// Each token is responsible for keys from the previous token up to itself
	ranges := make([]KeyRange, 0, len(sortedTokens))

	for i, token := range sortedTokens {
		var startToken uint32
		if i == 0 {
			// First token wraps around from the last
			startToken = sortedTokens[len(sortedTokens)-1]
		} else {
			startToken = sortedTokens[i-1]
		}

		ranges = append(ranges, KeyRange{
			Start: tokenToKeyPrefix(startToken),
			End:   tokenToKeyPrefix(token),
		})
	}

	return ranges
}

// selectReplica selects a random replica responsible for the given key range
func (ae *AntiEntropy) selectReplica(keyRange KeyRange) string {
	// Generate a sample key in the middle of the range to find responsible nodes
	sampleKey := keyRange.Start
	if keyRange.Start != "" || keyRange.End != "" {
		sampleKey = keyRange.Start + "sample"
	}

	// Get preference list for this key range
	prefList := ae.ring.GetPreferenceList(sampleKey, ae.config.GetN())
	if len(prefList) == 0 {
		return ""
	}

	// Filter out self and dead nodes
	aliveReplicas := make([]string, 0)
	for _, nodeID := range prefList {
		if nodeID == ae.nodeID {
			continue
		}
		member := ae.membership.GetMember(nodeID)
		if member != nil && member.Status == membership.StatusAlive {
			aliveReplicas = append(aliveReplicas, nodeID)
		}
	}

	if len(aliveReplicas) == 0 {
		return ""
	}

	// Select random replica
	return aliveReplicas[rand.Intn(len(aliveReplicas))]
}

// getRangeData queries storage for all keys in the given range
func (ae *AntiEntropy) getRangeData(keyRange KeyRange) map[string][]byte {
	data, err := ae.storage.GetRange(keyRange.Start, keyRange.End)
	if err != nil {
		return make(map[string][]byte)
	}

	// Convert versioned values to bytes for Merkle tree
	// We hash all versions together to detect any differences
	result := make(map[string][]byte)
	for key, versions := range data {
		// Combine all versions into a single hash for comparison
		result[key] = hashVersions(versions)
	}

	return result
}

// syncKey fetches a key from a replica and reconciles with local storage
func (ae *AntiEntropy) syncKey(replicaID string, key string) {
	member := ae.membership.GetMember(replicaID)
	if member == nil || member.Status != membership.StatusAlive {
		return
	}

	ae.mu.RLock()
	client := ae.rpcClient
	ae.mu.RUnlock()

	if client == nil {
		return
	}

	// Fetch key from replica
	ctx, cancel := context.WithTimeout(context.Background(), ae.config.GetRequestTimeout())
	defer cancel()

	remoteValues, err := client.GetValues(ctx, member.Address, key)
	if err != nil {
		return
	}

	// Get local values
	localValues, _ := ae.storage.Get(key)

	// Reconcile: merge remote values with local
	for _, remoteVal := range remoteValues {
		shouldStore := true

		for _, localVal := range localValues {
			cmp := localVal.VectorClock.Compare(remoteVal.VectorClock)
			if cmp == versioning.After || cmp == versioning.Equal {
				// Local is newer or equal, no need to store remote
				shouldStore = false
				break
			}
		}

		if shouldStore {
			ae.storage.Put(key, remoteVal)
		}
	}

	// Invalidate tree since data changed
	ae.InvalidateTree(KeyRange{Start: "", End: "\xff"})
}

// pushKeyToReplica pushes a local key to a replica
func (ae *AntiEntropy) pushKeyToReplica(replicaID string, key string) {
	member := ae.membership.GetMember(replicaID)
	if member == nil || member.Status != membership.StatusAlive {
		return
	}

	ae.mu.RLock()
	client := ae.rpcClient
	ae.mu.RUnlock()

	if client == nil {
		return
	}

	// Get local values
	localValues, err := ae.storage.Get(key)
	if err != nil || len(localValues) == 0 {
		return
	}

	// Push each version to the replica
	ctx, cancel := context.WithTimeout(context.Background(), ae.config.GetRequestTimeout())
	defer cancel()

	for _, val := range localValues {
		client.Put(ctx, member.Address, key, val)
	}
}

// tokenToKeyPrefix converts a uint32 token to a key prefix string
func tokenToKeyPrefix(token uint32) string {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, token)
	return string(buf)
}

// keyToToken converts a key to its token position on the ring
func keyToToken(key string) uint32 {
	hash := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(hash[:4])
}

// hashVersions creates a combined hash of all versions for comparison
func hashVersions(versions []versioning.VersionedValue) []byte {
	if len(versions) == 0 {
		return nil
	}

	// Sort versions by their vector clock for deterministic hashing
	sorted := make([]versioning.VersionedValue, len(versions))
	copy(sorted, versions)

	// Create a combined representation
	var combined []byte
	for _, v := range sorted {
		combined = append(combined, v.Data...)
		if v.VectorClock != nil {
			for nodeID, counter := range v.VectorClock.Versions {
				combined = append(combined, []byte(nodeID)...)
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, counter)
				combined = append(combined, buf...)
			}
		}
	}

	hash := md5.Sum(combined)
	return hash[:]
}
