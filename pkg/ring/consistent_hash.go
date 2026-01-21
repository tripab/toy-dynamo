package ring

import (
	"crypto/md5"
	"encoding/binary"
	"sort"
	"sync"
)

type Ring struct {
	vnodes     map[uint32]*VirtualNode
	nodes      map[string]*PhysicalNode
	sortedKeys []uint32
	replicas   int
	mu         sync.RWMutex
}

type VirtualNode struct {
	NodeID   string
	Position uint32
}

type PhysicalNode struct {
	NodeID string
	VNodes []uint32
}

func NewRing(replicas, vnodesPerNode int) *Ring {
	return &Ring{
		vnodes:   make(map[uint32]*VirtualNode),
		nodes:    make(map[string]*PhysicalNode),
		replicas: replicas,
	}
}

// AddNode adds a node with randomly assigned virtual nodes
func (r *Ring) AddNode(nodeID string, vnodesCount int) []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()

	tokens := make([]uint32, vnodesCount)
	physNode := &PhysicalNode{
		NodeID: nodeID,
		VNodes: tokens,
	}

	for i := 0; i < vnodesCount; i++ {
		// Generate token position
		hash := md5.Sum([]byte(nodeID + ":" + string(rune(i))))
		position := binary.BigEndian.Uint32(hash[:4])

		tokens[i] = position
		r.vnodes[position] = &VirtualNode{
			NodeID:   nodeID,
			Position: position,
		}
	}

	r.nodes[nodeID] = physNode
	r.rebuildSortedKeys()

	return tokens
}

// AddNodeWithTokens adds a node with specific token positions
func (r *Ring) AddNodeWithTokens(nodeID string, tokens []uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	physNode := &PhysicalNode{
		NodeID: nodeID,
		VNodes: tokens,
	}

	for _, pos := range tokens {
		r.vnodes[pos] = &VirtualNode{
			NodeID:   nodeID,
			Position: pos,
		}
	}

	r.nodes[nodeID] = physNode
	r.rebuildSortedKeys()
}

// RemoveNode removes a node from the ring
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node := r.nodes[nodeID]
	if node == nil {
		return
	}

	for _, pos := range node.VNodes {
		delete(r.vnodes, pos)
	}

	delete(r.nodes, nodeID)
	r.rebuildSortedKeys()
}

// GetPreferenceList returns the N unique nodes responsible for a key
func (r *Ring) GetPreferenceList(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedKeys) == 0 {
		return nil
	}

	hash := md5.Sum([]byte(key))
	position := binary.BigEndian.Uint32(hash[:4])

	// Find first vnode >= position
	idx := sort.Search(len(r.sortedKeys), func(i int) bool {
		return r.sortedKeys[i] >= position
	})

	if idx == len(r.sortedKeys) {
		idx = 0
	}

	// Collect N unique physical nodes
	seen := make(map[string]bool)
	list := make([]string, 0, n)

	for len(list) < n && len(seen) < len(r.nodes) {
		vnode := r.vnodes[r.sortedKeys[idx]]
		if !seen[vnode.NodeID] {
			list = append(list, vnode.NodeID)
			seen[vnode.NodeID] = true
		}
		idx = (idx + 1) % len(r.sortedKeys)
	}

	return list
}

// GetCoordinator returns the coordinator node for a key
func (r *Ring) GetCoordinator(key string) string {
	list := r.GetPreferenceList(key, 1)
	if len(list) == 0 {
		return ""
	}
	return list[0]
}

func (r *Ring) rebuildSortedKeys() {
	r.sortedKeys = make([]uint32, 0, len(r.vnodes))
	for k := range r.vnodes {
		r.sortedKeys = append(r.sortedKeys, k)
	}
	sort.Slice(r.sortedKeys, func(i, j int) bool {
		return r.sortedKeys[i] < r.sortedKeys[j]
	})
}

func (r *Ring) GetNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
