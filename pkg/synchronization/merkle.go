package synchronization

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

// MerkleTree provides efficient consistency checking between replicas
type MerkleTree struct {
	root     *MerkleNode
	leaves   map[string]*MerkleNode
	keyRange KeyRange
}

type MerkleNode struct {
	Hash     string
	Left     *MerkleNode
	Right    *MerkleNode
	KeyRange KeyRange
	IsLeaf   bool
	Key      string // Only for leaf nodes
	Value    []byte // Only for leaf nodes
}

type KeyRange struct {
	Start string
	End   string
}

// NewMerkleTree creates a new Merkle tree for a key range
func NewMerkleTree(keyRange KeyRange) *MerkleTree {
	return &MerkleTree{
		leaves:   make(map[string]*MerkleNode),
		keyRange: keyRange,
	}
}

// Build constructs the Merkle tree from key-value pairs
func (mt *MerkleTree) Build(data map[string][]byte) {
	// Clear existing tree
	mt.leaves = make(map[string]*MerkleNode)

	// Sort keys
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create leaf nodes
	leafNodes := make([]*MerkleNode, 0, len(keys))
	for _, key := range keys {
		value := data[key]
		hash := computeHash(append([]byte(key), value...))

		leaf := &MerkleNode{
			Hash:   hash,
			IsLeaf: true,
			Key:    key,
			Value:  value,
		}
		mt.leaves[key] = leaf
		leafNodes = append(leafNodes, leaf)
	}

	// Build tree bottom-up
	mt.root = mt.buildTree(leafNodes, mt.keyRange)
}

func (mt *MerkleTree) buildTree(nodes []*MerkleNode, keyRange KeyRange) *MerkleNode {
	if len(nodes) == 0 {
		// Empty tree
		return &MerkleNode{
			Hash:     computeHash([]byte("empty")),
			KeyRange: keyRange,
			IsLeaf:   false,
		}
	}

	if len(nodes) == 1 {
		return nodes[0]
	}

	// Split nodes into left and right subtrees
	mid := len(nodes) / 2
	leftNodes := nodes[:mid]
	rightNodes := nodes[mid:]

	// Determine split point
	var splitKey string
	if mid < len(nodes) && nodes[mid].IsLeaf {
		splitKey = nodes[mid].Key
	} else {
		splitKey = keyRange.End
	}

	leftRange := KeyRange{Start: keyRange.Start, End: splitKey}
	rightRange := KeyRange{Start: splitKey, End: keyRange.End}

	// Build subtrees recursively
	left := mt.buildTree(leftNodes, leftRange)
	right := mt.buildTree(rightNodes, rightRange)

	// Create parent node
	combinedHash := computeHash([]byte(left.Hash + right.Hash))

	return &MerkleNode{
		Hash:     combinedHash,
		Left:     left,
		Right:    right,
		KeyRange: keyRange,
		IsLeaf:   false,
	}
}

// GetRootHash returns the hash of the root node
func (mt *MerkleTree) GetRootHash() string {
	if mt.root == nil {
		return ""
	}
	return mt.root.Hash
}

// FindDifferences compares this tree with a remote tree to find differing keys
func (mt *MerkleTree) FindDifferences(localNode, remoteNode *MerkleNode) []string {
	if localNode == nil || remoteNode == nil {
		return []string{}
	}

	// If hashes match, no differences
	if localNode.Hash == remoteNode.Hash {
		return []string{}
	}

	// If leaf node, return the key
	if localNode.IsLeaf {
		return []string{localNode.Key}
	}

	// Recursively check children
	differences := []string{}

	if localNode.Left != nil && remoteNode.Left != nil {
		differences = append(differences,
			mt.FindDifferences(localNode.Left, remoteNode.Left)...)
	}

	if localNode.Right != nil && remoteNode.Right != nil {
		differences = append(differences,
			mt.FindDifferences(localNode.Right, remoteNode.Right)...)
	}

	return differences
}

// Update updates a single key in the tree
func (mt *MerkleTree) Update(key string, value []byte) {
	// Rebuild tree - in production, would update incrementally
	// For simplicity, we rebuild which is acceptable for periodic anti-entropy
	allData := make(map[string][]byte)

	for k, leaf := range mt.leaves {
		allData[k] = leaf.Value
	}
	allData[key] = value

	mt.Build(allData)
}

// Delete removes a key from the tree
func (mt *MerkleTree) Delete(key string) {
	delete(mt.leaves, key)

	// Rebuild
	allData := make(map[string][]byte)
	for k, leaf := range mt.leaves {
		allData[k] = leaf.Value
	}

	mt.Build(allData)
}

// Add GetRoot method accessor for testing
func (mt *MerkleTree) GetRoot() *MerkleNode {
	return mt.root
}

func computeHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
