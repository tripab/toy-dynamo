// Package types defines shared interfaces used across packages to avoid circular imports.
// These interfaces enable dependency injection and type safety in components like
// Replicator, HintedHandoff, AntiEntropy, and Membership.
//
// Note on interface design:
// - Components may import concrete types (e.g., *membership.Membership) directly when
//   Go's interface limitations prevent clean abstraction (return type variance).
// - The interfaces here are designed to be satisfied by existing implementations
//   without requiring changes to those implementations.
package types

import (
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// Storage provides storage operations for components.
// Implemented by: storage.MemoryStorage, storage.BoltDBStorage, storage.BadgerStorage
type Storage interface {
	Get(key string) ([]versioning.VersionedValue, error)
	Put(key string, value versioning.VersionedValue) error
	Delete(key string) error
	GetRange(start, end string) (map[string][]versioning.VersionedValue, error)
	GetAllKeys() ([]string, error)
	Close() error
}

// MemberInfo represents basic information about a cluster member.
// Implemented by: *membership.Member
type MemberInfo interface {
	GetNodeID() string
	GetAddress() string
	GetStatus() int
	GetTokens() []uint32
}

// MemberStatus constants matching membership.MemberStatus
const (
	StatusAlive     = 0
	StatusSuspected = 1
	StatusDead      = 2
)

// Ring provides consistent hash ring operations for components.
// Implemented by: ring.Ring
type Ring interface {
	GetPreferenceList(key string, n int) []string
	GetCoordinator(key string) string
	GetNodeCount() int
	GetTokens(nodeID string) []uint32
	AddNode(nodeID string, vnodesCount int) []uint32
	AddNodeWithTokens(nodeID string, tokens []uint32)
	RemoveNode(nodeID string)
}

// Config provides configuration access for components.
// Implemented by: dynamo.Config
type Config interface {
	GetN() int
	GetR() int
	GetW() int
	GetGossipInterval() time.Duration
	GetAntiEntropyInterval() time.Duration
	GetHintedHandoffEnabled() bool
	GetHintTimeout() time.Duration
	GetRequestTimeout() time.Duration
	GetReadRepairEnabled() bool
	GetVectorClockMaxSize() int
}

// NodeInfo provides basic node identification.
// Implemented by: dynamo.Node
type NodeInfo interface {
	GetID() string
	GetAddress() string
}

// RPCClient provides methods for making RPC calls to other nodes.
// Will be implemented by: rpc.Client (in Phase 1.6)
type RPCClient interface {
	// Get retrieves values from a remote node
	Get(address, key string) ([]versioning.VersionedValue, error)
	// Put sends a value to a remote node
	Put(address, key string, value versioning.VersionedValue) error
	// Gossip exchanges membership information
	GossipExchange(address string, members []MemberInfo) ([]MemberInfo, error)
	// DeliverHint sends a hinted handoff to the target node
	DeliverHint(address, key string, value versioning.VersionedValue) error
}
