// Package types defines shared interfaces used across packages to avoid circular imports.
// These interfaces enable dependency injection and type safety in components like
// Replicator, HintedHandoff, AntiEntropy, and Membership.
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
type MemberInfo interface {
	GetNodeID() string
	GetAddress() string
	GetStatus() int
	GetTokens() []uint32
}

// Membership provides membership operations for components.
// Implemented by: membership.Membership
type Membership interface {
	GetMember(nodeID string) MemberInfo
	GetAllMembers() []MemberInfo
	AddMember(nodeID, address string, status int, tokens []uint32)
}

// Ring provides consistent hash ring operations for components.
// Implemented by: ring.Ring
type Ring interface {
	GetPreferenceList(key string, n int) []string
	GetCoordinator(key string) string
	GetNodeCount() int
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
