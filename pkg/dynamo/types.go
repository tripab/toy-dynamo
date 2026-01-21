package dynamo

import (
	"errors"
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrReadQuorumFailed  = errors.New("read quorum not satisfied")
	ErrWriteQuorumFailed = errors.New("write quorum not satisfied")
	ErrNodeNotFound      = errors.New("node not found")
	ErrInvalidConfig     = errors.New("invalid configuration")
	ErrTimeout           = errors.New("operation timeout")
)

// Context contains versioning metadata for conflict resolution
type Context struct {
	VectorClock *versioning.VectorClock
}

// GetResult represents the result of a Get operation
type GetResult struct {
	Values  []versioning.VersionedValue
	Context *Context
}

// MemberStatus represents the status of a cluster member
type MemberStatus int

const (
	StatusAlive MemberStatus = iota
	StatusSuspected
	StatusDead
)

// Member represents a node in the cluster
type Member struct {
	NodeID    string
	Address   string
	Status    MemberStatus
	Heartbeat uint64
	Tokens    []uint32
	Timestamp time.Time
}

// Hint represents a hinted handoff entry
type Hint struct {
	ForNode   string
	Key       string
	Value     versioning.VersionedValue
	Timestamp time.Time
}
