// Package rpc provides HTTP-based RPC for inter-node communication in Dynamo.
// This uses JSON encoding for simplicity; can be migrated to gRPC later for performance.
package rpc

import (
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// GetRequest is sent for read operations
type GetRequest struct {
	Key string `json:"key"`
}

// GetResponse contains read results
type GetResponse struct {
	Values []VersionedValueDTO `json:"values,omitempty"`
	Error  string              `json:"error,omitempty"`
}

// PutRequest is sent for write operations
type PutRequest struct {
	Key   string            `json:"key"`
	Value VersionedValueDTO `json:"value"`
}

// PutResponse acknowledges a write
type PutResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// GossipRequest exchanges membership info
type GossipRequest struct {
	FromNode string      `json:"from_node"`
	Members  []MemberDTO `json:"members"`
}

// GossipResponse returns membership info
type GossipResponse struct {
	Members []MemberDTO `json:"members"`
	Error   string      `json:"error,omitempty"`
}

// SyncRequest for anti-entropy Merkle tree comparison
type SyncRequest struct {
	KeyRange KeyRange `json:"key_range"`
	TreeRoot []byte   `json:"tree_root,omitempty"`
}

// SyncResponse returns Merkle tree sync info
type SyncResponse struct {
	TreeRoot    []byte   `json:"tree_root,omitempty"`
	Differences []string `json:"differences,omitempty"`
	Error       string   `json:"error,omitempty"`
}

// HintRequest delivers a hinted handoff
type HintRequest struct {
	OriginalNode string            `json:"original_node"`
	Key          string            `json:"key"`
	Value        VersionedValueDTO `json:"value"`
}

// HintResponse acknowledges hint delivery
type HintResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// KeyRange represents a range of keys [Start, End)
type KeyRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// VersionedValueDTO is a JSON-serializable version of versioning.VersionedValue
type VersionedValueDTO struct {
	Data        []byte            `json:"data,omitempty"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	Timestamp   time.Time         `json:"timestamp"`
	IsTombstone bool              `json:"is_tombstone,omitempty"`
}

// MemberDTO is a JSON-serializable version of membership.Member
type MemberDTO struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	Status    int       `json:"status"`
	Heartbeat uint64    `json:"heartbeat"`
	Tokens    []uint32  `json:"tokens"`
	Timestamp time.Time `json:"timestamp"`
}

// ToVersionedValue converts DTO to internal versioning.VersionedValue
func (dto *VersionedValueDTO) ToVersionedValue() versioning.VersionedValue {
	vc := versioning.NewVectorClock()
	for nodeID, counter := range dto.VectorClock {
		vc.Versions[nodeID] = counter
	}
	return versioning.VersionedValue{
		Data:        dto.Data,
		VectorClock: vc,
		IsTombstone: dto.IsTombstone,
	}
}

// FromVersionedValue converts internal versioning.VersionedValue to DTO
func FromVersionedValue(vv versioning.VersionedValue) VersionedValueDTO {
	vcMap := make(map[string]uint64)
	if vv.VectorClock != nil {
		for nodeID, counter := range vv.VectorClock.Versions {
			vcMap[nodeID] = counter
		}
	}
	return VersionedValueDTO{
		Data:        vv.Data,
		VectorClock: vcMap,
		Timestamp:   time.Now(),
		IsTombstone: vv.IsTombstone,
	}
}

// FromVersionedValues converts a slice of VersionedValue to DTOs
func FromVersionedValues(values []versioning.VersionedValue) []VersionedValueDTO {
	dtos := make([]VersionedValueDTO, len(values))
	for i, v := range values {
		dtos[i] = FromVersionedValue(v)
	}
	return dtos
}

// ToVersionedValues converts a slice of DTOs to VersionedValues
func ToVersionedValues(dtos []VersionedValueDTO) []versioning.VersionedValue {
	values := make([]versioning.VersionedValue, len(dtos))
	for i, dto := range dtos {
		values[i] = dto.ToVersionedValue()
	}
	return values
}
