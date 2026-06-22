// Package rpc provides HTTP-based RPC for inter-node communication in Dynamo.
package rpc

import (
	"github.com/tripab/toy-dynamo/pkg/peer"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

type GetRequest = peer.GetRequest
type GetResponse = peer.GetResponse
type PutRequest = peer.PutRequest
type PutResponse = peer.PutResponse
type GossipRequest = peer.GossipRequest
type GossipResponse = peer.GossipResponse
type SyncRequest = peer.SyncRequest
type SyncResponse = peer.SyncResponse
type HintRequest = peer.HintRequest
type HintResponse = peer.HintResponse
type StoreHintRequest = peer.StoreHintRequest
type StoreHintResponse = peer.StoreHintResponse
type KeyRange = peer.KeyRange
type VersionedValueDTO = peer.VersionedValueDTO
type MemberDTO = peer.MemberDTO

func FromVersionedValue(vv versioning.VersionedValue) VersionedValueDTO {
	return peer.FromVersionedValue(vv)
}

func FromVersionedValues(values []versioning.VersionedValue) []VersionedValueDTO {
	return peer.FromVersionedValues(values)
}

func ToVersionedValues(dtos []VersionedValueDTO) []versioning.VersionedValue {
	return peer.ToVersionedValues(dtos)
}
