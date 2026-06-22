// Package maelstrom provides a Maelstrom protocol adapter for the Dynamo node.
// Maelstrom is a Jepsen workbench where nodes communicate via JSON over STDIN/STDOUT.
// See https://github.com/jepsen-io/maelstrom
package maelstrom

import (
	"encoding/json"

	coretransport "github.com/tripab/toy-dynamo/pkg/transport"
)

// Message is the top-level Maelstrom protocol envelope.
type Message struct {
	Src  string          `json:"src"`
	Dest string          `json:"dest"`
	Body json.RawMessage `json:"body"`
}

// MessageBody contains common fields present in every message body.
type MessageBody struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id,omitempty"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
}

// --- Maelstrom lifecycle messages ---

// InitBody is sent by Maelstrom to each node at startup.
type InitBody struct {
	Type    string   `json:"type"`
	MsgID   int      `json:"msg_id"`
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

// InitOKBody acknowledges successful initialization.
type InitOKBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

// ErrorBody is sent when an operation fails.
type ErrorBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
	Code      int    `json:"code"`
	Text      string `json:"text"`
}

// Maelstrom error codes.
const (
	ErrorTimeout            = 0
	ErrorNotSupported       = 10
	ErrorTemporarilyUnavail = 11
	ErrorMalformedRequest   = 12
	ErrorCrash              = 13
	ErrorAbort              = 14
	ErrorKeyNotFound        = 20
	ErrorKeyAlreadyExists   = 21
	ErrorPreconditionFailed = 22
	ErrorTxnConflict        = 30
)

// --- lin-kv workload messages ---

// ReadBody is a KV read request from a Maelstrom client.
type ReadBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Key   any    `json:"key"`
}

// ReadOKBody is the response to a KV read.
type ReadOKBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
	Value     any    `json:"value"`
}

// WriteBody is a KV write request from a Maelstrom client.
type WriteBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Key   any    `json:"key"`
	Value any    `json:"value"`
}

// WriteOKBody is the response to a KV write.
type WriteOKBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

// CASBody is a compare-and-swap request from a Maelstrom client.
type CASBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Key   any    `json:"key"`
	From  any    `json:"from"`
	To    any    `json:"to"`
}

// CASOKBody is the response to a successful CAS.
type CASOKBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

// --- Internal node-to-node replication messages ---
// These are sent between Dynamo nodes via Maelstrom's network
// instead of HTTP RPC.

// InternalGetBody is a node-to-node read request.
type InternalGetBody struct {
	Type  string `json:"type"`
	MsgID int    `json:"msg_id"`
	Key   string `json:"key"`
}

// InternalGetOKBody is a node-to-node read response.
type InternalGetOKBody struct {
	Type      string              `json:"type"`
	InReplyTo int                 `json:"in_reply_to"`
	Values    []VersionedValueDTO `json:"values"`
}

// InternalPutBody is a node-to-node write request.
type InternalPutBody struct {
	Type  string            `json:"type"`
	MsgID int               `json:"msg_id"`
	Key   string            `json:"key"`
	Value VersionedValueDTO `json:"value"`
}

// InternalPutOKBody is a node-to-node write response.
type InternalPutOKBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
	Success   bool   `json:"success"`
}

// InternalGossipBody is a node-to-node gossip exchange.
type InternalGossipBody struct {
	Type    string      `json:"type"`
	MsgID   int         `json:"msg_id"`
	Members []MemberDTO `json:"members"`
}

// InternalGossipOKBody is the gossip response.
type InternalGossipOKBody struct {
	Type      string      `json:"type"`
	InReplyTo int         `json:"in_reply_to"`
	Members   []MemberDTO `json:"members"`
}

// InternalHintBody is a node-to-node hinted handoff delivery.
type InternalHintBody struct {
	Type         string            `json:"type"`
	MsgID        int               `json:"msg_id"`
	OriginalNode string            `json:"original_node"`
	Key          string            `json:"key"`
	Value        VersionedValueDTO `json:"value"`
}

// InternalHintOKBody acknowledges hint delivery.
type InternalHintOKBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
	Success   bool   `json:"success"`
}

// InternalStoreHintBody asks a substitute node to store a hint for a failed node.
type InternalStoreHintBody struct {
	Type       string            `json:"type"`
	MsgID      int               `json:"msg_id"`
	TargetNode string            `json:"target_node"`
	Key        string            `json:"key"`
	Value      VersionedValueDTO `json:"value"`
}

// InternalStoreHintOKBody acknowledges that the hint was stored.
type InternalStoreHintOKBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
	Success   bool   `json:"success"`
}

// InternalPeerBody carries an opaque Toy Dynamo peer transport message through
// Maelstrom's JSON network.
type InternalPeerBody struct {
	Type    string                `json:"type"`
	MsgID   int                   `json:"msg_id"`
	Message coretransport.Message `json:"message"`
}

// InternalPeerOKBody is the response to an opaque peer transport message.
type InternalPeerOKBody struct {
	Type      string                `json:"type"`
	InReplyTo int                   `json:"in_reply_to"`
	Message   coretransport.Message `json:"message"`
}

// --- DTO types for serialization over Maelstrom ---

// VersionedValueDTO is a JSON-serializable versioned value for Maelstrom transport.
type VersionedValueDTO struct {
	Data        []byte            `json:"data,omitempty"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	IsTombstone bool              `json:"is_tombstone,omitempty"`
}

// MemberDTO is a JSON-serializable cluster member for Maelstrom transport.
type MemberDTO struct {
	NodeID    string   `json:"node_id"`
	Address   string   `json:"address"`
	Status    int      `json:"status"`
	Heartbeat uint64   `json:"heartbeat"`
	Tokens    []uint32 `json:"tokens"`
}

// Internal message type constants.
const (
	MsgTypeInit   = "init"
	MsgTypeInitOK = "init_ok"
	MsgTypeError  = "error"

	// Client-facing KV operations (lin-kv workload).
	MsgTypeRead    = "read"
	MsgTypeReadOK  = "read_ok"
	MsgTypeWrite   = "write"
	MsgTypeWriteOK = "write_ok"
	MsgTypeCAS     = "cas"
	MsgTypeCasOK   = "cas_ok"

	// Internal node-to-node operations.
	MsgTypeInternalGet         = "internal_get"
	MsgTypeInternalGetOK       = "internal_get_ok"
	MsgTypeInternalPut         = "internal_put"
	MsgTypeInternalPutOK       = "internal_put_ok"
	MsgTypeInternalGossip      = "internal_gossip"
	MsgTypeInternalGossipOK    = "internal_gossip_ok"
	MsgTypeInternalHint        = "internal_hint"
	MsgTypeInternalHintOK      = "internal_hint_ok"
	MsgTypeInternalStoreHint   = "internal_store_hint"
	MsgTypeInternalStoreHintOK = "internal_store_hint_ok"
	MsgTypeInternalPeer        = "internal_peer"
	MsgTypeInternalPeerOK      = "internal_peer_ok"
)
