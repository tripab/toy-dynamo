package maelstrom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// Node is a Maelstrom-adapted Dynamo node. It reads/writes JSON messages
// over STDIN/STDOUT and uses the real Dynamo coordinator, ring, storage,
// and versioning logic for request processing.
type Node struct {
	transport *Transport
	inner     *dynamo.Node
	nodeID    string
	nodeIDs   []string
}

// NewNode creates a MaelstromNode. Call Run() to start the message loop.
func NewNode() *Node {
	return &Node{}
}

// Run starts the STDIN read loop. Blocks until EOF.
func (n *Node) Run(transport *Transport) error {
	n.transport = transport

	// Register the init handler; KV handlers are registered after init.
	n.transport.Handle(MsgTypeInit, n.handleInit)

	log.Println("maelstrom: node starting, waiting for init")
	return n.transport.Run()
}

// --- Maelstrom message handlers ---

func (n *Node) handleInit(msg Message) error {
	var body InitBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("parse init: %w", err)
	}

	n.nodeID = body.NodeID
	n.nodeIDs = body.NodeIDs
	n.transport.SetNodeID(n.nodeID)

	log.Printf("maelstrom: init node=%s cluster=%v", n.nodeID, n.nodeIDs)

	// Build the Dynamo node with Maelstrom-appropriate config.
	if err := n.initDynamo(); err != nil {
		return fmt.Errorf("init dynamo: %w", err)
	}

	// Register KV workload handlers.
	n.transport.Handle(MsgTypeRead, n.handleRead)
	n.transport.Handle(MsgTypeWrite, n.handleWrite)
	n.transport.Handle(MsgTypeCAS, n.handleCAS)

	// Acknowledge init.
	return n.transport.Reply(msg, InitOKBody{
		Type:      MsgTypeInitOK,
		InReplyTo: body.MsgID,
	})
}

// initDynamo creates the underlying Dynamo node and wires the Maelstrom
// router as its remote transport.
func (n *Node) initDynamo() error {
	clusterSize := len(n.nodeIDs)

	// Configure quorum parameters based on cluster size.
	cfg := dynamo.DefaultConfig()
	cfg.StorageEngine = "memory"
	cfg.HintedHandoffEnabled = true
	cfg.ReadRepairEnabled = true
	cfg.RequestTimeout = 3 * time.Second

	// Disable features not needed in Maelstrom.
	cfg.AdmissionControlEnabled = false
	cfg.CoordinatorSelectionEnabled = false
	cfg.MetricsEnabled = false
	cfg.EnableCircuitBreaker = false
	cfg.EnableRetry = false
	cfg.DisableHTTPServer = true

	if clusterSize >= 3 {
		cfg.N = 3
		cfg.R = 2
		cfg.W = 2
	} else {
		cfg.N = clusterSize
		cfg.R = clusterSize
		cfg.W = clusterSize
	}

	// Use fewer vnodes for smaller clusters (faster ring setup).
	cfg.VirtualNodes = 64
	peerTransport := NewPeerTransport(n.transport, cfg.RequestTimeout)
	cfg.Transport = peerTransport

	var err error
	n.inner, err = dynamo.NewNode(n.nodeID, n.nodeID, cfg)
	if err != nil {
		return fmt.Errorf("create dynamo node: %w", err)
	}
	peerTransport.SetHandler(n.inner)

	// Set up the consistent hash ring and membership for all cluster nodes.
	for _, id := range n.nodeIDs {
		tokens := n.inner.InitRing(id, cfg.VirtualNodes)
		n.inner.InitMember(&membership.Member{
			NodeID:    id,
			Address:   id, // In Maelstrom, address == node ID.
			Status:    membership.StatusAlive,
			Heartbeat: 1,
			Tokens:    tokens,
			Timestamp: time.Now(),
		})
	}

	return nil
}

// handleRead handles a lin-kv "read" message.
func (n *Node) handleRead(msg Message) error {
	var body ReadBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return n.replyError(msg, 0, ErrorMalformedRequest, "bad read request")
	}

	key := fmt.Sprintf("%v", body.Key)

	result, err := n.inner.Get(context.Background(), key)
	if err != nil {
		return n.replyError(msg, body.MsgID, kvErrorCode(err), err.Error())
	}

	if len(result.Values) == 0 {
		return n.replyError(msg, body.MsgID, ErrorKeyNotFound, "key not found")
	}

	// Return the latest value (use last-write-wins for lin-kv compatibility).
	value := reconcileLWW(result.Values)

	return n.transport.Reply(msg, ReadOKBody{
		Type:      MsgTypeReadOK,
		InReplyTo: body.MsgID,
		Value:     value,
	})
}

// handleWrite handles a lin-kv "write" message.
func (n *Node) handleWrite(msg Message) error {
	var body WriteBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return n.replyError(msg, 0, ErrorMalformedRequest, "bad write request")
	}

	key := fmt.Sprintf("%v", body.Key)
	val, err := json.Marshal(body.Value)
	if err != nil {
		return n.replyError(msg, body.MsgID, ErrorMalformedRequest, "bad value")
	}

	// Best-effort read of current context for causal ordering.
	// Use a short timeout so a slow/lossy network doesn't stall writes;
	// if it fails, the write proceeds with a fresh vector clock.
	var ctx *dynamo.Context
	readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	result, getErr := n.inner.Get(readCtx, key)
	cancel()
	if getErr == nil && result != nil {
		ctx = result.Context
	}

	if err := n.inner.Put(context.Background(), key, val, ctx); err != nil {
		return n.replyError(msg, body.MsgID, ErrorTemporarilyUnavail, err.Error())
	}

	return n.transport.Reply(msg, WriteOKBody{
		Type:      MsgTypeWriteOK,
		InReplyTo: body.MsgID,
	})
}

// handleCAS handles a lin-kv "cas" (compare-and-swap) message.
func (n *Node) handleCAS(msg Message) error {
	var body CASBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return n.replyError(msg, 0, ErrorMalformedRequest, "bad cas request")
	}

	key := fmt.Sprintf("%v", body.Key)
	fromVal, _ := json.Marshal(body.From)
	toVal, _ := json.Marshal(body.To)

	// Read current value.
	result, err := n.inner.Get(context.Background(), key)
	if err != nil {
		return n.replyError(msg, body.MsgID, kvErrorCode(err), err.Error())
	}
	if len(result.Values) == 0 {
		return n.replyError(msg, body.MsgID, ErrorKeyNotFound, "key not found")
	}

	// Check that current value matches "from".
	currentVal := reconcileLWW(result.Values)
	currentJSON, _ := json.Marshal(currentVal)
	if string(currentJSON) != string(fromVal) {
		return n.replyError(msg, body.MsgID, ErrorPreconditionFailed,
			fmt.Sprintf("current value %s != expected %s", string(currentJSON), string(fromVal)))
	}

	// Write the new value with causal context.
	if err := n.inner.Put(context.Background(), key, toVal, result.Context); err != nil {
		return n.replyError(msg, body.MsgID, ErrorTemporarilyUnavail, err.Error())
	}

	return n.transport.Reply(msg, CASOKBody{
		Type:      MsgTypeCasOK,
		InReplyTo: body.MsgID,
	})
}

// kvErrorCode maps a Dynamo error to the appropriate Maelstrom error code.
// Timeout and quorum failures return TemporarilyUnavailable so that the
// linearizability checker treats them as indeterminate rather than as
// "key not found" (which would be a false consistency violation during partitions).
func kvErrorCode(err error) int {
	if errors.Is(err, dynamo.ErrTimeout) ||
		errors.Is(err, dynamo.ErrReadQuorumFailed) ||
		errors.Is(err, dynamo.ErrWriteQuorumFailed) ||
		errors.Is(err, dynamo.ErrNodeNotFound) {
		return ErrorTemporarilyUnavail
	}
	return ErrorKeyNotFound
}

func (n *Node) replyError(msg Message, inReplyTo int, code int, text string) error {
	return n.transport.Reply(msg, ErrorBody{
		Type:      MsgTypeError,
		InReplyTo: inReplyTo,
		Code:      code,
		Text:      text,
	})
}

// reconcileLWW picks a single value from concurrent versions using the
// vector clock timestamp as a tiebreaker (last-write-wins). This is needed
// because Maelstrom's lin-kv checker expects a single scalar value.
func reconcileLWW(values []versioning.VersionedValue) any {
	if len(values) == 0 {
		return nil
	}
	latest := values[0]
	for _, v := range values[1:] {
		if v.VectorClock != nil && latest.VectorClock != nil &&
			v.VectorClock.Timestamp.After(latest.VectorClock.Timestamp) {
			latest = v
		}
	}
	var result any
	if err := json.Unmarshal(latest.Data, &result); err != nil {
		return string(latest.Data)
	}
	return result
}
