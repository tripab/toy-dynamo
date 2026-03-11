package maelstrom

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// LocalStore is called by the Router to handle incoming requests from other
// nodes that need to read/write to local storage.
type LocalStore interface {
	Get(key string) ([]versioning.VersionedValue, error)
	Put(key string, value versioning.VersionedValue) error
}

// Router provides inter-node communication via Maelstrom message passing.
// It replaces the HTTP RPC client for Maelstrom testing.
type Router struct {
	transport *Transport
	store     LocalStore
	timeout   time.Duration
}

// NewRouter creates a Router that sends inter-node messages via the Transport.
func NewRouter(transport *Transport, store LocalStore, timeout time.Duration) *Router {
	r := &Router{
		transport: transport,
		store:     store,
		timeout:   timeout,
	}
	r.registerHandlers()
	return r
}

// registerHandlers sets up handlers for incoming inter-node messages.
func (r *Router) registerHandlers() {
	r.transport.Handle(MsgTypeInternalGet, r.handleInternalGet)
	r.transport.Handle(MsgTypeInternalPut, r.handleInternalPut)
	r.transport.Handle(MsgTypeInternalHint, r.handleInternalHint)
}

// --- Outgoing operations (called by the coordinator) ---

// RemoteGet reads from a remote node via Maelstrom message passing.
func (r *Router) RemoteGet(nodeID, key string) ([]versioning.VersionedValue, error) {
	msgID := r.transport.NextMsgID()
	body := InternalGetBody{
		Type:  MsgTypeInternalGet,
		MsgID: msgID,
		Key:   key,
	}

	resp, err := r.rpc(nodeID, body)
	if err != nil {
		return nil, fmt.Errorf("remote get from %s: %w", nodeID, err)
	}

	var respBody InternalGetOKBody
	if err := json.Unmarshal(resp.Body, &respBody); err != nil {
		return nil, fmt.Errorf("unmarshal get response: %w", err)
	}

	return toVersionedValues(respBody.Values), nil
}

// RemotePut writes to a remote node via Maelstrom message passing.
// Returns true on success.
func (r *Router) RemotePut(nodeID, key string, value versioning.VersionedValue) (bool, error) {
	msgID := r.transport.NextMsgID()
	body := InternalPutBody{
		Type:  MsgTypeInternalPut,
		MsgID: msgID,
		Key:   key,
		Value: fromVersionedValue(value),
	}

	resp, err := r.rpc(nodeID, body)
	if err != nil {
		return false, fmt.Errorf("remote put to %s: %w", nodeID, err)
	}

	var respBody InternalPutOKBody
	if err := json.Unmarshal(resp.Body, &respBody); err != nil {
		return false, fmt.Errorf("unmarshal put response: %w", err)
	}

	return respBody.Success, nil
}

// RemoteHint delivers a hinted handoff to a remote node.
func (r *Router) RemoteHint(nodeID, originalNode, key string, value versioning.VersionedValue) (bool, error) {
	msgID := r.transport.NextMsgID()
	body := InternalHintBody{
		Type:         MsgTypeInternalHint,
		MsgID:        msgID,
		OriginalNode: originalNode,
		Key:          key,
		Value:        fromVersionedValue(value),
	}

	resp, err := r.rpc(nodeID, body)
	if err != nil {
		return false, fmt.Errorf("remote hint to %s: %w", nodeID, err)
	}

	var respBody InternalHintOKBody
	if err := json.Unmarshal(resp.Body, &respBody); err != nil {
		return false, fmt.Errorf("unmarshal hint response: %w", err)
	}

	return respBody.Success, nil
}

// rpc sends a message and blocks until the reply arrives.
func (r *Router) rpc(dest string, body any) (Message, error) {
	return r.transport.RPC(dest, body)
}

// --- Incoming message handlers ---

func (r *Router) handleInternalGet(msg Message) error {
	var body InternalGetBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return r.replyError(msg, body.MsgID, ErrorMalformedRequest, "bad get request")
	}

	values, err := r.store.Get(body.Key)
	if err != nil {
		// Return empty values rather than error — key-not-found is normal
		values = nil
	}

	return r.transport.Reply(msg, InternalGetOKBody{
		Type:      MsgTypeInternalGetOK,
		InReplyTo: body.MsgID,
		Values:    fromVersionedValues(values),
	})
}

func (r *Router) handleInternalPut(msg Message) error {
	var body InternalPutBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return r.replyError(msg, body.MsgID, ErrorMalformedRequest, "bad put request")
	}

	value := toVersionedValue(body.Value)
	err := r.store.Put(body.Key, value)

	return r.transport.Reply(msg, InternalPutOKBody{
		Type:      MsgTypeInternalPutOK,
		InReplyTo: body.MsgID,
		Success:   err == nil,
	})
}

func (r *Router) handleInternalHint(msg Message) error {
	var body InternalHintBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return r.replyError(msg, body.MsgID, ErrorMalformedRequest, "bad hint request")
	}

	value := toVersionedValue(body.Value)
	err := r.store.Put(body.Key, value)

	return r.transport.Reply(msg, InternalHintOKBody{
		Type:      MsgTypeInternalHintOK,
		InReplyTo: body.MsgID,
		Success:   err == nil,
	})
}

func (r *Router) replyError(msg Message, inReplyTo int, code int, text string) error {
	return r.transport.Reply(msg, ErrorBody{
		Type:      MsgTypeError,
		InReplyTo: inReplyTo,
		Code:      code,
		Text:      text,
	})
}

// --- DTO conversion helpers ---

func fromVersionedValue(vv versioning.VersionedValue) VersionedValueDTO {
	vcMap := make(map[string]uint64)
	if vv.VectorClock != nil {
		for nodeID, counter := range vv.VectorClock.Versions {
			vcMap[nodeID] = counter
		}
	}
	return VersionedValueDTO{
		Data:        vv.Data,
		VectorClock: vcMap,
		IsTombstone: vv.IsTombstone,
	}
}

func fromVersionedValues(values []versioning.VersionedValue) []VersionedValueDTO {
	dtos := make([]VersionedValueDTO, len(values))
	for i, v := range values {
		dtos[i] = fromVersionedValue(v)
	}
	return dtos
}

func toVersionedValue(dto VersionedValueDTO) versioning.VersionedValue {
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

func toVersionedValues(dtos []VersionedValueDTO) []versioning.VersionedValue {
	values := make([]versioning.VersionedValue, len(dtos))
	for i, dto := range dtos {
		values[i] = toVersionedValue(dto)
	}
	return values
}
