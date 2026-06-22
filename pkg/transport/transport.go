package transport

import (
	"context"
	"encoding/json"
)

// Message is an opaque transport payload. Transport implementations route it
// to a target and return the peer's response without knowing the operation
// encoded in Payload.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Transport sends opaque request/response messages between nodes.
type Transport interface {
	Send(ctx context.Context, target string, msg Message) (Message, error)
	Close()
}

// Handler processes an inbound transport message.
type Handler interface {
	HandleMessage(ctx context.Context, msg Message) (Message, error)
}
