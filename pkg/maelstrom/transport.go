package maelstrom

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is a function that processes an incoming Maelstrom message.
type Handler func(msg Message) error

// Transport reads/writes JSON messages over STDIN/STDOUT.
// It is safe for concurrent use.
type Transport struct {
	reader  *bufio.Scanner
	writer  io.Writer
	writeMu sync.Mutex

	// Message ID counter (atomic, shared across all senders).
	nextMsgID atomic.Int64

	// Pending RPC callbacks keyed by msg_id.
	callbacks   map[int]chan Message
	callbacksMu sync.Mutex

	// Incoming message handler dispatch.
	handlers   map[string]Handler
	handlersMu sync.RWMutex

	nodeID string
}

// NewTransport creates a Transport reading from r and writing to w.
func NewTransport(r io.Reader, w io.Writer) *Transport {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20) // 1MB buffer
	return &Transport{
		reader:    scanner,
		writer:    w,
		callbacks: make(map[int]chan Message),
		handlers:  make(map[string]Handler),
	}
}

// SetNodeID records this node's Maelstrom ID (set after init).
func (t *Transport) SetNodeID(id string) {
	t.nodeID = id
}

// NodeID returns the node's Maelstrom ID.
func (t *Transport) NodeID() string {
	return t.nodeID
}

// NextMsgID returns a unique message ID.
func (t *Transport) NextMsgID() int {
	return int(t.nextMsgID.Add(1))
}

// Handle registers a handler for the given message type.
func (t *Transport) Handle(msgType string, h Handler) {
	t.handlersMu.Lock()
	defer t.handlersMu.Unlock()
	t.handlers[msgType] = h
}

// Send writes a message to STDOUT (thread-safe).
func (t *Transport) Send(dest string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	msg := Message{
		Src:  t.nodeID,
		Dest: dest,
		Body: raw,
	}
	return t.sendRaw(msg)
}

// Reply sends a response to an incoming message.
func (t *Transport) Reply(req Message, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	msg := Message{
		Src:  t.nodeID,
		Dest: req.Src,
		Body: raw,
	}
	return t.sendRaw(msg)
}

// RPC sends a message and blocks until a response arrives or timeout expires.
func (t *Transport) RPC(dest string, body any) (Message, error) {
	return t.RPCWithTimeout(dest, body, 5*time.Second)
}

// RPCWithTimeout sends a message and waits for a response with a timeout.
func (t *Transport) RPCWithTimeout(dest string, body any, timeout time.Duration) (Message, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return Message{}, fmt.Errorf("marshal body: %w", err)
	}

	// Extract msg_id from the body to register the callback.
	var peek struct {
		MsgID int `json:"msg_id"`
	}
	if err := json.Unmarshal(raw, &peek); err != nil {
		return Message{}, fmt.Errorf("peek msg_id: %w", err)
	}

	ch := make(chan Message, 1)
	t.callbacksMu.Lock()
	t.callbacks[peek.MsgID] = ch
	t.callbacksMu.Unlock()

	msg := Message{
		Src:  t.nodeID,
		Dest: dest,
		Body: raw,
	}
	if err := t.sendRaw(msg); err != nil {
		t.callbacksMu.Lock()
		delete(t.callbacks, peek.MsgID)
		t.callbacksMu.Unlock()
		return Message{}, err
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(timeout):
		t.callbacksMu.Lock()
		delete(t.callbacks, peek.MsgID)
		t.callbacksMu.Unlock()
		return Message{}, fmt.Errorf("RPC to %s timed out after %v", dest, timeout)
	}
}

// Run reads messages from STDIN and dispatches them. Blocks until EOF.
func (t *Transport) Run() error {
	for t.reader.Scan() {
		line := t.reader.Bytes()
		var msg Message
		if err := json.Unmarshal(line, &msg); err != nil {
			log.Printf("maelstrom: failed to parse message: %v", err)
			continue
		}

		// Check if this is a reply to a pending RPC.
		var peek MessageBody
		if err := json.Unmarshal(msg.Body, &peek); err == nil && peek.InReplyTo != 0 {
			t.callbacksMu.Lock()
			ch, ok := t.callbacks[peek.InReplyTo]
			if ok {
				delete(t.callbacks, peek.InReplyTo)
			}
			t.callbacksMu.Unlock()

			if ok {
				ch <- msg
				continue
			}
		}

		// Dispatch to registered handler.
		var body MessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Printf("maelstrom: failed to parse body type: %v", err)
			continue
		}

		t.handlersMu.RLock()
		handler, ok := t.handlers[body.Type]
		t.handlersMu.RUnlock()

		if !ok {
			log.Printf("maelstrom: no handler for message type %q", body.Type)
			_ = t.Reply(msg, ErrorBody{
				Type:      MsgTypeError,
				InReplyTo: body.MsgID,
				Code:      ErrorNotSupported,
				Text:      fmt.Sprintf("unsupported message type: %s", body.Type),
			})
			continue
		}

		go func(m Message) {
			if err := handler(m); err != nil {
				log.Printf("maelstrom: handler error for %s: %v", body.Type, err)
			}
		}(msg)
	}

	return t.reader.Err()
}

func (t *Transport) sendRaw(msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	_, err = fmt.Fprintf(t.writer, "%s\n", data)
	return err
}
