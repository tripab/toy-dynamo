package maelstrom

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	coretransport "github.com/tripab/toy-dynamo/pkg/transport"
)

// PeerTransport adapts Maelstrom JSON messages to the core opaque transport
// interface. It does not inspect Dynamo peer operation types.
type PeerTransport struct {
	transport *Transport
	timeout   time.Duration
	handler   coretransport.Handler
}

func NewPeerTransport(transport *Transport, timeout time.Duration) *PeerTransport {
	pt := &PeerTransport{
		transport: transport,
		timeout:   timeout,
	}
	transport.Handle(MsgTypeInternalPeer, pt.handleInternalPeer)
	return pt
}

func (pt *PeerTransport) SetHandler(handler coretransport.Handler) {
	pt.handler = handler
}

func (pt *PeerTransport) Send(ctx context.Context, target string, msg coretransport.Message) (coretransport.Message, error) {
	if pt.transport == nil {
		return coretransport.Message{}, fmt.Errorf("maelstrom peer transport is not configured")
	}

	msgID := pt.transport.NextMsgID()
	timeout := pt.timeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return coretransport.Message{}, ctx.Err()
		}
	}

	resp, err := pt.transport.RPCWithTimeout(target, InternalPeerBody{
		Type:    MsgTypeInternalPeer,
		MsgID:   msgID,
		Message: msg,
	}, timeout)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return coretransport.Message{}, ctxErr
		}
		return coretransport.Message{}, err
	}

	var env MessageBody
	if err := json.Unmarshal(resp.Body, &env); err != nil {
		return coretransport.Message{}, fmt.Errorf("decode maelstrom response envelope: %w", err)
	}
	if env.Type == MsgTypeError {
		var errBody ErrorBody
		if err := json.Unmarshal(resp.Body, &errBody); err != nil {
			return coretransport.Message{}, fmt.Errorf("decode maelstrom error: %w", err)
		}
		return coretransport.Message{}, fmt.Errorf("maelstrom error %d: %s", errBody.Code, errBody.Text)
	}
	if env.Type != MsgTypeInternalPeerOK {
		return coretransport.Message{}, fmt.Errorf("unexpected maelstrom response type %q", env.Type)
	}

	var body InternalPeerOKBody
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return coretransport.Message{}, fmt.Errorf("decode peer response: %w", err)
	}
	return body.Message, nil
}

func (pt *PeerTransport) Close() {}

func (pt *PeerTransport) handleInternalPeer(msg Message) error {
	if pt.handler == nil {
		return pt.replyError(msg, 0, ErrorTemporarilyUnavail, "peer handler is not configured")
	}

	var body InternalPeerBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return pt.replyError(msg, 0, ErrorMalformedRequest, "bad internal peer request")
	}

	resp, err := pt.handler.HandleMessage(context.Background(), body.Message)
	if err != nil {
		return pt.replyError(msg, body.MsgID, ErrorCrash, err.Error())
	}

	return pt.transport.Reply(msg, InternalPeerOKBody{
		Type:      MsgTypeInternalPeerOK,
		InReplyTo: body.MsgID,
		Message:   resp,
	})
}

func (pt *PeerTransport) replyError(msg Message, inReplyTo int, code int, text string) error {
	return pt.transport.Reply(msg, ErrorBody{
		Type:      MsgTypeError,
		InReplyTo: inReplyTo,
		Code:      code,
		Text:      text,
	})
}
