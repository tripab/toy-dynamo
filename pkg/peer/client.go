package peer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tripab/toy-dynamo/pkg/transport"
	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// Client exposes Dynamo peer operations over an opaque transport.
type Client struct {
	transport transport.Transport
}

func NewClient(t transport.Transport) *Client {
	return &Client{transport: t}
}

func (c *Client) Get(ctx context.Context, target, key string) (*GetResponse, error) {
	var resp GetResponse
	if err := c.call(ctx, target, TypeGet, TypeGetOK, GetRequest{Key: key}, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("peer get: %s", resp.Error)
	}
	return &resp, nil
}

func (c *Client) GetValues(ctx context.Context, target, key string) ([]versioning.VersionedValue, error) {
	resp, err := c.Get(ctx, target, key)
	if err != nil {
		return nil, err
	}
	return ToVersionedValues(resp.Values), nil
}

func (c *Client) Put(ctx context.Context, target, key string, value versioning.VersionedValue) (*PutResponse, error) {
	var resp PutResponse
	req := PutRequest{Key: key, Value: FromVersionedValue(value)}
	if err := c.call(ctx, target, TypePut, TypePutOK, req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("peer put: %s", resp.Error)
	}
	return &resp, nil
}

func (c *Client) Gossip(ctx context.Context, target, fromNode string, members []MemberDTO) (*GossipResponse, error) {
	var resp GossipResponse
	req := GossipRequest{FromNode: fromNode, Members: members}
	if err := c.call(ctx, target, TypeGossip, TypeGossipOK, req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("peer gossip: %s", resp.Error)
	}
	return &resp, nil
}

func (c *Client) Sync(ctx context.Context, target string, req *SyncRequest) (*SyncResponse, error) {
	var resp SyncResponse
	if err := c.call(ctx, target, TypeSync, TypeSyncOK, req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("peer sync: %s", resp.Error)
	}
	return &resp, nil
}

func (c *Client) DeliverHint(ctx context.Context, target, originalNode, key string, value versioning.VersionedValue) (*HintResponse, error) {
	var resp HintResponse
	req := HintRequest{
		OriginalNode: originalNode,
		Key:          key,
		Value:        FromVersionedValue(value),
	}
	if err := c.call(ctx, target, TypeHint, TypeHintOK, req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("peer hint: %s", resp.Error)
	}
	return &resp, nil
}

func (c *Client) StoreHint(ctx context.Context, target, targetNode, key string, value versioning.VersionedValue) (*StoreHintResponse, error) {
	var resp StoreHintResponse
	req := StoreHintRequest{
		TargetNode: targetNode,
		Key:        key,
		Value:      FromVersionedValue(value),
	}
	if err := c.call(ctx, target, TypeStoreHint, TypeStoreOK, req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("peer store hint: %s", resp.Error)
	}
	return &resp, nil
}

func (c *Client) Health(ctx context.Context, target string) error {
	var resp HealthResponse
	if err := c.call(ctx, target, TypeHealth, TypeHealthOK, struct{}{}, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("peer health: %s", resp.Error)
	}
	if resp.Status != "ok" {
		return fmt.Errorf("peer health: unexpected status %q", resp.Status)
	}
	return nil
}

func (c *Client) Close() {
	if c == nil || c.transport == nil {
		return
	}
	c.transport.Close()
}

func (c *Client) call(ctx context.Context, target, reqType, respType string, req, resp any) error {
	if c == nil || c.transport == nil {
		return fmt.Errorf("peer client: transport is not configured")
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("peer %s: marshal request: %w", reqType, err)
	}
	msg, err := c.transport.Send(ctx, target, transport.Message{
		Type:    reqType,
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("peer %s: send: %w", reqType, err)
	}
	if msg.Type != respType {
		return fmt.Errorf("peer %s: expected response %q, got %q", reqType, respType, msg.Type)
	}
	if resp == nil {
		return nil
	}
	if err := json.Unmarshal(msg.Payload, resp); err != nil {
		return fmt.Errorf("peer %s: decode response: %w", reqType, err)
	}
	return nil
}
