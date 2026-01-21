package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tripab/toy-dynamo/pkg/versioning"
)

// Client is an HTTP-based RPC client for inter-node communication
type Client struct {
	httpClient *http.Client
	timeout    time.Duration
}

// NewClient creates a new RPC client with the specified timeout
func NewClient(timeout time.Duration) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		timeout: timeout,
	}
}

// Get retrieves values from a remote node
func (c *Client) Get(ctx context.Context, address, key string) (*GetResponse, error) {
	req := GetRequest{Key: key}
	var resp GetResponse

	if err := c.doRequest(ctx, address, "/rpc/get", req, &resp); err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("%w: %s", ErrServerError, resp.Error)
	}

	return &resp, nil
}

// GetValues is a convenience method that returns VersionedValues directly
func (c *Client) GetValues(ctx context.Context, address, key string) ([]versioning.VersionedValue, error) {
	resp, err := c.Get(ctx, address, key)
	if err != nil {
		return nil, err
	}
	return ToVersionedValues(resp.Values), nil
}

// Put sends a value to a remote node
func (c *Client) Put(ctx context.Context, address, key string, value versioning.VersionedValue) (*PutResponse, error) {
	req := PutRequest{
		Key:   key,
		Value: FromVersionedValue(value),
	}
	var resp PutResponse

	if err := c.doRequest(ctx, address, "/rpc/put", req, &resp); err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("%w: %s", ErrServerError, resp.Error)
	}

	return &resp, nil
}

// Gossip exchanges membership information with a remote node
func (c *Client) Gossip(ctx context.Context, address string, fromNode string, members []MemberDTO) (*GossipResponse, error) {
	req := GossipRequest{
		FromNode: fromNode,
		Members:  members,
	}
	var resp GossipResponse

	if err := c.doRequest(ctx, address, "/rpc/gossip", req, &resp); err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("%w: %s", ErrServerError, resp.Error)
	}

	return &resp, nil
}

// Sync sends an anti-entropy sync request to a remote node
func (c *Client) Sync(ctx context.Context, address string, req *SyncRequest) (*SyncResponse, error) {
	var resp SyncResponse

	if err := c.doRequest(ctx, address, "/rpc/sync", req, &resp); err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("%w: %s", ErrServerError, resp.Error)
	}

	return &resp, nil
}

// DeliverHint sends a hinted handoff to the target node
func (c *Client) DeliverHint(ctx context.Context, address string, originalNode, key string, value versioning.VersionedValue) (*HintResponse, error) {
	req := HintRequest{
		OriginalNode: originalNode,
		Key:          key,
		Value:        FromVersionedValue(value),
	}
	var resp HintResponse

	if err := c.doRequest(ctx, address, "/rpc/hint", req, &resp); err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("%w: %s", ErrServerError, resp.Error)
	}

	return &resp, nil
}

// Health checks if a remote node is healthy
func (c *Client) Health(ctx context.Context, address string) error {
	url := fmt.Sprintf("http://%s/health", address)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: status %d", ErrServerError, resp.StatusCode)
	}

	return nil
}

// doRequest performs an HTTP POST request with JSON encoding
func (c *Client) doRequest(ctx context.Context, address, path string, reqBody any, respBody any) error {
	url := fmt.Sprintf("http://%s%s", address, path)

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: status %d, body: %s", ErrServerError, resp.StatusCode, string(bodyBytes))
	}

	if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	return nil
}

// Close closes the client and releases resources
func (c *Client) Close() {
	c.httpClient.CloseIdleConnections()
}
