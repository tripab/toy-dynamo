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
	pool           *ConnectionPool
	timeout        time.Duration
	httpClient     *http.Client // Fallback client when pool is not used
	circuitBreaker *CircuitBreakerManager
	retryer        *Retryer
}

// ClientConfig holds configuration for the RPC client
type ClientConfig struct {
	// Timeout for individual RPC requests
	Timeout time.Duration
	// PoolConfig configures connection pooling (nil uses defaults)
	PoolConfig *PoolConfig
	// UsePool enables connection pooling (default: true)
	UsePool bool
	// CircuitBreakerConfig configures circuit breaker behavior (nil uses defaults)
	CircuitBreakerConfig *CircuitBreakerConfig
	// RetryConfig configures retry behavior (nil uses defaults)
	RetryConfig *RetryConfig
	// EnableCircuitBreaker enables circuit breaker (default: true)
	EnableCircuitBreaker bool
	// EnableRetry enables retry with exponential backoff (default: true)
	EnableRetry bool
}

// DefaultClientConfig returns sensible defaults for the RPC client
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Timeout:              5 * time.Second,
		PoolConfig:           DefaultPoolConfig(),
		UsePool:              true,
		CircuitBreakerConfig: DefaultCircuitBreakerConfig(),
		RetryConfig:          DefaultRetryConfig(),
		EnableCircuitBreaker: true,
		EnableRetry:          true,
	}
}

// NewClient creates a new RPC client with the specified timeout
// Deprecated: Use NewClientWithConfig for connection pooling support
func NewClient(timeout time.Duration) *Client {
	// Create a default pool for backward compatibility
	poolConfig := DefaultPoolConfig()
	poolConfig.ConnTimeout = timeout
	pool := NewConnectionPool(poolConfig)

	return &Client{
		pool:           pool,
		timeout:        timeout,
		circuitBreaker: NewCircuitBreakerManager(DefaultCircuitBreakerConfig()),
		retryer:        NewRetryer(DefaultRetryConfig()),
	}
}

// NewClientWithConfig creates a new RPC client with the given configuration
func NewClientWithConfig(config *ClientConfig) *Client {
	if config == nil {
		config = DefaultClientConfig()
	}

	client := &Client{
		timeout: config.Timeout,
	}

	if config.UsePool {
		poolConfig := config.PoolConfig
		if poolConfig == nil {
			poolConfig = DefaultPoolConfig()
		}
		client.pool = NewConnectionPool(poolConfig)
	} else {
		// Create a simple HTTP client without pooling
		client.httpClient = &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}

	// Initialize circuit breaker if enabled
	if config.EnableCircuitBreaker {
		cbConfig := config.CircuitBreakerConfig
		if cbConfig == nil {
			cbConfig = DefaultCircuitBreakerConfig()
		}
		client.circuitBreaker = NewCircuitBreakerManager(cbConfig)
	}

	// Initialize retryer if enabled
	if config.EnableRetry {
		retryConfig := config.RetryConfig
		if retryConfig == nil {
			retryConfig = DefaultRetryConfig()
		}
		client.retryer = NewRetryer(retryConfig)
	}

	return client
}

// NewClientWithPool creates a new RPC client using the given connection pool
func NewClientWithPool(pool *ConnectionPool, timeout time.Duration) *Client {
	return &Client{
		pool:           pool,
		timeout:        timeout,
		circuitBreaker: NewCircuitBreakerManager(DefaultCircuitBreakerConfig()),
		retryer:        NewRetryer(DefaultRetryConfig()),
	}
}

// getHTTPClient returns the appropriate HTTP client for the given address
func (c *Client) getHTTPClient(address string) (*http.Client, error) {
	if c.pool != nil {
		return c.pool.GetClient(address)
	}
	return c.httpClient, nil
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
	// Check circuit breaker first - but health checks should bypass it
	// to allow recovery detection
	operation := func() error {
		return c.healthInternal(ctx, address)
	}

	var err error
	if c.retryer != nil {
		err = c.retryer.Do(ctx, operation)
	} else {
		err = operation()
	}

	// Update circuit breaker based on health check result
	if c.circuitBreaker != nil {
		if err != nil {
			c.circuitBreaker.RecordFailure(address)
		} else {
			c.circuitBreaker.RecordSuccess(address)
		}
	}

	return err
}

// healthInternal performs the actual health check
func (c *Client) healthInternal(ctx context.Context, address string) error {
	url := fmt.Sprintf("http://%s/health", address)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}

	httpClient, err := c.getHTTPClient(address)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		c.markNodeUnhealthy(address)
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.markNodeUnhealthy(address)
		return fmt.Errorf("%w: status %d", ErrServerError, resp.StatusCode)
	}

	c.markNodeHealthy(address)
	return nil
}

// doRequest performs an HTTP POST request with JSON encoding
// It integrates circuit breaker and retry logic when configured
func (c *Client) doRequest(ctx context.Context, address, path string, reqBody any, respBody any) error {
	// Check circuit breaker first
	if c.circuitBreaker != nil && !c.circuitBreaker.Allow(address) {
		return fmt.Errorf("%w for %s", ErrCircuitOpen, address)
	}

	// The actual request operation
	operation := func() error {
		return c.doRequestInternal(ctx, address, path, reqBody, respBody)
	}

	var err error
	if c.retryer != nil {
		err = c.retryer.Do(ctx, operation)
	} else {
		err = operation()
	}

	// Update circuit breaker based on result
	if c.circuitBreaker != nil {
		if err != nil {
			c.circuitBreaker.RecordFailure(address)
		} else {
			c.circuitBreaker.RecordSuccess(address)
		}
	}

	return err
}

// doRequestInternal performs the actual HTTP request without retry logic
func (c *Client) doRequestInternal(ctx context.Context, address, path string, reqBody any, respBody any) error {
	url := fmt.Sprintf("http://%s%s", address, path)

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient, err := c.getHTTPClient(address)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		c.markNodeUnhealthy(address)
		return fmt.Errorf("%w: %v", ErrNodeUnreachable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		c.markNodeUnhealthy(address)
		return fmt.Errorf("%w: status %d, body: %s", ErrServerError, resp.StatusCode, string(bodyBytes))
	}

	if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResponse, err)
	}

	c.markNodeHealthy(address)
	return nil
}

// markNodeUnhealthy marks a node as unhealthy in the pool
func (c *Client) markNodeUnhealthy(address string) {
	if c.pool != nil {
		c.pool.MarkUnhealthy(address)
	}
}

// markNodeHealthy marks a node as healthy in the pool
func (c *Client) markNodeHealthy(address string) {
	if c.pool != nil {
		c.pool.MarkHealthy(address)
	}
}

// IsNodeHealthy returns whether the node at the given address is considered healthy
func (c *Client) IsNodeHealthy(address string) bool {
	if c.pool != nil {
		return c.pool.IsHealthy(address)
	}
	return true // Without pooling, assume healthy
}

// GetPoolStats returns statistics about the connection pool
func (c *Client) GetPoolStats() *PoolStats {
	if c.pool != nil {
		stats := c.pool.Stats()
		return &stats
	}
	return nil
}

// RemoveNode removes a node from the connection pool
func (c *Client) RemoveNode(address string) {
	if c.pool != nil {
		c.pool.RemoveNode(address)
	}
}

// Close closes the client and releases resources
func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
}

// GetCircuitBreakerState returns the circuit breaker state for the given address
func (c *Client) GetCircuitBreakerState(address string) CircuitState {
	if c.circuitBreaker != nil {
		return c.circuitBreaker.State(address)
	}
	return StateClosed // No circuit breaker means always closed
}

// ResetCircuitBreaker resets the circuit breaker for the given address
func (c *Client) ResetCircuitBreaker(address string) {
	if c.circuitBreaker != nil {
		c.circuitBreaker.Reset(address)
	}
}

// GetCircuitBreakerStats returns statistics about all circuit breakers
func (c *Client) GetCircuitBreakerStats() map[string]CircuitBreakerStats {
	if c.circuitBreaker != nil {
		return c.circuitBreaker.Stats()
	}
	return nil
}

// IsCircuitOpen returns true if the circuit breaker for the address is open
func (c *Client) IsCircuitOpen(address string) bool {
	if c.circuitBreaker != nil {
		return c.circuitBreaker.State(address) == StateOpen
	}
	return false
}
