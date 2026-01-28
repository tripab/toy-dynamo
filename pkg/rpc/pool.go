package rpc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// PoolConfig configures the connection pool behavior
type PoolConfig struct {
	// MaxIdleConns is the maximum number of idle connections to keep per host
	MaxIdleConns int
	// MaxOpenConns is the maximum number of connections per host (0 = unlimited)
	MaxOpenConns int
	// ConnTimeout is the timeout for establishing a connection
	ConnTimeout time.Duration
	// IdleTimeout is the time after which idle connections are closed
	IdleTimeout time.Duration
	// HealthCheckInterval is how often to check connection health (0 = disabled)
	HealthCheckInterval time.Duration
}

// DefaultPoolConfig returns sensible defaults for connection pooling
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxIdleConns:        10,
		MaxOpenConns:        100,
		ConnTimeout:         5 * time.Second,
		IdleTimeout:         90 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}
}

// ConnectionPool manages HTTP connections to remote nodes with per-node pooling
type ConnectionPool struct {
	config  *PoolConfig
	pools   map[string]*nodePool // Per-node connection pools
	mu      sync.RWMutex
	closed  bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// nodePool manages connections to a single node
type nodePool struct {
	address    string
	transport  *http.Transport
	client     *http.Client
	openConns  int
	mu         sync.Mutex
	lastUsed   time.Time
	healthy    bool
	healthMu   sync.RWMutex
}

// NewConnectionPool creates a new connection pool with the given configuration
func NewConnectionPool(config *PoolConfig) *ConnectionPool {
	if config == nil {
		config = DefaultPoolConfig()
	}

	pool := &ConnectionPool{
		config: config,
		pools:  make(map[string]*nodePool),
		stopCh: make(chan struct{}),
	}

	// Start health check goroutine if enabled
	if config.HealthCheckInterval > 0 {
		pool.wg.Add(1)
		go pool.healthCheckLoop()
	}

	return pool
}

// GetClient returns an HTTP client configured for the given address.
// The client uses a shared transport with connection pooling.
func (p *ConnectionPool) GetClient(address string) (*http.Client, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("connection pool is closed")
	}
	np, exists := p.pools[address]
	p.mu.RUnlock()

	if exists {
		np.mu.Lock()
		np.lastUsed = time.Now()
		np.mu.Unlock()
		return np.client, nil
	}

	// Create new node pool
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if p.closed {
		return nil, fmt.Errorf("connection pool is closed")
	}

	np, exists = p.pools[address]
	if exists {
		np.mu.Lock()
		np.lastUsed = time.Now()
		np.mu.Unlock()
		return np.client, nil
	}

	// Create a new node pool with its own transport
	np = p.createNodePool(address)
	p.pools[address] = np

	return np.client, nil
}

// createNodePool creates a new node pool for the given address
func (p *ConnectionPool) createNodePool(address string) *nodePool {
	transport := &http.Transport{
		MaxIdleConns:        p.config.MaxIdleConns,
		MaxIdleConnsPerHost: p.config.MaxIdleConns,
		MaxConnsPerHost:     p.config.MaxOpenConns,
		IdleConnTimeout:     p.config.IdleTimeout,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   p.config.ConnTimeout,
				KeepAlive: 30 * time.Second,
			}
			return dialer.DialContext(ctx, network, addr)
		},
		ResponseHeaderTimeout: p.config.ConnTimeout,
		DisableKeepAlives:     false,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   p.config.ConnTimeout * 2, // Allow some buffer for request processing
	}

	return &nodePool{
		address:   address,
		transport: transport,
		client:    client,
		lastUsed:  time.Now(),
		healthy:   true,
	}
}

// IsHealthy returns whether the node at the given address is considered healthy
func (p *ConnectionPool) IsHealthy(address string) bool {
	p.mu.RLock()
	np, exists := p.pools[address]
	p.mu.RUnlock()

	if !exists {
		return true // Unknown nodes are assumed healthy until proven otherwise
	}

	np.healthMu.RLock()
	defer np.healthMu.RUnlock()
	return np.healthy
}

// MarkUnhealthy marks a node as unhealthy
func (p *ConnectionPool) MarkUnhealthy(address string) {
	p.mu.RLock()
	np, exists := p.pools[address]
	p.mu.RUnlock()

	if exists {
		np.healthMu.Lock()
		np.healthy = false
		np.healthMu.Unlock()
	}
}

// MarkHealthy marks a node as healthy
func (p *ConnectionPool) MarkHealthy(address string) {
	p.mu.RLock()
	np, exists := p.pools[address]
	p.mu.RUnlock()

	if exists {
		np.healthMu.Lock()
		np.healthy = true
		np.healthMu.Unlock()
	}
}

// healthCheckLoop periodically checks the health of all pooled connections
func (p *ConnectionPool) healthCheckLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.runHealthChecks()
		}
	}
}

// runHealthChecks performs health checks on all nodes
func (p *ConnectionPool) runHealthChecks() {
	p.mu.RLock()
	addresses := make([]string, 0, len(p.pools))
	for addr := range p.pools {
		addresses = append(addresses, addr)
	}
	p.mu.RUnlock()

	for _, addr := range addresses {
		p.checkNodeHealth(addr)
	}
}

// checkNodeHealth checks the health of a single node
func (p *ConnectionPool) checkNodeHealth(address string) {
	p.mu.RLock()
	np, exists := p.pools[address]
	p.mu.RUnlock()

	if !exists {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.config.ConnTimeout)
	defer cancel()

	url := fmt.Sprintf("http://%s/health", address)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		np.healthMu.Lock()
		np.healthy = false
		np.healthMu.Unlock()
		return
	}

	resp, err := np.client.Do(req)
	if err != nil {
		np.healthMu.Lock()
		np.healthy = false
		np.healthMu.Unlock()
		return
	}
	defer resp.Body.Close()

	np.healthMu.Lock()
	np.healthy = resp.StatusCode == http.StatusOK
	np.healthMu.Unlock()
}

// Stats returns statistics about the connection pool
func (p *ConnectionPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := PoolStats{
		NodeCount: len(p.pools),
		Nodes:     make(map[string]NodePoolStats),
	}

	for addr, np := range p.pools {
		np.mu.Lock()
		np.healthMu.RLock()
		stats.Nodes[addr] = NodePoolStats{
			Address:   addr,
			LastUsed:  np.lastUsed,
			Healthy:   np.healthy,
			OpenConns: np.openConns,
		}
		np.healthMu.RUnlock()
		np.mu.Unlock()
	}

	return stats
}

// PoolStats contains statistics about the connection pool
type PoolStats struct {
	NodeCount int
	Nodes     map[string]NodePoolStats
}

// NodePoolStats contains statistics about a single node's pool
type NodePoolStats struct {
	Address   string
	LastUsed  time.Time
	Healthy   bool
	OpenConns int
}

// CleanupIdle removes node pools that haven't been used recently
func (p *ConnectionPool) CleanupIdle(maxIdleTime time.Duration) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	removed := 0

	for addr, np := range p.pools {
		np.mu.Lock()
		idleTime := now.Sub(np.lastUsed)
		np.mu.Unlock()

		if idleTime > maxIdleTime {
			np.transport.CloseIdleConnections()
			delete(p.pools, addr)
			removed++
		}
	}

	return removed
}

// RemoveNode removes a node from the pool and closes its connections
func (p *ConnectionPool) RemoveNode(address string) {
	p.mu.Lock()
	np, exists := p.pools[address]
	if exists {
		delete(p.pools, address)
	}
	p.mu.Unlock()

	if exists && np != nil {
		np.transport.CloseIdleConnections()
	}
}

// Close closes the connection pool and all its connections
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.stopCh)

	// Close all transports
	for _, np := range p.pools {
		np.transport.CloseIdleConnections()
	}
	p.pools = nil
	p.mu.Unlock()

	// Wait for health check goroutine to finish
	p.wg.Wait()
}
