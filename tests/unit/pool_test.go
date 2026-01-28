package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/rpc"
)

func TestConnectionPoolDefaultConfig(t *testing.T) {
	config := rpc.DefaultPoolConfig()

	if config.MaxIdleConns != 10 {
		t.Errorf("Expected MaxIdleConns=10, got %d", config.MaxIdleConns)
	}
	if config.MaxOpenConns != 100 {
		t.Errorf("Expected MaxOpenConns=100, got %d", config.MaxOpenConns)
	}
	if config.ConnTimeout != 5*time.Second {
		t.Errorf("Expected ConnTimeout=5s, got %v", config.ConnTimeout)
	}
	if config.IdleTimeout != 90*time.Second {
		t.Errorf("Expected IdleTimeout=90s, got %v", config.IdleTimeout)
	}
	if config.HealthCheckInterval != 30*time.Second {
		t.Errorf("Expected HealthCheckInterval=30s, got %v", config.HealthCheckInterval)
	}
}

func TestConnectionPoolCreate(t *testing.T) {
	pool := rpc.NewConnectionPool(nil) // Use defaults
	if pool == nil {
		t.Fatal("Expected non-nil pool")
	}
	defer pool.Close()

	stats := pool.Stats()
	if stats.NodeCount != 0 {
		t.Errorf("Expected empty pool, got %d nodes", stats.NodeCount)
	}
}

func TestConnectionPoolGetClient(t *testing.T) {
	config := &rpc.PoolConfig{
		MaxIdleConns:        5,
		MaxOpenConns:        10,
		ConnTimeout:         1 * time.Second,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 0, // Disable for test
	}

	pool := rpc.NewConnectionPool(config)
	defer pool.Close()

	// Get client for first address
	client1, err := pool.GetClient("localhost:8001")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	if client1 == nil {
		t.Fatal("Expected non-nil client")
	}

	// Get client for same address should return same client
	client1Again, err := pool.GetClient("localhost:8001")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	if client1 != client1Again {
		t.Error("Expected same client for same address")
	}

	// Get client for different address should return different client
	client2, err := pool.GetClient("localhost:8002")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	if client2 == client1 {
		t.Error("Expected different client for different address")
	}

	// Check stats
	stats := pool.Stats()
	if stats.NodeCount != 2 {
		t.Errorf("Expected 2 nodes in pool, got %d", stats.NodeCount)
	}
}

func TestConnectionPoolHealthStatus(t *testing.T) {
	config := &rpc.PoolConfig{
		MaxIdleConns:        5,
		MaxOpenConns:        10,
		ConnTimeout:         1 * time.Second,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 0, // Disable for test
	}

	pool := rpc.NewConnectionPool(config)
	defer pool.Close()

	address := "localhost:8001"

	// Unknown node should be considered healthy
	if !pool.IsHealthy(address) {
		t.Error("Unknown node should be considered healthy")
	}

	// Get client to create node pool
	_, err := pool.GetClient(address)
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	// Should still be healthy
	if !pool.IsHealthy(address) {
		t.Error("Node should be healthy after creation")
	}

	// Mark as unhealthy
	pool.MarkUnhealthy(address)
	if pool.IsHealthy(address) {
		t.Error("Node should be unhealthy after marking")
	}

	// Mark as healthy again
	pool.MarkHealthy(address)
	if !pool.IsHealthy(address) {
		t.Error("Node should be healthy after marking healthy")
	}
}

func TestConnectionPoolConcurrentAccess(t *testing.T) {
	config := &rpc.PoolConfig{
		MaxIdleConns:        5,
		MaxOpenConns:        10,
		ConnTimeout:         1 * time.Second,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 0, // Disable for test
	}

	pool := rpc.NewConnectionPool(config)
	defer pool.Close()

	addresses := []string{
		"localhost:8001",
		"localhost:8002",
		"localhost:8003",
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Spawn multiple goroutines accessing the pool concurrently
	for i := 0; i < 10; i++ {
		for _, addr := range addresses {
			wg.Add(1)
			go func(address string) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_, err := pool.GetClient(address)
					if err != nil {
						errors <- err
					}
				}
			}(addr)
		}
	}

	wg.Wait()
	close(errors)

	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		t.Errorf("Got %d errors during concurrent access: %v", len(errs), errs)
	}

	stats := pool.Stats()
	if stats.NodeCount != len(addresses) {
		t.Errorf("Expected %d nodes, got %d", len(addresses), stats.NodeCount)
	}
}

func TestConnectionPoolRemoveNode(t *testing.T) {
	config := &rpc.PoolConfig{
		MaxIdleConns:        5,
		MaxOpenConns:        10,
		ConnTimeout:         1 * time.Second,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 0, // Disable for test
	}

	pool := rpc.NewConnectionPool(config)
	defer pool.Close()

	address := "localhost:8001"

	// Create node pool
	_, err := pool.GetClient(address)
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	stats := pool.Stats()
	if stats.NodeCount != 1 {
		t.Errorf("Expected 1 node, got %d", stats.NodeCount)
	}

	// Remove node
	pool.RemoveNode(address)

	stats = pool.Stats()
	if stats.NodeCount != 0 {
		t.Errorf("Expected 0 nodes after removal, got %d", stats.NodeCount)
	}

	// Can get new client for same address
	_, err = pool.GetClient(address)
	if err != nil {
		t.Fatalf("Failed to get client after removal: %v", err)
	}
}

func TestConnectionPoolCleanupIdle(t *testing.T) {
	config := &rpc.PoolConfig{
		MaxIdleConns:        5,
		MaxOpenConns:        10,
		ConnTimeout:         1 * time.Second,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 0, // Disable for test
	}

	pool := rpc.NewConnectionPool(config)
	defer pool.Close()

	// Create multiple node pools
	addresses := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	for _, addr := range addresses {
		_, err := pool.GetClient(addr)
		if err != nil {
			t.Fatalf("Failed to get client: %v", err)
		}
	}

	stats := pool.Stats()
	if stats.NodeCount != 3 {
		t.Errorf("Expected 3 nodes, got %d", stats.NodeCount)
	}

	// Wait a tiny bit
	time.Sleep(10 * time.Millisecond)

	// Access one address to update its last used time
	_, err := pool.GetClient("localhost:8001")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	// Cleanup nodes idle for more than 5ms
	removed := pool.CleanupIdle(5 * time.Millisecond)
	if removed != 2 {
		t.Errorf("Expected 2 nodes removed, got %d", removed)
	}

	stats = pool.Stats()
	if stats.NodeCount != 1 {
		t.Errorf("Expected 1 node after cleanup, got %d", stats.NodeCount)
	}

	// The actively used one should still be there
	if _, ok := stats.Nodes["localhost:8001"]; !ok {
		t.Error("Expected localhost:8001 to still be in pool")
	}
}

func TestConnectionPoolClose(t *testing.T) {
	config := &rpc.PoolConfig{
		MaxIdleConns:        5,
		MaxOpenConns:        10,
		ConnTimeout:         1 * time.Second,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 0, // Disable for test
	}

	pool := rpc.NewConnectionPool(config)

	// Create node pool
	_, err := pool.GetClient("localhost:8001")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	// Close pool
	pool.Close()

	// Getting client after close should fail
	_, err = pool.GetClient("localhost:8001")
	if err == nil {
		t.Error("Expected error getting client after close")
	}

	// Double close should not panic
	pool.Close()
}

func TestConnectionPoolStats(t *testing.T) {
	config := &rpc.PoolConfig{
		MaxIdleConns:        5,
		MaxOpenConns:        10,
		ConnTimeout:         1 * time.Second,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 0, // Disable for test
	}

	pool := rpc.NewConnectionPool(config)
	defer pool.Close()

	// Create node pools
	addresses := []string{"localhost:8001", "localhost:8002"}
	for _, addr := range addresses {
		_, err := pool.GetClient(addr)
		if err != nil {
			t.Fatalf("Failed to get client: %v", err)
		}
	}

	// Mark one as unhealthy
	pool.MarkUnhealthy("localhost:8002")

	stats := pool.Stats()

	if stats.NodeCount != 2 {
		t.Errorf("Expected 2 nodes, got %d", stats.NodeCount)
	}

	node1Stats, ok := stats.Nodes["localhost:8001"]
	if !ok {
		t.Fatal("Expected localhost:8001 in stats")
	}
	if !node1Stats.Healthy {
		t.Error("Expected localhost:8001 to be healthy")
	}

	node2Stats, ok := stats.Nodes["localhost:8002"]
	if !ok {
		t.Fatal("Expected localhost:8002 in stats")
	}
	if node2Stats.Healthy {
		t.Error("Expected localhost:8002 to be unhealthy")
	}
}

// TestClientWithPoolIntegration tests the RPC client with a real server
func TestClientWithPoolIntegration(t *testing.T) {
	// Create a test server
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)

		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/rpc/get":
			var req rpc.GetRequest
			json.NewDecoder(r.Body).Decode(&req)
			resp := rpc.GetResponse{Values: nil}
			json.NewEncoder(w).Encode(resp)
		case "/rpc/put":
			var req rpc.PutRequest
			json.NewDecoder(r.Body).Decode(&req)
			resp := rpc.PutResponse{Success: true}
			json.NewEncoder(w).Encode(resp)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Extract server address (remove http:// prefix)
	address := server.Listener.Addr().String()

	// Create client with custom pool config
	config := &rpc.ClientConfig{
		Timeout: 5 * time.Second,
		PoolConfig: &rpc.PoolConfig{
			MaxIdleConns:        5,
			MaxOpenConns:        10,
			ConnTimeout:         5 * time.Second,
			IdleTimeout:         30 * time.Second,
			HealthCheckInterval: 0, // Disable for test
		},
		UsePool: true,
	}

	client := rpc.NewClientWithConfig(config)
	defer client.Close()

	ctx := context.Background()

	// Test health check
	err := client.Health(ctx, address)
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}

	// Test Get
	_, err = client.Get(ctx, address, "test-key")
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}

	// Make multiple requests to test connection reuse
	for i := 0; i < 10; i++ {
		_, err = client.Get(ctx, address, "test-key")
		if err != nil {
			t.Errorf("Get %d failed: %v", i, err)
		}
	}

	// Check pool stats
	stats := client.GetPoolStats()
	if stats == nil {
		t.Fatal("Expected non-nil pool stats")
	}
	if stats.NodeCount != 1 {
		t.Errorf("Expected 1 node in pool stats, got %d", stats.NodeCount)
	}

	// Verify node is healthy
	if !client.IsNodeHealthy(address) {
		t.Error("Expected node to be healthy")
	}

	// Total requests: 1 health + 1 get + 10 gets = 12
	if count := atomic.LoadInt32(&requestCount); count != 12 {
		t.Errorf("Expected 12 requests, got %d", count)
	}
}

func TestClientHealthTracking(t *testing.T) {
	// Create a server that will fail after some requests
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count > 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		resp := rpc.GetResponse{Values: nil}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	address := server.Listener.Addr().String()

	config := &rpc.ClientConfig{
		Timeout: 5 * time.Second,
		PoolConfig: &rpc.PoolConfig{
			MaxIdleConns:        5,
			MaxOpenConns:        10,
			ConnTimeout:         5 * time.Second,
			IdleTimeout:         30 * time.Second,
			HealthCheckInterval: 0,
		},
		UsePool: true,
	}

	client := rpc.NewClientWithConfig(config)
	defer client.Close()

	ctx := context.Background()

	// First requests should succeed
	for i := 0; i < 3; i++ {
		_, err := client.Get(ctx, address, "test-key")
		if err != nil {
			t.Errorf("Request %d should have succeeded: %v", i, err)
		}
	}

	// Node should be healthy
	if !client.IsNodeHealthy(address) {
		t.Error("Node should be healthy after successful requests")
	}

	// Next request should fail and mark node unhealthy
	_, err := client.Get(ctx, address, "test-key")
	if err == nil {
		t.Error("Request should have failed")
	}

	// Node should now be marked unhealthy
	if client.IsNodeHealthy(address) {
		t.Error("Node should be unhealthy after failed request")
	}
}

func TestClientRemoveNode(t *testing.T) {
	config := &rpc.ClientConfig{
		Timeout: 5 * time.Second,
		PoolConfig: &rpc.PoolConfig{
			MaxIdleConns:        5,
			MaxOpenConns:        10,
			ConnTimeout:         5 * time.Second,
			IdleTimeout:         30 * time.Second,
			HealthCheckInterval: 0,
		},
		UsePool: true,
	}

	client := rpc.NewClientWithConfig(config)
	defer client.Close()

	// Create a test server just to get a valid address
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(rpc.GetResponse{Values: nil})
	}))
	defer server.Close()

	address := server.Listener.Addr().String()
	ctx := context.Background()

	// Make a request to create the pool entry
	_, err := client.Get(ctx, address, "test-key")
	if err != nil {
		t.Fatalf("Initial request failed: %v", err)
	}

	stats := client.GetPoolStats()
	if stats.NodeCount != 1 {
		t.Errorf("Expected 1 node, got %d", stats.NodeCount)
	}

	// Remove the node
	client.RemoveNode(address)

	stats = client.GetPoolStats()
	if stats.NodeCount != 0 {
		t.Errorf("Expected 0 nodes after removal, got %d", stats.NodeCount)
	}
}

func TestClientWithoutPool(t *testing.T) {
	config := &rpc.ClientConfig{
		Timeout: 5 * time.Second,
		UsePool: false,
	}

	client := rpc.NewClientWithConfig(config)
	defer client.Close()

	// Pool stats should be nil
	stats := client.GetPoolStats()
	if stats != nil {
		t.Error("Expected nil pool stats when not using pool")
	}

	// IsNodeHealthy should always return true without pool
	if !client.IsNodeHealthy("any-address") {
		t.Error("Expected node to be considered healthy without pool")
	}
}
