package rpc

import (
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker
type CircuitState int

const (
	// StateClosed is the normal operation state - requests allowed through
	StateClosed CircuitState = iota
	// StateOpen is the fail-fast state - requests rejected immediately
	StateOpen
	// StateHalfOpen is the recovery testing state - limited requests allowed
	StateHalfOpen
)

// String returns a string representation of the circuit state
func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig holds configuration for circuit breakers
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before opening
	FailureThreshold int
	// ResetTimeout is the time to wait before transitioning from Open to HalfOpen
	ResetTimeout time.Duration
	// HalfOpenMaxRequests is the maximum number of requests allowed in HalfOpen state
	HalfOpenMaxRequests int
	// SuccessThresholdToClose is the number of successes in HalfOpen to close the circuit
	SuccessThresholdToClose int
}

// DefaultCircuitBreakerConfig returns sensible default configuration
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		FailureThreshold:        5,
		ResetTimeout:            30 * time.Second,
		HalfOpenMaxRequests:     3,
		SuccessThresholdToClose: 2,
	}
}

// CircuitBreaker implements the circuit breaker pattern for a single endpoint
type CircuitBreaker struct {
	config *CircuitBreakerConfig

	state           CircuitState
	failures        int
	successes       int // Successes in half-open state
	lastFailureTime time.Time
	halfOpenReqs    int // Current requests in half-open state

	mu sync.Mutex
}

// NewCircuitBreaker creates a new circuit breaker with the given configuration
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}
	return &CircuitBreaker{
		config: config,
		state:  StateClosed,
	}
}

// Allow returns true if a request should be allowed through
// For HalfOpen state, it also tracks the request count
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true

	case StateOpen:
		// Check if enough time has passed to try again
		if time.Since(cb.lastFailureTime) >= cb.config.ResetTimeout {
			cb.transitionToHalfOpen()
			return true
		}
		return false

	case StateHalfOpen:
		// Allow limited requests in half-open state
		if cb.halfOpenReqs < cb.config.HalfOpenMaxRequests {
			cb.halfOpenReqs++
			return true
		}
		return false

	default:
		return false
	}
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		// Reset failure count on success
		cb.failures = 0

	case StateHalfOpen:
		cb.successes++
		// If we've had enough successes, close the circuit
		if cb.successes >= cb.config.SuccessThresholdToClose {
			cb.transitionToClosed()
		}
	}
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		cb.failures++
		if cb.failures >= cb.config.FailureThreshold {
			cb.transitionToOpen()
		}

	case StateHalfOpen:
		// Any failure in half-open state opens the circuit again
		cb.transitionToOpen()
	}
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Failures returns the current failure count
func (cb *CircuitBreaker) Failures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transitionToClosed()
}

// transitionToOpen moves the circuit to open state (must hold lock)
func (cb *CircuitBreaker) transitionToOpen() {
	cb.state = StateOpen
	cb.successes = 0
	cb.halfOpenReqs = 0
}

// transitionToHalfOpen moves the circuit to half-open state (must hold lock)
func (cb *CircuitBreaker) transitionToHalfOpen() {
	cb.state = StateHalfOpen
	cb.successes = 0
	cb.halfOpenReqs = 1 // The current request counts
}

// transitionToClosed moves the circuit to closed state (must hold lock)
func (cb *CircuitBreaker) transitionToClosed() {
	cb.state = StateClosed
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenReqs = 0
}

// CircuitBreakerManager manages circuit breakers for multiple endpoints
type CircuitBreakerManager struct {
	config   *CircuitBreakerConfig
	breakers map[string]*CircuitBreaker
	mu       sync.RWMutex
}

// NewCircuitBreakerManager creates a new circuit breaker manager
func NewCircuitBreakerManager(config *CircuitBreakerConfig) *CircuitBreakerManager {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}
	return &CircuitBreakerManager{
		config:   config,
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetBreaker returns the circuit breaker for the given address, creating one if needed
func (m *CircuitBreakerManager) GetBreaker(address string) *CircuitBreaker {
	m.mu.RLock()
	breaker, exists := m.breakers[address]
	m.mu.RUnlock()

	if exists {
		return breaker
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	breaker, exists = m.breakers[address]
	if exists {
		return breaker
	}

	breaker = NewCircuitBreaker(m.config)
	m.breakers[address] = breaker
	return breaker
}

// Allow checks if a request to the given address should be allowed
func (m *CircuitBreakerManager) Allow(address string) bool {
	return m.GetBreaker(address).Allow()
}

// RecordSuccess records a successful request to the given address
func (m *CircuitBreakerManager) RecordSuccess(address string) {
	m.GetBreaker(address).RecordSuccess()
}

// RecordFailure records a failed request to the given address
func (m *CircuitBreakerManager) RecordFailure(address string) {
	m.GetBreaker(address).RecordFailure()
}

// State returns the state of the circuit breaker for the given address
func (m *CircuitBreakerManager) State(address string) CircuitState {
	return m.GetBreaker(address).State()
}

// Reset resets the circuit breaker for the given address
func (m *CircuitBreakerManager) Reset(address string) {
	m.mu.RLock()
	breaker, exists := m.breakers[address]
	m.mu.RUnlock()

	if exists {
		breaker.Reset()
	}
}

// ResetAll resets all circuit breakers
func (m *CircuitBreakerManager) ResetAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, breaker := range m.breakers {
		breaker.Reset()
	}
}

// RemoveBreaker removes the circuit breaker for the given address
func (m *CircuitBreakerManager) RemoveBreaker(address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.breakers, address)
}

// Stats returns statistics about all circuit breakers
func (m *CircuitBreakerManager) Stats() map[string]CircuitBreakerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]CircuitBreakerStats)
	for addr, breaker := range m.breakers {
		breaker.mu.Lock()
		stats[addr] = CircuitBreakerStats{
			Address:  addr,
			State:    breaker.state,
			Failures: breaker.failures,
		}
		breaker.mu.Unlock()
	}
	return stats
}

// CircuitBreakerStats contains statistics for a circuit breaker
type CircuitBreakerStats struct {
	Address  string
	State    CircuitState
	Failures int
}
