package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/rpc"
)

func TestCircuitBreakerInitialState(t *testing.T) {
	cb := rpc.NewCircuitBreaker(nil)

	if cb.State() != rpc.StateClosed {
		t.Errorf("expected initial state to be Closed, got %v", cb.State())
	}

	if cb.Failures() != 0 {
		t.Errorf("expected initial failures to be 0, got %d", cb.Failures())
	}
}

func TestCircuitBreakerAllowsRequestsWhenClosed(t *testing.T) {
	cb := rpc.NewCircuitBreaker(nil)

	for i := 0; i < 10; i++ {
		if !cb.Allow() {
			t.Errorf("expected circuit breaker to allow request when closed")
		}
	}
}

func TestCircuitBreakerOpensAfterThresholdFailures(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold: 3,
		ResetTimeout:     1 * time.Second,
	}
	cb := rpc.NewCircuitBreaker(config)

	// Record failures up to threshold
	for i := 0; i < 3; i++ {
		if !cb.Allow() {
			t.Errorf("circuit should allow request before threshold")
		}
		cb.RecordFailure()
	}

	// Circuit should now be open
	if cb.State() != rpc.StateOpen {
		t.Errorf("expected circuit to be open after threshold failures, got %v", cb.State())
	}

	// Should not allow requests when open
	if cb.Allow() {
		t.Errorf("expected circuit breaker to reject requests when open")
	}
}

func TestCircuitBreakerResetsFailuresOnSuccess(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold: 5,
		ResetTimeout:     1 * time.Second,
	}
	cb := rpc.NewCircuitBreaker(config)

	// Record some failures
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.Failures() != 2 {
		t.Errorf("expected 2 failures, got %d", cb.Failures())
	}

	// Success should reset failures
	cb.RecordSuccess()

	if cb.Failures() != 0 {
		t.Errorf("expected failures to reset to 0 after success, got %d", cb.Failures())
	}
}

func TestCircuitBreakerTransitionsToHalfOpen(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold:    2,
		ResetTimeout:        100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}
	cb := rpc.NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != rpc.StateOpen {
		t.Errorf("expected circuit to be open, got %v", cb.State())
	}

	// Wait for reset timeout
	time.Sleep(150 * time.Millisecond)

	// Should transition to half-open on next Allow
	if !cb.Allow() {
		t.Errorf("expected Allow to return true after reset timeout")
	}

	if cb.State() != rpc.StateHalfOpen {
		t.Errorf("expected circuit to be half-open, got %v", cb.State())
	}
}

func TestCircuitBreakerClosesAfterSuccessInHalfOpen(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold:        2,
		ResetTimeout:            50 * time.Millisecond,
		HalfOpenMaxRequests:     3,
		SuccessThresholdToClose: 2,
	}
	cb := rpc.NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for reset timeout
	time.Sleep(100 * time.Millisecond)

	// Allow request to transition to half-open
	cb.Allow()

	if cb.State() != rpc.StateHalfOpen {
		t.Errorf("expected circuit to be half-open, got %v", cb.State())
	}

	// Record successes to close
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != rpc.StateClosed {
		t.Errorf("expected circuit to close after successes in half-open, got %v", cb.State())
	}
}

func TestCircuitBreakerReopensOnFailureInHalfOpen(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold:        2,
		ResetTimeout:            50 * time.Millisecond,
		HalfOpenMaxRequests:     3,
		SuccessThresholdToClose: 2,
	}
	cb := rpc.NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for reset timeout
	time.Sleep(100 * time.Millisecond)

	// Allow request to transition to half-open
	cb.Allow()

	if cb.State() != rpc.StateHalfOpen {
		t.Errorf("expected circuit to be half-open, got %v", cb.State())
	}

	// Record a failure - should re-open
	cb.RecordFailure()

	if cb.State() != rpc.StateOpen {
		t.Errorf("expected circuit to re-open after failure in half-open, got %v", cb.State())
	}
}

func TestCircuitBreakerHalfOpenLimitsRequests(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold:    2,
		ResetTimeout:        50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	cb := rpc.NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for reset timeout
	time.Sleep(100 * time.Millisecond)

	// First two requests should be allowed
	if !cb.Allow() {
		t.Errorf("expected first request to be allowed in half-open")
	}
	if !cb.Allow() {
		t.Errorf("expected second request to be allowed in half-open")
	}

	// Third request should be rejected
	if cb.Allow() {
		t.Errorf("expected third request to be rejected in half-open")
	}
}

func TestCircuitBreakerReset(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     1 * time.Second,
	}
	cb := rpc.NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != rpc.StateOpen {
		t.Errorf("expected circuit to be open")
	}

	// Reset should close it
	cb.Reset()

	if cb.State() != rpc.StateClosed {
		t.Errorf("expected circuit to be closed after reset, got %v", cb.State())
	}

	if cb.Failures() != 0 {
		t.Errorf("expected failures to be 0 after reset, got %d", cb.Failures())
	}
}

func TestCircuitBreakerStateString(t *testing.T) {
	tests := []struct {
		state    rpc.CircuitState
		expected string
	}{
		{rpc.StateClosed, "closed"},
		{rpc.StateOpen, "open"},
		{rpc.StateHalfOpen, "half-open"},
		{rpc.CircuitState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("CircuitState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

func TestCircuitBreakerManagerCreatesBreakers(t *testing.T) {
	manager := rpc.NewCircuitBreakerManager(nil)

	// Get breakers for different addresses
	breaker1 := manager.GetBreaker("addr1")
	breaker2 := manager.GetBreaker("addr2")

	if breaker1 == breaker2 {
		t.Errorf("expected different breakers for different addresses")
	}

	// Getting same address should return same breaker
	breaker1Again := manager.GetBreaker("addr1")
	if breaker1 != breaker1Again {
		t.Errorf("expected same breaker for same address")
	}
}

func TestCircuitBreakerManagerAllowAndRecord(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     1 * time.Second,
	}
	manager := rpc.NewCircuitBreakerManager(config)

	// Initially should allow
	if !manager.Allow("addr1") {
		t.Errorf("expected manager to allow initial request")
	}

	// Record failures to open circuit
	manager.RecordFailure("addr1")
	manager.RecordFailure("addr1")

	// Should now be open
	if manager.State("addr1") != rpc.StateOpen {
		t.Errorf("expected circuit to be open")
	}

	// Should not allow
	if manager.Allow("addr1") {
		t.Errorf("expected manager to reject request when open")
	}

	// Other addresses should still work
	if !manager.Allow("addr2") {
		t.Errorf("expected manager to allow request to different address")
	}
}

func TestCircuitBreakerManagerStats(t *testing.T) {
	manager := rpc.NewCircuitBreakerManager(nil)

	// Record some activity
	manager.RecordFailure("addr1")
	manager.RecordFailure("addr1")
	manager.RecordSuccess("addr2")

	stats := manager.Stats()

	if len(stats) != 2 {
		t.Errorf("expected 2 addresses in stats, got %d", len(stats))
	}

	if stats["addr1"].Failures != 2 {
		t.Errorf("expected addr1 to have 2 failures, got %d", stats["addr1"].Failures)
	}

	if stats["addr2"].Failures != 0 {
		t.Errorf("expected addr2 to have 0 failures, got %d", stats["addr2"].Failures)
	}
}

func TestCircuitBreakerManagerResetAll(t *testing.T) {
	config := &rpc.CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     1 * time.Second,
	}
	manager := rpc.NewCircuitBreakerManager(config)

	// Open circuits for multiple addresses
	manager.RecordFailure("addr1")
	manager.RecordFailure("addr1")
	manager.RecordFailure("addr2")
	manager.RecordFailure("addr2")

	// Both should be open
	if manager.State("addr1") != rpc.StateOpen {
		t.Errorf("expected addr1 circuit to be open")
	}
	if manager.State("addr2") != rpc.StateOpen {
		t.Errorf("expected addr2 circuit to be open")
	}

	// Reset all
	manager.ResetAll()

	// Both should be closed
	if manager.State("addr1") != rpc.StateClosed {
		t.Errorf("expected addr1 circuit to be closed after reset")
	}
	if manager.State("addr2") != rpc.StateClosed {
		t.Errorf("expected addr2 circuit to be closed after reset")
	}
}

func TestCircuitBreakerConcurrentAccess(t *testing.T) {
	cb := rpc.NewCircuitBreaker(&rpc.CircuitBreakerConfig{
		FailureThreshold: 100,
		ResetTimeout:     1 * time.Second,
	})

	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cb.Allow()
			if idx%2 == 0 {
				cb.RecordSuccess()
			} else {
				cb.RecordFailure()
			}
		}(i)
	}

	wg.Wait()

	// Should not panic and circuit should still be usable
	cb.Allow()
}

// Retry tests

func TestRetryerSucceedsOnFirstAttempt(t *testing.T) {
	retryer := rpc.NewRetryer(nil)

	attempts := 0
	err := retryer.Do(context.Background(), func() error {
		attempts++
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

func TestRetryerRetriesOnFailure(t *testing.T) {
	config := &rpc.RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
		Jitter:            0,
		RetryableErrors:   []error{rpc.ErrNodeUnreachable},
	}
	retryer := rpc.NewRetryer(config)

	attempts := 0
	err := retryer.Do(context.Background(), func() error {
		attempts++
		return rpc.ErrNodeUnreachable
	})

	// Should have retried 3 times + initial attempt = 4
	if attempts != 4 {
		t.Errorf("expected 4 attempts, got %d", attempts)
	}

	if !errors.Is(err, rpc.ErrNodeUnreachable) {
		t.Errorf("expected ErrNodeUnreachable, got %v", err)
	}
}

func TestRetryerSucceedsAfterRetries(t *testing.T) {
	config := &rpc.RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
		Jitter:            0,
		RetryableErrors:   []error{rpc.ErrNodeUnreachable},
	}
	retryer := rpc.NewRetryer(config)

	attempts := 0
	err := retryer.Do(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return rpc.ErrNodeUnreachable
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryerDoesNotRetryNonRetryableError(t *testing.T) {
	config := &rpc.RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
		RetryableErrors:   []error{rpc.ErrNodeUnreachable},
	}
	retryer := rpc.NewRetryer(config)

	nonRetryableErr := errors.New("non-retryable error")
	attempts := 0
	err := retryer.Do(context.Background(), func() error {
		attempts++
		return nonRetryableErr
	})

	if attempts != 1 {
		t.Errorf("expected 1 attempt for non-retryable error, got %d", attempts)
	}

	if !errors.Is(err, nonRetryableErr) {
		t.Errorf("expected non-retryable error, got %v", err)
	}
}

func TestRetryerRespectsContextCancellation(t *testing.T) {
	config := &rpc.RetryConfig{
		MaxRetries:        10,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
		RetryableErrors:   []error{rpc.ErrNodeUnreachable},
	}
	retryer := rpc.NewRetryer(config)

	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	err := retryer.Do(ctx, func() error {
		attempts++
		return rpc.ErrNodeUnreachable
	})

	// Should have stopped before all retries due to context cancellation
	if attempts >= 10 {
		t.Errorf("expected fewer attempts due to context cancellation, got %d", attempts)
	}

	if !errors.Is(err, rpc.ErrNodeUnreachable) && !errors.Is(err, context.Canceled) {
		t.Errorf("expected ErrNodeUnreachable or context.Canceled, got %v", err)
	}
}

func TestRetryerWithResult(t *testing.T) {
	retryer := rpc.NewRetryer(&rpc.RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
		RetryableErrors:   []error{rpc.ErrNodeUnreachable},
	})

	attempts := 0
	result, err := retryer.DoWithResult(context.Background(), func() (any, error) {
		attempts++
		if attempts < 2 {
			return nil, rpc.ErrNodeUnreachable
		}
		return "success", nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if result != "success" {
		t.Errorf("expected 'success', got %v", result)
	}

	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestRetryableOperationWithCircuitBreaker(t *testing.T) {
	cb := rpc.NewCircuitBreaker(&rpc.CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     1 * time.Second,
	})
	retryer := rpc.NewRetryer(&rpc.RetryConfig{
		MaxRetries:        1,
		InitialBackoff:    10 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		BackoffMultiplier: 2.0,
		RetryableErrors:   []error{rpc.ErrNodeUnreachable},
	})

	op := rpc.NewRetryableOperation(retryer, cb, "test-addr")

	// First few failures should be retried
	err := op.Execute(context.Background(), func() error {
		return rpc.ErrNodeUnreachable
	})
	if !errors.Is(err, rpc.ErrNodeUnreachable) {
		t.Errorf("expected ErrNodeUnreachable, got %v", err)
	}

	// After enough failures, circuit should open
	err = op.Execute(context.Background(), func() error {
		return rpc.ErrNodeUnreachable
	})

	// Circuit should now be open
	if cb.State() != rpc.StateOpen {
		t.Errorf("expected circuit to be open after failures")
	}

	// Next request should fail with circuit open error
	err = op.Execute(context.Background(), func() error {
		return nil
	})
	if !errors.Is(err, rpc.ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestBackoffCalculation(t *testing.T) {
	config := &rpc.RetryConfig{
		MaxRetries:        5,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
		Jitter:            0, // No jitter for predictable testing
		RetryableErrors:   []error{rpc.ErrNodeUnreachable},
	}
	retryer := rpc.NewRetryer(config)

	start := time.Now()
	attempts := 0
	_ = retryer.Do(context.Background(), func() error {
		attempts++
		if attempts <= 3 {
			return rpc.ErrNodeUnreachable
		}
		return nil
	})
	elapsed := time.Since(start)

	// Expected backoff: 100ms + 200ms + 400ms = 700ms (approximately)
	// Allow some tolerance
	if elapsed < 600*time.Millisecond || elapsed > 900*time.Millisecond {
		t.Errorf("expected elapsed time around 700ms, got %v", elapsed)
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{rpc.ErrNodeUnreachable, true},
		{rpc.ErrRequestTimeout, true},
		{rpc.ErrCircuitOpen, true},
		{rpc.ErrServerError, false},
		{rpc.ErrInvalidResponse, false},
		{errors.New("random error"), false},
	}

	for _, tt := range tests {
		if got := rpc.IsRetryableError(tt.err); got != tt.expected {
			t.Errorf("IsRetryableError(%v) = %v, want %v", tt.err, got, tt.expected)
		}
	}
}

func TestIsCircuitBreakerError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{rpc.ErrCircuitOpen, true},
		{rpc.ErrNodeUnreachable, false},
		{rpc.ErrServerError, false},
	}

	for _, tt := range tests {
		if got := rpc.IsCircuitBreakerError(tt.err); got != tt.expected {
			t.Errorf("IsCircuitBreakerError(%v) = %v, want %v", tt.err, got, tt.expected)
		}
	}
}
