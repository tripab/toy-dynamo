package rpc

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// RetryConfig holds configuration for retry behavior
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts (0 means no retries)
	MaxRetries int
	// InitialBackoff is the initial delay before the first retry
	InitialBackoff time.Duration
	// MaxBackoff is the maximum delay between retries
	MaxBackoff time.Duration
	// BackoffMultiplier is the factor by which the backoff increases each retry
	BackoffMultiplier float64
	// Jitter adds randomness to the backoff to prevent thundering herd
	Jitter float64
	// RetryableErrors is a list of errors that should trigger a retry
	// If nil, all errors are considered retryable
	RetryableErrors []error
}

// DefaultRetryConfig returns sensible default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 2.0,
		Jitter:            0.2, // 20% jitter
		RetryableErrors: []error{
			ErrNodeUnreachable,
			ErrRequestTimeout,
		},
	}
}

// Retryer handles retry logic with exponential backoff
type Retryer struct {
	config *RetryConfig
}

// NewRetryer creates a new retryer with the given configuration
func NewRetryer(config *RetryConfig) *Retryer {
	if config == nil {
		config = DefaultRetryConfig()
	}
	return &Retryer{config: config}
}

// Do executes the given operation with retry logic
// The operation should return nil on success or an error on failure
func (r *Retryer) Do(ctx context.Context, operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		// Check context before attempting
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}

		// Execute the operation
		err := operation()
		if err == nil {
			return nil // Success
		}

		lastErr = err

		// Check if error is retryable
		if !r.isRetryable(err) {
			return err
		}

		// Don't sleep after the last attempt
		if attempt < r.config.MaxRetries {
			backoff := r.calculateBackoff(attempt)

			select {
			case <-ctx.Done():
				return lastErr
			case <-time.After(backoff):
				// Continue to next attempt
			}
		}
	}

	return lastErr
}

// DoWithResult executes the given operation with retry logic and returns a result
func (r *Retryer) DoWithResult(ctx context.Context, operation func() (any, error)) (any, error) {
	var lastErr error
	var lastResult any

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		// Check context before attempting
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastResult, lastErr
			}
			return nil, err
		}

		// Execute the operation
		result, err := operation()
		if err == nil {
			return result, nil // Success
		}

		lastErr = err
		lastResult = result

		// Check if error is retryable
		if !r.isRetryable(err) {
			return result, err
		}

		// Don't sleep after the last attempt
		if attempt < r.config.MaxRetries {
			backoff := r.calculateBackoff(attempt)

			select {
			case <-ctx.Done():
				return lastResult, lastErr
			case <-time.After(backoff):
				// Continue to next attempt
			}
		}
	}

	return lastResult, lastErr
}

// isRetryable checks if an error should trigger a retry
func (r *Retryer) isRetryable(err error) bool {
	if len(r.config.RetryableErrors) == 0 {
		return true // All errors are retryable
	}

	for _, retryableErr := range r.config.RetryableErrors {
		if errors.Is(err, retryableErr) {
			return true
		}
	}
	return false
}

// calculateBackoff computes the backoff duration for the given attempt
func (r *Retryer) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: initialBackoff * (multiplier ^ attempt)
	backoff := float64(r.config.InitialBackoff) * math.Pow(r.config.BackoffMultiplier, float64(attempt))

	// Apply jitter: backoff * (1 +/- jitter)
	if r.config.Jitter > 0 {
		jitterRange := backoff * r.config.Jitter
		backoff = backoff + (rand.Float64()*2-1)*jitterRange
	}

	// Cap at max backoff
	if backoff > float64(r.config.MaxBackoff) {
		backoff = float64(r.config.MaxBackoff)
	}

	// Ensure non-negative
	if backoff < 0 {
		backoff = 0
	}

	return time.Duration(backoff)
}

// RetryableOperation wraps an operation to make it retryable
type RetryableOperation struct {
	retryer         *Retryer
	circuitBreaker  *CircuitBreaker
	address         string
}

// NewRetryableOperation creates a new retryable operation
func NewRetryableOperation(retryer *Retryer, circuitBreaker *CircuitBreaker, address string) *RetryableOperation {
	return &RetryableOperation{
		retryer:        retryer,
		circuitBreaker: circuitBreaker,
		address:        address,
	}
}

// Execute runs the operation with circuit breaker and retry logic
func (ro *RetryableOperation) Execute(ctx context.Context, operation func() error) error {
	// Check circuit breaker first
	if ro.circuitBreaker != nil && !ro.circuitBreaker.Allow() {
		return ErrCircuitOpen
	}

	err := ro.retryer.Do(ctx, operation)

	// Update circuit breaker based on result
	if ro.circuitBreaker != nil {
		if err != nil {
			ro.circuitBreaker.RecordFailure()
		} else {
			ro.circuitBreaker.RecordSuccess()
		}
	}

	return err
}

// ExecuteWithResult runs the operation with circuit breaker and retry logic, returning a result
func (ro *RetryableOperation) ExecuteWithResult(ctx context.Context, operation func() (any, error)) (any, error) {
	// Check circuit breaker first
	if ro.circuitBreaker != nil && !ro.circuitBreaker.Allow() {
		return nil, ErrCircuitOpen
	}

	result, err := ro.retryer.DoWithResult(ctx, operation)

	// Update circuit breaker based on result
	if ro.circuitBreaker != nil {
		if err != nil {
			ro.circuitBreaker.RecordFailure()
		} else {
			ro.circuitBreaker.RecordSuccess()
		}
	}

	return result, err
}

// ErrCircuitOpen is returned when the circuit breaker is open
var ErrCircuitOpen = errors.New("circuit breaker is open")

// ErrMaxRetriesExceeded is returned when all retry attempts have been exhausted
var ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")

// IsRetryableError returns true if the error should trigger a retry
func IsRetryableError(err error) bool {
	return errors.Is(err, ErrNodeUnreachable) ||
		errors.Is(err, ErrRequestTimeout) ||
		errors.Is(err, ErrCircuitOpen)
}

// IsCircuitBreakerError returns true if the error is related to circuit breaker
func IsCircuitBreakerError(err error) bool {
	return errors.Is(err, ErrCircuitOpen)
}
