package dynamo

import (
	"sync"
	"time"
)

// AdmissionController manages background task throttling based on foreground
// request latency. When foreground latency exceeds the threshold, background
// tasks (anti-entropy, gossip, hinted handoff) are throttled to preserve
// resources for user-facing operations.
//
// This implements the admission control pattern from Amazon's Dynamo paper
// (Section 6.2) which prioritizes foreground requests over background tasks.
type AdmissionController struct {
	// Configuration
	latencyThreshold time.Duration // p99 latency threshold for throttling
	maxSlots         int           // Maximum background work slots
	minSlots         int           // Minimum background work slots (never go below 1)
	windowSize       int           // Number of samples to keep for latency calculation

	// State
	backgroundSlots int               // Current number of allowed background tasks
	latencies       []time.Duration   // Rolling window of recent latencies
	latencyIndex    int               // Current index in rolling window
	latencyCount    int               // Number of valid samples in window

	mu sync.RWMutex
}

// AdmissionControlConfig holds configuration for the admission controller
type AdmissionControlConfig struct {
	// LatencyThreshold is the p99 latency above which background tasks are throttled
	// Default: 100ms
	LatencyThreshold time.Duration

	// MaxBackgroundSlots is the maximum number of background task "slots"
	// Higher values allow more background work when latency is healthy
	// Default: 10
	MaxBackgroundSlots int

	// MinBackgroundSlots is the minimum number of background task slots
	// This ensures some background work always happens
	// Default: 1
	MinBackgroundSlots int

	// WindowSize is the number of latency samples to keep for p99 calculation
	// Default: 1000
	WindowSize int
}

// DefaultAdmissionControlConfig returns the default admission control configuration
func DefaultAdmissionControlConfig() *AdmissionControlConfig {
	return &AdmissionControlConfig{
		LatencyThreshold:   100 * time.Millisecond,
		MaxBackgroundSlots: 10,
		MinBackgroundSlots: 1,
		WindowSize:         1000,
	}
}

// NewAdmissionController creates a new admission controller with the given config
func NewAdmissionController(config *AdmissionControlConfig) *AdmissionController {
	if config == nil {
		config = DefaultAdmissionControlConfig()
	}

	// Validate and apply defaults
	if config.LatencyThreshold <= 0 {
		config.LatencyThreshold = 100 * time.Millisecond
	}
	if config.MaxBackgroundSlots < 1 {
		config.MaxBackgroundSlots = 10
	}
	if config.MinBackgroundSlots < 1 {
		config.MinBackgroundSlots = 1
	}
	if config.MinBackgroundSlots > config.MaxBackgroundSlots {
		config.MinBackgroundSlots = config.MaxBackgroundSlots
	}
	if config.WindowSize < 10 {
		config.WindowSize = 1000
	}

	return &AdmissionController{
		latencyThreshold: config.LatencyThreshold,
		maxSlots:         config.MaxBackgroundSlots,
		minSlots:         config.MinBackgroundSlots,
		windowSize:       config.WindowSize,
		backgroundSlots:  config.MaxBackgroundSlots, // Start with max slots
		latencies:        make([]time.Duration, config.WindowSize),
	}
}

// RecordLatency records a foreground request latency measurement
func (ac *AdmissionController) RecordLatency(latency time.Duration) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// Add to rolling window
	ac.latencies[ac.latencyIndex] = latency
	ac.latencyIndex = (ac.latencyIndex + 1) % ac.windowSize
	if ac.latencyCount < ac.windowSize {
		ac.latencyCount++
	}
}

// AllowBackground checks if background work should be allowed and adjusts
// the background slot count based on current foreground latency
func (ac *AdmissionController) AllowBackground() bool {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// If we don't have enough samples, allow background work
	if ac.latencyCount < 10 {
		return true
	}

	// Calculate p99 latency
	p99 := ac.calculateP99Locked()

	// Adjust slots based on latency
	if p99 > ac.latencyThreshold {
		// Foreground under stress, reduce background work
		ac.backgroundSlots = max(ac.minSlots, ac.backgroundSlots-1)
	} else {
		// Foreground healthy, allow more background work
		ac.backgroundSlots = min(ac.maxSlots, ac.backgroundSlots+1)
	}

	return ac.backgroundSlots > 0
}

// ShouldThrottle returns true if background tasks should be throttled
// This is a read-only check that doesn't modify state
func (ac *AdmissionController) ShouldThrottle() bool {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	if ac.latencyCount < 10 {
		return false
	}

	p99 := ac.calculateP99Locked()
	return p99 > ac.latencyThreshold
}

// GetP99Latency returns the current p99 latency estimate
func (ac *AdmissionController) GetP99Latency() time.Duration {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	if ac.latencyCount == 0 {
		return 0
	}

	return ac.calculateP99Locked()
}

// GetBackgroundSlots returns the current number of background slots
func (ac *AdmissionController) GetBackgroundSlots() int {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.backgroundSlots
}

// GetStats returns admission control statistics
func (ac *AdmissionController) GetStats() AdmissionStats {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	var p99 time.Duration
	if ac.latencyCount > 0 {
		p99 = ac.calculateP99Locked()
	}

	return AdmissionStats{
		P99Latency:      p99,
		BackgroundSlots: ac.backgroundSlots,
		SampleCount:     ac.latencyCount,
		Throttling:      p99 > ac.latencyThreshold,
		Threshold:       ac.latencyThreshold,
	}
}

// AdmissionStats contains statistics about the admission controller state
type AdmissionStats struct {
	P99Latency      time.Duration // Current p99 latency
	BackgroundSlots int           // Current background slot count
	SampleCount     int           // Number of latency samples collected
	Throttling      bool          // Whether background tasks are being throttled
	Threshold       time.Duration // The latency threshold for throttling
}

// calculateP99Locked calculates the p99 latency from the rolling window
// Caller must hold the lock
func (ac *AdmissionController) calculateP99Locked() time.Duration {
	if ac.latencyCount == 0 {
		return 0
	}

	// Copy valid samples for sorting
	samples := make([]time.Duration, ac.latencyCount)
	copy(samples, ac.latencies[:ac.latencyCount])

	// Sort samples
	sortDurations(samples)

	// Calculate p99 index (99th percentile)
	p99Index := int(float64(len(samples)-1) * 0.99)
	return samples[p99Index]
}

// sortDurations sorts a slice of durations in ascending order using insertion sort
// Insertion sort is efficient for small arrays and nearly-sorted data
func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		key := d[i]
		j := i - 1
		for j >= 0 && d[j] > key {
			d[j+1] = d[j]
			j--
		}
		d[j+1] = key
	}
}
