package tests

import (
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

func TestAdmissionController_NewWithDefaults(t *testing.T) {
	ac := dynamo.NewAdmissionController(nil)

	// Should start with max slots
	slots := ac.GetBackgroundSlots()
	if slots != 10 {
		t.Errorf("Expected 10 initial slots, got %d", slots)
	}

	// Should have 0 p99 with no samples
	p99 := ac.GetP99Latency()
	if p99 != 0 {
		t.Errorf("Expected 0 p99 with no samples, got %v", p99)
	}
}

func TestAdmissionController_NewWithConfig(t *testing.T) {
	config := &dynamo.AdmissionControlConfig{
		LatencyThreshold:   50 * time.Millisecond,
		MaxBackgroundSlots: 5,
		MinBackgroundSlots: 2,
		WindowSize:         100,
	}

	ac := dynamo.NewAdmissionController(config)

	slots := ac.GetBackgroundSlots()
	if slots != 5 {
		t.Errorf("Expected 5 initial slots, got %d", slots)
	}
}

func TestAdmissionController_RecordLatency(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		WindowSize: 100,
	})

	// Record some latencies
	for i := 0; i < 50; i++ {
		ac.RecordLatency(time.Duration(i) * time.Millisecond)
	}

	p99 := ac.GetP99Latency()
	if p99 == 0 {
		t.Error("Expected non-zero p99 after recording latencies")
	}

	// p99 of 0-49ms should be around 48-49ms
	if p99 < 45*time.Millisecond || p99 > 50*time.Millisecond {
		t.Errorf("Expected p99 around 48-49ms, got %v", p99)
	}
}

func TestAdmissionController_AllowBackgroundWhenHealthy(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		LatencyThreshold:   100 * time.Millisecond,
		MaxBackgroundSlots: 10,
		MinBackgroundSlots: 1,
		WindowSize:         100,
	})

	// Record healthy latencies (well below threshold)
	for i := 0; i < 50; i++ {
		ac.RecordLatency(10 * time.Millisecond)
	}

	// Should allow background work
	allowed := ac.AllowBackground()
	if !allowed {
		t.Error("Expected background work to be allowed with healthy latencies")
	}

	// Slots should increase toward max
	slots := ac.GetBackgroundSlots()
	if slots < 10 {
		t.Errorf("Expected slots to be at max (10), got %d", slots)
	}
}

func TestAdmissionController_ThrottleWhenLatencyHigh(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		LatencyThreshold:   50 * time.Millisecond,
		MaxBackgroundSlots: 10,
		MinBackgroundSlots: 1,
		WindowSize:         100,
	})

	// Record high latencies (above threshold)
	for i := 0; i < 100; i++ {
		ac.RecordLatency(200 * time.Millisecond)
	}

	// Call AllowBackground multiple times to reduce slots
	for i := 0; i < 15; i++ {
		ac.AllowBackground()
	}

	// Should be throttling
	if !ac.ShouldThrottle() {
		t.Error("Expected throttling with high latencies")
	}

	// Slots should be at minimum
	slots := ac.GetBackgroundSlots()
	if slots != 1 {
		t.Errorf("Expected slots to be at minimum (1), got %d", slots)
	}
}

func TestAdmissionController_SlotRecovery(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		LatencyThreshold:   100 * time.Millisecond,
		MaxBackgroundSlots: 10,
		MinBackgroundSlots: 1,
		WindowSize:         100,
	})

	// First, create high latency to reduce slots
	for i := 0; i < 100; i++ {
		ac.RecordLatency(200 * time.Millisecond)
	}
	for i := 0; i < 15; i++ {
		ac.AllowBackground()
	}

	initialSlots := ac.GetBackgroundSlots()
	if initialSlots != 1 {
		t.Errorf("Expected slots to start at 1, got %d", initialSlots)
	}

	// Now record healthy latencies
	for i := 0; i < 100; i++ {
		ac.RecordLatency(10 * time.Millisecond)
	}

	// Call AllowBackground to recover slots
	for i := 0; i < 15; i++ {
		ac.AllowBackground()
	}

	// Slots should have recovered
	recoveredSlots := ac.GetBackgroundSlots()
	if recoveredSlots <= initialSlots {
		t.Errorf("Expected slots to recover, initial=%d, recovered=%d", initialSlots, recoveredSlots)
	}
}

func TestAdmissionController_MinSlotsRespected(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		LatencyThreshold:   10 * time.Millisecond, // Very low threshold
		MaxBackgroundSlots: 10,
		MinBackgroundSlots: 3, // Min slots = 3
		WindowSize:         100,
	})

	// Record high latencies
	for i := 0; i < 100; i++ {
		ac.RecordLatency(500 * time.Millisecond)
	}

	// Try to reduce slots many times
	for i := 0; i < 50; i++ {
		ac.AllowBackground()
	}

	// Should never go below minimum
	slots := ac.GetBackgroundSlots()
	if slots < 3 {
		t.Errorf("Expected slots to be at least 3 (min), got %d", slots)
	}
}

func TestAdmissionController_MaxSlotsRespected(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		LatencyThreshold:   100 * time.Millisecond,
		MaxBackgroundSlots: 5, // Max slots = 5
		MinBackgroundSlots: 1,
		WindowSize:         100,
	})

	// Record healthy latencies
	for i := 0; i < 100; i++ {
		ac.RecordLatency(10 * time.Millisecond)
	}

	// Try to increase slots many times
	for i := 0; i < 50; i++ {
		ac.AllowBackground()
	}

	// Should never exceed maximum
	slots := ac.GetBackgroundSlots()
	if slots > 5 {
		t.Errorf("Expected slots to be at most 5 (max), got %d", slots)
	}
}

func TestAdmissionController_GetStats(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		LatencyThreshold:   100 * time.Millisecond,
		MaxBackgroundSlots: 10,
		MinBackgroundSlots: 1,
		WindowSize:         100,
	})

	// Record some latencies
	for i := 0; i < 50; i++ {
		ac.RecordLatency(time.Duration(50+i) * time.Millisecond)
	}

	stats := ac.GetStats()

	if stats.SampleCount != 50 {
		t.Errorf("Expected 50 samples, got %d", stats.SampleCount)
	}

	if stats.Threshold != 100*time.Millisecond {
		t.Errorf("Expected threshold 100ms, got %v", stats.Threshold)
	}

	// p99 of 50-99ms should be around 98ms
	if stats.P99Latency < 90*time.Millisecond || stats.P99Latency > 100*time.Millisecond {
		t.Errorf("Expected p99 around 98ms, got %v", stats.P99Latency)
	}
}

func TestAdmissionController_AllowBackgroundWithFewSamples(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		WindowSize: 100,
	})

	// With only a few samples, should always allow background
	for i := 0; i < 5; i++ {
		ac.RecordLatency(500 * time.Millisecond) // High latency
	}

	// Should still allow because not enough samples
	if !ac.AllowBackground() {
		t.Error("Expected background to be allowed with few samples")
	}
}

func TestAdmissionController_RollingWindow(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		LatencyThreshold:   100 * time.Millisecond,
		MaxBackgroundSlots: 10,
		MinBackgroundSlots: 1,
		WindowSize:         50, // Small window
	})

	// Fill window with high latencies
	for i := 0; i < 50; i++ {
		ac.RecordLatency(200 * time.Millisecond)
	}

	// Verify high p99
	stats := ac.GetStats()
	if stats.P99Latency < 150*time.Millisecond {
		t.Errorf("Expected high p99 with high latencies, got %v", stats.P99Latency)
	}

	// Now overwrite with low latencies
	for i := 0; i < 50; i++ {
		ac.RecordLatency(10 * time.Millisecond)
	}

	// p99 should now be low
	stats = ac.GetStats()
	if stats.P99Latency > 50*time.Millisecond {
		t.Errorf("Expected low p99 after overwrite, got %v", stats.P99Latency)
	}
}

func TestAdmissionController_ShouldThrottleReadOnly(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		LatencyThreshold:   50 * time.Millisecond,
		MaxBackgroundSlots: 10,
		MinBackgroundSlots: 1,
		WindowSize:         100,
	})

	// Record high latencies
	for i := 0; i < 100; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}

	initialSlots := ac.GetBackgroundSlots()

	// ShouldThrottle is read-only and shouldn't change state
	throttled := ac.ShouldThrottle()
	if !throttled {
		t.Error("Expected ShouldThrottle to return true with high latency")
	}

	// Slots should not have changed
	if ac.GetBackgroundSlots() != initialSlots {
		t.Error("ShouldThrottle should not modify slot count")
	}
}

func TestAdmissionController_ConcurrentAccess(t *testing.T) {
	ac := dynamo.NewAdmissionController(&dynamo.AdmissionControlConfig{
		WindowSize: 1000,
	})

	// Run concurrent goroutines accessing the controller
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			ac.RecordLatency(time.Duration(i%100) * time.Millisecond)
		}
		done <- true
	}()

	// Reader goroutine 1
	go func() {
		for i := 0; i < 1000; i++ {
			_ = ac.GetP99Latency()
		}
		done <- true
	}()

	// Reader goroutine 2
	go func() {
		for i := 0; i < 1000; i++ {
			_ = ac.AllowBackground()
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 3; i++ {
		<-done
	}

	// If we get here without a race condition panic, the test passes
}
