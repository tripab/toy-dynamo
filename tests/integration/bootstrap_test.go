package integration

import (
	"strings"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
)

// bootstrapConfig returns a config with background features disabled,
// suitable for lifecycle-focused tests.
func bootstrapConfig() *dynamo.Config {
	config := dynamo.DefaultConfig()
	config.N = 1
	config.R = 1
	config.W = 1
	config.AdmissionControlEnabled = false
	config.CoordinatorSelectionEnabled = false
	config.MetricsEnabled = false
	config.EnableCircuitBreaker = false
	config.EnableRetry = false
	return config
}

// TestStartReportsPortConflict verifies that Start returns an error when the
// RPC port is already in use, instead of silently running without a server.
func TestStartReportsPortConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := bootstrapConfig()

	node1, err := dynamo.NewNode("boot-node1", "localhost:9950", config)
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	defer node1.Stop()
	if err := node1.Start(); err != nil {
		t.Fatalf("Failed to start node1: %v", err)
	}

	node2, err := dynamo.NewNode("boot-node2", "localhost:9950", config)
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	defer node2.Stop()
	if err := node2.Start(); err == nil {
		t.Fatal("Start on an in-use port succeeded; expected an error")
	} else {
		t.Logf("Start correctly reported port conflict: %v", err)
	}
}

// TestJoinRetriesUntilSeedIsUp verifies that Join keeps retrying while the
// seed node is still starting, rather than failing on the first attempt.
// RPC-level retry is disabled to prove Join's own retry loop does the work.
func TestJoinRetriesUntilSeedIsUp(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := bootstrapConfig()
	config.JoinRetryAttempts = 40
	config.JoinRetryBackoff = 50 * time.Millisecond

	seed, err := dynamo.NewNode("boot-seed", "localhost:9951", config)
	if err != nil {
		t.Fatalf("Failed to create seed: %v", err)
	}
	joiner, err := dynamo.NewNode("boot-joiner", "localhost:9952", config)
	if err != nil {
		t.Fatalf("Failed to create joiner: %v", err)
	}
	defer joiner.Stop()
	if err := joiner.Start(); err != nil {
		t.Fatalf("Failed to start joiner: %v", err)
	}

	// Start the seed only after the joiner has begun its join attempts.
	seedStarted := make(chan error, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		seedStarted <- seed.Start()
	}()
	defer func() {
		<-seedStarted
		seed.Stop()
	}()

	if err := joiner.Join([]string{"localhost:9951"}); err != nil {
		t.Fatalf("Join failed despite retries: %v", err)
	}
	t.Log("Join succeeded after seed came up")
}

// TestJoinFailsWithUnderlyingError verifies that an exhausted join reports
// the underlying seed error instead of discarding it.
func TestJoinFailsWithUnderlyingError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := bootstrapConfig()
	config.JoinRetryAttempts = 1

	joiner, err := dynamo.NewNode("boot-lonely", "localhost:9953", config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer joiner.Stop()
	if err := joiner.Start(); err != nil {
		t.Fatalf("Failed to start node: %v", err)
	}

	// Nothing listens on this port.
	err = joiner.Join([]string{"localhost:9954"})
	if err == nil {
		t.Fatal("Join to an unreachable seed succeeded; expected an error")
	}
	if !strings.Contains(err.Error(), "no seed reachable") {
		t.Errorf("Join error missing 'no seed reachable': %v", err)
	}
	if !strings.Contains(err.Error(), "failed to sync with seed") {
		t.Errorf("Join error missing underlying seed error: %v", err)
	}
	t.Logf("Join failed with informative error: %v", err)
}
