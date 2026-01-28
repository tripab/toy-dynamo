//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/membership"
)

// TestFailureDetection verifies that the failure detector correctly identifies
// failed nodes and transitions them through Alive -> Suspected -> Dead states
func TestFailureDetection(t *testing.T) {
	// Create a 3-node cluster with short timeouts for testing
	config1 := &dynamo.Config{
		N:                     3,
		R:                     2,
		W:                     2,
		VirtualNodes:          10,
		StorageEngine:         "memory",
		RequestTimeout:        500 * time.Millisecond,
		GossipInterval:        100 * time.Millisecond,
		AntiEntropyInterval:   1 * time.Hour, // Disable for this test
		HintedHandoffEnabled:  true,
		VectorClockMaxSize:    10,
		ReadRepairEnabled:     true,
		TombstoneTTL:          7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
	}

	config2 := *config1
	config3 := *config1

	node1, err := dynamo.NewNode("node1", "localhost:8001", config1)
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}

	node2, err := dynamo.NewNode("node2", "localhost:8002", &config2)
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}

	node3, err := dynamo.NewNode("node3", "localhost:8003", &config3)
	if err != nil {
		t.Fatalf("Failed to create node3: %v", err)
	}

	// Start all nodes
	if err := node1.Start(); err != nil {
		t.Fatalf("Failed to start node1: %v", err)
	}
	defer node1.Stop()

	if err := node2.Start(); err != nil {
		t.Fatalf("Failed to start node2: %v", err)
	}
	// Don't defer node2.Stop() - we'll stop it explicitly for testing

	if err := node3.Start(); err != nil {
		t.Fatalf("Failed to start node3: %v", err)
	}
	defer node3.Stop()

	// Join nodes 2 and 3 to node 1
	if err := node2.Join([]string{"localhost:8001"}); err != nil {
		t.Fatalf("Node2 failed to join: %v", err)
	}
	if err := node3.Join([]string{"localhost:8001"}); err != nil {
		t.Fatalf("Node3 failed to join: %v", err)
	}

	// Wait for gossip to propagate
	time.Sleep(500 * time.Millisecond)

	// Verify all nodes are alive initially
	fd1 := node1.GetFailureDetector()
	aliveNodes := fd1.GetAliveNodes()
	if len(aliveNodes) < 2 {
		t.Errorf("Expected at least 2 alive nodes, got %d: %v", len(aliveNodes), aliveNodes)
	}

	// Set up a callback to track status changes
	var statusChanges []struct {
		nodeID    string
		oldStatus membership.MemberStatus
		newStatus membership.MemberStatus
	}
	var changesMu sync.Mutex

	fd1.RegisterCallback(func(nodeID string, oldStatus, newStatus membership.MemberStatus) {
		changesMu.Lock()
		defer changesMu.Unlock()
		statusChanges = append(statusChanges, struct {
			nodeID    string
			oldStatus membership.MemberStatus
			newStatus membership.MemberStatus
		}{nodeID, oldStatus, newStatus})
		t.Logf("Status change: node=%s, %d -> %d", nodeID, oldStatus, newStatus)
	})

	// Stop node2 to simulate failure
	t.Log("Stopping node2 to simulate failure...")
	if err := node2.Stop(); err != nil {
		t.Fatalf("Failed to stop node2: %v", err)
	}

	// Wait for failure detection to kick in
	// The phi accrual detector should mark node2 as suspected then dead
	// With RequestTimeout of 500ms, the failure detector timeout is 1.5s
	// We need to wait at least 3x that (4.5s) to ensure detection
	time.Sleep(5 * time.Second)

	// Check that node2 is no longer considered alive
	if fd1.IsAlive("node2") {
		t.Error("node2 should not be considered alive after being stopped")
	}

	// Check that node2 is either suspected or dead
	if !fd1.IsSuspected("node2") && !fd1.IsDead("node2") {
		t.Error("node2 should be suspected or dead after being stopped")
	}

	// Verify status change callback was triggered
	changesMu.Lock()
	hasNode2Change := false
	for _, change := range statusChanges {
		if change.nodeID == "node2" && change.newStatus != membership.StatusAlive {
			hasNode2Change = true
			break
		}
	}
	changesMu.Unlock()

	if !hasNode2Change {
		t.Error("Expected status change callback for node2")
	}
}

// TestFailureDetectionWithWrite verifies that writes route around failed nodes
func TestFailureDetectionWithWrite(t *testing.T) {
	// Create a 3-node cluster
	config := &dynamo.Config{
		N:                     3,
		R:                     1, // Low R so reads succeed with 1 node
		W:                     1, // Low W so writes succeed with 1 node
		VirtualNodes:          10,
		StorageEngine:         "memory",
		RequestTimeout:        500 * time.Millisecond,
		GossipInterval:        100 * time.Millisecond,
		AntiEntropyInterval:   1 * time.Hour,
		HintedHandoffEnabled:  true,
		VectorClockMaxSize:    10,
		ReadRepairEnabled:     false,
		TombstoneTTL:          7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
	}

	node1, err := dynamo.NewNode("node1", "localhost:9001", config)
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}

	config2 := *config
	node2, err := dynamo.NewNode("node2", "localhost:9002", &config2)
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}

	config3 := *config
	node3, err := dynamo.NewNode("node3", "localhost:9003", &config3)
	if err != nil {
		t.Fatalf("Failed to create node3: %v", err)
	}

	// Start nodes
	if err := node1.Start(); err != nil {
		t.Fatalf("Failed to start node1: %v", err)
	}
	defer node1.Stop()

	if err := node2.Start(); err != nil {
		t.Fatalf("Failed to start node2: %v", err)
	}

	if err := node3.Start(); err != nil {
		t.Fatalf("Failed to start node3: %v", err)
	}
	defer node3.Stop()

	// Join cluster
	if err := node2.Join([]string{"localhost:9001"}); err != nil {
		t.Fatalf("Node2 failed to join: %v", err)
	}
	if err := node3.Join([]string{"localhost:9001"}); err != nil {
		t.Fatalf("Node3 failed to join: %v", err)
	}

	// Wait for gossip
	time.Sleep(300 * time.Millisecond)

	// Write some data while all nodes are alive
	ctx := context.Background()
	err = node1.Put(ctx, "test-key-1", []byte("value-before-failure"), nil)
	if err != nil {
		t.Fatalf("Failed to write before failure: %v", err)
	}

	// Stop node2
	t.Log("Stopping node2...")
	if err := node2.Stop(); err != nil {
		t.Fatalf("Failed to stop node2: %v", err)
	}

	// Wait a bit for failure detection
	time.Sleep(500 * time.Millisecond)

	// Write more data - should still succeed with W=1
	err = node1.Put(ctx, "test-key-2", []byte("value-after-failure"), nil)
	if err != nil {
		t.Fatalf("Failed to write after node failure: %v", err)
	}

	// Read should also succeed with R=1
	result, err := node1.Get(ctx, "test-key-2")
	if err != nil {
		t.Fatalf("Failed to read after node failure: %v", err)
	}
	if len(result.Values) == 0 {
		t.Error("Expected to read value after node failure")
	}
}

// TestPhiAccrualCalculation verifies that phi values are calculated correctly
func TestPhiAccrualCalculation(t *testing.T) {
	// Create a simple membership for testing
	config := &dynamo.Config{
		N:                     3,
		R:                     2,
		W:                     2,
		VirtualNodes:          10,
		StorageEngine:         "memory",
		RequestTimeout:        200 * time.Millisecond,
		GossipInterval:        50 * time.Millisecond,
		AntiEntropyInterval:   1 * time.Hour,
		HintedHandoffEnabled:  false,
		VectorClockMaxSize:    10,
		ReadRepairEnabled:     false,
		TombstoneTTL:          7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
	}

	m := membership.NewMembership("local", "localhost:7001", config)
	fd := membership.NewFailureDetector(m, 200*time.Millisecond)

	// Add a test member
	testMember := &membership.Member{
		NodeID:    "test-node",
		Address:   "localhost:7002",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Timestamp: time.Now(),
	}
	m.AddMember(testMember)

	// Record several heartbeats at regular intervals
	for i := 0; i < 10; i++ {
		fd.RecordHeartbeat("test-node")
		time.Sleep(50 * time.Millisecond)
	}

	// Get phi value immediately after heartbeat - should be low
	phi := fd.GetNodePhi("test-node")
	t.Logf("Phi immediately after heartbeat: %f", phi)
	if phi > 5.0 {
		t.Errorf("Phi should be low immediately after heartbeat, got %f", phi)
	}

	// Wait without heartbeats
	time.Sleep(500 * time.Millisecond)

	// Phi should have increased
	phi2 := fd.GetNodePhi("test-node")
	t.Logf("Phi after 500ms silence: %f", phi2)
	if phi2 <= phi {
		t.Errorf("Phi should increase without heartbeats, was %f, now %f", phi, phi2)
	}
}

// TestStatusCallbacks verifies that callbacks are triggered correctly
func TestStatusCallbacks(t *testing.T) {
	config := &dynamo.Config{
		N:                     3,
		R:                     2,
		W:                     2,
		VirtualNodes:          10,
		StorageEngine:         "memory",
		RequestTimeout:        100 * time.Millisecond,
		GossipInterval:        50 * time.Millisecond,
		AntiEntropyInterval:   1 * time.Hour,
		HintedHandoffEnabled:  false,
		VectorClockMaxSize:    10,
		ReadRepairEnabled:     false,
		TombstoneTTL:          7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
	}

	m := membership.NewMembership("local", "localhost:6001", config)
	fd := membership.NewFailureDetector(m, 100*time.Millisecond)

	// Set lower phi thresholds for faster testing
	fd.SetPhiThreshold(2.0)
	fd.SetPhiDeadThreshold(4.0)

	// Track callbacks
	var callbacks []struct {
		nodeID    string
		oldStatus membership.MemberStatus
		newStatus membership.MemberStatus
	}
	var mu sync.Mutex

	fd.RegisterCallback(func(nodeID string, oldStatus, newStatus membership.MemberStatus) {
		mu.Lock()
		defer mu.Unlock()
		callbacks = append(callbacks, struct {
			nodeID    string
			oldStatus membership.MemberStatus
			newStatus membership.MemberStatus
		}{nodeID, oldStatus, newStatus})
	})

	// Add a member with old timestamp (simulating it went silent)
	oldTime := time.Now().Add(-2 * time.Second)
	testMember := &membership.Member{
		NodeID:    "silent-node",
		Address:   "localhost:6002",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Timestamp: oldTime,
	}
	m.AddMember(testMember)

	// Run failure detection
	fd.CheckFailures()

	// Check that callback was triggered
	mu.Lock()
	defer mu.Unlock()

	if len(callbacks) == 0 {
		t.Error("Expected at least one status change callback")
	}

	hasTransition := false
	for _, cb := range callbacks {
		if cb.nodeID == "silent-node" && cb.oldStatus == membership.StatusAlive {
			hasTransition = true
			t.Logf("Received callback: %s changed from %d to %d", cb.nodeID, cb.oldStatus, cb.newStatus)
		}
	}

	if !hasTransition {
		t.Error("Expected status transition for silent-node")
	}
}

// TestGetAliveDeadSuspectedNodes verifies the helper methods work correctly
func TestGetAliveDeadSuspectedNodes(t *testing.T) {
	config := &dynamo.Config{
		N:                     3,
		R:                     2,
		W:                     2,
		VirtualNodes:          10,
		StorageEngine:         "memory",
		RequestTimeout:        100 * time.Millisecond,
		GossipInterval:        50 * time.Millisecond,
		AntiEntropyInterval:   1 * time.Hour,
		HintedHandoffEnabled:  false,
		VectorClockMaxSize:    10,
		ReadRepairEnabled:     false,
		TombstoneTTL:          7 * 24 * time.Hour,
		TombstoneCompactionInterval: 1 * time.Hour,
	}

	m := membership.NewMembership("local", "localhost:5001", config)
	fd := membership.NewFailureDetector(m, 100*time.Millisecond)

	// Add local member
	localMember := &membership.Member{
		NodeID:    "local",
		Address:   "localhost:5001",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Timestamp: time.Now(),
	}
	m.AddMember(localMember)

	// Add an alive member (fresh timestamp)
	aliveMember := &membership.Member{
		NodeID:    "alive-node",
		Address:   "localhost:5002",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Timestamp: time.Now(),
	}
	m.AddMember(aliveMember)
	fd.RecordHeartbeat("alive-node")

	// Add a member that will be detected as dead (very old timestamp)
	deadMember := &membership.Member{
		NodeID:    "dead-node",
		Address:   "localhost:5003",
		Status:    membership.StatusAlive, // Will be changed by CheckFailures
		Heartbeat: 1,
		Timestamp: time.Now().Add(-10 * time.Second),
	}
	m.AddMember(deadMember)

	// Set low thresholds for testing
	fd.SetPhiThreshold(1.0)
	fd.SetPhiDeadThreshold(2.0)

	// Run failure detection
	fd.CheckFailures()

	// Check results
	aliveNodes := fd.GetAliveNodes()
	t.Logf("Alive nodes: %v", aliveNodes)

	// dead-node should not be in alive list
	for _, n := range aliveNodes {
		if n == "dead-node" {
			t.Error("dead-node should not be in alive nodes list")
		}
	}

	// Check IsDead
	if !fd.IsDead("dead-node") && !fd.IsSuspected("dead-node") {
		t.Error("dead-node should be dead or suspected")
	}

	// alive-node should be alive
	if !fd.IsAlive("alive-node") {
		deadNodes := fd.GetDeadNodes()
		suspectedNodes := fd.GetSuspectedNodes()
		t.Errorf("alive-node should be alive, dead=%v, suspected=%v", deadNodes, suspectedNodes)
	}
}
