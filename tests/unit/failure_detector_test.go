package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/membership"
)

func newTestFailureDetector() (*membership.FailureDetector, *membership.Membership) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	fd := membership.NewFailureDetector(m, 1*time.Second)
	return fd, m
}

func TestFailureDetectorCreation(t *testing.T) {
	fd, _ := newTestFailureDetector()

	if fd.GetPhiThreshold() != 8.0 {
		t.Errorf("Expected default phi threshold 8.0, got %f", fd.GetPhiThreshold())
	}
	if fd.GetPhiDeadThreshold() != 16.0 {
		t.Errorf("Expected default phi dead threshold 16.0, got %f", fd.GetPhiDeadThreshold())
	}
}

func TestFailureDetectorRecordHeartbeat(t *testing.T) {
	fd, _ := newTestFailureDetector()

	// Record several heartbeats with small intervals
	for i := 0; i < 5; i++ {
		fd.RecordHeartbeat("node2")
		time.Sleep(10 * time.Millisecond)
	}

	// Node2 should be considered alive after recent heartbeats
	if !fd.IsAlive("node2") {
		t.Error("node2 should be alive after recent heartbeats")
	}
}

func TestFailureDetectorIsAlive_NewNode(t *testing.T) {
	fd, _ := newTestFailureDetector()

	// A node with no history and not in suspects/dead should be alive
	if !fd.IsAlive("unknown-node") {
		t.Error("Unknown node with no failure history should be considered alive")
	}
}

func TestFailureDetectorCheckFailures_AllAlive(t *testing.T) {
	fd, m := newTestFailureDetector()

	// Add node2 with recent timestamp
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Record heartbeats so phi calculation has data
	for i := 0; i < 5; i++ {
		fd.RecordHeartbeat("node2")
		time.Sleep(10 * time.Millisecond)
	}

	fd.CheckFailures()

	if !fd.IsAlive("node2") {
		t.Error("node2 should still be alive after recent heartbeats")
	}
}

func TestFailureDetectorCheckFailures_SuspectedAfterTimeout(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)
	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Use a very short timeout so nodes become suspected quickly
	fd := membership.NewFailureDetector(m, 50*time.Millisecond)

	// Add node2 with old timestamp (simulates no heartbeat for a while)
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now().Add(-500 * time.Millisecond),
	})

	// Wait enough for the timeout to trigger
	time.Sleep(100 * time.Millisecond)

	fd.CheckFailures()

	if fd.IsAlive("node2") {
		t.Error("node2 should be suspected or dead after timeout")
	}
}

func TestFailureDetectorCheckFailures_DeadAfterLongTimeout(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)
	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	fd := membership.NewFailureDetector(m, 50*time.Millisecond)

	// Add node2 with very old timestamp
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now().Add(-5 * time.Second),
	})

	fd.CheckFailures()

	if !fd.IsDead("node2") {
		t.Error("node2 should be dead after very long timeout")
	}
}

func TestFailureDetectorStatusCallbacks(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)
	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	fd := membership.NewFailureDetector(m, 50*time.Millisecond)

	var mu sync.Mutex
	var callbacks []struct {
		nodeID    string
		oldStatus membership.MemberStatus
		newStatus membership.MemberStatus
	}

	fd.RegisterCallback(func(nodeID string, oldStatus, newStatus membership.MemberStatus) {
		mu.Lock()
		defer mu.Unlock()
		callbacks = append(callbacks, struct {
			nodeID    string
			oldStatus membership.MemberStatus
			newStatus membership.MemberStatus
		}{nodeID, oldStatus, newStatus})
	})

	// Add node2 with old timestamp to trigger status change
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now().Add(-5 * time.Second),
	})

	fd.CheckFailures()

	mu.Lock()
	callbackCount := len(callbacks)
	mu.Unlock()

	if callbackCount == 0 {
		t.Error("Expected at least one status change callback")
	}
}

func TestFailureDetectorRecovery(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)
	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	fd := membership.NewFailureDetector(m, 50*time.Millisecond)

	// Add node2 with old timestamp - will be marked dead
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now().Add(-5 * time.Second),
	})

	fd.CheckFailures()

	if !fd.IsDead("node2") {
		t.Fatal("node2 should be dead before recovery")
	}

	// Simulate recovery: update timestamp and record heartbeats.
	// Keep Status as StatusDead to match the failure detector's tracking state;
	// CheckFailures will transition it back to Alive when phi drops.
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusDead,
		Heartbeat: 10, Timestamp: time.Now(),
	})

	// Record fresh heartbeats
	for i := 0; i < 5; i++ {
		fd.RecordHeartbeat("node2")
		time.Sleep(10 * time.Millisecond)
	}

	fd.CheckFailures()

	if fd.IsDead("node2") {
		t.Error("node2 should have recovered after fresh heartbeats")
	}
}

func TestFailureDetectorGetNodes(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)
	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	fd := membership.NewFailureDetector(m, 50*time.Millisecond)

	// Add alive node
	m.AddMember(&membership.Member{
		NodeID: "alive-node", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})
	for i := 0; i < 3; i++ {
		fd.RecordHeartbeat("alive-node")
		time.Sleep(10 * time.Millisecond)
	}

	// Add dead node
	m.AddMember(&membership.Member{
		NodeID: "dead-node", Address: "localhost:8002", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now().Add(-5 * time.Second),
	})

	fd.CheckFailures()

	dead := fd.GetDeadNodes()
	alive := fd.GetAliveNodes()

	foundDead := false
	for _, id := range dead {
		if id == "dead-node" {
			foundDead = true
		}
	}
	if !foundDead {
		t.Error("Expected dead-node in dead nodes list")
	}

	foundAlive := false
	for _, id := range alive {
		if id == "node1" || id == "alive-node" {
			foundAlive = true
		}
	}
	if !foundAlive {
		t.Error("Expected at least one alive node")
	}
}

func TestFailureDetectorPhiThresholds(t *testing.T) {
	fd, _ := newTestFailureDetector()

	fd.SetPhiThreshold(5.0)
	if fd.GetPhiThreshold() != 5.0 {
		t.Errorf("Expected phi threshold 5.0, got %f", fd.GetPhiThreshold())
	}

	fd.SetPhiDeadThreshold(12.0)
	if fd.GetPhiDeadThreshold() != 12.0 {
		t.Errorf("Expected phi dead threshold 12.0, got %f", fd.GetPhiDeadThreshold())
	}
}

func TestFailureDetectorCheckFailures_SkipsSelf(t *testing.T) {
	fd, m := newTestFailureDetector()

	// Set node1 (self) timestamp to old - should NOT be marked dead
	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now().Add(-10 * time.Second),
	})

	fd.CheckFailures()

	// Self should never be marked as suspected or dead
	if fd.IsSuspected("node1") {
		t.Error("Local node should not be marked as suspected")
	}
	if fd.IsDead("node1") {
		t.Error("Local node should not be marked as dead")
	}
}

func TestFailureDetectorGetNodePhi(t *testing.T) {
	fd, m := newTestFailureDetector()

	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Record heartbeats
	for i := 0; i < 5; i++ {
		fd.RecordHeartbeat("node2")
		time.Sleep(10 * time.Millisecond)
	}

	phi := fd.GetNodePhi("node2")

	// Phi should be low after recent heartbeats
	if phi > fd.GetPhiThreshold() {
		t.Errorf("Expected low phi after recent heartbeats, got %f", phi)
	}
}

func TestFailureDetectorIsSuspected(t *testing.T) {
	fd, _ := newTestFailureDetector()

	// Initially no node is suspected
	if fd.IsSuspected("node2") {
		t.Error("node2 should not be suspected initially")
	}

	suspected := fd.GetSuspectedNodes()
	if len(suspected) != 0 {
		t.Errorf("Expected 0 suspected nodes, got %d", len(suspected))
	}
}

func TestFailureDetectorMultipleCallbacks(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)
	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	fd := membership.NewFailureDetector(m, 50*time.Millisecond)

	var mu sync.Mutex
	count1 := 0
	count2 := 0

	fd.RegisterCallback(func(nodeID string, old, new membership.MemberStatus) {
		mu.Lock()
		count1++
		mu.Unlock()
	})
	fd.RegisterCallback(func(nodeID string, old, new membership.MemberStatus) {
		mu.Lock()
		count2++
		mu.Unlock()
	})

	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now().Add(-5 * time.Second),
	})

	fd.CheckFailures()

	mu.Lock()
	defer mu.Unlock()

	if count1 == 0 || count2 == 0 {
		t.Error("Both callbacks should have been triggered")
	}
	if count1 != count2 {
		t.Errorf("Both callbacks should have same count: %d vs %d", count1, count2)
	}
}
