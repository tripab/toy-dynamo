package tests

import (
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/rpc"
)

func TestMembershipAddAndGet(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	member := &membership.Member{
		NodeID:    "node2",
		Address:   "localhost:8001",
		Status:    membership.StatusAlive,
		Heartbeat: 1,
		Tokens:    []uint32{100, 200, 300},
		Timestamp: time.Now(),
	}

	m.AddMember(member)

	got := m.GetMember("node2")
	if got == nil {
		t.Fatal("Expected to find node2")
	}

	if got.NodeID != "node2" {
		t.Errorf("Expected NodeID 'node2', got '%s'", got.NodeID)
	}
	if got.Address != "localhost:8001" {
		t.Errorf("Expected Address 'localhost:8001', got '%s'", got.Address)
	}
	if got.Heartbeat != 1 {
		t.Errorf("Expected Heartbeat 1, got %d", got.Heartbeat)
	}
	if len(got.Tokens) != 3 {
		t.Errorf("Expected 3 tokens, got %d", len(got.Tokens))
	}
}

func TestMembershipGetMember_NotFound(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	got := m.GetMember("nonexistent")
	if got != nil {
		t.Error("Expected nil for non-existent member")
	}
}

func TestMembershipGetAllMembers(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})
	m.AddMember(&membership.Member{
		NodeID: "node3", Address: "localhost:8002", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	all := m.GetAllMembers()
	if len(all) != 3 {
		t.Errorf("Expected 3 members, got %d", len(all))
	}
}

func TestMembershipAddMember_UpdateExisting(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Update with higher heartbeat
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 5, Timestamp: time.Now(),
	})

	got := m.GetMember("node2")
	if got.Heartbeat != 5 {
		t.Errorf("Expected updated heartbeat 5, got %d", got.Heartbeat)
	}

	// Should still have just 1 member entry
	all := m.GetAllMembers()
	if len(all) != 1 {
		t.Errorf("Expected 1 member after update, got %d", len(all))
	}
}

func TestGossipSelectRandomPeer_NoPeers(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	// Only self in the membership - Gossip should be a no-op
	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Gossip with no peers should not panic
	m.Gossip()
}

func TestGossipSelectRandomPeer_EmptyMembership(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	// No members at all
	m.Gossip()
}

func TestGossipHeartbeatIncrement(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Gossip increments local node's heartbeat
	m.Gossip()

	got := m.GetMember("node1")
	if got.Heartbeat != 2 {
		t.Errorf("Expected heartbeat 2 after gossip, got %d", got.Heartbeat)
	}

	// Gossip again
	m.Gossip()
	got = m.GetMember("node1")
	if got.Heartbeat != 3 {
		t.Errorf("Expected heartbeat 3 after second gossip, got %d", got.Heartbeat)
	}
}

func TestGossipWithNoRPCClient(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Without RPC client, gossipWith should be a no-op
	m.Gossip()

	// Heartbeat should still increment for local node
	got := m.GetMember("node1")
	if got.Heartbeat != 2 {
		t.Errorf("Expected heartbeat 2, got %d", got.Heartbeat)
	}
}

func TestGossipMergeMembers_HigherHeartbeat(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	// Add node2 with heartbeat 1
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Simulate receiving gossip with higher heartbeat for node2
	// We test this indirectly through HandleGossip-like behavior
	// Using AddMember which overwrites
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 10, Timestamp: time.Now(),
	})

	got := m.GetMember("node2")
	if got.Heartbeat != 10 {
		t.Errorf("Expected heartbeat 10, got %d", got.Heartbeat)
	}
}

func TestGossipMergeMembers_NewMember(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 1, Timestamp: time.Now(),
	})

	// Simulate discovering a new member through gossip
	m.AddMember(&membership.Member{
		NodeID: "node3", Address: "localhost:8002", Status: membership.StatusAlive,
		Heartbeat: 5, Tokens: []uint32{100, 200},
		Timestamp: time.Now(),
	})

	got := m.GetMember("node3")
	if got == nil {
		t.Fatal("Expected to find new member node3")
	}
	if got.Heartbeat != 5 {
		t.Errorf("Expected heartbeat 5, got %d", got.Heartbeat)
	}
	if len(got.Tokens) != 2 {
		t.Errorf("Expected 2 tokens, got %d", len(got.Tokens))
	}
}

func TestSyncWithSeed_NoRPCClient(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	// Without RPC client, should return error
	_, err := m.SyncWithSeed("localhost:9000")
	if err == nil {
		t.Error("Expected error when syncing without RPC client")
	}
}

func TestMemberGetters(t *testing.T) {
	member := &membership.Member{
		NodeID:    "test-node",
		Address:   "localhost:9999",
		Status:    membership.StatusSuspected,
		Tokens:    []uint32{1, 2, 3},
		Heartbeat: 42,
		Timestamp: time.Now(),
	}

	if member.GetNodeID() != "test-node" {
		t.Errorf("Expected 'test-node', got '%s'", member.GetNodeID())
	}
	if member.GetAddress() != "localhost:9999" {
		t.Errorf("Expected 'localhost:9999', got '%s'", member.GetAddress())
	}
	if member.GetStatus() != 1 { // StatusSuspected = 1
		t.Errorf("Expected status 1, got %d", member.GetStatus())
	}
	if len(member.GetTokens()) != 3 {
		t.Errorf("Expected 3 tokens, got %d", len(member.GetTokens()))
	}
}

func TestGossipBuildMemberDTOs(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	m.AddMember(&membership.Member{
		NodeID: "node1", Address: "localhost:8000", Status: membership.StatusAlive,
		Heartbeat: 5, Tokens: []uint32{100}, Timestamp: time.Now(),
	})
	m.AddMember(&membership.Member{
		NodeID: "node2", Address: "localhost:8001", Status: membership.StatusAlive,
		Heartbeat: 3, Tokens: []uint32{200}, Timestamp: time.Now(),
	})

	// Verify members are accessible via GetAllMembers
	members := m.GetAllMembers()
	if len(members) != 2 {
		t.Fatalf("Expected 2 members, got %d", len(members))
	}

	// Verify DTOs can be created from members (simulating what buildMemberDTOs does)
	dtos := make([]rpc.MemberDTO, len(members))
	for i, member := range members {
		dtos[i] = rpc.MemberDTO{
			NodeID:    member.NodeID,
			Address:   member.Address,
			Status:    int(member.Status),
			Heartbeat: member.Heartbeat,
			Tokens:    member.Tokens,
			Timestamp: member.Timestamp,
		}
	}

	if len(dtos) != 2 {
		t.Errorf("Expected 2 DTOs, got %d", len(dtos))
	}
}

func TestGossipSetRPCClient(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	// Setting nil RPC client should not panic
	m.SetRPCClient(nil)
}

func TestGossipSetFailureDetector(t *testing.T) {
	config := newMockConfig()
	m := membership.NewMembership("node1", "localhost:8000", config)

	fd := membership.NewFailureDetector(m, 5*time.Second)

	// Setting failure detector should not panic
	m.SetFailureDetector(fd)
}
