package membership

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/tripab/toy-dynamo/pkg/rpc"
	"github.com/tripab/toy-dynamo/pkg/types"
)

// Membership manages cluster membership information via gossip protocol
type Membership struct {
	localID   string
	address   string
	members   map[string]*Member
	config    types.Config
	rpcClient *rpc.Client
	mu        sync.RWMutex
}

// Member represents a node in the cluster
type Member struct {
	NodeID    string
	Address   string
	Status    MemberStatus
	Heartbeat uint64
	Tokens    []uint32
	Timestamp time.Time
}

// Getter methods to implement types.MemberInfo interface

func (m *Member) GetNodeID() string   { return m.NodeID }
func (m *Member) GetAddress() string  { return m.Address }
func (m *Member) GetStatus() int      { return int(m.Status) }
func (m *Member) GetTokens() []uint32 { return m.Tokens }

// MemberStatus represents the health status of a cluster member
type MemberStatus int

const (
	StatusAlive MemberStatus = iota
	StatusSuspected
	StatusDead
)

// NewMembership creates a new Membership with typed config
func NewMembership(nodeID, address string, config types.Config) *Membership {
	return &Membership{
		localID:   nodeID,
		address:   address,
		members:   make(map[string]*Member),
		config:    config,
		rpcClient: nil, // Set via SetRPCClient after initialization
	}
}

// SetRPCClient sets the RPC client for network communication
func (m *Membership) SetRPCClient(client *rpc.Client) {
	m.rpcClient = client
}

// AddMember adds or updates a member
func (m *Membership) AddMember(member *Member) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.members[member.NodeID] = member
}

// GetMember retrieves a member
func (m *Membership) GetMember(nodeID string) *Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.members[nodeID]
}

// GetAllMembers returns all members
func (m *Membership) GetAllMembers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	members := make([]*Member, 0, len(m.members))
	for _, member := range m.members {
		members = append(members, member)
	}

	return members
}

// Gossip performs one gossip round
func (m *Membership) Gossip() {
	m.mu.Lock()
	local := m.members[m.localID]
	if local != nil {
		local.Heartbeat++
		local.Timestamp = time.Now()
	}
	m.mu.Unlock()

	// Select random peer
	peer := m.selectRandomPeer()
	if peer == "" {
		return
	}

	// Exchange membership information
	m.gossipWith(peer)
}

func (m *Membership) selectRandomPeer() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.members) <= 1 {
		return ""
	}

	// Select random member that's not self
	peers := make([]string, 0, len(m.members)-1)
	for id := range m.members {
		if id != m.localID {
			peers = append(peers, id)
		}
	}

	if len(peers) == 0 {
		return ""
	}

	return peers[rand.Intn(len(peers))]
}

func (m *Membership) gossipWith(peerID string) {
	if m.rpcClient == nil {
		return
	}

	m.mu.RLock()
	peer := m.members[peerID]
	if peer == nil {
		m.mu.RUnlock()
		return
	}
	peerAddress := peer.Address

	// Build our membership list to send
	memberDTOs := m.buildMemberDTOs()
	m.mu.RUnlock()

	// Exchange membership info via RPC
	ctx, cancel := context.WithTimeout(context.Background(), m.config.GetRequestTimeout())
	defer cancel()

	resp, err := m.rpcClient.Gossip(ctx, peerAddress, m.localID, memberDTOs)
	if err != nil {
		// Peer might be down - failure detector will handle it
		return
	}

	// Merge received membership info
	m.mergeMembers(resp.Members)
}

// SyncWithSeed contacts a seed node to get membership
func (m *Membership) SyncWithSeed(seedAddress string) ([]*Member, error) {
	if m.rpcClient == nil {
		return nil, fmt.Errorf("RPC client not initialized")
	}

	// Build our membership list (just ourselves initially)
	m.mu.RLock()
	memberDTOs := m.buildMemberDTOs()
	m.mu.RUnlock()

	// Contact seed node
	ctx, cancel := context.WithTimeout(context.Background(), m.config.GetRequestTimeout())
	defer cancel()

	resp, err := m.rpcClient.Gossip(ctx, seedAddress, m.localID, memberDTOs)
	if err != nil {
		return nil, fmt.Errorf("failed to sync with seed %s: %w", seedAddress, err)
	}

	// Merge received membership info and return
	m.mergeMembers(resp.Members)

	// Return the members we received
	m.mu.RLock()
	defer m.mu.RUnlock()
	members := make([]*Member, 0, len(m.members))
	for _, member := range m.members {
		members = append(members, member)
	}
	return members, nil
}

// buildMemberDTOs converts local members to DTOs for RPC
func (m *Membership) buildMemberDTOs() []rpc.MemberDTO {
	dtos := make([]rpc.MemberDTO, 0, len(m.members))
	for _, member := range m.members {
		dtos = append(dtos, rpc.MemberDTO{
			NodeID:    member.NodeID,
			Address:   member.Address,
			Status:    int(member.Status),
			Heartbeat: member.Heartbeat,
			Tokens:    member.Tokens,
			Timestamp: member.Timestamp,
		})
	}
	return dtos
}

// mergeMembers merges received membership information
func (m *Membership) mergeMembers(members []rpc.MemberDTO) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, dto := range members {
		existing := m.members[dto.NodeID]
		if existing == nil || dto.Heartbeat > existing.Heartbeat {
			m.members[dto.NodeID] = &Member{
				NodeID:    dto.NodeID,
				Address:   dto.Address,
				Status:    MemberStatus(dto.Status),
				Heartbeat: dto.Heartbeat,
				Tokens:    dto.Tokens,
				Timestamp: dto.Timestamp,
			}
		}
	}
}
