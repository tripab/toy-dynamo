package membership

import (
	"math/rand"
	"sync"
	"time"
)

type Membership struct {
	localID string
	address string
	members map[string]*Member
	config  interface{}
	mu      sync.RWMutex
}

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

type MemberStatus int

const (
	StatusAlive MemberStatus = iota
	StatusSuspected
	StatusDead
)

func NewMembership(nodeID, address string, config interface{}) *Membership {
	return &Membership{
		localID: nodeID,
		address: address,
		members: make(map[string]*Member),
		config:  config,
	}
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
	// In production, this would be an RPC call
	// For now, it's a no-op
}

// SyncWithSeed contacts a seed node to get membership
func (m *Membership) SyncWithSeed(seed string) ([]*Member, error) {
	// In production, make RPC call to seed
	// Return membership list
	return []*Member{}, nil
}
