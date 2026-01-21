package membership

import "time"

// MemberInfo provides detailed information about a cluster member
type MemberInfo struct {
	Member    *Member
	LastSeen  time.Time
	RTT       time.Duration
	IsHealthy bool
}

// GetMemberInfo returns detailed information about a member
func (m *Membership) GetMemberInfo(nodeID string) *MemberInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	member := m.members[nodeID]
	if member == nil {
		return nil
	}

	return &MemberInfo{
		Member:    member,
		LastSeen:  member.Timestamp,
		IsHealthy: member.Status == StatusAlive,
	}
}

// GetHealthyMembers returns all healthy members
func (m *Membership) GetHealthyMembers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthy := make([]*Member, 0)
	for _, member := range m.members {
		if member.Status == StatusAlive {
			healthy = append(healthy, member)
		}
	}

	return healthy
}
