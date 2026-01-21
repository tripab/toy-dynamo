package ring

// VirtualNodeManager handles virtual node operations
type VirtualNodeManager struct {
	vnodesPerNode int
}

// NewVirtualNodeManager creates a new virtual node manager
func NewVirtualNodeManager(vnodesPerNode int) *VirtualNodeManager {
	return &VirtualNodeManager{
		vnodesPerNode: vnodesPerNode,
	}
}

// AllocateVNodes allocates virtual nodes for a physical node
func (vm *VirtualNodeManager) AllocateVNodes(nodeID string, count int) []uint32 {
	// This is handled by Ring.AddNode
	return []uint32{}
}
