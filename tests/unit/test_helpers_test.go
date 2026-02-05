package tests

// mockNodeInfo implements types.NodeInfo for testing
type mockNodeInfo struct {
	id      string
	address string
}

func (m *mockNodeInfo) GetID() string      { return m.id }
func (m *mockNodeInfo) GetAddress() string { return m.address }
