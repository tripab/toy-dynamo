package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tripab/toy-dynamo/pkg/dynamo"
	"github.com/tripab/toy-dynamo/pkg/membership"
	"github.com/tripab/toy-dynamo/pkg/peer"
	"github.com/tripab/toy-dynamo/pkg/transport"
)

// TestReadYourWritesQuorumBoundary deterministically exercises both sides of
// the quorum-intersection rule. The scripted transport ensures the coordinator
// cannot accidentally contact a different replica and mask the boundary.
func TestReadYourWritesQuorumBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	tests := []struct {
		name        string
		readQuorum  int
		writeQuorum int
		writeRoutes []messageRoute
		readRoutes  []messageRoute
		expectValue bool
	}{
		{
			name:        "R+W greater than N intersects",
			readQuorum:  2,
			writeQuorum: 2,
			writeRoutes: []messageRoute{{source: "node0", target: "node1", msgType: peer.TypePut}},
			readRoutes:  []messageRoute{{source: "node2", target: "node1", msgType: peer.TypeGet}},
			expectValue: true,
		},
		{
			name:        "R+W equal to N can be disjoint",
			readQuorum:  2,
			writeQuorum: 1,
			readRoutes:  []messageRoute{{source: "node1", target: "node2", msgType: peer.TypeGet}},
			expectValue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network := newScriptedNetwork()
			nodes := createInMemoryCluster(t, network, tt.readQuorum, tt.writeQuorum)
			defer stopCluster(nodes)

			for _, route := range tt.writeRoutes {
				network.allow(route)
			}

			const key = "quorum-boundary-key"
			const value = "acknowledged-value"
			if err := nodes[0].Put(context.Background(), key, []byte(value), nil); err != nil {
				t.Fatalf("Put failed with W=%d: %v", tt.writeQuorum, err)
			}

			network.replaceAllowed(tt.readRoutes...)
			readNode := nodes[2]
			if !tt.expectValue {
				readNode = nodes[1]
			}

			result, err := readNode.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("Get failed with R=%d: %v", tt.readQuorum, err)
			}

			found := false
			for _, version := range result.Values {
				if string(version.Data) == value {
					found = true
					break
				}
			}
			if found != tt.expectValue {
				t.Fatalf("R=%d W=%d N=3: found acknowledged value=%v, want %v",
					tt.readQuorum, tt.writeQuorum, found, tt.expectValue)
			}
		})
	}
}

type messageRoute struct {
	source  string
	target  string
	msgType string
}

type scriptedNetwork struct {
	mu       sync.RWMutex
	handlers map[string]transport.Handler
	allowed  map[messageRoute]struct{}
}

func newScriptedNetwork() *scriptedNetwork {
	return &scriptedNetwork{
		handlers: make(map[string]transport.Handler),
		allowed:  make(map[messageRoute]struct{}),
	}
}

func (n *scriptedNetwork) register(nodeID string, handler transport.Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[nodeID] = handler
}

func (n *scriptedNetwork) allow(route messageRoute) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.allowed[route] = struct{}{}
}

func (n *scriptedNetwork) replaceAllowed(routes ...messageRoute) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.allowed = make(map[messageRoute]struct{}, len(routes))
	for _, route := range routes {
		n.allowed[route] = struct{}{}
	}
}

func (n *scriptedNetwork) send(ctx context.Context, source, target string, msg transport.Message) (transport.Message, error) {
	route := messageRoute{source: source, target: target, msgType: msg.Type}

	n.mu.RLock()
	_, allowed := n.allowed[route]
	handler := n.handlers[target]
	n.mu.RUnlock()

	if !allowed {
		return transport.Message{}, fmt.Errorf("scripted network blocked %s -> %s (%s)", source, target, msg.Type)
	}
	if handler == nil {
		return transport.Message{}, fmt.Errorf("scripted network has no handler for %s", target)
	}
	return handler.HandleMessage(ctx, msg)
}

type scriptedTransport struct {
	source  string
	network *scriptedNetwork
}

func (t *scriptedTransport) Send(ctx context.Context, target string, msg transport.Message) (transport.Message, error) {
	return t.network.send(ctx, t.source, target, msg)
}

func (t *scriptedTransport) Close() {}

func createInMemoryCluster(t *testing.T, network *scriptedNetwork, r, w int) []*dynamo.Node {
	t.Helper()

	const nodeCount = 3
	const virtualNodes = 16
	nodes := make([]*dynamo.Node, nodeCount)

	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		config := dynamo.DefaultConfig()
		config.N = nodeCount
		config.R = r
		config.W = w
		config.VirtualNodes = virtualNodes
		config.RequestTimeout = 500 * time.Millisecond
		config.HintedHandoffEnabled = false
		config.ReadRepairEnabled = false
		config.AdmissionControlEnabled = false
		config.CoordinatorSelectionEnabled = false
		config.MetricsEnabled = false
		config.EnableCircuitBreaker = false
		config.EnableRetry = false
		config.DisableHTTPServer = true
		config.Transport = &scriptedTransport{source: nodeID, network: network}

		node, err := dynamo.NewNode(nodeID, nodeID, config)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", nodeID, err)
		}
		nodes[i] = node
		network.register(nodeID, node)
	}

	for _, node := range nodes {
		for i := 0; i < nodeCount; i++ {
			nodeID := fmt.Sprintf("node%d", i)
			tokens := node.InitRing(nodeID, virtualNodes)
			node.InitMember(&membership.Member{
				NodeID:    nodeID,
				Address:   nodeID,
				Status:    membership.StatusAlive,
				Heartbeat: 1,
				Tokens:    tokens,
				Timestamp: time.Now(),
			})
		}
	}

	return nodes
}
