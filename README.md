# Toy Dynamo: Distributed Key-Value Store

A toy (or simplified) Go implementation of Amazon's Dynamo distributed key-value store based on the [SOSP 2007 paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).

## Overview

This implementation covers the core distributed systems techniques described in Sections 4, 5, and 6 of the Dynamo paper:

- **Consistent Hashing with Virtual Nodes**: For incremental scalability and load distribution
- **Replication**: Configurable N-way replication across nodes
- **Vector Clocks**: For capturing causality and conflict detection
- **Quorum-based Operations**: Tunable consistency with R/W/N parameters
- **Hinted Handoff**: Handling temporary node failures
- **Merkle Trees**: Anti-entropy for replica synchronization
- **Gossip Protocol**: Decentralized membership and failure detection
- **Read Repair**: Opportunistic replica synchronization

## Features

### Core Functionality
- ✅ Simple key-value interface (`Get`, `Put`)
- ✅ Consistent hashing ring with virtual nodes
- ✅ Configurable replication factor (N)
- ✅ Tunable consistency (R, W parameters)
- ✅ Vector clock versioning for conflict detection
- ✅ Application-level conflict resolution
- ✅ Hinted handoff for write availability
- ✅ Merkle tree-based anti-entropy
- ✅ Gossip-based membership protocol
- ✅ Pluggable storage engine interface

### Production Considerations
- Configurable per-instance settings
- Multiple storage backend support
- Performance metrics and monitoring hooks
- Graceful node addition/removal
- Client-driven and server-driven coordination

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Application                    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────┐
        │   Request Coordinator  │
        │  (State Machine Based) │
        └────────┬───────────────┘
                 │
     ┌───────────┼───────────┐
     ▼           ▼           ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│  Node A │ │  Node B │ │  Node C │
├─────────┤ ├─────────┤ ├─────────┤
│ Gossip  │ │ Gossip  │ │ Gossip  │
│ Merkle  │ │ Merkle  │ │ Merkle  │
│ Storage │ │ Storage │ │ Storage │
└─────────┘ └─────────┘ └─────────┘
```

## Installation

```bash
go get github.com/tripab/toy-dynamo
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/tripab/toy-dynamo"
)

func main() {
    // Create a new Dynamo instance
    config := &dynamo.Config{
        N: 3,  // Replication factor
        R: 2,  // Read quorum
        W: 2,  // Write quorum
        VirtualNodes: 256,
    }
    
    node, err := dynamo.NewNode("node1", "localhost:8001", config)
    if err != nil {
        panic(err)
    }
    defer node.Stop()
    
    // Start the node
    if err := node.Start(); err != nil {
        panic(err)
    }
    
    ctx := context.Background()
    
    // Put a value
    err = node.Put(ctx, "user:123", []byte("Alice"), nil)
    if err != nil {
        panic(err)
    }
    
    // Get the value
    result, err := node.Get(ctx, "user:123")
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Value: %s\n", result.Values[0].Data)
}
```

## Configuration

### Node Configuration

```go
type Config struct {
    // Replication factor
    N int
    
    // Read quorum size
    R int
    
    // Write quorum size
    W int
    
    // Number of virtual nodes per physical node
    VirtualNodes int
    
    // Storage engine type
    StorageEngine string
    
    // Gossip interval
    GossipInterval time.Duration
    
    // Anti-entropy interval
    AntiEntropyInterval time.Duration
    
    // Hinted handoff settings
    HintedHandoffEnabled bool
    HintTimeout time.Duration
    
    // Vector clock pruning
    VectorClockMaxSize int
}
```

### Typical Configurations

**High Availability (Shopping Cart)**
```go
config := &dynamo.Config{
    N: 3, R: 2, W: 2,
    VirtualNodes: 256,
}
```

**High Read Performance**
```go
config := &dynamo.Config{
    N: 3, R: 1, W: 3,
    VirtualNodes: 256,
}
```

**Strong Consistency**
```go
config := &dynamo.Config{
    N: 3, R: 2, W: 2, // R + W > N
    VirtualNodes: 256,
}
```

## API Reference

### Node Operations

```go
// Create a new node
func NewNode(id, address string, config *Config) (*Node, error)

// Start the node
func (n *Node) Start() error

// Stop the node
func (n *Node) Stop() error

// Join an existing ring
func (n *Node) Join(seeds []string) error
```

### Data Operations

```go
// Put a key-value pair
func (n *Node) Put(ctx context.Context, key string, value []byte, context *Context) error

// Get a value by key (returns all conflicting versions)
func (n *Node) Get(ctx context.Context, key string) (*GetResult, error)

// Delete a key
func (n *Node) Delete(ctx context.Context, key string, context *Context) error
```

### Conflict Resolution

```go
type GetResult struct {
    Values  []VersionedValue
    Context *Context
}

// Application provides reconciliation logic
func reconcile(values []VersionedValue) []byte {
    // Custom merge logic
    return merged
}
```

## Storage Engines

The implementation supports pluggable storage engines:

- **Memory**: In-memory storage (default, good for testing)
- **BoltDB**: Embedded key-value store
- **BadgerDB**: Fast embedded database

```go
config := &dynamo.Config{
    StorageEngine: "badger",
    // ...
}
```

## Testing

### Unit Tests
```bash
go test ./pkg/... ./cmd/... ./tests/unit/...
```

### Integration Tests
```bash
go test ./tests/integration/...
```

### Performance Tests
```bash
go test -bench=. ./tests/performance/...
```

### Maelstrom Correctness Tests

The project includes a [Maelstrom](https://github.com/jepsen-io/maelstrom) adapter that runs the real Dynamo node under Jepsen-style linearizability checking. The adapter replaces the HTTP RPC transport with Maelstrom's JSON-over-STDIO protocol while preserving all core logic: consistent hashing, quorum coordination, vector clock versioning, and reconciliation.

```bash
# Build the adapter binary
go build -o bin/maelstrom-dynamo ./cmd/maelstrom/

# Run a 3-node linearizability test
./maelstrom/maelstrom test -w lin-kv \
  --bin ./bin/maelstrom-dynamo \
  --node-count 3 \
  --time-limit 10 \
  --rate 10 \
  --concurrency 2n
```

Requires Java 11+ and Maelstrom v0.2.3. See [TESTING.md](TESTING.md)
for local profiles, CI artifacts, result interpretation, and workload extension
instructions. [MAELSTROM_GUIDE.md](MAELSTROM_GUIDE.md) remains a compact
command reference.

## Project Structure

```
.
├── README.md
├── DESIGN.md
├── go.mod
├── go.sum
├── pkg/
│   ├── dynamo/
│   │   ├── node.go              # Main node implementation
│   │   ├── coordinator.go       # Request coordination
│   │   ├── config.go            # Configuration
│   │   └── api.go               # Public API
│   ├── ring/
│   │   ├── consistent_hash.go   # Consistent hashing
│   │   ├── virtual_nodes.go     # Virtual node management
│   │   └── partitioner.go       # Partitioning strategy
│   ├── versioning/
│   │   ├── vector_clock.go      # Vector clock implementation
│   │   └── reconciler.go        # Version reconciliation
│   ├── replication/
│   │   ├── replicator.go        # Replication logic
│   │   ├── quorum.go            # Quorum operations
│   │   └── hinted_handoff.go    # Hinted handoff
│   ├── membership/
│   │   ├── gossip.go            # Gossip protocol
│   │   ├── failure_detector.go  # Failure detection
│   │   └── member.go            # Member information
│   ├── sync/
│   │   ├── merkle.go            # Merkle tree
│   │   └── anti_entropy.go      # Anti-entropy protocol
│   ├── storage/
│   │   ├── interface.go         # Storage engine interface
│   │   ├── memory.go            # In-memory engine
│   │   ├── boltdb.go            # BoltDB engine
│   │   └── badger.go            # BadgerDB engine
│   └── maelstrom/
│       ├── protocol.go          # Maelstrom message types
│       ├── transport.go         # JSON-over-STDIO transport
│       ├── router.go            # Inter-node message routing
│       └── node.go              # Maelstrom-adapted Dynamo node
├── cmd/
│   └── maelstrom/
│       └── main.go              # Maelstrom adapter binary
├── tests/
│   ├── unit/                    # Unit tests
│   ├── integration/             # Integration tests
│   └── performance/             # Performance benchmarks
└── examples/
    ├── simple/                  # Basic usage
    ├── shopping_cart/           # Shopping cart example
    └── cluster/                 # Multi-node cluster
```

## Performance

Based on the paper's observations (Section 6):

- **99.9th percentile latencies**: < 300ms (typical configuration)
- **Divergent versions**: < 0.06% of requests see multiple versions
- **Load balancing**: Within 15% deviation from average under high load
- **Availability**: 99.9995% success rate in production environments

### Benchmarks

See `tests/performance/` for detailed benchmarks. Sample results:

```
BenchmarkPut-8     100000    15234 ns/op
BenchmarkGet-8     200000     8456 ns/op
BenchmarkQuorum-8   50000    28901 ns/op
```

## Limitations

As noted in Section 6.6 of the paper:

1. **Scalability**: The full membership model works well for hundreds of nodes but would require hierarchical extensions for tens of thousands of nodes.

2. **Conflict Resolution**: Applications must implement semantic reconciliation for divergent versions.

3. **Consistency**: Eventual consistency model - not suitable for applications requiring strong consistency.

## Production Considerations

### Monitoring

Monitor these key metrics:
- Request latencies (p50, p99, p99.9)
- Divergent version frequency
- Hinted handoff queue size
- Anti-entropy sync progress
- Node availability

### Operational Tasks

- **Adding Nodes**: Use `Join()` to add new nodes incrementally
- **Removing Nodes**: Gracefully stop and redistribute data
- **Backups**: Use Merkle tree snapshots for consistent backups
- **Capacity Planning**: Monitor load distribution across virtual nodes

## References

- [Dynamo: Amazon's Highly Available Key-value Store (SOSP 2007)](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Consistent Hashing and Random Trees (Karger et al.)](https://dl.acm.org/doi/10.1145/258533.258660)
- [Time, Clocks, and the Ordering of Events (Lamport)](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please see CONTRIBUTING.md for guidelines.

## Acknowledgments

This implementation is based on the Dynamo paper by DeCandia et al. from Amazon. The paper describes techniques used in production at Amazon.com to provide highly available storage for critical services.
