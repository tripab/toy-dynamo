# Dynamo Design Document

This document provides an in-depth look at the design and implementation of our Dynamo key-value store, based on Amazon's SOSP 2007 paper.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Components](#core-components)
3. [Partitioning](#partitioning)
4. [Replication](#replication)
5. [Versioning](#versioning)
6. [Request Coordination](#request-coordination)
7. [Failure Handling](#failure-handling)
8. [Membership and Failure Detection](#membership-and-failure-detection)
9. [Storage Engine](#storage-engine)
10. [Performance Optimizations](#performance-optimizations)

## Architecture Overview

Dynamo is a decentralized, peer-to-peer distributed storage system designed for high availability. The key design principles are:

1. **Incremental Scalability**: Add one node at a time without system-wide impact
2. **Symmetry**: All nodes have equal responsibilities (no masters)
3. **Decentralization**: Peer-to-peer techniques over centralized control
4. **Heterogeneity**: Handle different node capacities through virtual nodes

### System Properties

- **Availability**: Always writeable, sacrifices consistency when necessary
- **Consistency**: Eventually consistent with tunable parameters (R, W, N)
- **Durability**: Configurable replication factor
- **Performance**: Sub-300ms latencies at 99.9th percentile

## Core Components

### 1. Node

The `Node` is the main entry point representing a single Dynamo instance.

```go
type Node struct {
    id              string
    address         string
    config          *Config
    ring            *Ring
    storage         Storage
    replicator      *Replicator
    coordinator     *Coordinator
    membership      *Membership
    antiEntropy     *AntiEntropy
    hintedHandoff   *HintedHandoff
}
```

Responsibilities:
- Lifecycle management (start/stop)
- Request routing to appropriate coordinator
- Integration of all subsystems

### 2. Ring (Consistent Hashing)

Implements consistent hashing with virtual nodes for data partitioning.

```go
type Ring struct {
    vnodes      map[uint32]*VirtualNode  // position -> vnode
    nodes       map[string]*PhysicalNode // nodeID -> node
    sortedKeys  []uint32                 // sorted positions
    replicas    int                      // replication factor N
}
```

Key algorithms:
- **Hash Function**: MD5 (128-bit) for key distribution
- **Token Assignment**: Multiple random positions per node
- **Preference List**: First N unique physical nodes walking clockwise

### 3. Versioning

Uses vector clocks to track causality between versions.

```go
type VectorClock struct {
    versions  map[string]uint64  // nodeID -> counter
    timestamp time.Time          // for pruning old entries
}

type VersionedValue struct {
    Data         []byte
    VectorClock  *VectorClock
}
```

Vector clock operations:
- **Increment**: When coordinating a write
- **Merge**: Combine multiple vector clocks
- **Compare**: Determine causal relationships (ancestor, descendant, concurrent)
- **Prune**: Remove oldest entries when size exceeds threshold

### 4. Coordinator

Implements state machine for request processing.

```go
type Coordinator struct {
    node      *Node
    router    *Router
    quorum    *QuorumManager
}
```

State machine phases:
1. **Route**: Determine preference list for key
2. **Execute**: Contact R or W replicas
3. **Wait**: Collect responses with timeout
4. **Reconcile**: Merge conflicting versions
5. **Repair**: Update stale replicas (read repair)

## Partitioning

### Consistent Hashing

Data is distributed using consistent hashing to achieve:
- Incremental scalability
- Minimal data movement on membership changes
- Load balancing

### Virtual Nodes

Each physical node is assigned Q/S tokens (where Q = total partitions, S = system size).

Benefits:
1. **Load Distribution**: Even distribution when nodes fail/join
2. **Heterogeneity**: Nodes can have different numbers of vnodes based on capacity
3. **Availability**: Load dispersed across many nodes when one fails

### Partitioning Strategy

We implement Strategy 3 from Section 6.2:
- Fixed equal-sized partitions (Q = 256 * num_nodes)
- Each node assigned Q/S random tokens
- Decouples partitioning from placement

```
Hash Space: [0, 2^128)
Partitions: Q = 256 * S
Per Node: Q/S tokens

Example (S=3, Q=768):
Node A: tokens [45, 187, 234, ...]  (256 tokens)
Node B: tokens [12, 98, 201, ...]   (256 tokens)
Node C: tokens [5, 156, 243, ...]   (256 tokens)
```

## Replication

### N-Way Replication

Each key k is replicated at N nodes:
1. Coordinator node (responsible for key range)
2. N-1 successive nodes walking clockwise on ring

```
Key "user:123" -> Hash -> Position on ring -> Coordinator
Replicas: [Coordinator, Next_1, Next_2]
```

### Preference List

The preference list contains > N nodes to handle failures:

```go
func (r *Ring) GetPreferenceList(key string, n int) []string {
    hash := md5.Sum([]byte(key))
    position := binary.BigEndian.Uint32(hash[:4])
    
    // Find first vnode >= position
    idx := sort.Search(len(r.sortedKeys), func(i int) bool {
        return r.sortedKeys[i] >= position
    })
    
    // Collect N unique physical nodes
    seen := make(map[string]bool)
    list := []string{}
    
    for len(list) < n {
        vnode := r.vnodes[r.sortedKeys[idx % len(r.sortedKeys)]]
        if !seen[vnode.NodeID] {
            list = append(list, vnode.NodeID)
            seen[vnode.NodeID] = true
        }
        idx++
    }
    
    return list
}
```

### Sloppy Quorum

Handles temporary failures by allowing any N healthy nodes to participate, not just the first N:

- Normal: First N nodes in preference list
- Failure: Skip failed nodes, use next available nodes
- Hint: Store metadata about intended recipient

## Versioning

### Vector Clocks

Track causality between versions to enable conflict detection.

#### Operations

**Increment** - When coordinating a write:
```go
func (vc *VectorClock) Increment(nodeID string) {
    vc.versions[nodeID]++
    vc.timestamp = time.Now()
}
```

**Compare** - Determine relationship:
```go
func (vc1 *VectorClock) Compare(vc2 *VectorClock) Relationship {
    vc1Greater := false
    vc2Greater := false
    
    // Check all nodes in both clocks
    for node := range allNodes(vc1, vc2) {
        v1 := vc1.versions[node]
        v2 := vc2.versions[node]
        
        if v1 > v2 {
            vc1Greater = true
        } else if v2 > v1 {
            vc2Greater = true
        }
    }
    
    if vc1Greater && !vc2Greater {
        return Descendent  // vc1 descends from vc2
    } else if vc2Greater && !vc1Greater {
        return Ancestor    // vc1 is ancestor of vc2
    } else if !vc1Greater && !vc2Greater {
        return Equal
    } else {
        return Concurrent  // conflict!
    }
}
```

#### Conflict Resolution

When concurrent versions exist:
1. **Syntactic**: Use vector clock comparison to eliminate ancestors
2. **Semantic**: Application provides merge logic

```go
type ConflictResolver interface {
    Resolve(values []VersionedValue) []byte
}

// Example: Last-write-wins
type TimestampResolver struct{}

func (r *TimestampResolver) Resolve(values []VersionedValue) []byte {
    latest := values[0]
    for _, v := range values[1:] {
        if v.VectorClock.timestamp.After(latest.VectorClock.timestamp) {
            latest = v
        }
    }
    return latest.Data
}

// Example: Shopping cart merge
type ShoppingCartResolver struct{}

func (r *ShoppingCartResolver) Resolve(values []VersionedValue) []byte {
    merged := NewCart()
    for _, v := range values {
        cart := ParseCart(v.Data)
        merged.Merge(cart)  // Union of items
    }
    return merged.Serialize()
}
```

#### Vector Clock Pruning

To prevent unbounded growth:
- Track timestamp with each (node, counter) pair
- When size exceeds threshold (default: 10), remove oldest entry
- Trade-off: May lose some causality information

## Request Coordination

### Get Operation

State machine for reads:

```
1. Route → Determine preference list for key
2. Send → Request data from N nodes
3. Wait → Collect R responses (with timeout)
4. Reconcile → Identify causally concurrent versions
5. Return → Send all concurrent versions to client
6. Repair → Update stale replicas (read repair)
```

```go
func (c *Coordinator) Get(ctx context.Context, key string) (*GetResult, error) {
    // 1. Route
    preferenceList := c.node.ring.GetPreferenceList(key, c.node.config.N)
    
    // 2. Send requests
    responses := make(chan *Response, len(preferenceList))
    for _, nodeID := range preferenceList {
        go c.sendGetRequest(nodeID, key, responses)
    }
    
    // 3. Wait for R responses
    values := []VersionedValue{}
    count := 0
    timeout := time.After(200 * time.Millisecond)
    
    for count < c.node.config.R {
        select {
        case resp := <-responses:
            if resp.Error == nil {
                values = append(values, resp.Value)
                count++
            }
        case <-timeout:
            return nil, ErrReadQuorumFailed
        case <-ctx.Done():
            return nil, ctx.Err()
        }
    }
    
    // 4. Reconcile - find concurrent versions
    concurrent := c.reconcileConcurrent(values)
    
    // 5. Return result
    result := &GetResult{
        Values: concurrent,
        Context: c.buildContext(concurrent),
    }
    
    // 6. Read repair (async)
    go c.readRepair(key, concurrent, preferenceList)
    
    return result, nil
}
```

### Put Operation

State machine for writes:

```
1. Route → Determine preference list
2. Version → Generate new vector clock
3. Send → Write to N nodes
4. Wait → Collect W acknowledgments
5. Return → Success if W responses received
```

```go
func (c *Coordinator) Put(ctx context.Context, key string, value []byte, 
                          context *Context) error {
    // 1. Route
    preferenceList := c.node.ring.GetPreferenceList(key, c.node.config.N)
    
    // 2. Generate new version
    newClock := context.VectorClock.Copy()
    newClock.Increment(c.node.id)
    
    versioned := &VersionedValue{
        Data: value,
        VectorClock: newClock,
    }
    
    // 3. Send writes
    responses := make(chan error, len(preferenceList))
    for _, nodeID := range preferenceList {
        go c.sendPutRequest(nodeID, key, versioned, responses)
    }
    
    // 4. Wait for W acks
    acks := 0
    timeout := time.After(300 * time.Millisecond)
    
    for acks < c.node.config.W {
        select {
        case err := <-responses:
            if err == nil {
                acks++
            }
        case <-timeout:
            return ErrWriteQuorumFailed
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    
    return nil
}
```

### Coordinator Selection

Two strategies:

1. **Load Balancer**: Client sends to any node, forwarded to coordinator
2. **Client Library**: Client directly contacts coordinator (lower latency)

Coordinator is typically the first node in preference list, but for writes, can be the node that responded fastest to the previous read (optimization for read-your-writes consistency).

## Failure Handling

### Hinted Handoff

Handles temporary failures without sacrificing write availability.

```go
type Hint struct {
    ForNode    string    // Intended recipient
    Key        string
    Value      VersionedValue
    Timestamp  time.Time
}

type HintedHandoff struct {
    hints      map[string][]*Hint  // node -> hints
    storage    Storage
    mu         sync.RWMutex
}
```

Process:
1. **Write**: If node A is down, send replica to node D with hint
2. **Store**: D stores hint in separate database
3. **Detect**: D periodically checks if A has recovered
4. **Transfer**: D sends data back to A
5. **Delete**: D removes hint after successful transfer

```go
func (h *HintedHandoff) HandleFailure(key string, value VersionedValue, 
                                       deadNode string, liveNode string) error {
    hint := &Hint{
        ForNode: deadNode,
        Key: key,
        Value: value,
        Timestamp: time.Now(),
    }
    
    h.mu.Lock()
    h.hints[liveNode] = append(h.hints[liveNode], hint)
    h.mu.Unlock()
    
    return h.storage.StoreHint(hint)
}

func (h *HintedHandoff) DeliverHints() {
    ticker := time.NewTicker(10 * time.Second)
    for range ticker.C {
        for nodeID, hints := range h.hints {
            if h.isNodeAlive(nodeID) {
                for _, hint := range hints {
                    h.deliverHint(nodeID, hint)
                }
            }
        }
    }
}
```

### Merkle Trees

Efficiently detect inconsistencies between replicas.

```go
type MerkleTree struct {
    root     *MerkleNode
    leaves   map[string]*MerkleNode  // key -> leaf
}

type MerkleNode struct {
    Hash     []byte
    Left     *MerkleNode
    Right    *MerkleNode
    KeyRange [2]string  // [start, end)
}
```

Anti-entropy process:
1. **Build**: Each node maintains Merkle tree per key range
2. **Compare**: Exchange root hashes with replicas
3. **Traverse**: If roots differ, recursively compare children
4. **Sync**: At leaves, identify specific out-of-sync keys
5. **Transfer**: Exchange only differing keys

```go
func (ae *AntiEntropy) SyncWithPeer(peerID string, keyRange KeyRange) error {
    // 1. Get local tree for key range
    localTree := ae.GetTree(keyRange)
    
    // 2. Request peer's root hash
    peerRoot := ae.requestRootHash(peerID, keyRange)
    
    // 3. Compare roots
    if bytes.Equal(localTree.root.Hash, peerRoot) {
        return nil  // Trees match, nothing to sync
    }
    
    // 4. Traverse to find differences
    diffs := ae.findDifferences(localTree, peerID, keyRange)
    
    // 5. Sync differing keys
    for _, diff := range diffs {
        ae.syncKey(peerID, diff.Key)
    }
    
    return nil
}
```

Benefits:
- Minimize data transfer (only exchange differing keys)
- Reduce disk I/O (tree traversal vs full scan)
- Fast consistency checks (O(log n) tree traversal)

## Membership and Failure Detection

### Gossip Protocol

Nodes exchange membership information periodically.

```go
type Member struct {
    NodeID    string
    Address   string
    Status    MemberStatus  // Alive, Suspected, Dead
    Heartbeat uint64
    Tokens    []uint32      // Virtual node positions
}

type Membership struct {
    members   map[string]*Member
    local     *Member
    mu        sync.RWMutex
}
```

Gossip cycle:
1. **Select**: Choose random peer
2. **Exchange**: Send/receive member lists
3. **Merge**: Update local view with new information
4. **Detect**: Identify failed nodes (missed heartbeats)
5. **Propagate**: Spread updates through network

```go
func (m *Membership) GossipLoop() {
    ticker := time.NewTicker(1 * time.Second)
    for range ticker.C {
        peer := m.selectRandomPeer()
        m.gossipWith(peer)
    }
}

func (m *Membership) gossipWith(peerID string) error {
    // Increment local heartbeat
    m.local.Heartbeat++
    
    // Send our member list to peer
    remoteMerge := m.sendMemberList(peerID, m.members)
    
    // Merge peer's view
    m.mu.Lock()
    defer m.mu.Unlock()
    
    for id, remoteMember := range remoteMerge {
        localMember := m.members[id]
        if localMember == nil || remoteMember.Heartbeat > localMember.Heartbeat {
            m.members[id] = remoteMember
        }
    }
    
    return nil
}
```

### Failure Detection

Local, decentralized failure detection:

```go
type FailureDetector struct {
    membership  *Membership
    suspects    map[string]time.Time
    timeout     time.Duration
}

func (fd *FailureDetector) CheckFailures() {
    fd.membership.mu.RLock()
    defer fd.membership.mu.RUnlock()
    
    now := time.Now()
    for id, member := range fd.membership.members {
        if id == fd.membership.local.NodeID {
            continue
        }
        
        lastSeen := member.Timestamp
        if now.Sub(lastSeen) > fd.timeout {
            // Mark as suspected
            fd.suspects[id] = now
            member.Status = Suspected
        }
        
        if now.Sub(lastSeen) > 3 * fd.timeout {
            // Mark as dead
            member.Status = Dead
        }
    }
}
```

Key properties:
- **Decentralized**: No central authority
- **Eventually Consistent**: All nodes converge on membership view
- **Adaptive**: Timeout adjusts based on network conditions

## Storage Engine

### Interface

Pluggable storage allows different backends:

```go
type Storage interface {
    Get(key string) ([]VersionedValue, error)
    Put(key string, value VersionedValue) error
    Delete(key string) error
    GetRange(start, end string) (map[string][]VersionedValue, error)
    Close() error
}
```

### Implementations

1. **Memory**: Fast, for testing
2. **BoltDB**: Embedded B+tree, ACID transactions
3. **BadgerDB**: LSM-tree, high write throughput

Selection criteria:
- Object size (BoltDB < 1MB, BadgerDB for larger)
- Access patterns (read-heavy vs write-heavy)
- Durability requirements

## Performance Optimizations

### 1. Write Buffering

Trade durability for performance:

```go
type BufferedStorage struct {
    underlying Storage
    buffer     map[string]VersionedValue
    bufferSize int
    mu         sync.RWMutex
}

func (bs *BufferedStorage) Put(key string, value VersionedValue) error {
    bs.mu.Lock()
    defer bs.mu.Unlock()
    
    bs.buffer[key] = value
    
    if len(bs.buffer) >= bs.bufferSize {
        return bs.flush()
    }
    
    return nil
}
```

Result: 5x reduction in 99.9th percentile latency (Section 6.1).

### 2. Load-Balanced Coordinator Selection

Choose coordinator based on read latency:

```go
type CoordinatorSelector struct {
    latencies map[string]*LatencyTracker
}

func (cs *CoordinatorSelector) SelectCoordinator(preferenceList []string) string {
    // For writes, choose node that responded fastest to previous read
    fastest := preferenceList[0]
    minLatency := cs.latencies[fastest].P99()
    
    for _, nodeID := range preferenceList[1:] {
        latency := cs.latencies[nodeID].P99()
        if latency < minLatency {
            fastest = nodeID
            minLatency = latency
        }
    }
    
    return fastest
}
```

Benefits:
- Reduces write latency variance
- Improves 99.9th percentile performance
- Increases read-your-writes consistency

### 3. Read Repair

Opportunistically fix stale replicas:

```go
func (c *Coordinator) readRepair(key string, values []VersionedValue, 
                                  preferenceList []string) {
    // Find the most recent version(s)
    latest := c.findLatestVersions(values)
    
    // Update any nodes with stale data
    for _, nodeID := range preferenceList {
        if c.hasStaleData(nodeID, latest) {
            c.sendPutRequest(nodeID, key, latest)
        }
    }
}
```

Reduces load on anti-entropy system.

### 4. Admission Control

Prioritize foreground requests over background tasks:

```go
type AdmissionController struct {
    foregroundLatency *metrics.Histogram
    threshold         time.Duration
    backgroundSlots   int
}

func (ac *AdmissionController) AllowBackground() bool {
    p99 := ac.foregroundLatency.Percentile(0.99)
    
    if p99 > ac.threshold {
        // Foreground under stress, reduce background work
        ac.backgroundSlots = max(1, ac.backgroundSlots - 1)
    } else {
        // Foreground healthy, allow more background work
        ac.backgroundSlots = min(10, ac.backgroundSlots + 1)
    }
    
    return ac.backgroundSlots > 0
}
```

Prevents background tasks (anti-entropy, hinted handoff) from impacting user requests.

## Configuration Tuning

### Consistency vs Availability

```
R + W > N: Strong consistency (quorum intersection)
R + W ≤ N: Weak consistency, higher availability

Examples:
(N=3, R=2, W=2): Balanced (typical production)
(N=3, R=1, W=1): Highest availability (shopping cart)
(N=3, R=3, W=1): Fast reads, slower writes
(N=3, R=1, W=3): Faster writes, potentially stale reads
```

### Virtual Nodes

More virtual nodes = Better load distribution but higher overhead

```
Typical: 256 virtual nodes per physical node
Range: 64-512 depending on cluster size
```

### Timeouts

Based on SLA requirements:

```
Read timeout:  200ms (fail fast)
Write timeout: 300ms (durability)
Gossip interval: 1s
Anti-entropy: 1-10 minutes (depending on data size)
```

## Testing Strategy

### Unit Tests
- Ring operations (hashing, preference list)
- Vector clock operations
- Storage engine compliance
- Conflict resolution

### Integration Tests
- Multi-node clusters
- Network partition scenarios
- Node failure/recovery
- Consistency verification

### Performance Tests
- Throughput benchmarks
- Latency distributions
- Load balancing fairness
- Scalability (add/remove nodes)

## Future Enhancements

1. **Hierarchical Membership**: Support for 1000s of nodes
2. **Compression**: Reduce storage/network costs
3. **Encryption**: At-rest and in-transit
4. **Metrics Dashboard**: Real-time monitoring
5. **Dynamic Quorum**: Adjust R/W based on load
6. **Cross-DC Replication**: Geo-distributed deployments