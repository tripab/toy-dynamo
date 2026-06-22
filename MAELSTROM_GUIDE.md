# Maelstrom Testing Guide

> For the current test profiles, CI workflow, artifact layout, adapter
> architecture, and extension guide, see [TESTING.md](TESTING.md). This file is
> retained as a compact command reference.

This guide covers how to build and run the Maelstrom correctness tests for Toy Dynamo.

## Prerequisites

- **Go 1.21+** (for building the adapter binary)
- **Java 11+** (for running Maelstrom)
- **Maelstrom v0.2.3** (downloaded automatically or manually)
- **gnuplot** (optional, for generating latency/rate graphs)

## Setup

### 1. Download Maelstrom

```bash
curl -sL https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 -o /tmp/maelstrom.tar.bz2
tar -xjf /tmp/maelstrom.tar.bz2 -C .
```

This creates a `maelstrom/` directory in the project root.

### 2. Build the adapter binary

```bash
go build -o bin/maelstrom-dynamo ./cmd/maelstrom/
```

## Running Tests

### Single-node lin-kv (basic smoke test)

Verifies that the adapter binary speaks the Maelstrom protocol correctly and handles read/write/cas operations on a single node.

```bash
./maelstrom/maelstrom test -w lin-kv \
  --bin ./bin/maelstrom-dynamo \
  --node-count 1 \
  --time-limit 10 \
  --rate 10 \
  --concurrency 2n
```

### 3-node lin-kv (quorum coordination)

Tests the actual distributed path: consistent hashing, quorum reads/writes (R=2, W=2, N=3), vector clock versioning, and inter-node message routing.

```bash
./maelstrom/maelstrom test -w lin-kv \
  --bin ./bin/maelstrom-dynamo \
  --node-count 3 \
  --time-limit 10 \
  --rate 10 \
  --concurrency 2n
```

### 5-node lin-kv (larger cluster)

```bash
./maelstrom/maelstrom test -w lin-kv \
  --bin ./bin/maelstrom-dynamo \
  --node-count 5 \
  --time-limit 30 \
  --rate 10 \
  --concurrency 2n
```

## Interpreting Results

Maelstrom prints a summary at the end of each run. The key fields to look for:

| Field | Meaning |
|-------|---------|
| `:workload {:valid? true}` | The linearizability checker found no violations |
| `:stats {:valid? true}` | All operation types (read/write/cas) behaved correctly |
| `:availability {:ok-fraction ...}` | Fraction of operations that succeeded (CAS precondition failures are expected) |
| `:timeline {:valid? true}` | The operation timeline is consistent |

A result of `:valid? :unknown` at the top level is normal if `gnuplot` is not installed -- it only means the latency/rate graphs couldn't be rendered. The correctness checks themselves are unaffected.

### Test artifacts

Maelstrom writes detailed results to `store/` in the project root, including:

- `history.txt` -- full operation history
- `jepsen.log` -- detailed test log
- `results.edn` -- machine-readable results
- `timeline.html` -- visual timeline (open in a browser)
- Latency/rate plots (if gnuplot is available)

## Architecture

The Maelstrom adapter (`cmd/maelstrom/main.go`) runs a real Dynamo node with the HTTP RPC transport replaced by Maelstrom's JSON-over-STDIO protocol:

```
Maelstrom workbench (JVM)
  |
  |  JSON messages over STDIN/STDOUT
  v
cmd/maelstrom/main.go
  |
  v
pkg/maelstrom/transport.go   -- STDIO reader/writer, message dispatch
pkg/maelstrom/node.go        -- init, read/write/cas handler wiring
pkg/maelstrom/router.go      -- inter-node get/put/hint via STDIO
pkg/maelstrom/protocol.go    -- message type definitions
  |
  v
pkg/dynamo/coordinator.go    -- real quorum coordination (unmodified logic)
pkg/dynamo/node.go            -- real Dynamo node with pluggable transport
pkg/ring/                     -- consistent hashing
pkg/versioning/               -- vector clocks, reconciliation
pkg/storage/                  -- in-memory storage engine
```

The coordinator uses the shared `transport.Transport` interface, with the
Maelstrom peer transport plugged in instead of the HTTP peer client. All quorum
logic, vector clock versioning, reconciliation, and read repair run exactly as
they would in production.
