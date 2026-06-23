# Testing Toy Dynamo

Toy Dynamo uses conventional Go tests for deterministic component and cluster
behavior, plus Maelstrom workloads for protocol-level correctness under
concurrency and network faults.

## Prerequisites

- Go 1.21 or newer
- Java 11 or newer
- `curl` and `tar` when Maelstrom must be downloaded
- `gnuplot` for Maelstrom latency and throughput plots

The Maelstrom runner downloads v0.2.3 automatically when
`maelstrom/maelstrom` is absent. To install it manually:

```sh
curl -fsSL \
  https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 \
  -o /tmp/maelstrom.tar.bz2
tar -xjf /tmp/maelstrom.tar.bz2
```

## Go tests

Run the maintained product, command, and test packages with:

```sh
go test ./pkg/... ./cmd/... ./tests/...
```

The integration tests start local RPC listeners on fixed ports. Run only one
copy of the integration package at a time, and ensure those ports are not
already occupied by another checkout or workspace.

Target an individual correctness property during development:

```sh
go test ./tests/integration -run TestVectorClockConflict -count=1
go test ./tests/integration -run TestHintedHandoffVerification -count=1
go test ./tests/integration -run TestReadYourWritesQuorumBoundary -count=1
```

## Maelstrom test profiles

The Makefile exposes three local profiles:

| Command | Purpose | Expected duration |
|---|---|---:|
| `make maelstrom-test-quick` | Single-node adapter and protocol smoke test | About 15 seconds |
| `make maelstrom-test` | Every predefined Maelstrom scenario | About 8 minutes |
| `make maelstrom-test-stress` | Five-node partition and convergence scenario | About 2 minutes |

The runner builds `bin/maelstrom-dynamo` before each profile. Individual tests
can be selected directly:

```sh
./tests/maelstrom-tests/run_tests.sh lin-kv-3
./tests/maelstrom-tests/run_tests.sh partition-5
./tests/maelstrom-tests/run_tests.sh lossy-3
./tests/maelstrom-tests/run_tests.sh read-your-writes
```

Run the same bounded core profile used in CI with:

```sh
./tests/maelstrom-tests/run_tests.sh ci
```

It runs the smoke, three-node linearizability, and three-node partition
scenarios. Their configured workload time is 100 seconds; including startup
and analysis, the profile is intended to remain below five minutes.

Latency scenarios such as `lossy-3` remain useful for local stress exploration,
but they are intentionally excluded from the required CI profile. They can
produce valid counterexamples under the strict `lin-kv` checker, so failures
there should be investigated as stress-test findings rather than treated as CI
infrastructure regressions.

## Interpreting Maelstrom results

The final summary combines several checkers. Correctness requires both the
top-level `:valid? true` and `:workload {:valid? true}`. The workload section
contains the linearizability result for each independent key. Expected CAS
precondition failures appear as failed operations in `:stats`; they do not
invalidate the workload.

Useful summary fields include:

| Field | Interpretation |
|---|---|
| `:valid? true` | Every enabled checker passed |
| `:workload {:valid? true}` | No linearizability violation was found |
| `:timeline {:valid? true}` | The HTML operation timeline was generated |
| `:exceptions {:valid? true}` | No unexpected client or node exception occurred |
| `:availability {:ok-fraction n}` | Fraction of operations that completed successfully |

Results are written beneath `store/lin-kv/<timestamp>/`. The `latest` symlink
points to the newest run. The most useful files are:

- `results.edn`: checker output and machine-readable verdict
- `history.edn` and `history.txt`: complete operation history
- `timeline.html`: Lamport-style operation timeline
- `jepsen.log`: node, network, and checker diagnostics
- `latency-quantiles.png`, `latency-raw.png`, and `rate.png`: performance plots

Start with `results.edn` when a check fails, inspect `timeline.html` around the
reported operation, then correlate its process and message IDs with
`history.txt` and `jepsen.log`. A missing graph usually means `gnuplot` is
unavailable. Maelstrom reports that as an analysis error, so CI installs
`gnuplot-nox` before running the suite.

## CI and archived reports

`.github/workflows/maelstrom.yml` runs on pull requests and pushes to `main`.
It installs Go, Java, and `gnuplot-nox`, downloads the pinned Maelstrom
release, runs Go unit tests, and executes the bounded `ci` profile.

Every run uploads `store/` as a `maelstrom-results-<run-id>` artifact, even
when a correctness step fails. Download that artifact from the workflow run's
Artifacts section and open each timestamped `timeline.html` locally. Reports
are retained for 14 days.

## TLA+ model checks

The focused formal specs live in `specs/tla/`. They model the protocol-level
arguments separately from the Go implementation:

| Model | Invariant or property |
|---|---|
| `QuorumReadWrite` | No acknowledged write is lost when `R + W > N`. |
| `VectorClock` | Increment, compare, and merge preserve causality. |
| `SloppyHandoff` | Hints are stored by write recipients and delivered at most once to the correct primary. |
| `Convergence` | Under fair anti-entropy after quiescence, replicas eventually converge. |

Run all bounded models with:

```sh
make tla-check
```

The runner uses `TLA2TOOLS_JAR` when set. If it is absent, it downloads the
pinned TLC jar to `.context/tla2tools.jar`. The checked configs are deliberately
small because vector-clock histories and handoff states grow quickly.

## Adapter architecture

Maelstrom launches one `bin/maelstrom-dynamo` process per logical node and
communicates over newline-delimited JSON on standard input and output. Logs
must go to standard error because standard output is reserved for protocol
messages.

```text
Maelstrom clients and simulated network
                  |
                  | JSON over stdin/stdout
                  v
cmd/maelstrom/main.go
                  |
                  v
pkg/maelstrom/transport.go      framing, response correlation, concurrent output
pkg/maelstrom/node.go           init and client operation dispatch
pkg/maelstrom/peer_transport.go transport.Transport implementation
pkg/maelstrom/protocol.go       wire message definitions
                  |
                  v
pkg/dynamo/coordinator.go       quorum reads, writes, and read repair
pkg/replication/                replication and hinted handoff
pkg/ring/                       consistent hashing
pkg/versioning/                 vector clocks and reconciliation
pkg/storage/                    in-memory node storage
```

The adapter uses the same `transport.Transport` boundary as the HTTP peer
client, so quorum and replication behavior remain in the core packages. Only
message delivery is replaced by Maelstrom's simulated network.

## Adding a workload or scenario

For another configuration of the existing `lin-kv` workload:

1. Add a `test_<name>` function to `tests/maelstrom-tests/run_tests.sh`.
2. Call `run_maelstrom` with a unique log name and fixed node count, time
   limit, rate, concurrency, and optional nemesis arguments.
3. Add the name to the runner's `case` statement and usage header.
4. Include it in `ci` only if it is deterministic and keeps that profile under
   five minutes; longer scenarios belong in `all` or the stress profile.
5. Run the new case directly, then run `make maelstrom-test-quick` to catch
   protocol regressions.

For a new wire operation, add request and response bodies in
`pkg/maelstrom/protocol.go`, register client dispatch in
`pkg/maelstrom/node.go`, and add peer request routing in
`pkg/maelstrom/peer_transport.go` when it crosses nodes. Keep all writes to
standard output serialized through `pkg/maelstrom/transport.go`.

Maelstrom's built-in `lin-kv` checker should be preferred when the property is
linearizability. Dynamo-specific properties such as conflict preservation or
shopping-cart reconciliation are currently deterministic Go integration
tests. A new external checker must define its invariant explicitly, record
enough history to reproduce a violation, and fail with a nonzero exit status
so the runner and CI job reject the run.
