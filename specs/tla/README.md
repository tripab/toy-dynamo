# TLA+ specifications

This directory contains small, executable TLA+ models for the core Toy Dynamo
correctness arguments. They are design-level models, not translations of the
Go implementation.

## Models

| Model | Checked property |
|---|---|
| `QuorumReadWrite.tla` | With `R + W > N`, every acknowledged write intersects every later read quorum. |
| `VectorClock.tla` | Increment, compare, and merge preserve vector-clock causality. |
| `SloppyHandoff.tla` | Sloppy writes can use substitute nodes, and hints are delivered at most once to the correct primary. |
| `Convergence.tla` | Under fair anti-entropy after quiescence, all replicas eventually hold the same version set. |

Each `.cfg` file intentionally uses a small bounded instance so TLC can run
quickly during development. Increase node counts, values, or clock bounds only
when investigating a specific protocol change; state space grows quickly.

## Running TLC

Run every bounded model:

```sh
make tla-check
```

Run one model directly:

```sh
./specs/tla/run_tlc.sh VectorClock
./specs/tla/run_tlc.sh QuorumReadWrite
```

The runner uses `TLA2TOOLS_JAR` when set. Otherwise it downloads the pinned
TLC jar to `.context/tla2tools.jar`.

## Scope limits

The specs deliberately exclude gossip, membership dissemination, Merkle-tree
anti-entropy, admission control, connection pooling, and storage-engine
internals. Those concerns are covered by Go tests and Maelstrom workloads.
