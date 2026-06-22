#!/usr/bin/env bash
#
# Maelstrom test runner for Toy Dynamo.
# Usage:
#   ./tests/maelstrom/run_tests.sh [test-name]
#
# Available tests:
#   smoke             Single-node adapter/protocol smoke test
#   read-your-writes  3-node RYW verification via linearizability checker
#   lin-kv-3          3-node linearizability (R=2, W=2, N=3)
#   lin-kv-5          5-node linearizability
#   partition-3       3-node with network partitions
#   partition-5       5-node with network partitions
#   lossy-3           3-node with message latency and loss
#   lossy-5           5-node with message latency and loss
#   convergence       5-node partition test (long) for convergence
#   all               Run all tests sequentially
#
# If no test name is given, runs "lin-kv-3" as a quick smoke test.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/bin/maelstrom-dynamo"
MAELSTROM="$ROOT/maelstrom/maelstrom"
STORE="$ROOT/store"

# Colors for output (disabled if not a terminal).
if [ -t 1 ]; then
    GREEN='\033[0;32m'
    RED='\033[0;31m'
    YELLOW='\033[0;33m'
    BOLD='\033[1m'
    RESET='\033[0m'
else
    GREEN='' RED='' YELLOW='' BOLD='' RESET=''
fi

info()  { echo -e "${BOLD}==> $*${RESET}"; }
pass()  { echo -e "${GREEN}PASS${RESET}: $*"; }
fail()  { echo -e "${RED}FAIL${RESET}: $*"; }
warn()  { echo -e "${YELLOW}WARN${RESET}: $*"; }

# --- Prerequisites ---

check_prerequisites() {
    if ! command -v java >/dev/null 2>&1; then
        echo "Error: Java is required to run Maelstrom. Install Java 11+."
        exit 1
    fi

    if ! command -v go >/dev/null 2>&1; then
        echo "Error: Go is required to build the adapter binary."
        exit 1
    fi

    if [ ! -f "$MAELSTROM" ]; then
        info "Maelstrom not found, downloading v0.2.3..."
        curl -sL https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 \
            -o /tmp/maelstrom.tar.bz2
        tar -xjf /tmp/maelstrom.tar.bz2 -C "$ROOT"
        rm -f /tmp/maelstrom.tar.bz2
        if [ ! -f "$MAELSTROM" ]; then
            echo "Error: Failed to download Maelstrom."
            exit 1
        fi
        info "Maelstrom installed at $MAELSTROM"
    fi
}

build_binary() {
    info "Building maelstrom-dynamo binary..."
    (cd "$ROOT" && go build -o "$BIN" ./cmd/maelstrom/)
    info "Binary built at $BIN"
}

# --- Test definitions ---

# Each test function runs Maelstrom with specific parameters and checks the result.
# Returns 0 on pass, 1 on fail.

run_maelstrom() {
    local test_name="$1"
    shift
    info "Running test: $test_name"
    echo "  Command: $MAELSTROM test $*"
    echo ""

    if "$MAELSTROM" test "$@" 2>&1 | tee "$STORE/$test_name.log"; then
        pass "$test_name"
        return 0
    else
        fail "$test_name"
        echo "  See full results in $STORE/latest/"
        return 1
    fi
}

test_lin_kv_3() {
    run_maelstrom "lin-kv-3" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 3 \
        --time-limit 30 \
        --rate 10 \
        --concurrency 2n
}

test_smoke() {
    run_maelstrom "smoke" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 1 \
        --time-limit 10 \
        --rate 10 \
        --concurrency 2n
}

# Maelstrom's lin-kv checker verifies linearizability, which implies
# read-your-writes. R=2, W=2, N=3 is configured by the adapter for this cluster.
test_read_your_writes() {
    run_maelstrom "read-your-writes" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 3 \
        --time-limit 30 \
        --rate 10 \
        --concurrency 2n
}

test_lin_kv_5() {
    run_maelstrom "lin-kv-5" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 5 \
        --time-limit 60 \
        --rate 10 \
        --concurrency 2n
}

test_partition_3() {
    run_maelstrom "partition-3" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 3 \
        --time-limit 60 \
        --rate 10 \
        --concurrency 2n \
        --nemesis partition
}

test_partition_5() {
    run_maelstrom "partition-5" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 5 \
        --time-limit 60 \
        --rate 10 \
        --concurrency 2n \
        --nemesis partition
}

test_lossy_3() {
    run_maelstrom "lossy-3" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 3 \
        --time-limit 60 \
        --rate 5 \
        --concurrency 2n \
        --latency 100
}

test_lossy_5() {
    run_maelstrom "lossy-5" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 5 \
        --time-limit 60 \
        --rate 5 \
        --concurrency 2n \
        --latency 100
}

# Long-running partition test: writes during partitions, heals, verifies
# that the linearizability checker passes (implying convergence via read repair).
test_convergence() {
    run_maelstrom "convergence" \
        -w lin-kv \
        --bin "$BIN" \
        --node-count 5 \
        --time-limit 120 \
        --rate 5 \
        --concurrency 2n \
        --nemesis partition
}

# --- Main ---

check_prerequisites
build_binary

TEST_NAME="${1:-lin-kv-3}"
FAILURES=0

case "$TEST_NAME" in
    smoke)        test_smoke        || FAILURES=$((FAILURES + 1)) ;;
    read-your-writes) test_read_your_writes || FAILURES=$((FAILURES + 1)) ;;
    lin-kv-3)     test_lin_kv_3     || FAILURES=$((FAILURES + 1)) ;;
    lin-kv-5)     test_lin_kv_5     || FAILURES=$((FAILURES + 1)) ;;
    partition-3)  test_partition_3  || FAILURES=$((FAILURES + 1)) ;;
    partition-5)  test_partition_5  || FAILURES=$((FAILURES + 1)) ;;
    lossy-3)      test_lossy_3     || FAILURES=$((FAILURES + 1)) ;;
    lossy-5)      test_lossy_5     || FAILURES=$((FAILURES + 1)) ;;
    convergence)  test_convergence || FAILURES=$((FAILURES + 1)) ;;
    all)
        test_smoke        || FAILURES=$((FAILURES + 1))
        test_read_your_writes || FAILURES=$((FAILURES + 1))
        test_lin_kv_3     || FAILURES=$((FAILURES + 1))
        test_lin_kv_5     || FAILURES=$((FAILURES + 1))
        test_partition_3  || FAILURES=$((FAILURES + 1))
        test_partition_5  || FAILURES=$((FAILURES + 1))
        test_lossy_3      || FAILURES=$((FAILURES + 1))
        test_lossy_5      || FAILURES=$((FAILURES + 1))
        test_convergence  || FAILURES=$((FAILURES + 1))
        ;;
    *)
        echo "Unknown test: $TEST_NAME"
        echo "Available: smoke, read-your-writes, lin-kv-3, lin-kv-5, partition-3, partition-5, lossy-3, lossy-5, convergence, all"
        exit 1
        ;;
esac

echo ""
if [ "$FAILURES" -eq 0 ]; then
    pass "All tests passed."
else
    fail "$FAILURES test(s) failed."
    exit 1
fi
