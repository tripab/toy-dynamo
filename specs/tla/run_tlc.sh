#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPEC_DIR="$ROOT_DIR/specs/tla"
JAR_PATH="${TLA2TOOLS_JAR:-$ROOT_DIR/.context/tla2tools.jar}"
TLA2TOOLS_URL="${TLA2TOOLS_URL:-https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar}"

MODELS=("$@")
if [[ ${#MODELS[@]} -eq 0 ]]; then
  MODELS=(VectorClock QuorumReadWrite SloppyHandoff Convergence)
fi

if [[ ! -f "$JAR_PATH" ]]; then
  mkdir -p "$(dirname "$JAR_PATH")"
  curl -fsSL "$TLA2TOOLS_URL" -o "$JAR_PATH"
fi

for model in "${MODELS[@]}"; do
  echo "==> TLC $model"
  java -XX:+UseParallelGC -cp "$JAR_PATH" tlc2.TLC \
    -config "$SPEC_DIR/$model.cfg" \
    -deadlock \
    -cleanup \
    "$SPEC_DIR/$model.tla"
done
