#!/usr/bin/env bash
# Run every harness, collect JSON, append to RESULTS.md.
#
# Prereqs:
#   * Docker daemon running.
#   * Python venv at .venv/ with mmbus + pyzmq + redis + nats-py installed.
#   * `mmbus` built with the `wal_v2` feature for the v0.2 perf path —
#     `maturin develop --release` is the recipe.
#
# Outputs:
#   results.json   — newline-delimited JSON, one row per run
#   RESULTS.md     — human-readable table appended each run

set -euo pipefail
cd "$(dirname "$0")"

PY="${PY:-../../.venv/bin/python}"
RESULTS_JSON="results.json"
RESULTS_MD="RESULTS.md"

start_services() {
  if ! docker ps --format '{{.Names}}' | grep -q '^mmbus-bench-redis$'; then
    echo "[run_all] booting redis..."
    docker run -d --rm --name mmbus-bench-redis -p 16379:6379 \
      redis:7-alpine redis-server --appendonly yes --appendfsync everysec
  fi
  if ! docker ps --format '{{.Names}}' | grep -q '^mmbus-bench-nats$'; then
    echo "[run_all] booting nats..."
    docker run -d --rm --name mmbus-bench-nats -p 14222:4222 \
      nats:latest -js
  fi
  # Give them a moment to come up.
  sleep 1
}

stop_services() {
  for c in mmbus-bench-redis mmbus-bench-nats; do
    if docker ps --format '{{.Names}}' | grep -q "^${c}$"; then
      docker stop "$c" >/dev/null || true
    fi
  done
}

run() {
  local label="$1"; shift
  echo "[run_all] $label..."
  "$PY" "$@" | tee -a "$RESULTS_JSON"
  echo >> "$RESULTS_JSON"
}

main() {
  start_services
  trap stop_services EXIT

  : > "$RESULTS_JSON"

  run "mmbus durable"     mmbus_bench.py durable
  run "mmbus nondurable"  mmbus_bench.py nondurable
  run "zmq"               zmq_bench.py
  run "redis durable"     redis_bench.py durable
  run "nats jetstream"    nats_bench.py durable

  echo "[run_all] all done.  Results in $RESULTS_JSON"
}

main "$@"
