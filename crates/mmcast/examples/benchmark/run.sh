#!/usr/bin/env bash
# Bring up both stacks, run the loadgen against each, capture results.
# Prereqs: docker + docker compose v2 + python3 with `websockets`.
set -euo pipefail

cd "$(dirname "$0")"

CLIENTS=${CLIENTS:-20}
PUBLISHERS=${PUBLISHERS:-4}
MESSAGES=${MESSAGES:-500}
PAYLOAD=${PAYLOAD:-128}

echo "==> Building images"
docker compose --profile redis --profile mmcast build

echo "==> Starting Redis stack"
docker compose --profile redis up -d redis redis_app
echo "==> Starting mmcast stack"
docker compose --profile mmcast up -d mmcast_app

# Crude readiness check — both apps print "Uvicorn running on" to stdout
# on startup.  Sleep a beat to let lifespan finish.
sleep 3

echo "==> Loadgen: Redis side  (8001)"
python3 loadgen/loadgen.py \
  --url ws://127.0.0.1:8001/ws \
  --clients "$CLIENTS" --publishers "$PUBLISHERS" \
  --messages-per-publisher "$MESSAGES" --payload-size "$PAYLOAD" \
  --label "redis" --out redis_results.json

echo "==> Loadgen: mmcast side (8002)"
python3 loadgen/loadgen.py \
  --url ws://127.0.0.1:8002/ws \
  --clients "$CLIENTS" --publishers "$PUBLISHERS" \
  --messages-per-publisher "$MESSAGES" --payload-size "$PAYLOAD" \
  --label "mmcast" --out mmcast_results.json

echo "==> Container counts:"
echo "    redis-stack:  $(docker compose --profile redis ps --format json | grep -c '\"State\":\"running\"' || true)"
echo "    mmcast-stack: $(docker compose --profile mmcast ps --format json | grep -c '\"State\":\"running\"' || true)"

echo "==> RSS (MiB) snapshot:"
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}' bench-redis bench-redis-app bench-mmcast-app || true

echo
echo "==> Summary"
python3 - <<'PY'
import json, pathlib
for label, path in [("redis", "redis_results.json"),
                    ("mmcast", "mmcast_results.json")]:
    if not pathlib.Path(path).exists():
        continue
    d = json.loads(open(path).read())
    lat = d["latency_ms"]
    print(f"\n--- {label} ---")
    print(f"  wall: {d['wall_clock_s']}s  delivered: {d['delivered_total']}  "
          f"thpt: {d['delivered_per_sec']} msg/s")
    print(f"  latency ms — p50 {lat['p50']}  p95 {lat['p95']}  p99 {lat['p99']}  max {lat['max']}")
PY

echo
echo "==> Tearing down"
docker compose --profile redis --profile mmcast down -v
