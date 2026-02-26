#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ORCH="$SCRIPT_DIR/src/gendb/orchestrator.mjs"
BENCH_DIR="$SCRIPT_DIR/benchmarks"

echo "============================================"
echo "  Run 1: TPC-H (SF=10)"
echo "============================================"
node "$ORCH" \
  --benchmark tpc-h \
  --schema "$BENCH_DIR/tpc-h/schema.sql" \
  --queries "$BENCH_DIR/tpc-h/queries.sql" \
  --data-dir "$BENCH_DIR/tpc-h/data/sf10" \
  --gendb-dir "$BENCH_DIR/tpc-h/gendb/sf10.gendb" \
  --sf 10

echo ""
echo "============================================"
echo "  Run 2: SEC-EDGAR (SF=3)"
echo "============================================"
node "$ORCH" \
  --benchmark sec-edgar \
  --schema "$BENCH_DIR/sec-edgar/schema.sql" \
  --queries "$BENCH_DIR/sec-edgar/queries.sql" \
  --data-dir "$BENCH_DIR/sec-edgar/data/sf3" \
  --gendb-dir "$BENCH_DIR/sec-edgar/gendb/sf3.gendb" \
  --sf 3
