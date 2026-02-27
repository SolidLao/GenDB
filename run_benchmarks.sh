#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ORCH="$SCRIPT_DIR/src/gendb/orchestrator.mjs"
SINGLE="$SCRIPT_DIR/src/gendb/single.mjs"

# ============================================================
# Single-Agent Mode — High-Level Prompt
# ============================================================

echo "============================================"
echo "  [Single / high-level] Run 1: TPC-H (SF=10)"
echo "============================================"
node "$SINGLE" --benchmark tpc-h --sf 10 --single-agent-prompt high-level || true

echo ""
echo "============================================"
echo "  [Single / high-level] Run 2: SEC-EDGAR (SF=3)"
echo "============================================"
node "$SINGLE" --benchmark sec-edgar --sf 3 --single-agent-prompt high-level || true

# ============================================================
# Single-Agent Mode — Guided Prompt
# ============================================================

echo ""
echo "============================================"
echo "  [Single / guided] Run 3: TPC-H (SF=10)"
echo "============================================"
node "$SINGLE" --benchmark tpc-h --sf 10 --single-agent-prompt guided || true

echo ""
echo "============================================"
echo "  [Single / guided] Run 4: SEC-EDGAR (SF=3)"
echo "============================================"
node "$SINGLE" --benchmark sec-edgar --sf 3 --single-agent-prompt guided || true

# ============================================================
# Multi-Agent Mode
# ============================================================

echo ""
echo "============================================"
echo "  [Multi-Agent] Run 5: TPC-H (SF=10)"
echo "============================================"
node "$ORCH" --benchmark tpc-h --sf 10 || true

echo ""
echo "============================================"
echo "  [Multi-Agent] Run 6: SEC-EDGAR (SF=3)"
echo "============================================"
node "$ORCH" --benchmark sec-edgar --sf 3 || true
