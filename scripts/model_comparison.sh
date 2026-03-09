#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ORCH="$REPO_DIR/src/gendb/orchestrator.mjs"

# gpt-5.3-codex: already completed (2026-03-07T01-54-48), skip

# echo "============================================"
# echo "  [Multi-Agent] gpt-5.4 (codex provider)"
# echo "============================================"
# node "$ORCH" --benchmark tpc-h --sf 10 --agent-provider codex --model-override gpt-5.4 || true

# echo ""
# echo "============================================"
# echo "  [Multi-Agent] sonnet (claude provider)"
# echo "============================================"
# node "$ORCH" --benchmark tpc-h --sf 10 --agent-provider claude --model-override sonnet || true

# echo ""
# echo "============================================"
# echo "  [Multi-Agent] opus (claude provider)"
# echo "============================================"
# node "$ORCH" --benchmark tpc-h --sf 10 --agent-provider claude --model-override opus || true

# ============================================================
# SEC-EDGAR Benchmark
# ============================================================

echo ""
echo "============================================"
echo "  [Multi-Agent] gpt-5.3-codex / SEC-EDGAR"
echo "============================================"
node "$ORCH" --benchmark sec-edgar --sf 3 --agent-provider codex --model-override gpt-5.3-codex || true

echo ""
echo "============================================"
echo "  [Multi-Agent] gpt-5.4 / SEC-EDGAR"
echo "============================================"
node "$ORCH" --benchmark sec-edgar --sf 3 --agent-provider codex --model-override gpt-5.4 || true

# echo ""
# echo "============================================"
# echo "  [Multi-Agent] sonnet / SEC-EDGAR"
# echo "============================================"
# node "$ORCH" --benchmark sec-edgar --sf 3 --agent-provider claude --model-override sonnet || true

# echo ""
# echo "============================================"
# echo "  [Multi-Agent] opus / SEC-EDGAR"
# echo "============================================"
# node "$ORCH" --benchmark sec-edgar --sf 3 --agent-provider claude --model-override opus || true
