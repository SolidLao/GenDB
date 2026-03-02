#!/usr/bin/env bash
#
# GenDB Environment Setup
#
# Usage:
#   bash scripts/setup.sh [--sf SCALE_FACTOR] [--years YEARS] [--skip-data]
#
# Options:
#   --sf SCALE_FACTOR    TPC-H scale factor (default: 10, ~10 GB)
#   --years YEARS        SEC-EDGAR years of data (default: 3, ~5 GB)
#   --skip-data          Skip data download (only install dependencies)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

SF="${SF:-10}"
YEARS="${YEARS:-3}"
SKIP_DATA=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        --years) YEARS="$2"; shift 2 ;;
        --skip-data) SKIP_DATA=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== GenDB Setup ==="
echo ""

# Step 1: Check prerequisites
echo "[1/4] Checking prerequisites..."

# Node.js
if ! command -v node &>/dev/null; then
    echo "  ERROR: Node.js not found. Install Node.js 18+ from https://nodejs.org/"
    exit 1
fi
NODE_VERSION=$(node -v | sed 's/v//' | cut -d. -f1)
if [ "$NODE_VERSION" -lt 18 ]; then
    echo "  ERROR: Node.js 18+ required (found $(node -v))"
    exit 1
fi
echo "  Node.js $(node -v) OK"

# g++
if ! command -v g++ &>/dev/null; then
    echo "  ERROR: g++ not found. Install with: sudo apt-get install build-essential"
    exit 1
fi
echo "  g++ $(g++ -dumpversion) OK"

# Check C++17 and OpenMP support
if ! echo '#include <optional>' | g++ -std=c++17 -fopenmp -x c++ -c - -o /dev/null 2>/dev/null; then
    echo "  ERROR: g++ does not support C++17 or OpenMP. Update g++ or install libgomp."
    exit 1
fi
echo "  C++17 + OpenMP OK"

# Claude access (API key or Claude Code subscription)
if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    if command -v claude &>/dev/null; then
        echo "  Claude Code CLI found — ensure you are logged in (claude login)"
    else
        echo "  WARNING: No Claude access detected. Either:"
        echo "    export ANTHROPIC_API_KEY=your_key"
        echo "    or log in to Claude Code with a Pro/Max/Team/Enterprise plan (claude login)"
    fi
else
    echo "  ANTHROPIC_API_KEY set"
fi

echo ""

# Step 2: Install Node.js dependencies
echo "[2/4] Installing Node.js dependencies..."
npm install
echo ""

# Step 3: Install Python dependencies (for benchmarking)
echo "[3/4] Installing Python dependencies (for benchmarking)..."
if command -v pip3 &>/dev/null; then
    pip3 install --quiet duckdb matplotlib numpy 2>/dev/null || \
    pip3 install duckdb matplotlib numpy
    echo "  Python packages installed"
elif command -v pip &>/dev/null; then
    pip install --quiet duckdb matplotlib numpy 2>/dev/null || \
    pip install duckdb matplotlib numpy
    echo "  Python packages installed"
else
    echo "  WARNING: pip not found. Python packages needed for benchmarking:"
    echo "    pip install duckdb matplotlib numpy"
fi
echo ""

# Step 4: Download benchmark data
if [ "$SKIP_DATA" = true ]; then
    echo "[4/4] Skipping data download (--skip-data)"
else
    echo "[4/4] Downloading benchmark data..."
    echo ""
    echo "  TPC-H (SF${SF})..."
    bash benchmarks/tpc-h/setup_data.sh "$SF"
    echo ""
    echo "  SEC-EDGAR (${YEARS} years)..."
    bash benchmarks/sec-edgar/setup_data.sh "$YEARS"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Run GenDB:"
echo "  node src/gendb/orchestrator.mjs --benchmark tpc-h --sf ${SF}"
echo ""
echo "Run all benchmarks:"
echo "  bash scripts/run_benchmarks.sh"
