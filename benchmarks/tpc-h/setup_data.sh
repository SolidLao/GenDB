#!/usr/bin/env bash
#
# Setup TPC-H benchmark data using the official dbgen tool.
#
# Usage:
#   bash benchmarks/tpc-h/setup_data.sh [SCALE_FACTOR]
#
# SCALE_FACTOR defaults to 1 (≈1 GB of data).
# Data is written to benchmarks/tpc-h/data/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALE_FACTOR="${1:-1}"
DATA_DIR="${SCRIPT_DIR}/data"
DBGEN_DIR="${SCRIPT_DIR}/tpch-dbgen"

echo "=== TPC-H Data Setup ==="
echo "Scale factor: ${SCALE_FACTOR}"
echo "Data directory: ${DATA_DIR}"

# Step 1: Clone dbgen if not already present
if [ ! -d "${DBGEN_DIR}" ]; then
    echo "Cloning tpch-dbgen..."
    git clone https://github.com/electrum/tpch-dbgen.git "${DBGEN_DIR}"
else
    echo "tpch-dbgen already cloned."
fi

# Step 2: Build dbgen
echo "Building dbgen..."
cd "${DBGEN_DIR}"
make clean || true
make
echo "dbgen built successfully."

# Step 3: Generate data
echo "Generating TPC-H data at scale factor ${SCALE_FACTOR}..."
./dbgen -s "${SCALE_FACTOR}" -f
echo "Data generated."

# Step 4: Move .tbl files to data directory
mkdir -p "${DATA_DIR}"
chmod 644 *.tbl 2>/dev/null || true
mv -f *.tbl "${DATA_DIR}/"

echo ""
echo "=== TPC-H data ready ==="
echo "Files in ${DATA_DIR}:"
ls -lh "${DATA_DIR}"/*.tbl
echo ""
echo "Row counts:"
for f in "${DATA_DIR}"/*.tbl; do
    name=$(basename "$f")
    count=$(wc -l < "$f")
    echo "  ${name}: ${count} rows"
done
