#!/bin/bash

# Test script for parallelized Q3 query

GENDB_DIR="/home/jl4492/GenDB/data/gendb"
GENERATED_DIR="/home/jl4492/GenDB/output/tpc-h/2026-02-11T01-26-47/iterations/1/generated"

echo "==================================================================="
echo "Running Parallelized Q3 Query Test"
echo "==================================================================="
echo ""
echo "Hardware Info:"
echo "  CPU cores: $(nproc)"
echo "  CPU info: $(lscpu | grep 'Model name' | cut -d: -f2 | xargs)"
echo ""
echo "GenDB Directory: $GENDB_DIR"
echo "Generated Code: $GENERATED_DIR"
echo ""
echo "==================================================================="
echo ""

cd "$GENERATED_DIR"

if [ ! -f "main" ]; then
    echo "ERROR: main binary not found. Run 'make' first."
    exit 1
fi

if [ ! -d "$GENDB_DIR" ]; then
    echo "ERROR: GenDB directory not found: $GENDB_DIR"
    exit 1
fi

echo "Running Q3 query with parallelism..."
echo ""

./main "$GENDB_DIR"

echo ""
echo "==================================================================="
echo "Test Complete"
echo "==================================================================="
