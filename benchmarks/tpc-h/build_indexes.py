#!/usr/bin/env python3
"""
build_indexes.py — Build sorted index files (.idx) from Parquet data for GenDB.

Index file format:
  Header:    num_entries (int64)
  Keys:      sorted array of num_entries keys (int32 or int64)
  RowGroups: array of num_entries row_group IDs (int32), parallel to keys

For each key value, the index records which row group(s) contain that value.
This enables O(log N) binary search to find relevant row groups for selective lookups.

Usage:
  python3 build_indexes.py --parquet-dir <dir> --output-dir <dir> --config <config.json>

The config JSON should have an "indexes" section:
{
  "indexes": {
    "lineitem": ["l_orderkey", "l_partkey"],
    "orders": ["o_orderkey", "o_custkey"],
    "customer": ["c_custkey"]
  }
}
"""

import argparse
import json
import os
import struct
import sys
import numpy as np

try:
    import pyarrow.parquet as pq
except ImportError:
    print("Error: pyarrow is required. Install with: pip install pyarrow", file=sys.stderr)
    sys.exit(1)


def build_sorted_index(parquet_path, column_name, output_path):
    """Build a sorted index file for a single column."""
    pf = pq.ParquetFile(parquet_path)
    metadata = pf.metadata

    keys_list = []
    rg_ids_list = []

    for rg_idx in range(metadata.num_row_groups):
        # Read just this column from this row group
        table = pf.read_row_group(rg_idx, columns=[column_name])
        col = table.column(column_name)

        # Convert to numpy
        arr = col.to_pandas().values

        # Handle different types
        if arr.dtype == np.float64 or arr.dtype == np.float32:
            # Skip float indexes (not useful for equality lookups)
            print(f"  Skipping {column_name}: float type not suitable for sorted index")
            return False

        # For each value, record (key, row_group_id)
        n = len(arr)
        keys_list.append(arr)
        rg_ids_list.append(np.full(n, rg_idx, dtype=np.int32))

    if not keys_list:
        return False

    all_keys = np.concatenate(keys_list)
    all_rg_ids = np.concatenate(rg_ids_list)

    # Sort by key value
    sort_idx = np.argsort(all_keys, kind='mergesort')
    sorted_keys = all_keys[sort_idx]
    sorted_rg_ids = all_rg_ids[sort_idx]

    # Deduplicate: keep only unique (key, rg_id) pairs
    if len(sorted_keys) > 0:
        # Create combined key for dedup
        combined = np.column_stack([sorted_keys.view(np.int64) if sorted_keys.dtype != np.int64
                                    else sorted_keys,
                                    sorted_rg_ids.astype(np.int64)])
        _, unique_idx = np.unique(combined, axis=0, return_index=True)
        unique_idx.sort()
        sorted_keys = sorted_keys[unique_idx]
        sorted_rg_ids = sorted_rg_ids[unique_idx]

    num_entries = len(sorted_keys)

    # Write binary index file
    with open(output_path, 'wb') as f:
        # Header: num_entries
        f.write(struct.pack('q', num_entries))

        # Keys array (int32 or int64)
        if sorted_keys.dtype in (np.int32, np.int16, np.int8):
            sorted_keys = sorted_keys.astype(np.int32)
        elif sorted_keys.dtype in (np.int64,):
            pass  # already int64
        else:
            sorted_keys = sorted_keys.astype(np.int32)

        f.write(sorted_keys.tobytes())

        # Row group IDs array (int32)
        f.write(sorted_rg_ids.astype(np.int32).tobytes())

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  Built index: {output_path} ({num_entries} entries, {size_mb:.1f} MB)")
    return True


def main():
    parser = argparse.ArgumentParser(description='Build sorted indexes from Parquet files')
    parser.add_argument('--parquet-dir', required=True, help='Directory containing .parquet files')
    parser.add_argument('--output-dir', required=True, help='Directory to write .idx files')
    parser.add_argument('--config', required=True, help='JSON config with indexes section')
    args = parser.parse_args()

    # Read config
    with open(args.config) as f:
        config = json.load(f)

    indexes = config.get('indexes', {})
    if not indexes:
        print("No indexes specified in config. Skipping index building.")
        return

    os.makedirs(args.output_dir, exist_ok=True)

    total_built = 0
    for table_name, columns in indexes.items():
        parquet_path = os.path.join(args.parquet_dir, f"{table_name}.parquet")
        if not os.path.exists(parquet_path):
            print(f"Warning: {parquet_path} not found, skipping")
            continue

        print(f"Building indexes for {table_name}:")
        for col_name in columns:
            output_path = os.path.join(args.output_dir, f"{table_name}_{col_name}.idx")
            try:
                if build_sorted_index(parquet_path, col_name, output_path):
                    total_built += 1
            except Exception as e:
                print(f"  Error building index for {table_name}.{col_name}: {e}")

    print(f"\nBuilt {total_built} indexes in {args.output_dir}")


if __name__ == '__main__':
    main()
