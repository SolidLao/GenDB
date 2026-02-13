#!/usr/bin/env python3
"""Convert TPC-H .tbl files to Parquet format using PyArrow.

Usage:
  python3 benchmarks/tpc-h/convert_to_parquet.py --data-dir <path> --output-dir <path> [--config <path>]

Reads pipe-delimited .tbl files and writes Parquet files with workload-optimized settings.
When --config is provided, reads a JSON config (from Workload Analyzer) that specifies
per-table sort orders, row group sizes, compression, and dictionary encoding choices.
Without --config, uses sensible defaults.
"""

import argparse
import json
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

# TPC-H table schemas (fixed — these are the .tbl column definitions)
TPC_H_SCHEMAS = {
    "nation": {
        "columns": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
        "types": [pa.int32(), pa.string(), pa.int32(), pa.string()],
    },
    "region": {
        "columns": ["r_regionkey", "r_name", "r_comment"],
        "types": [pa.int32(), pa.string(), pa.string()],
    },
    "supplier": {
        "columns": ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
        "types": [pa.int32(), pa.string(), pa.string(), pa.int32(), pa.string(), pa.decimal128(15, 2), pa.string()],
    },
    "part": {
        "columns": ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"],
        "types": [pa.int32(), pa.string(), pa.string(), pa.string(), pa.string(), pa.int32(), pa.string(), pa.decimal128(15, 2), pa.string()],
    },
    "partsupp": {
        "columns": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
        "types": [pa.int32(), pa.int32(), pa.int32(), pa.decimal128(15, 2), pa.string()],
    },
    "customer": {
        "columns": ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"],
        "types": [pa.int32(), pa.string(), pa.string(), pa.int32(), pa.string(), pa.decimal128(15, 2), pa.string(), pa.string()],
    },
    "orders": {
        "columns": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"],
        "types": [pa.int32(), pa.int32(), pa.string(), pa.decimal128(15, 2), pa.date32(), pa.string(), pa.string(), pa.int32(), pa.string()],
    },
    "lineitem": {
        "columns": ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"],
        "types": [pa.int32(), pa.int32(), pa.int32(), pa.int32(), pa.decimal128(15, 2), pa.decimal128(15, 2), pa.decimal128(15, 2), pa.decimal128(15, 2), pa.string(), pa.string(), pa.date32(), pa.date32(), pa.date32(), pa.string(), pa.string(), pa.string()],
    },
}

# Defaults when no config is provided
DEFAULTS = {
    "row_group_size": 100_000,
    "compression": "snappy",
    "dictionary_columns": [],  # empty = use pyarrow's auto-detection (use_dictionary=True)
}


def load_config(config_path):
    """Load Parquet config JSON from Workload Analyzer.

    Expected format:
    {
      "default_row_group_size": 100000,
      "default_compression": "snappy",
      "tables": {
        "lineitem": {
          "sort_by": ["l_shipdate"],
          "row_group_size": 100000,
          "compression": "snappy",
          "dictionary_columns": ["l_returnflag", "l_linestatus", "l_shipinstruct", "l_shipmode"],
          "no_dictionary_columns": ["l_comment"]
        },
        ...
      }
    }
    """
    if not config_path or not os.path.exists(config_path):
        return None
    with open(config_path) as f:
        return json.load(f)


def get_table_config(config, table_name):
    """Get per-table Parquet settings, with fallback to defaults."""
    if not config:
        return {
            "sort_by": None,
            "row_group_size": DEFAULTS["row_group_size"],
            "compression": DEFAULTS["compression"],
            "dictionary_columns": None,  # None = auto
            "no_dictionary_columns": [],
        }

    default_rgs = config.get("default_row_group_size", DEFAULTS["row_group_size"])
    default_comp = config.get("default_compression", DEFAULTS["compression"])

    tc = config.get("tables", {}).get(table_name, {})
    sort_by = tc.get("sort_by", None)
    # sort_by can be a string or list; normalize to list
    if isinstance(sort_by, str):
        sort_by = [sort_by]

    return {
        "sort_by": sort_by,
        "row_group_size": tc.get("row_group_size", default_rgs),
        "compression": tc.get("compression", default_comp),
        "dictionary_columns": tc.get("dictionary_columns", None),
        "no_dictionary_columns": tc.get("no_dictionary_columns", []),
    }


def convert_table(tbl_path, parquet_path, schema_info, table_config):
    """Convert a single .tbl file to Parquet with workload-optimized settings."""
    columns = schema_info["columns"]
    arrow_types = schema_info["types"]

    # TPC-H .tbl files are pipe-delimited with trailing pipe
    read_options = pcsv.ReadOptions(column_names=columns + ["_trailing"])
    parse_options = pcsv.ParseOptions(delimiter="|")

    # Build convert options for proper types
    column_types = {}
    for name, typ in zip(columns, arrow_types):
        if pa.types.is_date(typ):
            column_types[name] = pa.string()  # read as string, convert after
        elif pa.types.is_decimal(typ):
            column_types[name] = typ
        elif pa.types.is_int32(typ):
            column_types[name] = pa.int32()
        else:
            column_types[name] = pa.string()
    column_types["_trailing"] = pa.string()

    convert_options = pcsv.ConvertOptions(column_types=column_types)

    table = pcsv.read_csv(tbl_path, read_options=read_options, parse_options=parse_options, convert_options=convert_options)
    table = table.drop(["_trailing"])

    # Convert string date columns to date32
    for name, typ in zip(columns, arrow_types):
        if pa.types.is_date(typ):
            str_col = table.column(name)
            date_col = pc.cast(pc.strptime(str_col, format="%Y-%m-%d", unit="s"), pa.date32())
            idx = table.schema.get_field_index(name)
            table = table.set_column(idx, pa.field(name, pa.date32()), date_col)

    # Sort by configured column(s) for row group pruning
    sort_by = table_config["sort_by"]
    if sort_by:
        sort_keys = [(col, "ascending") for col in sort_by]
        indices = pc.sort_indices(table, sort_keys=sort_keys)
        table = table.take(indices)

    # Build per-column dictionary settings
    # use_dictionary accepts: bool, or list of column name strings to enable dictionary for
    dict_cols = table_config["dictionary_columns"]
    no_dict_cols = table_config["no_dictionary_columns"]
    if dict_cols is not None:
        # Explicit list of columns to dictionary-encode
        use_dictionary = dict_cols
    elif no_dict_cols:
        # All columns except excluded ones
        use_dictionary = [col for col in columns if col not in no_dict_cols]
    else:
        use_dictionary = True  # pyarrow auto-detection

    # Write Parquet
    pq.write_table(
        table,
        parquet_path,
        row_group_size=table_config["row_group_size"],
        compression=table_config["compression"],
        write_statistics=True,
        use_dictionary=use_dictionary,
    )

    return len(table), table_config


def main():
    parser = argparse.ArgumentParser(description="Convert TPC-H .tbl files to Parquet")
    parser.add_argument("--data-dir", required=True, help="Directory containing .tbl files")
    parser.add_argument("--output-dir", required=True, help="Output directory for .parquet files")
    parser.add_argument("--config", default=None, help="Path to parquet_config.json from Workload Analyzer")
    args = parser.parse_args()

    data_dir = args.data_dir
    parquet_dir = args.output_dir

    if not os.path.isdir(data_dir):
        print(f"Error: data directory not found: {data_dir}")
        return 1

    config = load_config(args.config)
    if config:
        print(f"Using Parquet config from: {args.config}")
    else:
        print("No config provided — using defaults")

    os.makedirs(parquet_dir, exist_ok=True)
    print(f"Converting .tbl → Parquet: {data_dir} → {parquet_dir}")

    total_start = time.time()
    total_rows = 0

    for table_name, schema_info in TPC_H_SCHEMAS.items():
        tbl_path = os.path.join(data_dir, f"{table_name}.tbl")
        parquet_path = os.path.join(parquet_dir, f"{table_name}.parquet")

        if not os.path.exists(tbl_path):
            print(f"  Skipping {table_name} (no .tbl file)")
            continue

        tc = get_table_config(config, table_name)
        start = time.time()
        rows, used_config = convert_table(tbl_path, parquet_path, schema_info, tc)
        elapsed = time.time() - start
        total_rows += rows

        details = []
        if used_config["sort_by"]:
            details.append(f"sorted by {','.join(used_config['sort_by'])}")
        details.append(f"rg={used_config['row_group_size']}")
        details.append(used_config["compression"])
        print(f"  {table_name}: {rows:,} rows ({elapsed:.1f}s) [{', '.join(details)}]")

    total_elapsed = time.time() - total_start
    print(f"\nDone: {total_rows:,} total rows in {total_elapsed:.1f}s")
    print(f"Parquet files: {parquet_dir}/")
    return 0


if __name__ == "__main__":
    exit(main())
