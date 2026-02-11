#!/usr/bin/env python3
"""Generate ground truth query results for TPC-H using DuckDB.

Reads schema.sql and queries.sql, loads .tbl data, runs each query,
and saves results to benchmarks/tpc-h/query_results/Q<N>.csv.

Usage:
    python3 benchmarks/tpc-h/generate_ground_truth.py --sf 1
    python3 benchmarks/tpc-h/generate_ground_truth.py --sf 10 --data-dir /path/to/data
"""

import argparse
import csv
import os
import re
import sys

import duckdb


def parse_queries(queries_path):
    """Parse queries.sql into a dict of {query_name: sql}."""
    with open(queries_path, "r") as f:
        content = f.read()

    queries = {}
    # Split on query comments like "-- Q1: ..." or "-- Q3: ..."
    parts = re.split(r"--\s*(Q\d+):\s*([^\n]*)\n", content)
    # parts[0] is preamble, then groups of (name, description, sql)
    i = 1
    while i + 2 <= len(parts):
        name = parts[i].strip()
        sql = parts[i + 2].strip()
        # Remove trailing comments and empty lines
        sql = re.sub(r"--[^\n]*$", "", sql, flags=re.MULTILINE).strip()
        if sql and not sql.endswith(";"):
            sql += ";"
        if sql:
            queries[name] = sql
        i += 3

    return queries


def load_tpch_data(con, schema_path, data_dir):
    """Load TPC-H schema and data from .tbl files into DuckDB."""
    # Define TPC-H tables with their columns (avoids parsing schema.sql constraints)
    table_defs = {
        "nation": "n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR",
        "region": "r_regionkey INTEGER, r_name VARCHAR, r_comment VARCHAR",
        "supplier": "s_suppkey INTEGER, s_name VARCHAR, s_address VARCHAR, s_nationkey INTEGER, s_phone VARCHAR, s_acctbal DECIMAL(15,2), s_comment VARCHAR",
        "part": "p_partkey INTEGER, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, p_type VARCHAR, p_size INTEGER, p_container VARCHAR, p_retailprice DECIMAL(15,2), p_comment VARCHAR",
        "partsupp": "ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER, ps_supplycost DECIMAL(15,2), ps_comment VARCHAR",
        "customer": "c_custkey INTEGER, c_name VARCHAR, c_address VARCHAR, c_nationkey INTEGER, c_phone VARCHAR, c_acctbal DECIMAL(15,2), c_mktsegment VARCHAR, c_comment VARCHAR",
        "orders": "o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus VARCHAR, o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority INTEGER, o_comment VARCHAR",
        "lineitem": "l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR",
    }

    # Load order matters (no FK enforcement needed in DuckDB for this)
    load_order = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]

    for table in load_order:
        tbl_path = os.path.join(data_dir, f"{table}.tbl")
        if not os.path.exists(tbl_path):
            print(f"  Warning: {tbl_path} not found, skipping {table}")
            continue
        print(f"  Loading {table} from {tbl_path}...")
        con.execute(f"CREATE TABLE {table} ({table_defs[table]})")
        con.execute(
            f"COPY {table} FROM '{tbl_path}' (DELIMITER '|', HEADER false, AUTO_DETECT false)"
        )
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"    {table}: {count:,} rows")


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H ground truth results")
    parser.add_argument("--sf", type=int, default=1, help="Scale factor (default: 1)")
    parser.add_argument("--data-dir", type=str, help="Path to .tbl files directory")
    parser.add_argument(
        "--output-dir", type=str, help="Output directory for query results"
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(script_dir, "schema.sql")
    queries_path = os.path.join(script_dir, "queries.sql")

    if args.data_dir:
        data_dir = args.data_dir
    else:
        data_dir = os.path.join(script_dir, "data", f"sf{args.sf}")

    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = os.path.join(script_dir, "query_results")

    if not os.path.exists(data_dir):
        print(f"Error: data directory not found: {data_dir}")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    print(f"Scale factor: {args.sf}")
    print(f"Data directory: {data_dir}")
    print(f"Output directory: {output_dir}")

    # Create in-memory DuckDB and load data
    con = duckdb.connect(":memory:")
    print("\nLoading TPC-H data...")
    load_tpch_data(con, schema_path, data_dir)

    # Parse and run queries
    queries = parse_queries(queries_path)
    print(f"\nFound {len(queries)} queries: {', '.join(sorted(queries.keys()))}")

    for name, sql in sorted(queries.items()):
        print(f"\nRunning {name}...")
        try:
            result = con.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            output_path = os.path.join(output_dir, f"{name}.csv")
            with open(output_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                for row in rows:
                    # Convert values: round floats to 2 decimal places
                    processed = []
                    for val in row:
                        if isinstance(val, float):
                            processed.append(f"{val:.2f}")
                        elif val is None:
                            processed.append("")
                        else:
                            processed.append(str(val))
                    writer.writerow(processed)

            print(f"  {name}: {len(rows)} rows -> {output_path}")
        except Exception as e:
            print(f"  {name}: ERROR - {e}")

    con.close()
    print(f"\nGround truth generated in: {output_dir}")


if __name__ == "__main__":
    main()
