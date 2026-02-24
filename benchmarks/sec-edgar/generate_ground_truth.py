#!/usr/bin/env python3
"""Generate ground truth query results for SEC EDGAR benchmark using DuckDB.

Reads queries.sql, executes each query against the DuckDB database,
and saves results to query_results/Q<N>.csv.

Usage:
    python3 benchmarks/sec-edgar/generate_ground_truth.py
    python3 benchmarks/sec-edgar/generate_ground_truth.py --db-path /path/to/db
"""

import argparse
import csv
import os
import re
import sys
from pathlib import Path

import duckdb


def parse_queries(queries_path):
    """Parse queries.sql into a dict of {query_name: sql}."""
    with open(queries_path, "r") as f:
        content = f.read()

    queries = {}
    # Split on query comments like "-- Q1: ..."
    parts = re.split(r"--\s*(Q\d+):\s*([^\n]*)\n", content)
    # parts[0] is preamble, then groups of (name, description, sql)
    i = 1
    while i + 2 <= len(parts):
        name = parts[i].strip()
        sql = parts[i + 2].strip()
        # Remove trailing comments and empty lines
        sql = re.sub(r"--[^\n]*$", "", sql, flags=re.MULTILINE).strip()
        # Remove trailing semicolons (DuckDB doesn't need them)
        sql = sql.rstrip(";").strip()
        if sql:
            queries[name] = sql
        i += 3

    return queries


def main():
    parser = argparse.ArgumentParser(description="Generate SEC EDGAR ground truth results")
    parser.add_argument("--db-path", type=Path, default=None,
                        help="Path to DuckDB database")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Output directory for query results")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    db_path = args.db_path or (script_dir / "duckdb" / "sec_edgar.duckdb")
    queries_path = script_dir / "queries.sql"
    output_dir = args.output_dir or (script_dir / "query_results")

    if not db_path.exists():
        print(f"Error: database not found at {db_path}")
        print("Run: python3 benchmarks/sec-edgar/load_data.py")
        sys.exit(1)

    if not queries_path.exists():
        print(f"Error: queries file not found at {queries_path}")
        print("Run: python3 benchmarks/sec-edgar/generate_queries.py")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Database: {db_path}")
    print(f"Queries: {queries_path}")
    print(f"Output: {output_dir}")

    con = duckdb.connect(str(db_path), read_only=True)

    queries = parse_queries(queries_path)
    print(f"\nFound {len(queries)} queries: "
          f"{', '.join(sorted(queries.keys(), key=lambda q: int(q[1:])))}")

    for name, sql in sorted(queries.items(), key=lambda x: int(x[0][1:])):
        print(f"\nRunning {name}...")
        try:
            result = con.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            output_path = output_dir / f"{name}.csv"
            with open(output_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                for row in rows:
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
