#!/usr/bin/env python3
"""
TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB

Runs TPC-H Q1, Q3, Q6 on all three systems and plots execution time comparison.
For GenDB, runs all iterations (baseline + optimizations) and uses best performance.

Usage:
    python3 benchmarks/tpc-h/benchmark.py --sf <N> [options]

Options:
    --sf <N>              Scale factor (required, e.g., 1, 10)
    --gendb-run <path>    Path to GenDB run directory (default: output/tpc-h/latest/)
    --data-dir <path>     Path to TPC-H data directory (default: benchmarks/tpc-h/data/sf{N})
    --runs <N>            Number of runs per query (default: 3)
    --setup               Force database setup/reload (default: skip if DB exists)
    --output <path>       Output plot path (default: results/sf{N}/figures/benchmark_results_per_query.png)

Output:
    - benchmark_results_per_query.png: Per-query comparison across all systems
    - benchmark_results_total.png: Total execution time comparison across all systems
    - benchmark_results_gendb_iterations.png: GenDB performance evolution (total + per-query)

Prerequisites:
    - TPC-H data generated: bash benchmarks/tpc-h/setup_data.sh <SF>
    - GenDB pipeline executed: node src/gendb/orchestrator.mjs --sf <SF> --max-iterations <N>
    - PostgreSQL running with accessible 'postgres' user
    - Python packages: psycopg2, duckdb, matplotlib, pandas
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

# ---------------------------------------------------------------------------
# TPC-H queries (standard SQL, compatible with both PostgreSQL and DuckDB)
# ---------------------------------------------------------------------------

QUERIES = {
    "Q1": """
        SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            AVG(l_quantity) AS avg_qty,
            AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc,
            COUNT(*) AS count_order
        FROM lineitem
        WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    "Q3": """
        SELECT
            l_orderkey,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue,
            o_orderdate,
            o_shippriority
        FROM customer, orders, lineitem
        WHERE
            c_mktsegment = 'BUILDING'
            AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate < DATE '1995-03-15'
            AND l_shipdate > DATE '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10
    """,
    "Q6": """
        SELECT
            SUM(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE
            l_shipdate >= DATE '1994-01-01'
            AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
            AND l_quantity < 24
    """,
}

# ---------------------------------------------------------------------------
# PostgreSQL benchmark
# ---------------------------------------------------------------------------

def get_pg_conn_params(scale_factor: int) -> dict:
    """Get PostgreSQL connection parameters for the given scale factor."""
    return {
        "host": "/var/run/postgresql",
        "port": 5433,
        "user": "postgres",
        "dbname": f"tpch_sf{scale_factor}",
    }


def _strip_fk_constraints(schema_sql: str) -> str:
    """Remove FK constraints, SQL comments, and fix trailing commas so DDL is valid."""
    # Remove SQL comments
    result = re.sub(r"--[^\n]*", "", schema_sql)
    # Remove standalone FOREIGN KEY ... REFERENCES ... (must come first)
    result = re.sub(r",?\s*FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s+\w+\([^)]+\)", "", result)
    # Remove inline REFERENCES clauses
    result = re.sub(r"\s*REFERENCES\s+\w+\([^)]+\)", "", result)
    # Fix trailing comma before closing paren:  ", )" → " )"
    result = re.sub(r",\s*\)", "\n)", result)
    return result


def pg_database_exists(scale_factor: int) -> bool:
    """Check if PostgreSQL database exists and has data."""
    try:
        conn_params = get_pg_conn_params(scale_factor)
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lineitem")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count > 0
    except Exception:
        return False


def pg_setup(data_dir: Path, scale_factor: int, force_setup: bool = False):
    """Create TPC-H tables in PostgreSQL and load data from .tbl files."""
    dbname = f"tpch_sf{scale_factor}"

    # Check if database exists and has data
    if not force_setup and pg_database_exists(scale_factor):
        print(f"  Database '{dbname}' already exists with data, skipping setup.")
        print(f"  Use --setup flag to force data reload.")
        return

    schema_path = Path(__file__).parent / "schema.sql"
    schema_sql = schema_path.read_text()

    # Connect to postgres database to create/drop target database
    conn_default = psycopg2.connect(
        host="/var/run/postgresql",
        port=5433,
        user="postgres",
        dbname="postgres",
    )
    conn_default.autocommit = True
    cur_default = conn_default.cursor()

    # Drop and recreate database
    print(f"  Creating database '{dbname}'...")
    cur_default.execute(f"DROP DATABASE IF EXISTS {dbname}")
    cur_default.execute(f"CREATE DATABASE {dbname}")
    cur_default.close()
    conn_default.close()

    # Connect to the new database
    conn_params = get_pg_conn_params(scale_factor)
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cur = conn.cursor()

    # Create tables (without FK constraints for faster loading)
    schema_no_fk = _strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            cur.execute(stmt)

    # Load data using COPY
    # TPC-H .tbl files have a trailing pipe on each line; strip it for PostgreSQL COPY
    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        import io
        # Read file, strip trailing pipe from each line, feed to COPY
        with open(tbl_file, "r") as f:
            cleaned = io.StringIO()
            for line in f:
                line = line.rstrip("\n")
                if line.endswith("|"):
                    line = line[:-1]
                cleaned.write(line + "\n")
            cleaned.seek(0)
            cur.copy_expert(
                f"COPY {table} FROM STDIN WITH (DELIMITER '|')",
                cleaned,
            )
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")

    cur.close()
    conn.close()


def pg_benchmark(scale_factor: int, num_runs: int) -> dict:
    """Run queries on PostgreSQL and return timing results."""
    conn_params = get_pg_conn_params(scale_factor)
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    results = {}
    for qname, sql in QUERIES.items():
        times = []
        for _ in range(num_runs):
            start = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            elapsed = (time.perf_counter() - start) * 1000  # ms
            times.append(elapsed)
        results[qname] = times
        print(f"  PostgreSQL {qname}: {min(times):.1f} ms (best of {num_runs})")

    cur.close()
    conn.close()
    return results


# ---------------------------------------------------------------------------
# DuckDB benchmark
# ---------------------------------------------------------------------------


def duckdb_setup(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """Create TPC-H tables in DuckDB and load data from .tbl files."""
    conn = duckdb.connect()

    schema_path = Path(__file__).parent / "schema.sql"
    schema_sql = schema_path.read_text()

    schema_no_fk = _strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            conn.execute(stmt)

    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        conn.execute(
            f"COPY {table} FROM '{tbl_file}' (DELIMITER '|')"
        )
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f" {count:,} rows")

    return conn


def duckdb_benchmark(conn: duckdb.DuckDBPyConnection, num_runs: int) -> dict:
    """Run queries on DuckDB and return timing results."""
    results = {}
    for qname, sql in QUERIES.items():
        times = []
        for _ in range(num_runs):
            start = time.perf_counter()
            conn.execute(sql).fetchall()
            elapsed = (time.perf_counter() - start) * 1000  # ms
            times.append(elapsed)
        results[qname] = times
        print(f"  DuckDB {qname}: {min(times):.1f} ms (best of {num_runs})")

    return results


# ---------------------------------------------------------------------------
# GenDB (compiled C++) benchmark
# ---------------------------------------------------------------------------


def gendb_benchmark_single(gendb_bin: Path, data_dir: Path, num_runs: int) -> dict:
    """Run a single GenDB compiled C++ binary and parse timing from its output."""
    results = {}

    for _ in range(num_runs):
        proc = subprocess.run(
            [str(gendb_bin), str(data_dir)],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if proc.returncode != 0:
            print(f"  GenDB error: {proc.stderr}")
            return {}

        # Parse timing: look for pattern "Execution time: <N> ms"
        # paired with query headers like "=== Q1: ..."
        current_query = None
        for line in proc.stdout.splitlines():
            if "Q1" in line and "===" in line:
                current_query = "Q1"
            elif "Q3" in line and "===" in line:
                current_query = "Q3"
            elif "Q6" in line and "===" in line:
                current_query = "Q6"

            match = re.search(r"Execution time:\s*([\d.]+)\s*ms", line, re.IGNORECASE)
            if match and current_query:
                ms = float(match.group(1))
                if current_query not in results:
                    results[current_query] = []
                results[current_query].append(ms)
                current_query = None

    return results


def gendb_benchmark_all_iterations(run_dir: Path, data_dir: Path, num_runs: int) -> dict:
    """
    Run all GenDB iterations (baseline + optimizations) and return performance history.
    Returns:
    {
      "baseline": {"Q1": [times], "Q3": [times], "Q6": [times]},
      "iterations": [
        {"iteration": 1, "Q1": [times], "Q3": [times], "Q6": [times]},
        {"iteration": 2, ...},
      ],
      "best": {"Q1": [times], "Q3": [times], "Q6": [times]}  # best overall
    }
    """
    history = {"baseline": {}, "iterations": [], "best": {}}

    # Run baseline
    baseline_bin = run_dir / "generated" / "main"
    if baseline_bin.exists():
        print("  Running baseline...")
        baseline_results = gendb_benchmark_single(baseline_bin, data_dir, num_runs)
        history["baseline"] = baseline_results
        for qname in QUERIES:
            if qname in baseline_results:
                print(f"    Baseline {qname}: {min(baseline_results[qname]):.1f} ms (best of {num_runs})")

    # Run all optimization iterations
    iterations_dir = run_dir / "iterations"
    if iterations_dir.exists():
        iter_dirs = sorted([d for d in iterations_dir.iterdir() if d.is_dir() and d.name.isdigit()],
                          key=lambda d: int(d.name))

        for iter_dir in iter_dirs:
            iter_num = int(iter_dir.name)
            iter_bin = iter_dir / "generated" / "main"

            if iter_bin.exists():
                print(f"  Running iteration {iter_num}...")
                iter_results = gendb_benchmark_single(iter_bin, data_dir, num_runs)

                iter_entry = {"iteration": iter_num}
                for qname in QUERIES:
                    if qname in iter_results:
                        iter_entry[qname] = iter_results[qname]
                        print(f"    Iteration {iter_num} {qname}: {min(iter_results[qname]):.1f} ms (best of {num_runs})")

                history["iterations"].append(iter_entry)

    # Determine best overall performance per query
    all_results = [history["baseline"]] + history["iterations"]
    for qname in QUERIES:
        best_times = []
        for result in all_results:
            if qname in result and isinstance(result[qname], list):
                best_times.extend(result[qname])
        if best_times:
            history["best"][qname] = [min(best_times)]  # Keep as list for consistency

    return history


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def plot_gendb_iterations(gendb_history: dict, output_path: Path, scale_factor: str = "?"):
    """
    Create a two-subplot figure showing GenDB's performance evolution:
    - Left: Total execution time across iterations
    - Right: Per-query execution time across iterations
    """
    queries = ["Q1", "Q3", "Q6"]
    colors = {"Q1": "#2196F3", "Q3": "#4CAF50", "Q6": "#FF9800", "Total": "#9C27B0"}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Prepare data: baseline + iterations
    labels = ["Baseline"]
    baseline_data = {q: min(gendb_history["baseline"].get(q, [0])) if gendb_history["baseline"].get(q) else 0
                     for q in queries}

    iter_data = []
    for iter_entry in gendb_history["iterations"]:
        iter_num = iter_entry["iteration"]
        labels.append(f"Iter {iter_num}")
        iter_data.append({
            q: min(iter_entry.get(q, [0])) if iter_entry.get(q) else 0
            for q in queries
        })

    x_pos = range(len(labels))

    # Left subplot: Total execution time
    total_values = [sum(baseline_data.values())]
    for d in iter_data:
        total_values.append(sum(d.values()))

    ax1.plot(x_pos, total_values, marker='o', linewidth=2.5, markersize=10,
             color=colors["Total"], label="Total")

    # Add value labels
    for x, y in zip(x_pos, total_values):
        if y > 0:
            ax1.text(x, y * 1.02, f"{y:.1f}", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax1.set_xlabel("Iteration", fontsize=12)
    ax1.set_ylabel("Total Execution Time (ms)", fontsize=12)
    ax1.set_title(f"Total Execution Time Evolution (SF={scale_factor})", fontsize=13, fontweight="bold")
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(labels, fontsize=11)
    ax1.grid(axis="y", alpha=0.3)

    # Right subplot: Per-query execution time
    for q in queries:
        y_values = [baseline_data[q]] + [d[q] for d in iter_data]
        ax2.plot(x_pos, y_values, marker='o', linewidth=2, markersize=8,
                label=q, color=colors[q])

        # Add value labels
        for x, y in zip(x_pos, y_values):
            if y > 0:
                ax2.text(x, y * 1.05, f"{y:.0f}", ha="center", va="bottom", fontsize=9)

    ax2.set_xlabel("Iteration", fontsize=12)
    ax2.set_ylabel("Execution Time (ms)", fontsize=12)
    ax2.set_title(f"Per-Query Execution Time Evolution (SF={scale_factor})", fontsize=13, fontweight="bold")
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(labels, fontsize=11)
    ax2.legend(fontsize=11)
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"GenDB iteration plot saved to: {output_path}")


def plot_results(all_results: dict, output_path: Path, scale_factor: str = "?"):
    """Create a grouped bar chart comparing execution times."""
    queries = list(QUERIES.keys())
    systems = list(all_results.keys())
    colors = {"GenDB": "#2196F3", "PostgreSQL": "#4CAF50", "DuckDB": "#FF9800"}

    # Use best (minimum) time for each query
    data = {}
    for system in systems:
        data[system] = []
        for q in queries:
            times = all_results[system].get(q, [])
            data[system].append(min(times) if times else 0)

    fig, ax = plt.subplots(figsize=(10, 6))

    x = range(len(queries))
    width = 0.25
    offsets = [i * width for i in range(len(systems))]

    for i, system in enumerate(systems):
        bars = ax.bar(
            [xi + offsets[i] for xi in x],
            data[system],
            width,
            label=system,
            color=colors.get(system, "#999999"),
        )
        # Add value labels on bars
        for bar, val in zip(bars, data[system]):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.5,
                    f"{val:.1f}",
                    ha="center",
                    va="bottom",
                    fontsize=9,
                )

    ax.set_xlabel("TPC-H Query", fontsize=12)
    ax.set_ylabel("Execution Time (ms)", fontsize=12)
    ax.set_title(f"TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB (SF={scale_factor})", fontsize=14)
    ax.set_xticks([xi + width for xi in x])
    ax.set_xticklabels(queries, fontsize=11)
    ax.legend(fontsize=11)
    ax.grid(axis="y", alpha=0.3)

    # Use log scale for per-query plot to handle large differences
    ax.set_yscale("log")
    ax.set_ylabel("Execution Time (ms, log scale)", fontsize=12)
    ax.set_title(f"TPC-H Per-Query Execution Time (SF={scale_factor})", fontsize=14)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"\nPer-query plot saved to: {output_path}")

    # Create a second plot showing total execution time
    fig2, ax2 = plt.subplots(figsize=(8, 6))

    # Calculate total execution time for each system
    totals = {}
    for system in systems:
        totals[system] = sum(data[system])

    x_pos = range(len(systems))
    bars = ax2.bar(
        x_pos,
        [totals[s] for s in systems],
        color=[colors.get(s, "#999999") for s in systems],
    )

    # Add value labels on bars
    for bar, system in zip(bars, systems):
        val = totals[system]
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() * 1.02,
            f"{val:.1f} ms",
            ha="center",
            va="bottom",
            fontsize=10,
            fontweight="bold",
        )

    ax2.set_xlabel("System", fontsize=12)
    ax2.set_ylabel("Total Execution Time (ms, log scale)", fontsize=12)
    ax2.set_title(f"TPC-H Total Execution Time (SF={scale_factor})", fontsize=14)
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(systems, fontsize=11)
    ax2.set_yscale("log")
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    # Remove "_per_query" suffix if present for total plot
    base_name = output_path.stem.replace("_per_query", "")
    total_path = output_path.with_name(base_name + "_total" + output_path.suffix)
    plt.savefig(total_path, dpi=150)
    print(f"Total execution time plot saved to: {total_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB")
    parser.add_argument(
        "--sf",
        type=int,
        required=True,
        help="Scale factor (e.g., 1, 10)",
    )
    parser.add_argument(
        "--gendb-run",
        type=Path,
        default=None,
        help="Path to GenDB run directory with iterations (default: auto-detect from output/tpc-h/latest/)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Path to TPC-H .tbl data files (default: benchmarks/tpc-h/data/sf{N})",
    )
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per query (default: 3)")
    parser.add_argument("--setup", action="store_true", help="Force database setup/reload (default: skip if DB exists)")
    parser.add_argument("--output", type=Path, default=None, help="Output plot path (default: results/sf{N}/figures/benchmark_results.png)")
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent.parent
    benchmark_root = Path(__file__).parent

    # Set default data directory to data/sf{N}
    if args.data_dir is None:
        args.data_dir = benchmark_root / "data" / f"sf{args.sf}"

    data_dir = args.data_dir.resolve()
    if not (data_dir / "lineitem.tbl").exists():
        print(f"Error: TPC-H data not found at {data_dir}")
        print(f"Run: bash benchmarks/tpc-h/setup_data.sh {args.sf}")
        sys.exit(1)

    # Set default output path to results/sf{N}/figures/
    if args.output is None:
        results_dir = benchmark_root / "results" / f"sf{args.sf}" / "figures"
        results_dir.mkdir(parents=True, exist_ok=True)
        args.output = results_dir / "benchmark_results_per_query.png"

    # Auto-detect GenDB run directory
    if args.gendb_run is None:
        latest_link = project_root / "output" / "tpc-h" / "latest"
        if latest_link.exists() and latest_link.is_dir():
            args.gendb_run = latest_link
        else:
            print("Warning: GenDB run directory not found. Run the GenDB pipeline first.")
            print(f"  Expected at: {latest_link}")

    print(f"Scale factor:   {args.sf}")
    print(f"Data directory: {data_dir}")
    print(f"Runs per query: {args.runs}")
    print(f"Force setup:    {args.setup}")
    print()

    all_results = {}
    gendb_history = None

    # --- GenDB ---
    if args.gendb_run and args.gendb_run.exists():
        print(f"=== GenDB ({args.gendb_run}) ===")
        gendb_history = gendb_benchmark_all_iterations(args.gendb_run, data_dir, args.runs)
        if gendb_history and gendb_history["best"]:
            all_results["GenDB"] = gendb_history["best"]
            print(f"\n  Best overall performance:")
            for qname in QUERIES:
                if qname in gendb_history["best"]:
                    print(f"    {qname}: {min(gendb_history['best'][qname]):.1f} ms")
    else:
        print("=== GenDB: SKIPPED (run directory not found) ===")

    # --- PostgreSQL ---
    print("\n=== PostgreSQL Setup ===")
    try:
        pg_setup(data_dir, args.sf, args.setup)
        print("\n=== PostgreSQL Benchmark ===")
        all_results["PostgreSQL"] = pg_benchmark(args.sf, args.runs)
    except Exception as e:
        print(f"  PostgreSQL error: {e}")

    # --- DuckDB ---
    print("\n=== DuckDB Setup ===")
    try:
        duck_conn = duckdb_setup(data_dir)
        print("\n=== DuckDB Benchmark ===")
        all_results["DuckDB"] = duckdb_benchmark(duck_conn, args.runs)
        duck_conn.close()
    except Exception as e:
        print(f"  DuckDB error: {e}")

    # --- Results summary ---
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY (best of N runs, in ms)")
    print("=" * 60)
    queries = list(QUERIES.keys())
    header = f"{'Query':<8}" + "".join(f"{s:<15}" for s in all_results.keys())
    print(header)
    print("-" * len(header))
    for q in queries:
        row = f"{q:<8}"
        for system in all_results:
            times = all_results[system].get(q, [])
            if times:
                row += f"{min(times):<15.2f}"
            else:
                row += f"{'N/A':<15}"
        print(row)
    print()

    # --- Save JSON results ---
    metrics_dir = benchmark_root / "results" / f"sf{args.sf}" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    json_path = metrics_dir / "benchmark_results.json"
    json_data = {}
    for system, results in all_results.items():
        json_data[system] = {q: {"all_ms": times, "best_ms": min(times)} for q, times in results.items()}
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    print(f"Results saved to: {json_path}")

    # --- Plot system comparison ---
    if len(all_results) >= 2:
        plot_results(all_results, args.output, scale_factor=str(args.sf))
    else:
        print("Not enough systems to plot (need at least 2).")

    # --- Plot GenDB iteration evolution ---
    if gendb_history and (gendb_history["baseline"] or gendb_history["iterations"]):
        # Remove "_per_query" suffix if present for iterations plot
        base_name = args.output.stem.replace("_per_query", "")
        iter_plot_path = args.output.with_name(base_name + "_gendb_iterations" + args.output.suffix)
        plot_gendb_iterations(gendb_history, iter_plot_path, scale_factor=str(args.sf))
    else:
        print("No GenDB iteration data available for evolution plot.")


if __name__ == "__main__":
    main()
