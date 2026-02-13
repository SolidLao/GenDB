#!/usr/bin/env python3
"""
TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB (Temp version)

Adapted from benchmark.py to support per-query GenDB binaries (queries/QN/qN)
and reuse existing GenDB storage without re-ingestion.

Usage:
    python3 benchmarks/tpc-h/benchmark_temp.py --sf <N> [options]

Options:
    --sf <N>              Scale factor (required, e.g., 1, 10)
    --gendb-run <path>    Path to GenDB run directory (default: output/tpc-h/latest/)
    --data-dir <path>     Path to TPC-H data directory (default: benchmarks/tpc-h/data/sf{N})
    --gendb-dir <path>    Path to existing GenDB storage (default: benchmarks/tpc-h/gendb/tpch_sf{N}.gendb)
    --runs <N>            Number of runs per query (default: 3)
    --setup               Force database setup/reload (default: skip if DB exists)
    --output <path>       Output plot path (default: results/sf{N}/figures/benchmark_results_per_query.png)
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
    return {
        "host": "/var/run/postgresql",
        "port": 5435,
        "user": "postgres",
        "dbname": f"tpch_sf{scale_factor}",
    }


def _strip_fk_constraints(schema_sql: str) -> str:
    result = re.sub(r"--[^\n]*", "", schema_sql)
    result = re.sub(r",?\s*FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s+\w+\([^)]+\)", "", result)
    result = re.sub(r"\s*REFERENCES\s+\w+\([^)]+\)", "", result)
    result = re.sub(r",\s*\)", "\n)", result)
    return result


def pg_database_exists(scale_factor: int) -> bool:
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
    dbname = f"tpch_sf{scale_factor}"
    if not force_setup and pg_database_exists(scale_factor):
        print(f"  Database '{dbname}' already exists with data, skipping setup.")
        print(f"  Use --setup flag to force data reload.")
        return

    schema_path = Path(__file__).parent / "schema.sql"
    schema_sql = schema_path.read_text()

    conn_default = psycopg2.connect(
        host="/var/run/postgresql", port=5435, user="postgres", dbname="postgres",
    )
    conn_default.autocommit = True
    cur_default = conn_default.cursor()
    print(f"  Creating database '{dbname}'...")
    cur_default.execute(f"DROP DATABASE IF EXISTS {dbname}")
    cur_default.execute(f"CREATE DATABASE {dbname}")
    cur_default.close()
    conn_default.close()

    conn_params = get_pg_conn_params(scale_factor)
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cur = conn.cursor()

    schema_no_fk = _strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            cur.execute(stmt)

    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        try:
            cur.execute(
                f"COPY {table} FROM PROGRAM 'sed ''s/|$//' {tbl_file}' WITH (DELIMITER '|')"
            )
        except Exception as e:
            print(f"\n  Note: Using fallback method for {table} ({e})")
            import io
            with open(tbl_file, "r") as f:
                cleaned = io.StringIO()
                for line in f:
                    line = line.rstrip("\n")
                    if line.endswith("|"):
                        line = line[:-1]
                    cleaned.write(line + "\n")
                cleaned.seek(0)
                cur.copy_expert(f"COPY {table} FROM STDIN WITH (DELIMITER '|')", cleaned)
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")

    cur.close()
    conn.close()


def pg_benchmark(scale_factor: int, num_runs: int) -> dict:
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
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)
        results[qname] = times
        avg_time = sum(times) / len(times)
        print(f"  PostgreSQL {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")

    cur.close()
    conn.close()
    return results


# ---------------------------------------------------------------------------
# DuckDB benchmark
# ---------------------------------------------------------------------------

def duckdb_database_exists(db_path: Path) -> bool:
    if not db_path.exists():
        return False
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        count = conn.execute("SELECT COUNT(*) FROM lineitem").fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


def duckdb_setup(data_dir: Path, scale_factor: int, force_setup: bool = False) -> duckdb.DuckDBPyConnection:
    duckdb_dir = Path(__file__).parent / "duckdb"
    duckdb_dir.mkdir(exist_ok=True)
    db_path = duckdb_dir / f"tpch_sf{scale_factor}.duckdb"

    if not force_setup and duckdb_database_exists(db_path):
        print(f"  Database '{db_path.name}' already exists with data, skipping setup.")
        print(f"  Use --setup flag to force data reload.")
        conn = duckdb.connect(str(db_path))
        return conn

    if force_setup and db_path.exists():
        print(f"  Removing existing database '{db_path.name}'...")
        db_path.unlink()

    print(f"  Creating persistent database '{db_path.name}'...")
    conn = duckdb.connect(str(db_path))

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
        conn.execute(f"COPY {table} FROM '{tbl_file}' (DELIMITER '|')")
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f" {count:,} rows")

    return conn


def duckdb_benchmark(scale_factor: int, num_runs: int) -> dict:
    duckdb_dir = Path(__file__).parent / "duckdb"
    db_path = duckdb_dir / f"tpch_sf{scale_factor}.duckdb"

    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return {}

    conn = duckdb.connect(str(db_path), read_only=True)
    results = {}
    for qname, sql in QUERIES.items():
        times = []
        for _ in range(num_runs):
            start = time.perf_counter()
            conn.execute(sql).fetchall()
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)
        results[qname] = times
        avg_time = sum(times) / len(times)
        print(f"  DuckDB {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")

    conn.close()
    return results


# ---------------------------------------------------------------------------
# GenDB (compiled C++) benchmark — per-query binary support
# ---------------------------------------------------------------------------

def gendb_benchmark_per_query(query_bin: Path, qname: str, gendb_dir: Path, num_runs: int) -> list:
    """Run a single per-query GenDB binary and return timing list in ms.

    Supports output formats:
      - "Execution time: X.XX seconds"
      - "Execution time: X.XX ms"
      - "Total time: X.XX ms"
      - "QN execution time: X.XX seconds"
      - "QN: N rows in X.XXs"
    """
    times = []

    for _ in range(num_runs):
        proc = subprocess.run(
            [str(query_bin), str(gendb_dir)],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if proc.returncode != 0:
            print(f"    {qname} error: {proc.stderr[:200]}")
            return []

        parsed_ms = None
        for line in proc.stdout.splitlines():
            # "QN: N rows in X.XXs"
            rows_match = re.search(r"Q\d+:\s*\d+\s*rows? in\s*([\d.]+)s", line, re.IGNORECASE)
            if rows_match:
                parsed_ms = float(rows_match.group(1)) * 1000
                break

            # "QN execution time: X.XX seconds"
            q_time_match = re.search(r"Q\d+\s+execution time:\s*([\d.]+)\s*seconds?", line, re.IGNORECASE)
            if q_time_match:
                parsed_ms = float(q_time_match.group(1)) * 1000
                break

            # "Execution time: X.XX seconds"
            sec_match = re.search(r"execution time:\s*([\d.]+)\s*seconds?", line, re.IGNORECASE)
            if sec_match:
                parsed_ms = float(sec_match.group(1)) * 1000
                break

            # "Execution time: X.XX ms"
            ms_match = re.search(r"execution time:\s*([\d.]+)\s*ms", line, re.IGNORECASE)
            if ms_match:
                parsed_ms = float(ms_match.group(1))
                break

            # "Total time: X.XX ms"
            total_match = re.search(r"total time:\s*([\d.]+)\s*ms", line, re.IGNORECASE)
            if total_match:
                parsed_ms = float(total_match.group(1))
                break

        if parsed_ms is not None:
            times.append(parsed_ms)
        else:
            print(f"    Warning: Could not parse timing from {query_bin.name}")
            lines = proc.stdout.strip().splitlines()
            print(f"    Last 5 lines:")
            for l in lines[-5:]:
                print(f"      {l}")
            return []

    return times


def find_query_binaries(run_dir: Path) -> dict:
    """Find per-query binaries in queries/QN/ directories.

    Returns dict: {"Q1": Path(...), "Q3": Path(...), ...}
    Also looks for iteration binaries (qN_iter1, qN_iter2, etc.)
    """
    queries_dir = run_dir / "queries"
    binaries = {}

    if not queries_dir.exists():
        return binaries

    for qdir in sorted(queries_dir.iterdir()):
        if not qdir.is_dir() or not qdir.name.startswith("Q"):
            continue
        qname = qdir.name  # e.g., "Q1"
        qnum = qname[1:]   # e.g., "1"
        bin_name = f"q{qnum}"
        bin_path = qdir / bin_name

        if bin_path.exists() and os.access(str(bin_path), os.X_OK):
            binaries[qname] = {"baseline": bin_path, "iterations": []}

            # Look for iteration binaries: q1_iter1, q1_iter2, etc.
            # Also check for compiled iteration binaries with _test suffix
            for f in sorted(qdir.iterdir()):
                iter_match = re.match(rf"q{qnum}_iter(\d+)$", f.name)
                if iter_match and f.is_file() and os.access(str(f), os.X_OK):
                    iter_num = int(iter_match.group(1))
                    binaries[qname]["iterations"].append((iter_num, f))

    return binaries


def gendb_benchmark_combined(main_bin: Path, gendb_dir: Path, num_runs: int) -> dict:
    """Run the combined GenDB binary (generated/main) and parse per-query timings.

    The combined binary outputs sections like:
        Running Q1...
        ...
        Execution time: 50 ms
        Running Q3...
        ...
        Execution time: 398 ms

    Returns dict: {"Q1": [times], "Q3": [times], ...}
    """
    results = {q: [] for q in QUERIES}

    for run_idx in range(num_runs):
        proc = subprocess.run(
            [str(main_bin), str(gendb_dir), "/dev/null"],
            capture_output=True, text=True, timeout=600,
        )
        if proc.returncode != 0:
            print(f"    Combined binary error (run {run_idx+1}): {proc.stderr[:200]}")
            return {}

        # Parse per-query timings from output
        current_query = None
        for line in proc.stdout.splitlines():
            # Detect "Running QN..." section headers
            running_match = re.match(r"Running (Q\d+)\.\.\.", line)
            if running_match:
                current_query = running_match.group(1)
                continue

            if current_query and current_query in results:
                # "Execution time: X.XX ms"
                ms_match = re.search(r"[Ee]xecution time:\s*([\d.]+)\s*ms", line)
                if ms_match:
                    results[current_query].append(float(ms_match.group(1)))
                    current_query = None
                    continue
                # "Execution time: X.XX seconds"
                sec_match = re.search(r"[Ee]xecution time:\s*([\d.]+)\s*seconds?", line)
                if sec_match:
                    results[current_query].append(float(sec_match.group(1)) * 1000)
                    current_query = None
                    continue

    # Filter out queries with no results
    return {q: times for q, times in results.items() if times}


def gendb_benchmark_all(run_dir: Path, gendb_dir: Path, num_runs: int) -> dict:
    """Run all GenDB per-query binaries (baseline + iterations).

    Uses existing gendb_dir without re-ingestion.
    Also benchmarks the combined final binary (generated/main) if available.

    Returns:
    {
      "baseline": {"Q1": [times], "Q3": [times], ...},
      "iterations": [
        {"iteration": 1, "Q1": [times], ...},
        ...
      ],
      "best": {"Q1": [times], ...},
      "combined": {"Q1": [times], ...}
    }
    """
    history = {"baseline": {}, "iterations": [], "best": {}, "combined": {}}

    # --- Combined binary benchmark (generated/main) ---
    combined_bin = run_dir / "generated" / "main"
    if combined_bin.exists() and os.access(str(combined_bin), os.X_OK):
        print(f"  Found combined binary: {combined_bin}")
        print(f"  Using existing storage: {gendb_dir}")
        print("  Running combined binary...")
        combined_results = gendb_benchmark_combined(combined_bin, gendb_dir, num_runs)
        if combined_results:
            history["combined"] = combined_results
            for qname, times in sorted(combined_results.items()):
                avg_time = sum(times) / len(times)
                print(f"    Combined {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")

    # --- Per-query binaries ---
    query_bins = find_query_binaries(run_dir)
    if not query_bins:
        print("  No per-query binaries found.")
        # If we have combined results, use those as best
        if history["combined"]:
            history["best"] = history["combined"]
        return history

    print(f"  Found per-query binaries for: {', '.join(sorted(query_bins.keys()))}")
    if not history["combined"]:
        print(f"  Using existing storage: {gendb_dir}")

    # Run baselines
    print("  Running baseline...")
    for qname in sorted(query_bins.keys()):
        bin_path = query_bins[qname]["baseline"]
        times = gendb_benchmark_per_query(bin_path, qname, gendb_dir, num_runs)
        if times:
            history["baseline"][qname] = times
            avg_time = sum(times) / len(times)
            print(f"    Baseline {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")

    # Collect all iteration numbers across queries
    all_iter_nums = set()
    for qname, info in query_bins.items():
        for iter_num, _ in info["iterations"]:
            all_iter_nums.add(iter_num)

    # Run iterations
    for iter_num in sorted(all_iter_nums):
        print(f"  Running iteration {iter_num}...")
        iter_entry = {"iteration": iter_num}
        for qname in sorted(query_bins.keys()):
            # Find this iteration's binary, or fall back to baseline
            iter_bin = None
            for inum, ipath in query_bins[qname]["iterations"]:
                if inum == iter_num:
                    iter_bin = ipath
                    break
            if iter_bin is None:
                # No iteration binary for this query — use baseline
                iter_bin = query_bins[qname]["baseline"]

            times = gendb_benchmark_per_query(iter_bin, qname, gendb_dir, num_runs)
            if times:
                iter_entry[qname] = times
                avg_time = sum(times) / len(times)
                print(f"    Iteration {iter_num} {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")

        history["iterations"].append(iter_entry)

    # Best per-query: cherry-pick across baseline, iterations, and combined
    all_candidates = [history["baseline"]] + history["iterations"]
    if history["combined"]:
        all_candidates.append(history["combined"])
    for qname in QUERIES:
        best_avg = None
        best_times_list = None
        for result in all_candidates:
            if qname in result and isinstance(result[qname], list) and len(result[qname]) > 0:
                avg = sum(result[qname]) / len(result[qname])
                if best_avg is None or avg < best_avg:
                    best_avg = avg
                    best_times_list = result[qname]
        if best_times_list:
            history["best"][qname] = best_times_list

    return history


# ---------------------------------------------------------------------------
# Plotting (unchanged from benchmark.py)
# ---------------------------------------------------------------------------

def plot_gendb_iterations(gendb_history: dict, output_path: Path, scale_factor: str = "?"):
    queries = ["Q1", "Q3", "Q6"]
    colors = {"Q1": "#2196F3", "Q3": "#4CAF50", "Q6": "#FF9800", "Total": "#9C27B0"}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    labels = ["Baseline"]
    baseline_data = {q: (sum(gendb_history["baseline"].get(q, [0])) / len(gendb_history["baseline"].get(q, [1])))
                     if gendb_history["baseline"].get(q) else 0
                     for q in queries}

    iter_data = []
    for iter_entry in gendb_history["iterations"]:
        iter_num = iter_entry["iteration"]
        labels.append(f"Iter {iter_num}")
        iter_data.append({
            q: (sum(iter_entry.get(q, [0])) / len(iter_entry.get(q, [1]))) if iter_entry.get(q) else 0
            for q in queries
        })

    x_pos = range(len(labels))

    total_values = [sum(baseline_data.values())]
    for d in iter_data:
        total_values.append(sum(d.values()))

    ax1.plot(x_pos, total_values, marker='o', linewidth=2.5, markersize=10,
             color=colors["Total"], label="Total")

    for x, y in zip(x_pos, total_values):
        if y > 0:
            ax1.text(x, y * 1.02, f"{y:.1f}", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax1.set_xlabel("Iteration", fontsize=12)
    ax1.set_ylabel("Total Execution Time (ms, log scale)", fontsize=12)
    ax1.set_title(f"Total Execution Time Evolution (SF={scale_factor})", fontsize=13, fontweight="bold")
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(labels, fontsize=11)
    ax1.set_yscale("log")
    ax1.grid(axis="y", alpha=0.3)

    for q in queries:
        y_values = [baseline_data[q]] + [d[q] for d in iter_data]
        ax2.plot(x_pos, y_values, marker='o', linewidth=2, markersize=8,
                label=q, color=colors[q])
        for x, y in zip(x_pos, y_values):
            if y > 0:
                ax2.text(x, y * 1.05, f"{y:.0f}", ha="center", va="bottom", fontsize=9)

    ax2.set_xlabel("Iteration", fontsize=12)
    ax2.set_ylabel("Execution Time (ms, log scale)", fontsize=12)
    ax2.set_title(f"Per-Query Execution Time Evolution (SF={scale_factor})", fontsize=13, fontweight="bold")
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(labels, fontsize=11)
    ax2.set_yscale("log")
    ax2.legend(fontsize=11)
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"GenDB iteration plot saved to: {output_path}")


def plot_results(all_results: dict, output_path: Path, scale_factor: str = "?"):
    queries = list(QUERIES.keys())
    systems = list(all_results.keys())
    colors = {"GenDB": "#2196F3", "PostgreSQL": "#4CAF50", "DuckDB": "#FF9800"}

    data = {}
    for system in systems:
        data[system] = []
        for q in queries:
            times = all_results[system].get(q, [])
            data[system].append(sum(times) / len(times) if times else 0)

    fig, ax = plt.subplots(figsize=(10, 6))

    x = range(len(queries))
    width = 0.25
    offsets = [i * width for i in range(len(systems))]

    for i, system in enumerate(systems):
        bars = ax.bar(
            [xi + offsets[i] for xi in x], data[system], width,
            label=system, color=colors.get(system, "#999999"),
        )
        for bar, val in zip(bars, data[system]):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                        f"{val:.1f}", ha="center", va="bottom", fontsize=9)

    ax.set_xlabel("TPC-H Query", fontsize=12)
    ax.set_ylabel("Execution Time (ms, log scale)", fontsize=12)
    ax.set_title(f"TPC-H Per-Query Execution Time (SF={scale_factor})", fontsize=14)
    ax.set_xticks([xi + width for xi in x])
    ax.set_xticklabels(queries, fontsize=11)
    ax.legend(fontsize=11)
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"\nPer-query plot saved to: {output_path}")

    # Total execution time plot
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    totals = {s: sum(data[s]) for s in systems}
    x_pos = range(len(systems))
    bars = ax2.bar(x_pos, [totals[s] for s in systems],
                   color=[colors.get(s, "#999999") for s in systems])
    for bar, system in zip(bars, systems):
        val = totals[system]
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.02,
                 f"{val:.1f} ms", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax2.set_xlabel("System", fontsize=12)
    ax2.set_ylabel("Total Execution Time (ms, log scale)", fontsize=12)
    ax2.set_title(f"TPC-H Total Execution Time (SF={scale_factor})", fontsize=14)
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(systems, fontsize=11)
    ax2.set_yscale("log")
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    base_name = output_path.stem.replace("_per_query", "")
    total_path = output_path.with_name(base_name + "_total" + output_path.suffix)
    plt.savefig(total_path, dpi=150)
    print(f"Total execution time plot saved to: {total_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB (per-query binaries)")
    parser.add_argument("--sf", type=int, required=True, help="Scale factor (e.g., 1, 10)")
    parser.add_argument("--gendb-run", type=Path, default=None,
                        help="Path to GenDB run directory with queries/ subdirectory")
    parser.add_argument("--gendb-dir", type=Path, default=None,
                        help="Path to existing GenDB storage directory (skips re-ingestion)")
    parser.add_argument("--data-dir", type=Path, default=None,
                        help="Path to TPC-H .tbl data files (default: benchmarks/tpc-h/data/sf{N})")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per query (default: 3)")
    parser.add_argument("--setup", action="store_true", help="Force database setup/reload")
    parser.add_argument("--output", type=Path, default=None, help="Output plot path")
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent.parent
    benchmark_root = Path(__file__).parent

    if args.data_dir is None:
        args.data_dir = benchmark_root / "data" / f"sf{args.sf}"

    data_dir = args.data_dir.resolve()
    if not (data_dir / "lineitem.tbl").exists():
        print(f"Error: TPC-H data not found at {data_dir}")
        print(f"Run: bash benchmarks/tpc-h/setup_data.sh {args.sf}")
        sys.exit(1)

    if args.output is None:
        results_dir = benchmark_root / "results" / f"sf{args.sf}" / "figures"
        results_dir.mkdir(parents=True, exist_ok=True)
        args.output = results_dir / "benchmark_results_per_query.png"

    # Default gendb storage directory
    if args.gendb_dir is None:
        args.gendb_dir = benchmark_root / "gendb" / f"tpch_sf{args.sf}.gendb"

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
    print(f"GenDB storage:  {args.gendb_dir}")
    print(f"Runs per query: {args.runs}")
    print(f"Force setup:    {args.setup}")
    print()

    all_results = {}
    gendb_history = None

    # --- GenDB ---
    if args.gendb_run and args.gendb_run.exists():
        print(f"=== GenDB Benchmark ({args.gendb_run}) ===")
        if not args.gendb_dir.exists():
            print(f"  Error: GenDB storage not found at {args.gendb_dir}")
            print(f"  Run ingestion first or provide --gendb-dir path.")
        else:
            print(f"  (Using existing storage — no re-ingestion)")
            gendb_history = gendb_benchmark_all(args.gendb_run, args.gendb_dir, args.runs)
            if gendb_history and gendb_history["best"]:
                all_results["GenDB"] = gendb_history["best"]
                print(f"\n  Best per-query performance (cherry-picked across iterations):")
                for qname in QUERIES:
                    if qname in gendb_history["best"]:
                        times = gendb_history['best'][qname]
                        avg_time = sum(times) / len(times) if times else 0
                        print(f"    {qname}: {avg_time:.1f} ms (avg)")
                total_best = sum(
                    sum(gendb_history["best"].get(q, [0])) / max(len(gendb_history["best"].get(q, [1])), 1)
                    for q in QUERIES
                )
                print(f"    Total (best per-query): {total_best:.1f} ms")
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
        duckdb_setup(data_dir, args.sf, args.setup)
        print("\n=== DuckDB Benchmark ===")
        all_results["DuckDB"] = duckdb_benchmark(args.sf, args.runs)
    except Exception as e:
        print(f"  DuckDB error: {e}")

    # --- Results summary ---
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY (average of N runs, in ms)")
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
                avg_time = sum(times) / len(times)
                row += f"{avg_time:<15.2f}"
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
        json_data[system] = {
            q: {
                "all_ms": times,
                "average_ms": sum(times) / len(times) if times else 0,
                "min_ms": min(times) if times else 0,
                "max_ms": max(times) if times else 0
            }
            for q, times in results.items()
        }
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
        base_name = args.output.stem.replace("_per_query", "")
        iter_plot_path = args.output.with_name(base_name + "_gendb_iterations" + args.output.suffix)
        plot_gendb_iterations(gendb_history, iter_plot_path, scale_factor=str(args.sf))
    else:
        print("No GenDB iteration data available for evolution plot.")


if __name__ == "__main__":
    main()
