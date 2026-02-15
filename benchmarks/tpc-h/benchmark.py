#!/usr/bin/env python3
"""
TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB

Usage:
    python3 benchmarks/tpc-h/benchmark.py --sf <N> [options]

Options:
    --sf <N>              Scale factor (required, e.g., 1, 10)
    --gendb-run <path>    Path to GenDB run directory (default: output/tpc-h/latest/)
    --data-dir <path>     Path to TPC-H data directory (default: benchmarks/tpc-h/data/sf{N})
    --runs <N>            Number of runs per query (default: 3)
    --setup               Force database setup/reload (default: skip if DB exists)
    --output <path>       Output plot path (default: results/sf{N}/figures/benchmark_results_per_query.png)
    --gendb-only          Skip PostgreSQL and DuckDB benchmarks
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psycopg2

# ANSI color codes
GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

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
        conn = psycopg2.connect(**get_pg_conn_params(scale_factor))
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

    conn = psycopg2.connect(**get_pg_conn_params(scale_factor))
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


def pg_benchmark(scale_factor: int, num_runs: int, query_ids: list) -> dict:
    conn = psycopg2.connect(**get_pg_conn_params(scale_factor))
    cur = conn.cursor()
    results = {}
    for qname in query_ids:
        if qname not in QUERIES:
            continue
        times = []
        for _ in range(num_runs):
            start = time.perf_counter()
            cur.execute(QUERIES[qname])
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


def duckdb_setup(data_dir: Path, scale_factor: int, force_setup: bool = False):
    duckdb_dir = Path(__file__).parent / "duckdb"
    duckdb_dir.mkdir(exist_ok=True)
    db_path = duckdb_dir / f"tpch_sf{scale_factor}.duckdb"

    if not force_setup and duckdb_database_exists(db_path):
        print(f"  Database '{db_path.name}' already exists with data, skipping setup.")
        print(f"  Use --setup flag to force data reload.")
        return

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

    conn.close()


def duckdb_benchmark(scale_factor: int, num_runs: int, query_ids: list) -> dict:
    duckdb_dir = Path(__file__).parent / "duckdb"
    db_path = duckdb_dir / f"tpch_sf{scale_factor}.duckdb"
    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return {}
    conn = duckdb.connect(str(db_path), read_only=True)
    results = {}
    for qname in query_ids:
        if qname not in QUERIES:
            continue
        times = []
        for _ in range(num_runs):
            start = time.perf_counter()
            conn.execute(QUERIES[qname]).fetchall()
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)
        results[qname] = times
        avg_time = sum(times) / len(times)
        print(f"  DuckDB {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")
    conn.close()
    return results


# ---------------------------------------------------------------------------
# GenDB: read iteration history from execution_results.json
# ---------------------------------------------------------------------------

def read_gendb_iteration_history(run_dir: Path) -> dict:
    """Read iteration timing/validation from execution_results.json files.

    Returns:
    {
      "query_ids": ["Q1", "Q3", "Q6"],
      "num_iters": 11,
      "data": {
        "Q1": {0: {"timing_ms": 65.13, "validation": "pass"}, 1: {...}, ...},
        "Q3": {0: {...}, ...},
        ...
      },
      "best": {"Q1": {"iter": 4, "timing_ms": 61.06}, ...}
    }
    """
    queries_dir = run_dir / "queries"
    if not queries_dir.exists():
        return {"query_ids": [], "num_iters": 0, "data": {}, "best": {}}

    query_ids = sorted([d.name for d in queries_dir.iterdir()
                        if d.is_dir() and d.name.startswith("Q")])

    data = {}
    max_iter = -1
    for qid in query_ids:
        data[qid] = {}
        i = 0
        while True:
            exec_path = queries_dir / qid / f"iter_{i}" / "execution_results.json"
            if not exec_path.exists():
                break
            with open(exec_path) as f:
                exec_data = json.load(f)
            data[qid][i] = {
                "timing_ms": exec_data.get("timing_ms"),
                "validation": exec_data.get("validation", {}).get("status", "unknown"),
            }
            if i > max_iter:
                max_iter = i
            i += 1

    # Compute best valid timing per query
    best = {}
    for qid in query_ids:
        best_t = None
        best_i = None
        for i, entry in data[qid].items():
            if entry["validation"] == "pass" and entry["timing_ms"] is not None:
                if best_t is None or entry["timing_ms"] < best_t:
                    best_t = entry["timing_ms"]
                    best_i = i
        if best_t is not None:
            best[qid] = {"iter": best_i, "timing_ms": best_t}

    return {
        "query_ids": query_ids,
        "num_iters": max_iter + 1,
        "data": data,
        "best": best,
    }


def print_iteration_table(history: dict):
    """Print a colored iteration table to the terminal."""
    query_ids = history["query_ids"]
    num_iters = history["num_iters"]
    data = history["data"]
    best = history["best"]

    if not query_ids or num_iters == 0:
        print("  No iteration data available.")
        return

    COL_W = 10
    Q_COL_W = 7

    # Header
    header_cells = [f"{'Query':<{Q_COL_W}}"]
    for i in range(num_iters):
        header_cells.append(f"{'Iter ' + str(i):>{COL_W}}")
    header_cells.append(f"{'Best':>{COL_W}}")
    header = " | ".join(header_cells)
    sep = "-" * Q_COL_W + "-+-" + ("-" * COL_W + "-+-") * num_iters + "-" * COL_W + "-"

    print(header)
    print(sep)

    for qid in query_ids:
        best_iter = best.get(qid, {}).get("iter")

        # Timing row
        cells = [f"{qid:<{Q_COL_W}}"]
        for i in range(num_iters):
            entry = data.get(qid, {}).get(i)
            if entry and entry["timing_ms"] is not None:
                t = entry["timing_ms"]
                v = entry["validation"]
                cell = f"{int(round(t))} ms"
                formatted = f"{cell:>{COL_W}}"
                if v == "pass":
                    if i == best_iter:
                        cells.append(f"{BOLD}{GREEN}{formatted}{RESET}")
                    else:
                        cells.append(f"{GREEN}{formatted}{RESET}")
                else:
                    cells.append(f"{RED}{formatted}{RESET}")
            else:
                cells.append(f"{'-':>{COL_W}}")
        # Best column
        if qid in best:
            cell = f"{int(round(best[qid]['timing_ms']))} ms"
            cells.append(f"{BOLD}{GREEN}{cell:>{COL_W}}{RESET}")
        else:
            cells.append(f"{'-':>{COL_W}}")
        print(" | ".join(cells))

        # Validation row
        cells = [f"{'':>{Q_COL_W}}"]
        for i in range(num_iters):
            entry = data.get(qid, {}).get(i)
            if entry:
                v = entry["validation"]
                formatted = f"{v.upper():>{COL_W}}"
                if v == "pass":
                    cells.append(f"{GREEN}{formatted}{RESET}")
                else:
                    cells.append(f"{RED}{formatted}{RESET}")
            else:
                cells.append(f"{'-':>{COL_W}}")
        if qid in best:
            cells.append(f"{GREEN}{'PASS':>{COL_W}}{RESET}")
        else:
            cells.append(f"{'-':>{COL_W}}")
        print(" | ".join(cells))

    # Total row (sum of best valid timings)
    if best:
        total = sum(b["timing_ms"] for b in best.values())
        print(sep)
        cells = [f"{'Total':<{Q_COL_W}}"]
        for _ in range(num_iters):
            cells.append(f"{'':>{COL_W}}")
        cells.append(f"{BOLD}{int(round(total))} ms{RESET}".rjust(COL_W + len(BOLD) + len(RESET)))
        # Simpler: just build it manually
        total_cell = f"{int(round(total))} ms"
        cells[-1] = f"{BOLD}{total_cell:>{COL_W}}{RESET}"
        print(" | ".join(cells))


# ---------------------------------------------------------------------------
# GenDB: benchmark best per-query binaries
# ---------------------------------------------------------------------------

def parse_timing_output(stdout: str) -> dict:
    """Parse [TIMING] lines from GenDB binary output."""
    timings = {}
    for line in stdout.splitlines():
        m = re.match(r"\[TIMING\]\s+(\w+):\s+([\d.]+)\s*ms", line)
        if m:
            timings[m.group(1)] = float(m.group(2))
    if "total" in timings:
        timings["query_ms"] = timings["total"] - timings.get("output", 0)
    return timings


def find_best_binaries(run_dir: Path) -> dict:
    """Find the best per-query binary from run.json's bestCppPath.
    Returns {"Q1": Path("/path/to/q1"), ...}
    """
    run_json_path = run_dir / "run.json"
    if not run_json_path.exists():
        return {}

    with open(run_json_path) as f:
        run_data = json.load(f)

    pipelines = run_data.get("phase2", {}).get("pipelines", {})
    binaries = {}
    for qid, info in pipelines.items():
        cpp_path = info.get("bestCppPath")
        if cpp_path:
            iter_dir = Path(cpp_path).parent
            binary = iter_dir / qid.lower()  # Q1 -> q1
            if binary.exists() and os.access(str(binary), os.X_OK):
                binaries[qid] = binary

    return binaries


def gendb_benchmark_best(run_dir: Path, gendb_dir: Path, num_runs: int) -> dict:
    """Execute the best per-query GenDB binaries and return timing results.
    Returns {"Q1": [times], "Q3": [times], ...}
    """
    best_binaries = find_best_binaries(run_dir)
    if not best_binaries:
        print("  No best binaries found in run.json")
        return {}

    results = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        for qid in sorted(best_binaries):
            binary = best_binaries[qid]
            print(f"  Running {qid} ({binary.parent.name}/{binary.name})...", end="", flush=True)

            times = []
            ok = True
            for _ in range(num_runs):
                proc = subprocess.run(
                    [str(binary), str(gendb_dir), tmpdir],
                    capture_output=True, text=True, timeout=600,
                )
                if proc.returncode != 0:
                    print(f" ERROR: {proc.stderr[:200]}")
                    ok = False
                    break
                timings = parse_timing_output(proc.stdout)
                if "query_ms" in timings:
                    times.append(timings["query_ms"])
                else:
                    print(f" WARNING: no [TIMING] total in output")
                    ok = False
                    break

            if ok and times:
                results[qid] = times
                avg = sum(times) / len(times)
                print(f" {avg:.2f} ms (avg of {num_runs} runs)")
            elif not ok:
                pass  # error already printed

    return results


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

QUERY_COLORS = ["#2196F3", "#4CAF50", "#FF9800", "#E91E63", "#9C27B0", "#00BCD4", "#FF5722", "#795548"]
SYSTEM_COLORS = {"GenDB": "#2196F3", "PostgreSQL": "#4CAF50", "DuckDB": "#FF9800"}


def _compute_best_so_far(data: dict, query_ids: list, num_iters: int) -> dict:
    """For each query, compute the running best (minimum) valid timing up to each iteration.

    Returns:
        {qid: [(iter, timing_ms, status), ...]} where:
        - timing_ms = best valid time from iter 0..i
        - status = "valid" (iteration passed), "fail" (attempted but failed),
                   or "skipped" (not attempted, optimizer stopped early)
    """
    result = {}
    for qid in query_ids:
        series = []
        best_t = None
        for i in range(num_iters):
            entry = data.get(qid, {}).get(i)
            if entry and entry["validation"] == "pass" and entry["timing_ms"] is not None:
                t = entry["timing_ms"]
                if best_t is None or t < best_t:
                    best_t = t
                series.append((i, best_t, "valid"))
            elif entry is not None and best_t is not None:
                # Iteration was attempted but failed validation
                series.append((i, best_t, "fail"))
            elif entry is None and best_t is not None:
                # Iteration not attempted — optimizer skipped this query
                series.append((i, best_t, "skipped"))
            # else: no valid data yet, skip this iteration
        result[qid] = series
    return result


def plot_gendb_iterations(history: dict, output_path: Path, scale_factor: str = "?"):
    """Plot GenDB performance evolution across iterations."""
    query_ids = history["query_ids"]
    data = history["data"]
    num_iters = history["num_iters"]
    if not query_ids or num_iters == 0:
        return

    q_colors = {q: QUERY_COLORS[i % len(QUERY_COLORS)] for i, q in enumerate(query_ids)}
    best_so_far = _compute_best_so_far(data, query_ids, num_iters)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    iter_labels = [f"Iter {i}" for i in range(num_iters)]
    x_pos = list(range(num_iters))

    # --- Left: Total execution time (sum of best-so-far per query at each iteration) ---
    # Only plot iteration i if ALL queries have at least one valid result by iter i
    total_x, total_y = [], []
    for i in range(num_iters):
        total = 0
        all_have_data = True
        for qid in query_ids:
            # Find best-so-far at iteration i
            found = False
            for (si, st, _) in best_so_far.get(qid, []):
                if si == i:
                    total += st
                    found = True
                    break
            if not found:
                all_have_data = False
                break
        if all_have_data:
            total_x.append(i)
            total_y.append(total)

    if total_y:
        ax1.plot(total_x, total_y, marker="o", linewidth=2.5, markersize=10,
                 color="#9C27B0", label="Total")
        for x, y in zip(total_x, total_y):
            ax1.text(x, y * 1.02, f"{y:.0f}", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax1.set_xlabel("Iteration", fontsize=12)
    ax1.set_ylabel("Total Execution Time (ms, log scale)", fontsize=12)
    ax1.set_title(f"Total Execution Time (SF={scale_factor})", fontsize=13, fontweight="bold")
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(iter_labels, fontsize=9, rotation=45 if num_iters > 6 else 0)
    if total_y:
        ax1.set_yscale("log")
    ax1.grid(axis="y", alpha=0.3)

    # --- Right: Per-query execution time (best-so-far with fail/skipped markers) ---
    for qid in query_ids:
        series = best_so_far.get(qid, [])
        if not series:
            continue
        color = q_colors[qid]

        # Draw solid line through attempted iterations (valid + fail)
        attempted = [(x, y) for x, y, s in series if s in ("valid", "fail")]
        if attempted:
            ax2.plot([p[0] for p in attempted], [p[1] for p in attempted],
                     marker="o", linewidth=2, markersize=8, label=qid, color=color)

        # Draw dashed line through skipped iterations (optimizer stopped early)
        # Connect from last attempted point through skipped points
        skipped = [(x, y) for x, y, s in series if s == "skipped"]
        if skipped and attempted:
            # Include the last attempted point as the start of the dashed segment
            dash_x = [attempted[-1][0]] + [p[0] for p in skipped]
            dash_y = [attempted[-1][1]] + [p[1] for p in skipped]
            ax2.plot(dash_x, dash_y, linestyle="--", linewidth=1.5, markersize=0,
                     color=color, alpha=0.4)

        # Overlay red X on failed iterations
        fail_x = [x for x, y, s in series if s == "fail"]
        fail_y = [y for x, y, s in series if s == "fail"]
        if fail_x:
            ax2.scatter(fail_x, fail_y, marker="x", s=120, linewidths=2.5,
                        color="red", zorder=5)

        # Add time labels on valid points only
        for xi, yi, status in series:
            if status == "valid":
                ax2.text(xi, yi * 1.05, f"{yi:.0f}", ha="center", va="bottom", fontsize=9)

    # Add legend entries for fail and skipped markers
    ax2.scatter([], [], marker="x", s=120, linewidths=2.5, color="red", label="Failed iteration")
    ax2.plot([], [], linestyle="--", linewidth=1.5, color="gray", alpha=0.4, label="Not optimized")

    ax2.set_xlabel("Iteration", fontsize=12)
    ax2.set_ylabel("Execution Time (ms, log scale)", fontsize=12)
    ax2.set_title(f"Per-Query Execution Time (SF={scale_factor})", fontsize=13, fontweight="bold")
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(iter_labels, fontsize=9, rotation=45 if num_iters > 6 else 0)
    ax2.set_yscale("log")
    ax2.legend(fontsize=10)
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  GenDB iteration plot saved to: {output_path}")


def plot_results(all_results: dict, output_path: Path, scale_factor: str = "?"):
    """Create per-query and total execution time comparison plots."""
    queries = sorted(set(q for results in all_results.values() for q in results))
    systems = list(all_results.keys())

    data = {}
    for system in systems:
        data[system] = []
        for q in queries:
            times = all_results[system].get(q, [])
            data[system].append(sum(times) / len(times) if times else 0)

    # Per-query grouped bar chart
    fig, ax = plt.subplots(figsize=(10, 6))
    x = range(len(queries))
    width = 0.8 / max(len(systems), 1)
    offsets = [(i - len(systems) / 2 + 0.5) * width for i in range(len(systems))]

    for i, system in enumerate(systems):
        bars = ax.bar(
            [xi + offsets[i] for xi in x], data[system], width,
            label=system, color=SYSTEM_COLORS.get(system, "#999999"),
        )
        for bar, val in zip(bars, data[system]):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                        f"{val:.1f}", ha="center", va="bottom", fontsize=9)

    ax.set_xlabel("TPC-H Query", fontsize=12)
    ax.set_ylabel("Execution Time (ms, log scale)", fontsize=12)
    ax.set_title(f"TPC-H Per-Query Execution Time (SF={scale_factor})", fontsize=14)
    ax.set_xticks(list(x))
    ax.set_xticklabels(queries, fontsize=11)
    ax.legend(fontsize=11)
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Per-query plot saved to: {output_path}")

    # Total execution time bar chart
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    totals = {s: sum(data[s]) for s in systems}
    x_pos = range(len(systems))
    bars = ax2.bar(x_pos, [totals[s] for s in systems],
                   color=[SYSTEM_COLORS.get(s, "#999999") for s in systems])
    for bar, system in zip(bars, systems):
        val = totals[system]
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.02,
                 f"{val:.1f} ms", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax2.set_xlabel("System", fontsize=12)
    ax2.set_ylabel("Total Execution Time (ms, log scale)", fontsize=12)
    ax2.set_title(f"TPC-H Total Execution Time (SF={scale_factor})", fontsize=14)
    ax2.set_xticks(list(x_pos))
    ax2.set_xticklabels(systems, fontsize=11)
    ax2.set_yscale("log")
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    base_name = output_path.stem.replace("_per_query", "")
    total_path = output_path.with_name(base_name + "_total" + output_path.suffix)
    plt.savefig(total_path, dpi=150)
    plt.close()
    print(f"  Total plot saved to: {total_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB")
    parser.add_argument("--sf", type=int, required=True, help="Scale factor (e.g., 1, 10)")
    parser.add_argument("--gendb-run", type=Path, default=None,
                        help="Path to GenDB run directory (default: output/tpc-h/latest/)")
    parser.add_argument("--data-dir", type=Path, default=None,
                        help="Path to TPC-H .tbl data files (default: benchmarks/tpc-h/data/sf{N})")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per query (default: 3)")
    parser.add_argument("--setup", action="store_true", help="Force database setup/reload")
    parser.add_argument("--output", type=Path, default=None, help="Output plot path")
    parser.add_argument("--gendb-only", action="store_true",
                        help="Skip PostgreSQL and DuckDB benchmarks")
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent.parent
    benchmark_root = Path(__file__).parent

    if args.data_dir is None:
        args.data_dir = benchmark_root / "data" / f"sf{args.sf}"
    data_dir = args.data_dir.resolve()

    if not args.gendb_only and not (data_dir / "lineitem.tbl").exists():
        print(f"Error: TPC-H data not found at {data_dir}")
        print(f"Run: bash benchmarks/tpc-h/setup_data.sh {args.sf}")
        sys.exit(1)

    if args.output is None:
        results_dir = benchmark_root / "results" / f"sf{args.sf}" / "figures"
        results_dir.mkdir(parents=True, exist_ok=True)
        args.output = results_dir / "benchmark_results_per_query.png"

    # Auto-detect GenDB run directory
    if args.gendb_run is None:
        latest_link = project_root / "output" / "tpc-h" / "latest"
        if latest_link.exists() and latest_link.is_dir():
            args.gendb_run = latest_link.resolve()

    # Auto-detect GenDB storage directory from run.json
    gendb_dir = None
    if args.gendb_run:
        run_json_path = args.gendb_run / "run.json"
        if run_json_path.exists():
            with open(run_json_path) as f:
                run_data = json.load(f)
            gd = run_data.get("gendbDir", "")
            if gd and Path(gd).exists():
                gendb_dir = Path(gd)
    if gendb_dir is None:
        gendb_dir = benchmark_root / "gendb" / f"tpch_sf{args.sf}.gendb"

    print(f"Scale factor:   {args.sf}")
    print(f"Runs per query: {args.runs}")
    if args.gendb_run:
        print(f"GenDB run:      {args.gendb_run}")
        print(f"GenDB storage:  {gendb_dir}")
    print()

    all_results = {}
    gendb_history = None
    gendb_query_ids = list(QUERIES.keys())

    # --- GenDB: iteration history (from JSON, no re-execution) ---
    if args.gendb_run and args.gendb_run.exists():
        print("=== GenDB Iteration History ===")
        gendb_history = read_gendb_iteration_history(args.gendb_run)
        if gendb_history["query_ids"]:
            gendb_query_ids = gendb_history["query_ids"]
            print()
            print_iteration_table(gendb_history)
            print()

            # Save iteration data for plotting
            metrics_dir = benchmark_root / "results" / f"sf{args.sf}" / "metrics"
            metrics_dir.mkdir(parents=True, exist_ok=True)
            iter_json_path = metrics_dir / "gendb_iteration_history.json"
            # Convert int keys to strings for JSON serialization
            json_data = {}
            for qid, iters in gendb_history["data"].items():
                json_data[qid] = {str(k): v for k, v in iters.items()}
            with open(iter_json_path, "w") as f:
                json.dump({"data": json_data, "best": gendb_history["best"],
                           "query_ids": gendb_history["query_ids"],
                           "num_iters": gendb_history["num_iters"]}, f, indent=2)
            print(f"  Iteration history saved to: {iter_json_path}")

        # --- GenDB: benchmark best binaries (re-execute) ---
        print(f"\n=== GenDB Benchmark (best per-query, {args.runs} runs) ===")
        best_results = gendb_benchmark_best(args.gendb_run, gendb_dir, args.runs)
        if best_results:
            all_results["GenDB"] = best_results
    else:
        print("=== GenDB: SKIPPED (run directory not found) ===")

    if not args.gendb_only:
        # --- PostgreSQL ---
        print("\n=== PostgreSQL Setup ===")
        try:
            pg_setup(data_dir, args.sf, args.setup)
            print("\n=== PostgreSQL Benchmark ===")
            all_results["PostgreSQL"] = pg_benchmark(args.sf, args.runs, gendb_query_ids)
        except Exception as e:
            print(f"  PostgreSQL error: {e}")

        # --- DuckDB ---
        print("\n=== DuckDB Setup ===")
        try:
            duckdb_setup(data_dir, args.sf, args.setup)
            print("\n=== DuckDB Benchmark ===")
            all_results["DuckDB"] = duckdb_benchmark(args.sf, args.runs, gendb_query_ids)
        except Exception as e:
            print(f"  DuckDB error: {e}")

    # --- Results summary ---
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY (average of N runs, in ms)")
    print("=" * 60)
    queries = sorted(set(q for results in all_results.values() for q in results))
    header = f"{'Query':<8}" + "".join(f"{s:<15}" for s in all_results.keys())
    print(header)
    print("-" * len(header))
    for q in queries:
        row = f"{q:<8}"
        for system in all_results:
            times = all_results[system].get(q, [])
            if times:
                row += f"{sum(times) / len(times):<15.2f}"
            else:
                row += f"{'N/A':<15}"
        print(row)
    print("-" * len(header))
    row = f"{'Total':<8}"
    for system in all_results:
        total = sum(
            sum(all_results[system].get(q, [0])) / max(len(all_results[system].get(q, [1])), 1)
            for q in queries
        )
        row += f"{total:<15.2f}"
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
                "max_ms": max(times) if times else 0,
            }
            for q, times in results.items()
        }
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    print(f"Results saved to: {json_path}")

    # --- Plots ---
    if len(all_results) >= 2:
        print()
        plot_results(all_results, args.output, scale_factor=str(args.sf))

    if gendb_history and gendb_history["num_iters"] > 0:
        base_name = args.output.stem.replace("_per_query", "")
        iter_plot_path = args.output.with_name(base_name + "_gendb_iterations" + args.output.suffix)
        plot_gendb_iterations(gendb_history, iter_plot_path, scale_factor=str(args.sf))


if __name__ == "__main__":
    main()
