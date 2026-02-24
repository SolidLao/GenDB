#!/usr/bin/env python3
"""
SEC EDGAR Benchmark: GenDB vs PostgreSQL vs DuckDB vs ClickHouse vs Umbra vs MonetDB

Usage:
    python3 benchmarks/sec-edgar/benchmark.py --years <N> [options]

Options:
    --years <N>           Number of years of data (required, e.g., 3 for 2022-2024)
    --gendb-run <path>    Path to GenDB run directory (default: output/sec-edgar/latest/)
    --mode <hot|cold>     Benchmark mode (default: hot)
                          hot:  4 runs per query, discard first (warmup), avg last 3
                          cold: clear OS cache before each query, 1 run only
    --setup               Force database setup/reload (default: skip if DB exists)
    --output <path>       Output plot path
    --gendb-only          Skip all baseline benchmarks
    --skip-clickhouse     Skip ClickHouse benchmark
    --skip-umbra          Skip Umbra benchmark
    --skip-monetdb        Skip MonetDB benchmark
    --timeout <N>         Per-query timeout in seconds (default: 300)
    --plot-only           Skip all benchmarks; read existing metrics and re-plot figures
"""

import argparse
import io
import json
import os
import re
import shutil
import signal
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

# Number of threads for all systems (set to core count for fair comparison)
BENCHMARK_THREADS = os.cpu_count() or 64


def drop_os_caches():
    """Clear OS page cache so the first query run starts cold."""
    try:
        subprocess.run(
            ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            check=True, capture_output=True, timeout=10,
        )
        print("  OS page cache cleared.")
    except Exception:
        print("  Warning: could not clear OS page cache (need passwordless sudo).")
        print("  Run: sudo bash benchmarks/setup_drop_caches.sh")


def restart_postgresql():
    """Restart PostgreSQL to clear shared_buffers (internal buffer pool)."""
    try:
        subprocess.run(
            ["sudo", "-n", "pg_ctlcluster", "18", "tpch", "restart"],
            check=True, capture_output=True, timeout=30,
        )
        time.sleep(2)
        print("  PostgreSQL restarted (buffer pool cleared).")
    except Exception:
        print("  Warning: could not restart PostgreSQL (need passwordless sudo).")


# ---------------------------------------------------------------------------
# Query loading
# ---------------------------------------------------------------------------

def load_queries(queries_path: Path) -> dict:
    """Load queries from queries.sql file."""
    with open(queries_path, "r") as f:
        content = f.read()

    queries = {}
    parts = re.split(r"--\s*(Q\d+):\s*([^\n]*)\n", content)
    i = 1
    while i + 2 <= len(parts):
        name = parts[i].strip()
        sql = parts[i + 2].strip()
        sql = re.sub(r"--[^\n]*$", "", sql, flags=re.MULTILINE).strip()
        sql = sql.rstrip(";").strip()
        if sql:
            queries[name] = sql
        i += 3

    return queries


# ---------------------------------------------------------------------------
# PostgreSQL benchmark
# ---------------------------------------------------------------------------

def get_pg_conn_params(years: int) -> dict:
    return {
        "host": "/var/run/postgresql",
        "port": 5435,
        "user": "postgres",
        "dbname": f"sec_edgar_{years}y",
    }


def _strip_fk_constraints(schema_sql: str) -> str:
    result = re.sub(r"--[^\n]*", "", schema_sql)
    result = re.sub(r",?\s*FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s+\w+\([^)]+\)", "", result)
    result = re.sub(r"\s*REFERENCES\s+\w+\([^)]+\)", "", result)
    result = re.sub(r",\s*\)", "\n)", result)
    # DuckDB -> PostgreSQL type compatibility
    result = re.sub(r'\bDOUBLE\b', 'DOUBLE PRECISION', result, flags=re.IGNORECASE)
    return result


def pg_database_exists(years: int) -> bool:
    try:
        conn = psycopg2.connect(**get_pg_conn_params(years))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM num")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count > 0
    except Exception:
        return False


def pg_setup(years: int, force_setup: bool = False):
    dbname = f"sec_edgar_{years}y"
    if not force_setup and pg_database_exists(years):
        print(f"  Database '{dbname}' already exists with data, skipping setup.")
        print(f"  Use --setup flag to force data reload.")
        return

    benchmark_root = Path(__file__).parent
    schema_path = benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    db_path = benchmark_root / "duckdb" / "sec_edgar.duckdb"

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

    conn = psycopg2.connect(**get_pg_conn_params(years))
    conn.autocommit = True
    cur = conn.cursor()

    schema_no_fk = _strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            cur.execute(stmt)

    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        cur.close()
        conn.close()
        return

    # Export from DuckDB to CSV, then use psql \copy for fast bulk load
    duck_con = duckdb.connect(str(db_path), read_only=True)
    tmp_dir = benchmark_root / "pg_tmp"
    tmp_dir.mkdir(exist_ok=True)
    for table in ["sub", "tag", "num", "pre"]:
        print(f"  Loading {table}...", end="", flush=True)
        tmp_csv = tmp_dir / f"{table}.csv"
        duck_con.execute(f"COPY {table} TO '{tmp_csv}' (FORMAT CSV, HEADER true)")
        # Use psql \copy — bypasses Python I/O, much faster for large tables
        psql_cmd = (
            f"\\copy {table} FROM '{tmp_csv.resolve()}' WITH (FORMAT CSV, HEADER true)"
        )
        subprocess.run(
            ["psql", "-h", "/var/run/postgresql", "-p", "5435", "-U", "postgres",
             "-d", dbname, "-c", psql_cmd],
            check=True, capture_output=True, timeout=600,
        )
        tmp_csv.unlink()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")
    duck_con.close()
    tmp_dir.rmdir()

    cur.close()
    conn.close()


def pg_benchmark(years: int, mode: str, query_ids: list,
                 queries: dict, timeout: int = 300) -> dict:
    conn = psycopg2.connect(**get_pg_conn_params(years))
    cur = conn.cursor()
    cur.execute(f"SET statement_timeout = '{timeout * 1000}'")
    cur.execute(f"SET max_parallel_workers_per_gather = {BENCHMARK_THREADS}")
    cur.execute(f"SET max_parallel_workers = {BENCHMARK_THREADS}")
    total_runs = 4 if mode == "hot" else 1
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        if mode == "cold":
            drop_os_caches()
        times = []
        timed_out = False
        for _ in range(total_runs):
            try:
                start = time.perf_counter()
                cur.execute(queries[qname])
                cur.fetchall()
                elapsed = (time.perf_counter() - start) * 1000
                times.append(elapsed)
            except psycopg2.errors.QueryCanceled:
                conn.rollback()
                cur.execute(f"SET statement_timeout = '{timeout * 1000}'")
                print(f"  PostgreSQL {qname}: TIMEOUT ({timeout}s)")
                timed_out = True
                break
            except Exception as e:
                conn.rollback()
                cur.execute(f"SET statement_timeout = '{timeout * 1000}'")
                print(f"  PostgreSQL {qname}: ERROR - {e}")
                timed_out = True
                break
        if not timed_out and times:
            measured = times[1:] if mode == "hot" and len(times) > 1 else times
            results[qname] = measured
            avg_time = sum(measured) / len(measured)
            label = f"avg of {len(measured)} hot runs" if mode == "hot" else "cold run"
            print(f"  PostgreSQL {qname}: {avg_time:.1f} ms ({label})")
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
        count = conn.execute("SELECT COUNT(*) FROM num").fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


def duckdb_benchmark(years: int, mode: str, query_ids: list,
                     queries: dict, timeout: int = 300) -> dict:
    benchmark_root = Path(__file__).parent
    db_path = benchmark_root / "duckdb" / "sec_edgar.duckdb"
    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return {}
    conn = duckdb.connect(str(db_path), read_only=True)
    conn.execute(f"SET threads = {BENCHMARK_THREADS}")
    total_runs = 4 if mode == "hot" else 1
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        if mode == "cold":
            drop_os_caches()
        times = []
        timed_out = False
        for _ in range(total_runs):
            try:
                start = time.perf_counter()
                conn.execute(queries[qname]).fetchall()
                elapsed = (time.perf_counter() - start) * 1000
                if elapsed > timeout * 1000:
                    print(f"  DuckDB {qname}: TIMEOUT ({timeout}s)")
                    timed_out = True
                    break
                times.append(elapsed)
            except Exception as e:
                print(f"  DuckDB {qname}: ERROR - {e}")
                timed_out = True
                break
        if not timed_out and times:
            measured = times[1:] if mode == "hot" and len(times) > 1 else times
            results[qname] = measured
            avg_time = sum(measured) / len(measured)
            label = f"avg of {len(measured)} hot runs" if mode == "hot" else "cold run"
            print(f"  DuckDB {qname}: {avg_time:.1f} ms ({label})")
    conn.close()
    return results


# ---------------------------------------------------------------------------
# ClickHouse benchmark
# ---------------------------------------------------------------------------

CLICKHOUSE_TABLES = {
    "sub": ("adsh String, cik Nullable(Int32), name Nullable(String), "
            "sic Nullable(Int32), countryba Nullable(String), stprba Nullable(String), "
            "cityba Nullable(String), countryinc Nullable(String), form Nullable(String), "
            "period Nullable(Int32), fy Nullable(Int32), fp Nullable(String), "
            "filed Nullable(Int32), accepted Nullable(String), prevrpt Nullable(Int32), "
            "nciks Nullable(Int32), afs Nullable(String), wksi Nullable(Int32), "
            "fye Nullable(String), instance Nullable(String)"),
    "num": ("adsh String, tag String, version String, "
            "ddate Nullable(Int32), qtrs Nullable(Int32), uom Nullable(String), "
            "coreg Nullable(String), value Nullable(Float64), footnote Nullable(String)"),
    "tag": ("tag String, version String, custom Nullable(Int32), "
            "abstract Nullable(Int32), datatype Nullable(String), "
            "iord Nullable(String), crdr Nullable(String), "
            "tlabel Nullable(String), doc Nullable(String)"),
    "pre": ("adsh String, report Nullable(Int32), line Nullable(Int32), "
            "stmt Nullable(String), inpth Nullable(Int32), rfile Nullable(String), "
            "tag String, version String, plabel Nullable(String), "
            "negating Nullable(Int32)"),
}

CLICKHOUSE_ORDER_KEYS = {
    "sub": "adsh",
    "num": "(adsh, tag, version)",
    "tag": "(tag, version)",
    "pre": "(adsh, tag, version)",
}


def clickhouse_install():
    ch_dir = Path(__file__).parent / "clickhouse"
    ch_dir.mkdir(exist_ok=True)
    binary = ch_dir / "clickhouse"
    if binary.exists() and os.access(str(binary), os.X_OK):
        print("  ClickHouse binary already installed.")
        return True
    print("  Downloading ClickHouse...")
    try:
        subprocess.run(
            ["bash", "-c", f"cd {ch_dir} && curl -fsSL https://clickhouse.com/ | sh"],
            check=True, capture_output=True, timeout=120,
        )
        if binary.exists():
            print("  ClickHouse installed successfully.")
            return True
        for f in ch_dir.iterdir():
            if f.name.startswith("clickhouse") and os.access(str(f), os.X_OK):
                if f.name != "clickhouse":
                    f.rename(binary)
                print("  ClickHouse installed successfully.")
                return True
        print("  ClickHouse installation failed: binary not found after download.")
        return False
    except Exception as e:
        print(f"  ClickHouse installation failed: {e}")
        return False


def clickhouse_available() -> bool:
    binary = Path(__file__).parent / "clickhouse" / "clickhouse"
    return binary.exists() and os.access(str(binary), os.X_OK)


def clickhouse_server_start(years: int):
    ch_dir = Path(__file__).parent / "clickhouse"
    binary = ch_dir / "clickhouse"
    data_path = ch_dir / f"data_{years}y"
    data_path.mkdir(parents=True, exist_ok=True)
    log_path = ch_dir / f"server_{years}y.log"

    proc = subprocess.Popen(
        [str(binary), "server",
         "--daemon",
         "--",
         f"--path={data_path}",
         "--tcp_port=9000",
         "--http_port=8123",
         "--mysql_port=0",
         "--interserver_http_port=0",
         f"--logger.log={log_path}",
         f"--logger.errorlog={log_path}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for attempt in range(30):
        time.sleep(1)
        try:
            result = subprocess.run(
                [str(binary), "client", "--port", "9000", "-q", "SELECT 1"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                print(f"  ClickHouse server started.")
                return proc
        except Exception:
            pass
    print("  Warning: ClickHouse server may not be ready.")
    return proc


def clickhouse_server_stop(proc):
    ch_dir = Path(__file__).parent / "clickhouse"
    binary = ch_dir / "clickhouse"
    try:
        subprocess.run(
            [str(binary), "client", "--port", "9000", "-q", "SYSTEM SHUTDOWN"],
            capture_output=True, timeout=10,
        )
    except Exception:
        pass
    if proc:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    print("  ClickHouse server stopped.")


def clickhouse_setup(years: int, force_setup: bool = False):
    ch_dir = Path(__file__).parent / "clickhouse"
    binary = ch_dir / "clickhouse"
    benchmark_root = Path(__file__).parent
    db_path = benchmark_root / "duckdb" / "sec_edgar.duckdb"

    def ch_query(sql):
        subprocess.run(
            [str(binary), "client", "--port", "9000", "-q", sql],
            check=True, capture_output=True, text=True, timeout=300,
        )

    if not force_setup:
        try:
            result = subprocess.run(
                [str(binary), "client", "--port", "9000", "-q",
                 "SELECT count() FROM num"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and int(result.stdout.strip()) > 0:
                print("  ClickHouse already has data, skipping setup.")
                return
        except Exception:
            pass

    for table, cols in CLICKHOUSE_TABLES.items():
        order_key = CLICKHOUSE_ORDER_KEYS[table]
        ch_query(f"DROP TABLE IF EXISTS {table}")
        ch_query(f"CREATE TABLE {table} ({cols}) ENGINE = MergeTree() ORDER BY {order_key}")

    # Export from DuckDB and load into ClickHouse
    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return

    duck_con = duckdb.connect(str(db_path), read_only=True)
    for table in ["sub", "tag", "num", "pre"]:
        print(f"  Loading {table}...", end="", flush=True)
        # Export to temporary CSV
        tmp_csv = benchmark_root / "clickhouse" / f"{table}_export.csv"
        duck_con.execute(f"COPY {table} TO '{tmp_csv}' (FORMAT CSV, HEADER false)")
        subprocess.run(
            f"'{binary}' client --port 9000 "
            f"--query='INSERT INTO {table} FORMAT CSV' < '{tmp_csv}'",
            shell=True, check=True, capture_output=True, timeout=600,
        )
        tmp_csv.unlink()
        result = subprocess.run(
            [str(binary), "client", "--port", "9000", "-q",
             f"SELECT count() FROM {table}"],
            capture_output=True, text=True, timeout=30,
        )
        count = result.stdout.strip()
        print(f" {int(count):,} rows")
    duck_con.close()


def clickhouse_benchmark(years: int, mode: str, query_ids: list,
                         queries: dict, timeout: int = 300) -> dict:
    try:
        from clickhouse_driver import Client
    except ImportError:
        print("  clickhouse-driver not installed. Run: pip install clickhouse-driver")
        return {}

    client = Client(host="localhost", port=9000,
                    settings={"max_execution_time": timeout,
                              "max_threads": BENCHMARK_THREADS,
                              "use_query_cache": 0,
                              "join_use_nulls": 1})
    total_runs = 4 if mode == "hot" else 1
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        if mode == "cold":
            drop_os_caches()
        sql = queries[qname]
        times = []
        timed_out = False
        for _ in range(total_runs):
            try:
                start = time.perf_counter()
                client.execute(sql)
                elapsed = (time.perf_counter() - start) * 1000
                times.append(elapsed)
            except Exception as e:
                err_str = str(e)
                if "TIMEOUT" in err_str.upper() or "exceeded" in err_str.lower():
                    print(f"  ClickHouse {qname}: TIMEOUT ({timeout}s)")
                else:
                    print(f"  ClickHouse {qname}: ERROR - {e}")
                timed_out = True
                break
        if not timed_out and times:
            measured = times[1:] if mode == "hot" and len(times) > 1 else times
            results[qname] = measured
            avg_time = sum(measured) / len(measured)
            label = f"avg of {len(measured)} hot runs" if mode == "hot" else "cold run"
            print(f"  ClickHouse {qname}: {avg_time:.1f} ms ({label})")
    return results


# ---------------------------------------------------------------------------
# Umbra benchmark (Docker-based)
# ---------------------------------------------------------------------------

def umbra_install():
    if not shutil.which("docker"):
        print("  Docker not available, cannot install Umbra.")
        return False
    print("  Pulling Umbra Docker image...")
    try:
        subprocess.run(
            ["docker", "pull", "umbradb/umbra:latest"],
            check=True, capture_output=True, timeout=600,
        )
        print("  Umbra image pulled successfully.")
        return True
    except Exception as e:
        print(f"  Umbra pull failed: {e}")
        return False


def umbra_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "images", "-q", "umbradb/umbra"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0 and result.stdout.strip() != ""
    except Exception:
        return False


UMBRA_HOST_PORT = 5441  # Different from TPC-H to avoid conflicts


def umbra_start(years: int) -> str:
    container_name = f"umbra_sec_edgar_{years}y"
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Status}}", container_name],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode == 0:
        status = result.stdout.strip()
        if status == "running":
            try:
                conn = psycopg2.connect(
                    host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
                    password="postgres", dbname="postgres", connect_timeout=3,
                )
                conn.close()
                print(f"  Umbra container '{container_name}' already running.")
                return container_name
            except Exception:
                pass
        if status in ("exited", "created"):
            print(f"  Restarting existing Umbra container '{container_name}'...")
            subprocess.run(
                ["docker", "start", container_name],
                capture_output=True, timeout=30,
            )
            for attempt in range(60):
                time.sleep(1)
                try:
                    conn = psycopg2.connect(
                        host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
                        password="postgres", dbname="postgres", connect_timeout=3,
                    )
                    conn.close()
                    print(f"  Umbra container '{container_name}' restarted.")
                    return container_name
                except Exception:
                    pass

    # Start new container
    print(f"  Starting new Umbra container '{container_name}'...")
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True, timeout=10,
    )
    # Mount benchmark dir for fast COPY FROM inside container
    benchmark_root = Path(__file__).parent
    subprocess.run(
        ["docker", "run", "-d", "--name", container_name,
         "--privileged", "--ulimit", "memlock=-1:-1", "--shm-size=4g",
         "-p", f"{UMBRA_HOST_PORT}:5432",
         "-v", f"{benchmark_root}:/data",
         "umbradb/umbra:latest"],
        check=True, capture_output=True, timeout=30,
    )
    for attempt in range(60):
        time.sleep(1)
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
                password="postgres", dbname="postgres", connect_timeout=3,
            )
            conn.close()
            print(f"  Umbra container '{container_name}' started.")
            return container_name
        except Exception:
            pass
    print("  Warning: Umbra may not be ready.")
    return container_name


def umbra_stop(container_name: str):
    try:
        subprocess.run(
            ["docker", "stop", container_name],
            capture_output=True, timeout=30,
        )
        print(f"  Umbra container '{container_name}' stopped.")
    except Exception:
        pass


def umbra_setup(years: int, force_setup: bool = False):
    benchmark_root = Path(__file__).parent
    schema_path = benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    db_path = benchmark_root / "duckdb" / "sec_edgar.duckdb"

    conn = psycopg2.connect(
        host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
        password="postgres", dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()

    if not force_setup:
        try:
            cur.execute("SELECT COUNT(*) FROM num")
            count = cur.fetchone()[0]
            if count > 0:
                print("  Umbra already has data, skipping setup.")
                cur.close()
                conn.close()
                return
        except Exception:
            conn.rollback()

    schema_no_fk = _strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            try:
                cur.execute(f"DROP TABLE IF EXISTS {stmt.split()[2]}")
            except Exception:
                conn.rollback()
            cur.execute(stmt)

    # Export from DuckDB to CSV, then COPY into Umbra (bulk load)
    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        cur.close()
        conn.close()
        return

    duck_con = duckdb.connect(str(db_path), read_only=True)
    tmp_dir = benchmark_root / "umbra_tmp"
    tmp_dir.mkdir(exist_ok=True)
    for table in ["sub", "tag", "num", "pre"]:
        print(f"  Loading {table}...", end="", flush=True)
        tmp_csv = tmp_dir / f"{table}.csv"
        duck_con.execute(f"COPY {table} TO '{tmp_csv}' (FORMAT CSV, HEADER true)")
        # Try COPY FROM mounted volume first (fast), fall back to STDIN
        try:
            cur.execute(
                f"COPY {table} FROM '/data/umbra_tmp/{table}.csv' WITH (FORMAT CSV, HEADER true)"
            )
        except Exception:
            conn.rollback()
            with open(tmp_csv, "r") as f:
                header = f.readline().strip()
                col_names = header.split(",")
                cur.copy_expert(
                    f"COPY {table} ({', '.join(col_names)}) FROM STDIN WITH (FORMAT CSV)",
                    f,
                )
        tmp_csv.unlink()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")
    duck_con.close()
    tmp_dir.rmdir()

    cur.close()
    conn.close()


def umbra_benchmark(years: int, mode: str, query_ids: list,
                    queries: dict, timeout: int = 300) -> dict:
    conn = psycopg2.connect(
        host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
        password="postgres", dbname="postgres",
    )
    cur = conn.cursor()
    try:
        cur.execute(f"SET statement_timeout = '{timeout * 1000}'")
    except Exception:
        conn.rollback()

    total_runs = 4 if mode == "hot" else 1
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        if mode == "cold":
            drop_os_caches()
        times = []
        timed_out = False
        for _ in range(total_runs):
            try:
                start = time.perf_counter()
                cur.execute(queries[qname])
                cur.fetchall()
                elapsed = (time.perf_counter() - start) * 1000
                times.append(elapsed)
            except Exception as e:
                conn.rollback()
                try:
                    cur.execute(f"SET statement_timeout = '{timeout * 1000}'")
                except Exception:
                    conn.rollback()
                err_str = str(e).lower()
                if "timeout" in err_str or "cancel" in err_str:
                    print(f"  Umbra {qname}: TIMEOUT ({timeout}s)")
                else:
                    print(f"  Umbra {qname}: ERROR - {e}")
                timed_out = True
                break
        if not timed_out and times:
            measured = times[1:] if mode == "hot" and len(times) > 1 else times
            results[qname] = measured
            avg_time = sum(measured) / len(measured)
            label = f"avg of {len(measured)} hot runs" if mode == "hot" else "cold run"
            print(f"  Umbra {qname}: {avg_time:.1f} ms ({label})")
    cur.close()
    conn.close()
    return results


# ---------------------------------------------------------------------------
# MonetDB benchmark
# ---------------------------------------------------------------------------

def monetdb_available() -> bool:
    return shutil.which("monetdbd") is not None or shutil.which("mserver5") is not None


def monetdb_install():
    print("  MonetDB must be installed via system package manager.")
    return False


def monetdb_server_start(years: int):
    farm_dir = Path(__file__).parent / "monetdb" / f"farm_{years}y"
    farm_dir.mkdir(parents=True, exist_ok=True)
    dbname = f"sec_edgar_{years}y"

    # Check if already running by trying to connect
    try:
        import pymonetdb
        conn = pymonetdb.connect(database=dbname, hostname="localhost",
                                 port=50000, username="monetdb", password="monetdb")
        conn.close()
        print(f"  MonetDB already running with database '{dbname}'.")
        return farm_dir
    except Exception:
        pass

    # Create farm if needed
    try:
        subprocess.run(
            ["monetdbd", "create", str(farm_dir)],
            capture_output=True, text=True, timeout=10,
        )
    except Exception:
        pass  # May already exist

    # Set port
    subprocess.run(
        ["monetdbd", "set", "port=50000", str(farm_dir)],
        capture_output=True, timeout=10,
    )

    # Start farm — monetdbd daemonizes and may send signals to the process group,
    # so use os.system() which is isolated from signal propagation.
    os.system(f"monetdbd start '{farm_dir}'")

    # Wait for daemon to be ready
    for attempt in range(30):
        time.sleep(1)
        try:
            result = subprocess.run(
                ["monetdb", "-p", "50000", "status"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                break
        except Exception:
            pass

    # Create database if needed
    try:
        subprocess.run(
            ["monetdb", "-p", "50000", "create", dbname],
            capture_output=True, text=True, timeout=10,
        )
    except Exception:
        pass
    subprocess.run(
        ["monetdb", "-p", "50000", "release", dbname],
        capture_output=True, text=True, timeout=10,
    )
    time.sleep(1)
    print(f"  MonetDB server started with database '{dbname}'.")
    return farm_dir


def monetdb_server_stop(farm_dir):
    if farm_dir:
        os.system(f"monetdbd stop '{farm_dir}'")
        time.sleep(2)
        print("  MonetDB server stopped.")


def _mclient(dbname: str, sql: str, timeout: int = 600):
    """Run a SQL statement via mclient (fast, server-side execution)."""
    return subprocess.run(
        ["mclient", "-p", "50000", "-d", dbname, "-s", sql],
        check=True, capture_output=True, text=True, timeout=timeout,
    )


def monetdb_setup(years: int, force_setup: bool = False):
    benchmark_root = Path(__file__).parent
    schema_path = benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    db_path = benchmark_root / "duckdb" / "sec_edgar.duckdb"
    dbname = f"sec_edgar_{years}y"

    if not force_setup:
        try:
            import pymonetdb
            conn = pymonetdb.connect(database=dbname, hostname="localhost",
                                     port=50000, username="monetdb", password="monetdb")
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM num")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            if count > 0:
                print("  MonetDB already has data, skipping setup.")
                return
        except Exception:
            pass

    # Build MonetDB-compatible schema DDL
    schema_no_fk = _strip_fk_constraints(schema_sql)
    schema_no_fk = re.sub(r'\bTEXT\b', 'CLOB', schema_no_fk, flags=re.IGNORECASE)
    # Right-size VARCHAR columns to prevent storage blowup in MonetDB's
    # column store (it pre-allocates based on declared size).
    monetdb_varchar_overrides = {
        "256": "210",    # tag names: actual max 204
        "1024": "540",   # footnote/plabel: actual max 530/511
        "512": "400",    # tlabel: actual max 387
    }
    for old_sz, new_sz in monetdb_varchar_overrides.items():
        schema_no_fk = schema_no_fk.replace(
            f"VARCHAR({old_sz})", f"VARCHAR({new_sz})")

    # Create tables via mclient (avoids transaction isolation issues with pymonetdb)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            table_name = stmt.split()[2]
            try:
                _mclient(dbname, f"DROP TABLE {table_name};", timeout=10)
            except Exception:
                pass
            _mclient(dbname, stmt + ";")

    # Export from DuckDB to pipe-delimited files, then bulk-load into MonetDB
    # using mclient for fast server-side COPY INTO.
    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return

    duck_con = duckdb.connect(str(db_path), read_only=True)
    tmp_dir = benchmark_root / "monetdb_tmp"
    tmp_dir.mkdir(exist_ok=True)
    for table in ["sub", "tag", "num", "pre"]:
        print(f"  Loading {table}...", end="", flush=True)
        tmp_csv = tmp_dir / f"{table}.tbl"
        # Export pipe-delimited without header; strip trailing backslashes
        # from text fields that MonetDB would misinterpret as escape chars.
        col_info = duck_con.execute(f"DESCRIBE {table}").fetchall()
        select_exprs = []
        for row in col_info:
            c, ctype = row[0], row[1].upper()
            if "VARCHAR" in ctype or "TEXT" in ctype:
                select_exprs.append(
                    f"REPLACE({c}, '\\', '') AS {c}")
            else:
                select_exprs.append(c)
        duck_con.execute(
            f"COPY (SELECT {', '.join(select_exprs)} FROM {table}) "
            f"TO '{tmp_csv}' (FORMAT CSV, HEADER false, DELIMITER '|')"
        )
        abs_path = str(tmp_csv.resolve())
        _mclient(dbname,
                 f"COPY INTO {table} FROM '{abs_path}' "
                 f"USING DELIMITERS '|',E'\\n','\"' NULL AS '';")
        tmp_csv.unlink()
        result = _mclient(dbname, f"SELECT COUNT(*) FROM {table};")
        # Parse count from mclient output (format: "| <number> |")
        for line in result.stdout.splitlines():
            line = line.strip().strip("|").strip()
            if line.isdigit():
                print(f" {int(line):,} rows")
                break
    duck_con.close()
    try:
        tmp_dir.rmdir()
    except OSError:
        pass


def monetdb_benchmark(years: int, mode: str, query_ids: list,
                      queries: dict, timeout: int = 300) -> dict:
    try:
        import pymonetdb
    except ImportError:
        print("  pymonetdb not installed. Run: pip install pymonetdb")
        return {}

    dbname = f"sec_edgar_{years}y"
    conn = pymonetdb.connect(database=dbname, hostname="localhost",
                             port=50000, username="monetdb", password="monetdb")
    cur = conn.cursor()
    try:
        cur.execute(f"SET nthreads = {BENCHMARK_THREADS}")
    except Exception:
        conn.rollback()

    total_runs = 4 if mode == "hot" else 1
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        if mode == "cold":
            drop_os_caches()
        times = []
        timed_out = False
        for _ in range(total_runs):
            try:
                start = time.perf_counter()
                cur.execute(queries[qname])
                cur.fetchall()
                elapsed = (time.perf_counter() - start) * 1000
                if elapsed > timeout * 1000:
                    print(f"  MonetDB {qname}: TIMEOUT ({timeout}s)")
                    timed_out = True
                    break
                times.append(elapsed)
            except Exception as e:
                print(f"  MonetDB {qname}: ERROR - {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                timed_out = True
                break
        if not timed_out and times:
            measured = times[1:] if mode == "hot" and len(times) > 1 else times
            results[qname] = measured
            avg_time = sum(measured) / len(measured)
            label = f"avg of {len(measured)} hot runs" if mode == "hot" else "cold run"
            print(f"  MonetDB {qname}: {avg_time:.1f} ms ({label})")
    cur.close()
    conn.close()
    return results


# ---------------------------------------------------------------------------
# GenDB: read iteration history from execution_results.json
# ---------------------------------------------------------------------------

def read_gendb_iteration_history(run_dir: Path) -> dict:
    """Read iteration timing/validation from execution_results.json files."""
    queries_dir = run_dir / "queries"
    if not queries_dir.exists():
        return {"query_ids": [], "num_iters": 0, "data": {}, "best": {}}

    query_ids = sorted([d.name for d in queries_dir.iterdir()
                        if d.is_dir() and d.name.startswith("Q")])

    data = {}
    max_iter = -1
    for qid in query_ids:
        data[qid] = {}
        qid_dir = queries_dir / qid
        for iter_dir in sorted(qid_dir.glob("iter_*")):
            match = re.match(r"^iter_(\d+)$", iter_dir.name)
            if not match:
                continue
            i = int(match.group(1))
            exec_path = iter_dir / "execution_results.json"
            if not exec_path.exists():
                continue
            with open(exec_path) as f:
                exec_data = json.load(f)
            data[qid][i] = {
                "timing_ms": exec_data.get("timing_ms"),
                "validation": exec_data.get("validation", {}).get("status", "unknown"),
            }
            if i > max_iter:
                max_iter = i

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
        if qid in best:
            cell = f"{int(round(best[qid]['timing_ms']))} ms"
            cells.append(f"{BOLD}{GREEN}{cell:>{COL_W}}{RESET}")
        else:
            cells.append(f"{'-':>{COL_W}}")
        print(" | ".join(cells))

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

    if best:
        total = sum(b["timing_ms"] for b in best.values())
        print(sep)
        cells = [f"{'Total':<{Q_COL_W}}"]
        for _ in range(num_iters):
            cells.append(f"{'':>{COL_W}}")
        total_cell = f"{int(round(total))} ms"
        cells.append(f"{BOLD}{total_cell:>{COL_W}}{RESET}")
        print(" | ".join(cells))


# ---------------------------------------------------------------------------
# GenDB: benchmark best per-query binaries
# ---------------------------------------------------------------------------

def parse_timing_output(stdout: str) -> dict:
    timings = {}
    for line in stdout.splitlines():
        m = re.match(r"\[TIMING\]\s+(\w+):\s+([\d.]+)\s*ms", line)
        if m:
            timings[m.group(1)] = float(m.group(2))
    if "total" in timings:
        timings["query_ms"] = timings["total"] - timings.get("output", 0)
    return timings


def find_best_binaries(run_dir: Path) -> dict:
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
            binary = iter_dir / qid.lower()
            if binary.exists() and os.access(str(binary), os.X_OK):
                binaries[qid] = binary

    return binaries


def gendb_benchmark_best(run_dir: Path, gendb_dir: Path, mode: str) -> dict:
    best_binaries = find_best_binaries(run_dir)
    if not best_binaries:
        print("  No best binaries found in run.json")
        return {}

    total_runs = 4 if mode == "hot" else 1
    results = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        for qid in sorted(best_binaries):
            binary = best_binaries[qid]
            print(f"  Running {qid} ({binary.parent.name}/{binary.name})...", end="", flush=True)

            if mode == "cold":
                drop_os_caches()
            times = []
            ok = True
            for _ in range(total_runs):
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
                measured = times[1:] if mode == "hot" and len(times) > 1 else times
                results[qid] = measured
                avg = sum(measured) / len(measured)
                label = f"avg of {len(measured)} hot runs" if mode == "hot" else "cold run"
                print(f" {avg:.2f} ms ({label})")
            elif not ok:
                pass

    return results


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

SYSTEM_COLORS = {
    "GenDB": "#E41A1C",
    "PostgreSQL": "#377EB8",
    "DuckDB": "#FF7F00",
    "ClickHouse": "#4DAF4A",
    "Umbra": "#984EA3",
    "MonetDB": "#A65628",
}


def _compute_best_so_far(data: dict, query_ids: list, num_iters: int) -> dict:
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
                series.append((i, best_t, "fail"))
            elif entry is None and best_t is not None:
                series.append((i, best_t, "skipped"))
        result[qid] = series
    return result


def plot_results(all_results: dict, output_path: Path, years: str = "?",
                 timeout_ms: float = 300000, all_query_ids: list = None,
                 gendb_history: dict = None):
    """Create a single 4-panel figure."""
    import matplotlib
    import matplotlib.ticker as ticker
    import numpy as np
    matplotlib.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "DejaVu Serif"],
        "font.size": 11,
        "axes.labelsize": 12,
        "axes.titlesize": 11,
        "axes.titlepad": 4,
        "axes.labelpad": 1,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "xtick.major.pad": 2,
        "ytick.major.pad": 2,
        "figure.dpi": 300,
        "axes.linewidth": 0.5,
        "grid.linewidth": 0.3,
        "xtick.major.size": 2.5,
        "ytick.major.size": 2.5,
        "xtick.minor.size": 1.2,
        "ytick.minor.size": 1.2,
    })

    sci_fmt = ticker.LogFormatterSciNotation(base=10, labelOnlyBase=True)

    if all_query_ids:
        all_qs = sorted(all_query_ids, key=lambda q: int(q[1:]))
    else:
        all_qs = sorted(set(q for results in all_results.values() for q in results),
                        key=lambda q: int(q[1:]))
    systems = list(all_results.keys())

    data_all = {}
    is_timeout_all = {}
    for system in systems:
        data_all[system] = []
        is_timeout_all[system] = []
        for q in all_qs:
            times = all_results[system].get(q, [])
            if times:
                data_all[system].append(sum(times) / len(times))
                is_timeout_all[system].append(False)
            else:
                data_all[system].append(timeout_ms)
                is_timeout_all[system].append(True)

    common_idx = [j for j, q in enumerate(all_qs)
                  if all(not is_timeout_all[s][j] for s in systems)]
    queries = [all_qs[j] for j in common_idx]
    data = {s: [data_all[s][j] for j in common_idx] for s in systems}
    totals = {s: sum(data[s]) for s in systems}

    has_iter = gendb_history and gendb_history["num_iters"] > 0

    def _style_log(a, axis="y"):
        if axis == "y":
            a.set_yscale("log")
            a.yaxis.set_major_formatter(sci_fmt)
            a.yaxis.set_minor_formatter(ticker.NullFormatter())
            a.grid(axis="y", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
        else:
            a.set_xscale("log")
            a.xaxis.set_major_formatter(sci_fmt)
            a.xaxis.set_minor_formatter(ticker.NullFormatter())
            a.grid(axis="x", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
        a.set_axisbelow(True)

    if has_iter:
        fig = plt.figure(figsize=(13, 3.0))
        sq = 0.17
        ax  = fig.add_axes([0.05, 0.15, 0.27, 0.68])
        ax2 = fig.add_axes([0.34, 0.15, sq,   0.68])
        ax3 = fig.add_axes([0.53, 0.15, sq,   0.68])
        ax4 = fig.add_axes([0.73, 0.15, sq,   0.68])
    else:
        fig, (ax, ax2) = plt.subplots(
            1, 2, figsize=(8, 2.8),
            gridspec_kw={"width_ratios": [1.6, 1], "wspace": 0.32})

    # (a) Per-query grouped bar chart
    x = np.arange(len(queries))
    width = 0.92 / max(len(systems), 1)
    offsets = [(i - len(systems) / 2 + 0.5) * width for i in range(len(systems))]
    for i, system in enumerate(systems):
        ax.bar(x + offsets[i], data[system], width,
               label=system, color=SYSTEM_COLORS.get(system, "#999999"),
               edgecolor="white", linewidth=0.2, zorder=3)
    ax.set_ylabel("Time (ms)", fontsize=13)
    ax.set_title("(a) Per-Query Time", fontweight="bold", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(queries, fontsize=12)
    ax.set_xlim(x[0] - 0.5, x[-1] + 0.5)
    ax.tick_params(axis="x", length=0)
    ax.tick_params(axis="y", labelsize=12)
    _style_log(ax, "y")

    # (b) Total time horizontal bar chart
    y_pos = np.arange(len(systems))
    bar_vals = [totals[s] for s in systems]
    hbars = ax2.barh(y_pos, bar_vals, height=0.6,
                     color=[SYSTEM_COLORS.get(s, "#999999") for s in systems],
                     edgecolor="white", linewidth=0.2, zorder=3)
    max_val = max(bar_vals)
    for bar, system in zip(hbars, systems):
        val = totals[system]
        bw = bar.get_width()
        cy = bar.get_y() + bar.get_height() / 2
        if bw > max_val * 0.15:
            ax2.text(bw * 0.85, cy, f"{val:,.0f}",
                     ha="right", va="center", fontsize=12,
                     fontweight="bold", color="white", zorder=4)
        else:
            ax2.text(bw * 1.15, cy, f"{val:,.0f}",
                     ha="left", va="center", fontsize=12,
                     fontweight="bold", color="black", zorder=4)
    ax2.set_xlabel("(ms)", labelpad=6, fontsize=13)
    ax2.xaxis.set_label_coords(1.0, -0.12)
    ax2.xaxis.label.set_ha("right")
    ax2.set_title("(b) Total Time", fontweight="bold", fontsize=13)
    ax2.tick_params(axis="x", labelsize=12)
    ax2.set_yticks([])
    ax2.set_ylim(-0.5, len(systems) - 0.5)
    ax2.set_xlim(left=100, right=max_val * 1.1)
    ax2.invert_yaxis()
    _style_log(ax2, "x")

    # (c) & (d) GenDB iteration panels
    if has_iter:
        query_ids = gendb_history["query_ids"]
        iter_data = gendb_history["data"]
        num_iters = gendb_history["num_iters"]
        best_so_far = _compute_best_so_far(iter_data, query_ids, num_iters)
        cmap = matplotlib.colormaps.get_cmap("tab10")
        q_colors = {q: cmap(i) for i, q in enumerate(query_ids)}
        x_iters = list(range(num_iters))

        # (c) Total execution time across iterations
        valid_qids = [qid for qid in query_ids
                      if any(s == "valid" for _, _, s in best_so_far.get(qid, []))]
        total_x, total_y = [], []
        for i in range(num_iters):
            total, n_q = 0, 0
            for qid in valid_qids:
                for (si, st, _) in best_so_far.get(qid, []):
                    if si == i:
                        total += st
                        n_q += 1
                        break
            if n_q > 0:
                total_x.append(i)
                total_y.append(total)

        if total_y:
            ax3.plot(total_x, total_y, marker="o", linewidth=2.5, markersize=6,
                     color="#9C27B0")

        # Horizontal lines with markers for other systems' total time
        hline_markers = ["+", "x", "*", "s", "D"]
        non_gendb = [s for s in systems if s != "GenDB"]
        n_pts = 8
        for idx, sys_name in enumerate(non_gendb):
            sys_total = totals.get(sys_name, 0)
            if sys_total > 0:
                # Offset marker positions per system to avoid overlap
                offset = (idx - len(non_gendb) / 2) * 0.15
                hline_x = np.linspace(-0.3 + offset, num_iters - 0.7 + offset, n_pts)
                ax3.plot(hline_x, [sys_total] * n_pts, linestyle=":",
                         linewidth=2.0, marker=hline_markers[idx % len(hline_markers)],
                         markersize=7,
                         color=SYSTEM_COLORS.get(sys_name, "#999999"), alpha=0.7, zorder=2)

        ax3.set_xlabel("Iteration", fontsize=13)
        ax3.set_title("(c) GenDB Total / Iter.", fontweight="bold", fontsize=13)
        ax3.set_xticks(x_iters)
        ax3.set_xticklabels([str(i) for i in x_iters], fontsize=12)
        ax3.set_xlim(-0.3, num_iters - 0.7)
        ax3.set_yscale("log")
        ax3.yaxis.set_major_formatter(ticker.NullFormatter())
        ax3.yaxis.set_minor_formatter(ticker.NullFormatter())
        ax3.grid(axis="y", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
        ax3.set_axisbelow(True)
        ax3.text(0.04, 0.04, "(ms)", transform=ax3.transAxes, fontsize=12,
                 va="bottom", ha="left")

        # (d) Per-query execution time across iterations
        sorted_qids = sorted(query_ids, key=lambda q: int(q[1:]))
        for qid in sorted_qids:
            series = best_so_far.get(qid, [])
            if not series:
                continue
            color = q_colors[qid]
            attempted = [(xi, yi) for xi, yi, s in series if s in ("valid", "fail")]
            if attempted:
                ax4.plot([p[0] for p in attempted], [p[1] for p in attempted],
                         marker="o", linewidth=2.5, markersize=6, label=qid, color=color)
            skipped = [(xi, yi) for xi, yi, s in series if s == "skipped"]
            if skipped and attempted:
                dash_x = [attempted[-1][0]] + [p[0] for p in skipped]
                dash_y = [attempted[-1][1]] + [p[1] for p in skipped]
                ax4.plot(dash_x, dash_y, linestyle="--", linewidth=1.5, markersize=0,
                         color=color, alpha=0.4)
            fail_x = [xi for xi, yi, s in series if s == "fail"]
            fail_y = [yi for xi, yi, s in series if s == "fail"]
            if fail_x:
                ax4.scatter(fail_x, fail_y, marker="x", s=50, linewidths=1.5,
                            color="red", zorder=5)

        ax4.set_xlabel("Iteration", fontsize=13)
        ax4.set_title("(d) GenDB Per-Query / Iter.", fontweight="bold", fontsize=13)
        ax4.set_xticks(x_iters)
        ax4.set_xticklabels([str(i) for i in x_iters], fontsize=12)
        ax4.set_xlim(-0.3, num_iters - 0.7)
        ax4.yaxis.set_major_formatter(sci_fmt)
        ax4.yaxis.set_minor_formatter(ticker.NullFormatter())
        ax4.tick_params(axis="y", direction="out", pad=2, labelsize=12)
        ax4.text(-0.09, 0.96, "(ms)", transform=ax4.transAxes, fontsize=11,
                 va="top", ha="center")
        # Expand y-axis top to make room for legend above data (log scale)
        all_vals = []
        for qid in sorted_qids:
            series = best_so_far.get(qid, [])
            all_vals.extend([yi for _, yi, _ in series if yi > 0])
        if all_vals:
            ax4.set_ylim(bottom=min(all_vals) * 0.5, top=max(all_vals) * 5)
        ax4.legend(fontsize=10, loc="upper center", framealpha=0.9,
                   handlelength=1.2, handletextpad=0.3, borderpad=0.15,
                   labelspacing=0.15, ncol=3)
        _style_log(ax4, "y")

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(systems),
               bbox_to_anchor=(0.5, 1.08), frameon=True, fontsize=13,
               columnspacing=1.0, handletextpad=0.4, handlelength=1.0,
               edgecolor="#CCCCCC", fancybox=False, framealpha=1.0)

    combined_path = output_path.with_name(
        output_path.stem.replace("_per_query", "") + "_combined" + output_path.suffix)
    fig.savefig(combined_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    pdf_path = combined_path.with_suffix(".pdf")
    fig.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
    plt.close()
    print(f"  Combined plot saved to: {combined_path}")
    print(f"  Combined plot (PDF) saved to: {pdf_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="SEC EDGAR Benchmark: GenDB vs PostgreSQL vs DuckDB vs ClickHouse vs Umbra vs MonetDB")
    parser.add_argument("--years", type=int, required=True,
                        help="Number of years of data (e.g., 3 for 2022-2024)")
    parser.add_argument("--gendb-run", type=Path, default=None,
                        help="Path to GenDB run directory (default: output/sec-edgar/latest/)")
    parser.add_argument("--mode", type=str, default="hot", choices=["hot", "cold"],
                        help="Benchmark mode: hot (4 runs, avg last 3) or cold (cache clear, 1 run) (default: hot)")
    parser.add_argument("--setup", action="store_true", help="Force database setup/reload")
    parser.add_argument("--output", type=Path, default=None, help="Output plot path")
    parser.add_argument("--gendb-only", action="store_true",
                        help="Skip all baseline benchmarks")
    parser.add_argument("--skip-postgres", action="store_true",
                        help="Skip PostgreSQL benchmark")
    parser.add_argument("--skip-duckdb", action="store_true",
                        help="Skip DuckDB benchmark")
    parser.add_argument("--skip-clickhouse", action="store_true",
                        help="Skip ClickHouse benchmark")
    parser.add_argument("--skip-umbra", action="store_true",
                        help="Skip Umbra benchmark")
    parser.add_argument("--skip-monetdb", action="store_true",
                        help="Skip MonetDB benchmark")
    parser.add_argument("--timeout", type=int, default=300,
                        help="Per-query timeout in seconds (default: 300)")
    parser.add_argument("--plot-only", action="store_true",
                        help="Skip all benchmarks; read existing metrics JSON and re-plot figures")
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent.parent
    benchmark_root = Path(__file__).parent

    # Load queries from queries.sql
    queries_path = benchmark_root / "queries.sql"
    if not queries_path.exists():
        print(f"Error: queries file not found at {queries_path}")
        print("Run: python3 benchmarks/sec-edgar/generate_queries.py")
        sys.exit(1)
    queries = load_queries(queries_path)

    if args.output is None:
        results_dir = benchmark_root / "results" / f"sf{args.years}" / "figures"
        results_dir.mkdir(parents=True, exist_ok=True)
        args.output = results_dir / "benchmark_results_per_query.png"

    # Auto-detect GenDB run directory
    if args.gendb_run is None:
        latest_link = project_root / "output" / "sec-edgar" / "latest"
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
        gendb_dir = benchmark_root / "gendb" / f"sf{args.years}.gendb"

    print(f"Years:          {args.years}")
    print(f"Benchmark mode: {args.mode} ({'4 runs, avg last 3' if args.mode == 'hot' else '1 cold run per query'})")
    print(f"Query timeout:  {args.timeout}s")
    print(f"Queries:        {len(queries)} ({', '.join(sorted(queries.keys(), key=lambda q: int(q[1:])))})")
    if args.gendb_run:
        print(f"GenDB run:      {args.gendb_run}")
        print(f"GenDB storage:  {gendb_dir}")
    print()

    all_results = {}
    gendb_history = None
    gendb_query_ids = sorted(queries.keys(), key=lambda q: int(q[1:]))

    # --- Plot-only mode ---
    if args.plot_only:
        metrics_dir = benchmark_root / "results" / f"sf{args.years}" / "metrics"
        json_path = metrics_dir / "benchmark_results.json"
        if not json_path.exists():
            print(f"Error: metrics file not found: {json_path}")
            sys.exit(1)
        print(f"=== Plot-only mode: loading metrics from {json_path} ===")
        with open(json_path) as f:
            saved_data = json.load(f)
        for system, system_data in saved_data.items():
            all_results[system] = {}
            for qid, qdata in system_data.items():
                all_results[system][qid] = qdata["all_ms"]

        iter_json_path = metrics_dir / "gendb_iteration_history.json"
        if iter_json_path.exists():
            with open(iter_json_path) as f:
                iter_data = json.load(f)
            data = {}
            for qid, iters in iter_data["data"].items():
                data[qid] = {int(k): v for k, v in iters.items()}
            gendb_history = {
                "query_ids": iter_data["query_ids"],
                "num_iters": iter_data["num_iters"],
                "data": data,
                "best": iter_data["best"],
            }
        print()

    # --- GenDB ---
    if not args.plot_only and args.gendb_run and args.gendb_run.exists():
        print("=== GenDB Iteration History ===")
        gendb_history = read_gendb_iteration_history(args.gendb_run)
        if gendb_history["query_ids"]:
            gendb_query_ids = gendb_history["query_ids"]
            print()
            print_iteration_table(gendb_history)
            print()

            metrics_dir = benchmark_root / "results" / f"sf{args.years}" / "metrics"
            metrics_dir.mkdir(parents=True, exist_ok=True)
            iter_json_path = metrics_dir / "gendb_iteration_history.json"
            json_data = {}
            for qid, iters in gendb_history["data"].items():
                json_data[qid] = {str(k): v for k, v in iters.items()}
            with open(iter_json_path, "w") as f:
                json.dump({"data": json_data, "best": gendb_history["best"],
                           "query_ids": gendb_history["query_ids"],
                           "num_iters": gendb_history["num_iters"]}, f, indent=2)
            print(f"  Iteration history saved to: {iter_json_path}")

        print(f"\n=== GenDB Benchmark (best per-query, {args.mode} mode) ===")
        best_results = gendb_benchmark_best(args.gendb_run, gendb_dir, args.mode)
        if best_results:
            all_results["GenDB"] = best_results
    elif not args.plot_only:
        print("=== GenDB: SKIPPED (run directory not found) ===")

    if not args.plot_only and not args.gendb_only:
        # --- PostgreSQL ---
        if not args.skip_postgres:
            print("\n=== PostgreSQL Setup ===")
            try:
                pg_setup(args.years, args.setup)
                restart_postgresql()
                print(f"\n=== PostgreSQL Benchmark ({args.mode} mode) ===")
                all_results["PostgreSQL"] = pg_benchmark(
                    args.years, args.mode, gendb_query_ids, queries, args.timeout)
            except Exception as e:
                print(f"  PostgreSQL error: {e}")

        # --- DuckDB ---
        if not args.skip_duckdb:
            print(f"\n=== DuckDB Benchmark ({args.mode} mode) ===")
            try:
                all_results["DuckDB"] = duckdb_benchmark(
                    args.years, args.mode, gendb_query_ids, queries, args.timeout)
            except Exception as e:
                print(f"  DuckDB error: {e}")

        # --- ClickHouse ---
        ch_proc = None
        if not args.skip_clickhouse:
            print("\n=== ClickHouse Setup ===")
            try:
                if not clickhouse_available():
                    clickhouse_install()
                if clickhouse_available():
                    ch_proc = clickhouse_server_start(args.years)
                    clickhouse_setup(args.years, args.setup)
                    # Clear ClickHouse internal caches
                    ch_dir = Path(__file__).parent / "clickhouse"
                    subprocess.run(
                        [str(ch_dir / "clickhouse"), "client", "--port", "9000",
                         "-q", "SYSTEM DROP CACHE"],
                        capture_output=True, timeout=10,
                    )
                    print("  ClickHouse internal caches cleared.")
                    print(f"\n=== ClickHouse Benchmark ({args.mode} mode) ===")
                    all_results["ClickHouse"] = clickhouse_benchmark(
                        args.years, args.mode, gendb_query_ids, queries, args.timeout)
                else:
                    print("  ClickHouse not available, skipping.")
            except Exception as e:
                print(f"  ClickHouse error: {e}")
            finally:
                if ch_proc:
                    clickhouse_server_stop(ch_proc)

        # --- Umbra ---
        umbra_container = None
        if not args.skip_umbra:
            print("\n=== Umbra Setup ===")
            try:
                if not umbra_available():
                    umbra_install()
                if umbra_available():
                    umbra_container = umbra_start(args.years)
                    umbra_setup(args.years, args.setup)
                    # Restart Umbra to clear internal buffer pool
                    subprocess.run(
                        ["docker", "restart", umbra_container],
                        capture_output=True, timeout=30,
                    )
                    for _ in range(60):
                        time.sleep(1)
                        try:
                            c = psycopg2.connect(
                                host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
                                password="postgres", dbname="postgres", connect_timeout=3)
                            c.close()
                            break
                        except Exception:
                            pass
                    print("  Umbra container restarted (buffer pool cleared).")
                    print(f"\n=== Umbra Benchmark ({args.mode} mode) ===")
                    all_results["Umbra"] = umbra_benchmark(
                        args.years, args.mode, gendb_query_ids, queries, args.timeout)
                else:
                    print("  Umbra not available, skipping.")
            except Exception as e:
                print(f"  Umbra error: {e}")
            finally:
                if umbra_container:
                    umbra_stop(umbra_container)

        # --- MonetDB ---
        monetdb_farm = None
        if not args.skip_monetdb:
            print("\n=== MonetDB Setup ===")
            try:
                if not monetdb_available():
                    monetdb_install()
                if monetdb_available():
                    monetdb_farm = monetdb_server_start(args.years)
                    monetdb_setup(args.years, args.setup)
                    # Restart MonetDB to clear internal caches
                    monetdb_server_stop(monetdb_farm)
                    time.sleep(2)
                    monetdb_farm = monetdb_server_start(args.years)
                    print("  MonetDB restarted (internal caches cleared).")
                    print(f"\n=== MonetDB Benchmark ({args.mode} mode) ===")
                    all_results["MonetDB"] = monetdb_benchmark(
                        args.years, args.mode, gendb_query_ids, queries, args.timeout)
                else:
                    print("  MonetDB not available, skipping.")
            except Exception as e:
                print(f"  MonetDB error: {e}")
            finally:
                if monetdb_farm:
                    monetdb_server_stop(monetdb_farm)

    # --- Results summary ---
    print("\n" + "=" * 60)
    mode_label = "hot avg of 3" if args.mode == "hot" else "cold single run"
    print(f"RESULTS SUMMARY ({mode_label}, in ms)")
    print("=" * 60)
    all_queries = sorted(
        set(q for results in all_results.values() for q in results),
        key=lambda q: int(q[1:]),
    )
    header = f"{'Query':<8}" + "".join(f"{s:<15}" for s in all_results.keys())
    print(header)
    print("-" * len(header))
    for q in all_queries:
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
            for q in all_queries
        )
        row += f"{total:<15.2f}"
    print(row)
    print()

    # --- Save JSON results ---
    if not args.plot_only:
        metrics_dir = benchmark_root / "results" / f"sf{args.years}" / "metrics"
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
    min_systems = 1 if args.plot_only else 2
    all_query_ids = sorted(queries.keys(), key=lambda q: int(q[1:]))
    if len(all_results) >= min_systems:
        print()
        plot_results(all_results, args.output, years=str(args.years),
                     timeout_ms=args.timeout * 1000, all_query_ids=all_query_ids,
                     gendb_history=gendb_history)


if __name__ == "__main__":
    main()
