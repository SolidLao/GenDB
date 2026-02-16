#!/usr/bin/env python3
"""
TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB vs ClickHouse vs Umbra vs MonetDB

Usage:
    python3 benchmarks/tpc-h/benchmark.py --sf <N> [options]

Options:
    --sf <N>              Scale factor (required, e.g., 1, 10)
    --gendb-run <path>    Path to GenDB run directory (default: output/tpc-h/latest/)
    --data-dir <path>     Path to TPC-H data directory (default: benchmarks/tpc-h/data/sf{N})
    --runs <N>            Number of runs per query (default: 3)
    --setup               Force database setup/reload (default: skip if DB exists)
    --output <path>       Output plot path (default: results/sf{N}/figures/benchmark_results_per_query.png)
    --gendb-only          Skip all baseline benchmarks
    --skip-clickhouse     Skip ClickHouse benchmark
    --skip-umbra          Skip Umbra benchmark
    --skip-monetdb        Skip MonetDB benchmark
    --timeout <N>         Per-query timeout in seconds (default: 300)
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

# ---------------------------------------------------------------------------
# TPC-H queries (standard SQL, cross-system compatible)
# ---------------------------------------------------------------------------

def get_queries(scale_factor: int = 1) -> dict:
    """Return all 22 TPC-H queries with default parameter substitutions.

    Q11's HAVING threshold is parameterized by scale factor (0.0001/SF).
    """
    q11_threshold = 0.0001 / scale_factor

    return {
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
        "Q2": """
            SELECT
                s_acctbal, s_name, n_name, p_partkey, p_mfgr,
                s_address, s_phone, s_comment
            FROM part, supplier, partsupp, nation, region
            WHERE
                p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND p_size = 15
                AND p_type LIKE '%BRASS'
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE'
                AND ps_supplycost = (
                    SELECT MIN(ps_supplycost)
                    FROM partsupp, supplier, nation, region
                    WHERE
                        p_partkey = ps_partkey
                        AND s_suppkey = ps_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE'
                )
            ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
            LIMIT 100
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
        "Q4": """
            SELECT
                o_orderpriority,
                COUNT(*) AS order_count
            FROM orders
            WHERE
                o_orderdate >= DATE '1993-07-01'
                AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
                AND EXISTS (
                    SELECT * FROM lineitem
                    WHERE l_orderkey = o_orderkey
                      AND l_commitdate < l_receiptdate
                )
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority
        """,
        "Q5": """
            SELECT
                n_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM customer, orders, lineitem, supplier, nation, region
            WHERE
                c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey
                AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'ASIA'
                AND o_orderdate >= DATE '1994-01-01'
                AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY n_name
            ORDER BY revenue DESC
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
        "Q7": """
            SELECT
                supp_nation, cust_nation, l_year,
                SUM(volume) AS revenue
            FROM (
                SELECT
                    n1.n_name AS supp_nation,
                    n2.n_name AS cust_nation,
                    EXTRACT(YEAR FROM l_shipdate) AS l_year,
                    l_extendedprice * (1 - l_discount) AS volume
                FROM supplier, lineitem, orders, customer, nation n1, nation n2
                WHERE
                    s_suppkey = l_suppkey
                    AND o_orderkey = l_orderkey
                    AND c_custkey = o_custkey
                    AND s_nationkey = n1.n_nationkey
                    AND c_nationkey = n2.n_nationkey
                    AND (
                        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                    )
                    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            ) AS shipping
            GROUP BY supp_nation, cust_nation, l_year
            ORDER BY supp_nation, cust_nation, l_year
        """,
        "Q8": """
            SELECT
                o_year,
                SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
            FROM (
                SELECT
                    EXTRACT(YEAR FROM o_orderdate) AS o_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    n2.n_name AS nation
                FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
                WHERE
                    p_partkey = l_partkey
                    AND s_suppkey = l_suppkey
                    AND l_orderkey = o_orderkey
                    AND o_custkey = c_custkey
                    AND c_nationkey = n1.n_nationkey
                    AND n1.n_regionkey = r_regionkey
                    AND r_name = 'AMERICA'
                    AND s_nationkey = n2.n_nationkey
                    AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                    AND p_type = 'ECONOMY ANODIZED STEEL'
            ) AS all_nations
            GROUP BY o_year
            ORDER BY o_year
        """,
        "Q9": """
            SELECT
                nation, o_year,
                SUM(amount) AS sum_profit
            FROM (
                SELECT
                    n_name AS nation,
                    EXTRACT(YEAR FROM o_orderdate) AS o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                FROM part, supplier, lineitem, partsupp, orders, nation
                WHERE
                    s_suppkey = l_suppkey
                    AND ps_suppkey = l_suppkey
                    AND ps_partkey = l_partkey
                    AND p_partkey = l_partkey
                    AND o_orderkey = l_orderkey
                    AND s_nationkey = n_nationkey
                    AND p_name LIKE '%green%'
            ) AS profit
            GROUP BY nation, o_year
            ORDER BY nation, o_year DESC
        """,
        "Q10": """
            SELECT
                c_custkey, c_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                c_acctbal, n_name, c_address, c_phone, c_comment
            FROM customer, orders, lineitem, nation
            WHERE
                c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate >= DATE '1993-10-01'
                AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
                AND l_returnflag = 'R'
                AND c_nationkey = n_nationkey
            GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
            ORDER BY revenue DESC
            LIMIT 20
        """,
        "Q11": f"""
            SELECT
                ps_partkey,
                SUM(ps_supplycost * ps_availqty) AS value
            FROM partsupp, supplier, nation
            WHERE
                ps_suppkey = s_suppkey
                AND s_nationkey = n_nationkey
                AND n_name = 'GERMANY'
            GROUP BY ps_partkey
            HAVING SUM(ps_supplycost * ps_availqty) > (
                SELECT SUM(ps_supplycost * ps_availqty) * {q11_threshold:.10f}
                FROM partsupp, supplier, nation
                WHERE
                    ps_suppkey = s_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_name = 'GERMANY'
            )
            ORDER BY value DESC
        """,
        "Q12": """
            SELECT
                l_shipmode,
                SUM(CASE
                    WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1
                    ELSE 0
                END) AS high_line_count,
                SUM(CASE
                    WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1
                    ELSE 0
                END) AS low_line_count
            FROM orders, lineitem
            WHERE
                o_orderkey = l_orderkey
                AND l_shipmode IN ('MAIL', 'SHIP')
                AND l_commitdate < l_receiptdate
                AND l_shipdate < l_commitdate
                AND l_receiptdate >= DATE '1994-01-01'
                AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY l_shipmode
            ORDER BY l_shipmode
        """,
        "Q13": """
            SELECT
                c_count,
                COUNT(*) AS custdist
            FROM (
                SELECT
                    c_custkey,
                    COUNT(o_orderkey) AS c_count
                FROM customer LEFT OUTER JOIN orders ON
                    c_custkey = o_custkey
                    AND o_comment NOT LIKE '%special%requests%'
                GROUP BY c_custkey
            ) AS c_orders
            GROUP BY c_count
            ORDER BY custdist DESC, c_count DESC
        """,
        "Q14": """
            SELECT
                100.00 * SUM(CASE
                    WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
                    ELSE 0
                END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM lineitem, part
            WHERE
                l_partkey = p_partkey
                AND l_shipdate >= DATE '1995-09-01'
                AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
        """,
        "Q15": """
            WITH revenue0 AS (
                SELECT
                    l_suppkey AS supplier_no,
                    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM lineitem
                WHERE
                    l_shipdate >= DATE '1996-01-01'
                    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
                GROUP BY l_suppkey
            )
            SELECT
                s_suppkey, s_name, s_address, s_phone, total_revenue
            FROM supplier, revenue0
            WHERE
                s_suppkey = supplier_no
                AND total_revenue = (
                    SELECT MAX(total_revenue) FROM revenue0
                )
            ORDER BY s_suppkey
        """,
        "Q16": """
            SELECT
                p_brand, p_type, p_size,
                COUNT(DISTINCT ps_suppkey) AS supplier_cnt
            FROM partsupp, part
            WHERE
                p_partkey = ps_partkey
                AND p_brand <> 'Brand#45'
                AND p_type NOT LIKE 'MEDIUM POLISHED%'
                AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                AND ps_suppkey NOT IN (
                    SELECT s_suppkey FROM supplier
                    WHERE s_comment LIKE '%Customer%Complaints%'
                )
            GROUP BY p_brand, p_type, p_size
            ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
        """,
        "Q17": """
            SELECT
                SUM(l_extendedprice) / 7.0 AS avg_yearly
            FROM lineitem, part
            WHERE
                p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container = 'MED BOX'
                AND l_quantity < (
                    SELECT 0.2 * AVG(l_quantity)
                    FROM lineitem
                    WHERE l_partkey = p_partkey
                )
        """,
        "Q18": """
            SELECT
                c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
                SUM(l_quantity) AS sum_qty
            FROM customer, orders, lineitem
            WHERE
                o_orderkey IN (
                    SELECT l_orderkey FROM lineitem
                    GROUP BY l_orderkey
                    HAVING SUM(l_quantity) > 300
                )
                AND c_custkey = o_custkey
                AND o_orderkey = l_orderkey
            GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
            ORDER BY o_totalprice DESC, o_orderdate
            LIMIT 100
        """,
        "Q19": """
            SELECT
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM lineitem, part
            WHERE
                (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#12'
                    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    AND l_quantity >= 1 AND l_quantity <= 1 + 10
                    AND p_size BETWEEN 1 AND 5
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
                OR (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#23'
                    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    AND l_quantity >= 10 AND l_quantity <= 10 + 10
                    AND p_size BETWEEN 1 AND 10
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
                OR (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#34'
                    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    AND l_quantity >= 20 AND l_quantity <= 20 + 10
                    AND p_size BETWEEN 1 AND 15
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
        """,
        "Q20": """
            SELECT s_name, s_address
            FROM supplier, nation
            WHERE
                s_suppkey IN (
                    SELECT ps_suppkey FROM partsupp
                    WHERE
                        ps_partkey IN (
                            SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
                        )
                        AND ps_availqty > (
                            SELECT 0.5 * SUM(l_quantity)
                            FROM lineitem
                            WHERE
                                l_partkey = ps_partkey
                                AND l_suppkey = ps_suppkey
                                AND l_shipdate >= DATE '1994-01-01'
                                AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
                        )
                )
                AND s_nationkey = n_nationkey
                AND n_name = 'CANADA'
            ORDER BY s_name
        """,
        "Q21": """
            SELECT
                s_name,
                COUNT(*) AS numwait
            FROM supplier, lineitem l1, orders, nation
            WHERE
                s_suppkey = l1.l_suppkey
                AND o_orderkey = l1.l_orderkey
                AND o_orderstatus = 'F'
                AND l1.l_receiptdate > l1.l_commitdate
                AND EXISTS (
                    SELECT * FROM lineitem l2
                    WHERE l2.l_orderkey = l1.l_orderkey
                      AND l2.l_suppkey <> l1.l_suppkey
                )
                AND NOT EXISTS (
                    SELECT * FROM lineitem l3
                    WHERE l3.l_orderkey = l1.l_orderkey
                      AND l3.l_suppkey <> l1.l_suppkey
                      AND l3.l_receiptdate > l3.l_commitdate
                )
                AND s_nationkey = n_nationkey
                AND n_name = 'SAUDI ARABIA'
            GROUP BY s_name
            ORDER BY numwait DESC, s_name
            LIMIT 100
        """,
        "Q22": """
            SELECT
                cntrycode,
                COUNT(*) AS numcust,
                SUM(c_acctbal) AS totacctbal
            FROM (
                SELECT
                    SUBSTRING(c_phone, 1, 2) AS cntrycode,
                    c_acctbal
                FROM customer
                WHERE
                    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                    AND c_acctbal > (
                        SELECT AVG(c_acctbal) FROM customer
                        WHERE
                            c_acctbal > 0.00
                            AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                    )
                    AND NOT EXISTS (
                        SELECT * FROM orders WHERE o_custkey = c_custkey
                    )
            ) AS custsale
            GROUP BY cntrycode
            ORDER BY cntrycode
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
        # Pipe through sed to strip trailing '|' from TPC-H .tbl files
        proc = subprocess.Popen(
            ["sed", "s/|$//", str(tbl_file)],
            stdout=subprocess.PIPE,
        )
        cur.copy_expert(f"COPY {table} FROM STDIN WITH (DELIMITER '|')", proc.stdout)
        proc.wait()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")

    cur.close()
    conn.close()


def pg_benchmark(scale_factor: int, num_runs: int, query_ids: list,
                 queries: dict, timeout: int = 300) -> dict:
    conn = psycopg2.connect(**get_pg_conn_params(scale_factor))
    cur = conn.cursor()
    cur.execute(f"SET statement_timeout = '{timeout * 1000}'")
    cur.execute(f"SET max_parallel_workers_per_gather = {BENCHMARK_THREADS}")
    cur.execute(f"SET max_parallel_workers = {BENCHMARK_THREADS}")
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        times = []
        timed_out = False
        for _ in range(num_runs):
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


def duckdb_benchmark(scale_factor: int, num_runs: int, query_ids: list,
                     queries: dict, timeout: int = 300) -> dict:
    duckdb_dir = Path(__file__).parent / "duckdb"
    db_path = duckdb_dir / f"tpch_sf{scale_factor}.duckdb"
    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return {}
    conn = duckdb.connect(str(db_path), read_only=True)
    conn.execute(f"SET threads = {BENCHMARK_THREADS}")
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        times = []
        timed_out = False
        for _ in range(num_runs):
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
            results[qname] = times
            avg_time = sum(times) / len(times)
            print(f"  DuckDB {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")
    conn.close()
    return results


# ---------------------------------------------------------------------------
# ClickHouse benchmark
# ---------------------------------------------------------------------------

CLICKHOUSE_TABLES = {
    "nation": "n_nationkey Int32, n_name String, n_regionkey Int32, n_comment String",
    "region": "r_regionkey Int32, r_name String, r_comment String",
    "supplier": "s_suppkey Int32, s_name String, s_address String, s_nationkey Int32, s_phone String, s_acctbal Decimal(15,2), s_comment String",
    "part": "p_partkey Int32, p_name String, p_mfgr String, p_brand String, p_type String, p_size Int32, p_container String, p_retailprice Decimal(15,2), p_comment String",
    "partsupp": "ps_partkey Int32, ps_suppkey Int32, ps_availqty Int32, ps_supplycost Decimal(15,2), ps_comment String",
    "customer": "c_custkey Int32, c_name String, c_address String, c_nationkey Int32, c_phone String, c_acctbal Decimal(15,2), c_mktsegment String, c_comment String",
    "orders": "o_orderkey Int32, o_custkey Int32, o_orderstatus String, o_totalprice Decimal(15,2), o_orderdate Date, o_orderpriority String, o_clerk String, o_shippriority Int32, o_comment String",
    "lineitem": "l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32, l_quantity Decimal(15,2), l_extendedprice Decimal(15,2), l_discount Decimal(15,2), l_tax Decimal(15,2), l_returnflag String, l_linestatus String, l_shipdate Date, l_commitdate Date, l_receiptdate Date, l_shipinstruct String, l_shipmode String, l_comment String",
}

CLICKHOUSE_ORDER_KEYS = {
    "nation": "n_nationkey",
    "region": "r_regionkey",
    "supplier": "s_suppkey",
    "part": "p_partkey",
    "partsupp": "(ps_partkey, ps_suppkey)",
    "customer": "c_custkey",
    "orders": "o_orderkey",
    "lineitem": "(l_orderkey, l_linenumber)",
}


def clickhouse_install():
    """Download ClickHouse single binary."""
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
        # Try alternative: the script may name it differently
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


def clickhouse_server_start(scale_factor: int):
    """Start a ClickHouse server process for the given SF."""
    ch_dir = Path(__file__).parent / "clickhouse"
    binary = ch_dir / "clickhouse"
    data_path = ch_dir / f"data_sf{scale_factor}"
    data_path.mkdir(parents=True, exist_ok=True)
    log_path = ch_dir / f"server_sf{scale_factor}.log"

    # ClickHouse uses -- --key=value syntax for config overrides
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
    # Wait for server to be ready
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
    """Stop ClickHouse server."""
    ch_dir = Path(__file__).parent / "clickhouse"
    binary = ch_dir / "clickhouse"
    try:
        # Graceful shutdown via SQL
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


def clickhouse_setup(data_dir: Path, scale_factor: int, force_setup: bool = False):
    """Create tables and load data into ClickHouse."""
    ch_dir = Path(__file__).parent / "clickhouse"
    binary = ch_dir / "clickhouse"

    def ch_query(sql):
        subprocess.run(
            [str(binary), "client", "--port", "9000", "-q", sql],
            check=True, capture_output=True, text=True, timeout=300,
        )

    if not force_setup:
        try:
            result = subprocess.run(
                [str(binary), "client", "--port", "9000", "-q",
                 "SELECT count() FROM lineitem"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and int(result.stdout.strip()) > 0:
                print("  ClickHouse already has data, skipping setup.")
                print("  Use --setup flag to force data reload.")
                return
        except Exception:
            pass

    # Create tables
    for table, cols in CLICKHOUSE_TABLES.items():
        order_key = CLICKHOUSE_ORDER_KEYS[table]
        ch_query(f"DROP TABLE IF EXISTS {table}")
        ch_query(f"CREATE TABLE {table} ({cols}) ENGINE = MergeTree() ORDER BY {order_key}")

    # Load data
    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        # Pipe through sed to strip trailing | then load as CSV with | delimiter
        subprocess.run(
            f"sed 's/|$//' '{tbl_file}' | '{binary}' client --port 9000 "
            f"--query='INSERT INTO {table} FORMAT CSV' "
            f"--format_csv_delimiter='|'",
            shell=True, check=True, capture_output=True, timeout=600,
        )
        result = subprocess.run(
            [str(binary), "client", "--port", "9000", "-q",
             f"SELECT count() FROM {table}"],
            capture_output=True, text=True, timeout=30,
        )
        count = result.stdout.strip()
        print(f" {int(count):,} rows")


def adapt_query_for_clickhouse(sql: str) -> str:
    """Adapt standard SQL for ClickHouse compatibility."""
    # INTERVAL 'N' UNIT -> INTERVAL N UNIT (ClickHouse doesn't quote interval values)
    sql = re.sub(
        r"INTERVAL\s+'(\d+)'\s+(DAY|MONTH|YEAR)",
        r"INTERVAL \1 \2",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def clickhouse_benchmark(scale_factor: int, num_runs: int, query_ids: list,
                         queries: dict, timeout: int = 300) -> dict:
    """Run benchmark queries on ClickHouse using clickhouse-driver."""
    try:
        from clickhouse_driver import Client
    except ImportError:
        print("  clickhouse-driver not installed. Run: pip install clickhouse-driver")
        return {}

    client = Client(host="localhost", port=9000,
                    settings={"max_execution_time": timeout,
                              "max_threads": BENCHMARK_THREADS})
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        sql = adapt_query_for_clickhouse(queries[qname])
        times = []
        timed_out = False
        for _ in range(num_runs):
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
            results[qname] = times
            avg_time = sum(times) / len(times)
            print(f"  ClickHouse {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")
    return results


# ---------------------------------------------------------------------------
# Umbra benchmark (Docker-based)
# ---------------------------------------------------------------------------

def umbra_install():
    """Pull the Umbra Docker image."""
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
    """Check if Docker is available and Umbra image is pulled."""
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


UMBRA_HOST_PORT = 5440  # Use a high port to avoid conflicts with existing PG instances


def umbra_start(data_dir: Path, scale_factor: int) -> str:
    """Start Umbra Docker container. Reuses existing container if running."""
    container_name = f"umbra_tpch_sf{scale_factor}"

    # Check if container exists (running or stopped)
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Status}}", container_name],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode == 0:
        status = result.stdout.strip()
        if status == "running":
            # Already running — check if it accepts connections
            try:
                conn = psycopg2.connect(
                    host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
                    password="postgres", dbname="postgres", connect_timeout=3,
                )
                conn.close()
                print(f"  Umbra container '{container_name}' already running (port {UMBRA_HOST_PORT}).")
                return container_name
            except Exception:
                pass  # Running but not accepting connections, restart below
        if status in ("exited", "created"):
            # Stopped container with data — just restart it
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
                    print(f"  Umbra container restarted and ready (port {UMBRA_HOST_PORT}).")
                    return container_name
                except Exception:
                    pass
            print("  Warning: Umbra may not be ready after restart.")
            return container_name

    # No existing container — remove any broken remnant and create fresh
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True, timeout=10,
    )
    print(f"  Starting Umbra container '{container_name}'...")
    # Umbra needs --privileged for io_uring and memlock.
    # Use port mapping (not --net=host) to avoid conflicts with existing PG instances.
    # The entrypoint sets password='postgres' and umbra-server listens on port 5432 inside container.
    subprocess.run(
        ["docker", "run", "-d",
         "--name", container_name,
         "--privileged",
         "--ulimit", "memlock=-1:-1",
         "--shm-size=4g",
         "-p", f"{UMBRA_HOST_PORT}:5432",
         "-v", f"{data_dir}:/data",
         "umbradb/umbra:latest"],
        check=True, capture_output=True, timeout=30,
    )
    # Wait for Umbra to accept connections
    for attempt in range(60):
        time.sleep(1)
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
                password="postgres", dbname="postgres", connect_timeout=3,
            )
            conn.close()
            print(f"  Umbra container started and ready (port {UMBRA_HOST_PORT}).")
            return container_name
        except Exception:
            pass
    print("  Warning: Umbra may not be ready.")
    return container_name


def umbra_stop(container_name: str):
    """Stop Umbra container (keep it so data persists for next run)."""
    subprocess.run(
        ["docker", "stop", container_name],
        capture_output=True, timeout=30,
    )
    print(f"  Umbra container '{container_name}' stopped (data preserved).")


def umbra_setup(data_dir: Path, scale_factor: int, force_setup: bool = False):
    """Create tables and load data into Umbra."""
    conn = psycopg2.connect(
        host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
        password="postgres", dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()

    if not force_setup:
        try:
            cur.execute("SELECT COUNT(*) FROM lineitem")
            count = cur.fetchone()[0]
            if count > 0:
                print("  Umbra already has data, skipping setup.")
                print("  Use --setup flag to force data reload.")
                cur.close()
                conn.close()
                return
        except Exception:
            conn.rollback()

    schema_path = Path(__file__).parent / "schema.sql"
    schema_sql = schema_path.read_text()
    schema_no_fk = _strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            try:
                cur.execute(f"DROP TABLE IF EXISTS {stmt.split()[2]}")
            except Exception:
                conn.rollback()
            cur.execute(stmt)

    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        # Load via COPY from mounted data dir
        try:
            cur.execute(
                f"COPY {table} FROM '/data/{table}.tbl' WITH (DELIMITER '|')"
            )
        except Exception:
            conn.rollback()
            # Fallback: pipe cleaned data via STDIN
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


def umbra_benchmark(scale_factor: int, num_runs: int, query_ids: list,
                    queries: dict, timeout: int = 300) -> dict:
    """Run benchmark queries on Umbra (PostgreSQL-compatible wire protocol)."""
    conn = psycopg2.connect(
        host="127.0.0.1", port=UMBRA_HOST_PORT, user="postgres",
        password="postgres", dbname="postgres",
    )
    cur = conn.cursor()
    try:
        cur.execute(f"SET statement_timeout = '{timeout * 1000}'")
    except Exception:
        conn.rollback()

    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        times = []
        timed_out = False
        for _ in range(num_runs):
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
            results[qname] = times
            avg_time = sum(times) / len(times)
            print(f"  Umbra {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")
    cur.close()
    conn.close()
    return results


# ---------------------------------------------------------------------------
# MonetDB benchmark
# ---------------------------------------------------------------------------

def monetdb_install():
    """Install MonetDB via apt."""
    if shutil.which("monetdbd") or shutil.which("mserver5"):
        print("  MonetDB already installed.")
        return True
    print("  Installing MonetDB...")
    try:
        # Detect suite name
        result = subprocess.run(
            ["lsb_release", "-cs"], capture_output=True, text=True, timeout=10,
        )
        suite = result.stdout.strip() if result.returncode == 0 else "focal"

        # Add repo
        list_content = f"deb https://dev.monetdb.org/downloads/deb/ {suite} monetdb\n"
        subprocess.run(
            ["sudo", "tee", "/etc/apt/sources.list.d/monetdb.list"],
            input=list_content, capture_output=True, text=True, check=True, timeout=10,
        )
        subprocess.run(
            ["sudo", "bash", "-c",
             "wget -qO /etc/apt/trusted.gpg.d/monetdb.gpg "
             "https://dev.monetdb.org/downloads/MonetDB-GPG-KEY.gpg"],
            check=True, capture_output=True, timeout=30,
        )
        subprocess.run(
            ["sudo", "apt-get", "update", "-y"],
            check=True, capture_output=True, timeout=60,
        )
        subprocess.run(
            ["sudo", "apt-get", "install", "-y", "monetdb5-sql", "monetdb-client"],
            check=True, capture_output=True, timeout=120,
        )
        print("  MonetDB installed successfully.")
        return True
    except Exception as e:
        print(f"  MonetDB installation failed: {e}")
        return False


def monetdb_available() -> bool:
    return shutil.which("monetdbd") is not None or shutil.which("mserver5") is not None


def monetdb_server_start(scale_factor: int):
    """Start MonetDB server for the given SF."""
    farm_dir = Path(__file__).parent / "monetdb" / f"farm_sf{scale_factor}"
    farm_dir.mkdir(parents=True, exist_ok=True)
    dbname = f"tpch_sf{scale_factor}"

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
    """Stop MonetDB server."""
    if farm_dir:
        os.system(f"monetdbd stop '{farm_dir}'")
        time.sleep(2)
        print("  MonetDB server stopped.")


def monetdb_setup(data_dir: Path, scale_factor: int, force_setup: bool = False):
    """Create tables and load data into MonetDB."""
    try:
        import pymonetdb
    except ImportError:
        print("  pymonetdb not installed. Run: pip install pymonetdb")
        return

    dbname = f"tpch_sf{scale_factor}"
    conn = pymonetdb.connect(database=dbname, hostname="localhost",
                             port=50000, username="monetdb", password="monetdb")
    conn.autocommit = False  # Use explicit commits for COPY INTO durability
    cur = conn.cursor()

    if not force_setup:
        try:
            cur.execute("SELECT COUNT(*) FROM lineitem")
            count = cur.fetchone()[0]
            if count > 0:
                print("  MonetDB already has data, skipping setup.")
                print("  Use --setup flag to force data reload.")
                cur.close()
                conn.close()
                return
        except Exception:
            conn.rollback()

    # Create tables (standard SQL, no FK constraints)
    schema_path = Path(__file__).parent / "schema.sql"
    schema_sql = schema_path.read_text()
    schema_no_fk = _strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            table_name = stmt.split()[2]
            try:
                cur.execute(f"DROP TABLE {table_name}")
                conn.commit()
            except Exception:
                conn.rollback()
            cur.execute(stmt)
            conn.commit()

    # TPC-H .tbl files have a trailing '|' on each line. MonetDB interprets
    # this as an extra column, causing COPY INTO to silently load 0 rows.
    # Strip trailing '|' into temp files before loading.
    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    tmp_dir = data_dir / "_monetdb_clean"
    tmp_dir.mkdir(exist_ok=True)

    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)

        # Strip trailing '|' from each line
        clean_file = tmp_dir / f"{table}.tbl"
        if not clean_file.exists():
            subprocess.run(
                ["sed", "s/|$//", str(tbl_file)],
                stdout=open(str(clean_file), "w"),
                check=True, timeout=120,
            )

        abs_path = str(clean_file.resolve())
        cur.execute(
            f"COPY INTO {table} FROM '{abs_path}' "
            f"USING DELIMITERS '|','\\n','\"' NULL AS ''"
        )
        conn.commit()  # Explicit commit after each COPY INTO
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")

    cur.close()
    conn.close()


def monetdb_benchmark(scale_factor: int, num_runs: int, query_ids: list,
                      queries: dict, timeout: int = 300) -> dict:
    """Run benchmark queries on MonetDB."""
    try:
        import pymonetdb
    except ImportError:
        print("  pymonetdb not installed. Run: pip install pymonetdb")
        return {}

    dbname = f"tpch_sf{scale_factor}"
    conn = pymonetdb.connect(database=dbname, hostname="localhost",
                             port=50000, username="monetdb", password="monetdb")
    cur = conn.cursor()
    try:
        cur.execute(f"SET nthreads = {BENCHMARK_THREADS}")
    except Exception:
        conn.rollback()  # MonetDB aborts transaction on failed SET

    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        times = []
        timed_out = False
        for _ in range(num_runs):
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
                timed_out = True
                break
        if not timed_out and times:
            results[qname] = times
            avg_time = sum(times) / len(times)
            print(f"  MonetDB {qname}: {avg_time:.1f} ms (avg of {num_runs} runs)")
    cur.close()
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
        total_cell = f"{int(round(total))} ms"
        cells.append(f"{BOLD}{total_cell:>{COL_W}}{RESET}")
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
SYSTEM_COLORS = {
    "GenDB": "#2196F3",
    "PostgreSQL": "#4CAF50",
    "DuckDB": "#FF9800",
    "ClickHouse": "#E91E63",
    "Umbra": "#9C27B0",
    "MonetDB": "#00BCD4",
}


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
    queries = sorted(set(q for results in all_results.values() for q in results),
                     key=lambda q: int(q[1:]))
    systems = list(all_results.keys())

    data = {}
    for system in systems:
        data[system] = []
        for q in queries:
            times = all_results[system].get(q, [])
            data[system].append(sum(times) / len(times) if times else 0)

    # Per-query grouped bar chart (wider for 22 queries)
    fig_width = max(20, len(queries) * 0.9)
    fig, ax = plt.subplots(figsize=(fig_width, 7))
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
                        f"{val:.1f}", ha="center", va="bottom", fontsize=7,
                        rotation=45)

    ax.set_xlabel("TPC-H Query", fontsize=12)
    ax.set_ylabel("Execution Time (ms, log scale)", fontsize=12)
    ax.set_title(f"TPC-H Per-Query Execution Time (SF={scale_factor})", fontsize=14)
    ax.set_xticks(list(x))
    ax.set_xticklabels(queries, fontsize=10, rotation=45, ha="right")
    ax.legend(fontsize=10)
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Per-query plot saved to: {output_path}")

    # Total execution time bar chart
    fig2, ax2 = plt.subplots(figsize=(max(8, len(systems) * 1.5), 6))
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
    parser = argparse.ArgumentParser(
        description="TPC-H Benchmark: GenDB vs PostgreSQL vs DuckDB vs ClickHouse vs Umbra vs MonetDB")
    parser.add_argument("--sf", type=int, required=True, help="Scale factor (e.g., 1, 10)")
    parser.add_argument("--gendb-run", type=Path, default=None,
                        help="Path to GenDB run directory (default: output/tpc-h/latest/)")
    parser.add_argument("--data-dir", type=Path, default=None,
                        help="Path to TPC-H .tbl data files (default: benchmarks/tpc-h/data/sf{N})")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per query (default: 3)")
    parser.add_argument("--setup", action="store_true", help="Force database setup/reload")
    parser.add_argument("--output", type=Path, default=None, help="Output plot path")
    parser.add_argument("--gendb-only", action="store_true",
                        help="Skip all baseline benchmarks")
    parser.add_argument("--skip-clickhouse", action="store_true",
                        help="Skip ClickHouse benchmark")
    parser.add_argument("--skip-umbra", action="store_true",
                        help="Skip Umbra benchmark")
    parser.add_argument("--skip-monetdb", action="store_true",
                        help="Skip MonetDB benchmark")
    parser.add_argument("--timeout", type=int, default=300,
                        help="Per-query timeout in seconds (default: 300)")
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

    # Build query dict
    queries = get_queries(args.sf)

    print(f"Scale factor:   {args.sf}")
    print(f"Runs per query: {args.runs}")
    print(f"Query timeout:  {args.timeout}s")
    print(f"Queries:        {len(queries)} (Q1-Q22)")
    if args.gendb_run:
        print(f"GenDB run:      {args.gendb_run}")
        print(f"GenDB storage:  {gendb_dir}")
    print()

    all_results = {}
    gendb_history = None
    gendb_query_ids = sorted(queries.keys(), key=lambda q: int(q[1:]))

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
            all_results["PostgreSQL"] = pg_benchmark(
                args.sf, args.runs, gendb_query_ids, queries, args.timeout)
        except Exception as e:
            print(f"  PostgreSQL error: {e}")

        # --- DuckDB ---
        print("\n=== DuckDB Setup ===")
        try:
            duckdb_setup(data_dir, args.sf, args.setup)
            print("\n=== DuckDB Benchmark ===")
            all_results["DuckDB"] = duckdb_benchmark(
                args.sf, args.runs, gendb_query_ids, queries, args.timeout)
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
                    ch_proc = clickhouse_server_start(args.sf)
                    clickhouse_setup(data_dir, args.sf, args.setup)
                    print("\n=== ClickHouse Benchmark ===")
                    all_results["ClickHouse"] = clickhouse_benchmark(
                        args.sf, args.runs, gendb_query_ids, queries, args.timeout)
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
                    umbra_container = umbra_start(data_dir, args.sf)
                    umbra_setup(data_dir, args.sf, args.setup)
                    print("\n=== Umbra Benchmark ===")
                    all_results["Umbra"] = umbra_benchmark(
                        args.sf, args.runs, gendb_query_ids, queries, args.timeout)
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
                    monetdb_farm = monetdb_server_start(args.sf)
                    monetdb_setup(data_dir, args.sf, args.setup)
                    print("\n=== MonetDB Benchmark ===")
                    all_results["MonetDB"] = monetdb_benchmark(
                        args.sf, args.runs, gendb_query_ids, queries, args.timeout)
                else:
                    print("  MonetDB not available, skipping.")
            except Exception as e:
                print(f"  MonetDB error: {e}")
            finally:
                if monetdb_farm:
                    monetdb_server_stop(monetdb_farm)

    # --- Results summary ---
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY (average of N runs, in ms)")
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
