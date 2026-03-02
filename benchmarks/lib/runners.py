"""Benchmark runners: pg, duckdb, clickhouse, umbra, monetdb — parameterized by WorkloadConfig."""

import time

import duckdb as duckdb_mod
import psycopg2
import psycopg2.errors

from .utils import BENCHMARK_THREADS, drop_os_caches


def get_pg_conn_params(config) -> dict:
    return {
        "host": "/var/run/postgresql",
        "port": 5435,
        "user": "postgres",
        "dbname": config.db_name,
    }


def pg_benchmark(config, mode: str, query_ids: list,
                 queries: dict, timeout: int = 300) -> dict:
    conn = psycopg2.connect(**get_pg_conn_params(config))
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


def duckdb_benchmark(config, mode: str, query_ids: list,
                     queries: dict, timeout: int = 300) -> dict:
    db_path = config.duckdb_path
    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return {}
    conn = duckdb_mod.connect(str(db_path), read_only=True)
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


def clickhouse_benchmark(config, mode: str, query_ids: list,
                         queries: dict, timeout: int = 300) -> dict:
    try:
        from clickhouse_driver import Client
    except ImportError:
        print("  clickhouse-driver not installed. Run: pip install clickhouse-driver")
        return {}

    settings = {
        "max_execution_time": timeout,
        "max_threads": BENCHMARK_THREADS,
        "use_query_cache": 0,
    }
    settings.update(config.clickhouse_settings)
    client = Client(host="localhost", port=9000, settings=settings)
    total_runs = 4 if mode == "hot" else 1
    results = {}
    for qname in query_ids:
        if qname not in queries:
            continue
        if mode == "cold":
            drop_os_caches()
        sql = config.adapt_query_fn(queries[qname])
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


def umbra_benchmark(config, mode: str, query_ids: list,
                    queries: dict, timeout: int = 300) -> dict:
    conn = psycopg2.connect(
        host="127.0.0.1", port=config.umbra_port, user="postgres",
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


def monetdb_benchmark(config, mode: str, query_ids: list,
                      queries: dict, timeout: int = 300) -> dict:
    try:
        import pymonetdb
    except ImportError:
        print("  pymonetdb not installed. Run: pip install pymonetdb")
        return {}

    conn = pymonetdb.connect(database=config.db_name, hostname="localhost",
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
