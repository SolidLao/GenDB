"""Shared main() logic that orchestrates setup/benchmark/index/plot/save."""

import json
import subprocess
import sys
import time
from pathlib import Path

import psycopg2

from .config import WorkloadConfig
from .gendb import (
    read_gendb_iteration_history, print_iteration_table,
    gendb_benchmark_best,
)
from .indexes import (
    parse_gendb_indexes, create_indexes_pg, create_indexes_duckdb,
    create_indexes_clickhouse, create_indexes_umbra, create_indexes_monetdb,
    drop_indexes_pg, drop_indexes_duckdb, drop_indexes_clickhouse,
    drop_indexes_umbra, drop_indexes_monetdb,
)
from .plotting import plot_results, plot_combined_results
from .runners import (
    pg_benchmark, duckdb_benchmark, clickhouse_benchmark,
    umbra_benchmark, monetdb_benchmark,
)
from .systems import (
    clickhouse_install, clickhouse_available, clickhouse_server_start,
    clickhouse_server_stop, umbra_install, umbra_available, umbra_start,
    umbra_stop, monetdb_available, monetdb_server_start, monetdb_server_stop,
)
from .utils import restart_postgresql


def run_benchmark(config: WorkloadConfig, args, skip_plot=False):
    """Run the full benchmark pipeline: GenDB history -> baselines -> with-indexes -> save -> plot.

    If skip_plot=True, skip final plotting and return a dict with collected data.
    """
    project_root = config.benchmark_root.parent.parent
    benchmark_root = config.benchmark_root
    queries = config.queries

    # Auto-detect GenDB run directory
    if args.gendb_run is None:
        latest_link = project_root / "output" / config.name / "latest"
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
        gendb_dir = benchmark_root / "gendb" / f"{config.db_name}.gendb"

    print(f"Workload:       {config.name}")
    print(f"Scale:          {config.scale_label}")
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

    # --- Plot-only mode: load existing metrics ---
    if args.plot_only:
        metrics_dir = benchmark_root / "results" / f"sf{config.scale_value}" / "metrics"
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

    # --- GenDB: iteration history ---
    if not args.plot_only and args.gendb_run and args.gendb_run.exists():
        print("=== GenDB Iteration History ===")
        gendb_history = read_gendb_iteration_history(args.gendb_run)
        if gendb_history["query_ids"]:
            gendb_query_ids = gendb_history["query_ids"]
            print()
            print_iteration_table(gendb_history)
            print()

            metrics_dir = benchmark_root / "results" / f"sf{config.scale_value}" / "metrics"
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

        # --- GenDB: benchmark best binaries ---
        print(f"\n=== GenDB Benchmark (best per-query, {args.mode} mode) ===")
        best_results = gendb_benchmark_best(args.gendb_run, gendb_dir, args.mode)
        if best_results:
            all_results["GenDB"] = best_results
    elif not args.plot_only:
        print("=== GenDB: SKIPPED (run directory not found) ===")

    # Parse indexes for cleanup
    _cleanup_indexes = []
    if args.gendb_run:
        sd_path = args.gendb_run / "storage_design.json"
        if sd_path and sd_path.exists():
            if config.parse_indexes_fn:
                _cleanup_indexes = config.parse_indexes_fn(sd_path)
            else:
                _cleanup_indexes = parse_gendb_indexes(sd_path, config.pk_columns)

    # Drop any previously created indexes to ensure clean baseline runs
    if not args.plot_only and not args.gendb_only and _cleanup_indexes:
        print("\n=== Dropping previously created indexes (if any) ===")
        try:
            drop_indexes_pg(config, _cleanup_indexes)
            print("  PostgreSQL: done")
        except Exception:
            pass
        try:
            drop_indexes_duckdb(config, _cleanup_indexes)
            print("  DuckDB: done")
        except Exception:
            pass
        try:
            drop_indexes_clickhouse(config, _cleanup_indexes)
            print("  ClickHouse: done")
        except Exception:
            pass

    if not args.plot_only and not args.gendb_only:
        # --- PostgreSQL ---
        if not config.skip_postgres:
            print("\n=== PostgreSQL Setup ===")
            try:
                config.pg_setup_fn(config, args.setup)
                restart_postgresql()
                print(f"\n=== PostgreSQL Benchmark ({args.mode} mode) ===")
                all_results["PostgreSQL"] = pg_benchmark(
                    config, args.mode, gendb_query_ids, queries, args.timeout)
            except Exception as e:
                print(f"  PostgreSQL error: {e}")

        # --- DuckDB ---
        if not config.skip_duckdb:
            if config.duckdb_setup_fn:
                print("\n=== DuckDB Setup ===")
                try:
                    config.duckdb_setup_fn(config, args.setup)
                except Exception as e:
                    print(f"  DuckDB setup error: {e}")
            print(f"\n=== DuckDB Benchmark ({args.mode} mode) ===")
            try:
                all_results["DuckDB"] = duckdb_benchmark(
                    config, args.mode, gendb_query_ids, queries, args.timeout)
            except Exception as e:
                print(f"  DuckDB error: {e}")

        # --- ClickHouse ---
        ch_proc = None
        if not config.skip_clickhouse:
            print("\n=== ClickHouse Setup ===")
            try:
                if not clickhouse_available(benchmark_root):
                    clickhouse_install(benchmark_root)
                if clickhouse_available(benchmark_root):
                    ch_proc = clickhouse_server_start(benchmark_root, config)
                    config.clickhouse_setup_fn(config, args.setup)
                    ch_dir = benchmark_root / "clickhouse"
                    subprocess.run(
                        [str(ch_dir / "clickhouse"), "client", "--port", "9000",
                         "-q", "SYSTEM DROP CACHE"],
                        capture_output=True, timeout=10,
                    )
                    print("  ClickHouse internal caches cleared.")
                    print(f"\n=== ClickHouse Benchmark ({args.mode} mode) ===")
                    all_results["ClickHouse"] = clickhouse_benchmark(
                        config, args.mode, gendb_query_ids, queries, args.timeout)
                else:
                    print("  ClickHouse not available, skipping.")
            except Exception as e:
                print(f"  ClickHouse error: {e}")
            finally:
                if ch_proc:
                    clickhouse_server_stop(ch_proc, benchmark_root)

        # --- Umbra ---
        umbra_container = None
        if not config.skip_umbra:
            print("\n=== Umbra Setup ===")
            try:
                if not umbra_available():
                    umbra_install()
                if umbra_available():
                    umbra_container = umbra_start(config)
                    config.umbra_setup_fn(config, args.setup)
                    # Restart Umbra to clear internal buffer pool
                    subprocess.run(
                        ["docker", "restart", umbra_container],
                        capture_output=True, timeout=30,
                    )
                    for _ in range(60):
                        time.sleep(1)
                        try:
                            c = psycopg2.connect(
                                host="127.0.0.1", port=config.umbra_port, user="postgres",
                                password="postgres", dbname="postgres", connect_timeout=3)
                            c.close()
                            break
                        except Exception:
                            pass
                    print("  Umbra container restarted (buffer pool cleared).")
                    if _cleanup_indexes:
                        try:
                            drop_indexes_umbra(config, _cleanup_indexes)
                            print("  Umbra: indexes dropped.")
                        except Exception:
                            pass
                    print(f"\n=== Umbra Benchmark ({args.mode} mode) ===")
                    all_results["Umbra"] = umbra_benchmark(
                        config, args.mode, gendb_query_ids, queries, args.timeout)
                else:
                    print("  Umbra not available, skipping.")
            except Exception as e:
                print(f"  Umbra error: {e}")
            finally:
                if umbra_container:
                    umbra_stop(umbra_container)

        # --- MonetDB ---
        monetdb_farm = None
        if not config.skip_monetdb:
            print("\n=== MonetDB Setup ===")
            try:
                if not monetdb_available():
                    config.monetdb_install_fn()
                if monetdb_available():
                    monetdb_farm = monetdb_server_start(config)
                    config.monetdb_setup_fn(config, args.setup)
                    # Restart MonetDB to clear internal caches
                    monetdb_server_stop(monetdb_farm)
                    time.sleep(2)
                    monetdb_farm = monetdb_server_start(config)
                    print("  MonetDB restarted (internal caches cleared).")
                    if _cleanup_indexes:
                        try:
                            drop_indexes_monetdb(config, _cleanup_indexes)
                            print("  MonetDB: indexes dropped.")
                        except Exception:
                            pass
                    print(f"\n=== MonetDB Benchmark ({args.mode} mode) ===")
                    all_results["MonetDB"] = monetdb_benchmark(
                        config, args.mode, gendb_query_ids, queries, args.timeout)
                else:
                    print("  MonetDB not available, skipping.")
            except Exception as e:
                print(f"  MonetDB error: {e}")
            finally:
                if monetdb_farm:
                    monetdb_server_stop(monetdb_farm)

    # --- With-indexes benchmark ---
    indexed_results = {}
    if not args.plot_only and args.with_indexes and not args.gendb_only:
        sd_path = None
        if args.gendb_run:
            sd_path = args.gendb_run / "storage_design.json"
        if sd_path and sd_path.exists():
            print(f"\n{'=' * 60}")
            print("WITH-INDEXES BENCHMARK")
            print(f"  Reading indexes from: {sd_path}")
            if config.parse_indexes_fn:
                gendb_indexes = config.parse_indexes_fn(sd_path)
            else:
                gendb_indexes = parse_gendb_indexes(sd_path, config.pk_columns)
            if gendb_indexes:
                print(f"  Found {len(gendb_indexes)} non-PK indexes:")
                for idx in gendb_indexes:
                    print(f"    {idx['table']}({', '.join(idx['columns'])})")

                # PostgreSQL with indexes
                if "PostgreSQL" in all_results and not config.skip_postgres:
                    print(f"\n=== PostgreSQL + Indexes ===")
                    try:
                        create_indexes_pg(config, gendb_indexes)
                        restart_postgresql()
                        print(f"  Running benchmark with indexes ({args.mode} mode)...")
                        indexed_results["PostgreSQL"] = pg_benchmark(
                            config, args.mode, gendb_query_ids, queries, args.timeout)
                    except Exception as e:
                        print(f"  PostgreSQL indexed error: {e}")

                # DuckDB with indexes
                if "DuckDB" in all_results and not config.skip_duckdb:
                    print(f"\n=== DuckDB + Indexes ===")
                    try:
                        create_indexes_duckdb(config, gendb_indexes)
                        print(f"  Running benchmark with indexes ({args.mode} mode)...")
                        indexed_results["DuckDB"] = duckdb_benchmark(
                            config, args.mode, gendb_query_ids, queries, args.timeout)
                    except Exception as e:
                        print(f"  DuckDB indexed error: {e}")

                # ClickHouse with indexes
                if "ClickHouse" in all_results and not config.skip_clickhouse:
                    ch_proc = None
                    try:
                        ch_proc = clickhouse_server_start(benchmark_root, config)
                        print(f"\n=== ClickHouse + Indexes ===")
                        create_indexes_clickhouse(config, gendb_indexes)
                        ch_dir = benchmark_root / "clickhouse"
                        subprocess.run(
                            [str(ch_dir / "clickhouse"), "client", "--port", "9000",
                             "-q", "SYSTEM DROP CACHE"],
                            capture_output=True, timeout=10,
                        )
                        print(f"  Running benchmark with indexes ({args.mode} mode)...")
                        indexed_results["ClickHouse"] = clickhouse_benchmark(
                            config, args.mode, gendb_query_ids, queries, args.timeout)
                    except Exception as e:
                        print(f"  ClickHouse indexed error: {e}")
                    finally:
                        if ch_proc:
                            clickhouse_server_stop(ch_proc, benchmark_root)

                # Umbra with indexes
                if "Umbra" in all_results and not config.skip_umbra:
                    umbra_container = None
                    try:
                        umbra_container = umbra_start(config)
                        print(f"\n=== Umbra + Indexes ===")
                        create_indexes_umbra(config, gendb_indexes)
                        subprocess.run(
                            ["docker", "restart", umbra_container],
                            capture_output=True, timeout=30,
                        )
                        for _ in range(60):
                            time.sleep(1)
                            try:
                                c = psycopg2.connect(
                                    host="127.0.0.1", port=config.umbra_port, user="postgres",
                                    password="postgres", dbname="postgres", connect_timeout=3)
                                c.close()
                                break
                            except Exception:
                                pass
                        print(f"  Running benchmark with indexes ({args.mode} mode)...")
                        indexed_results["Umbra"] = umbra_benchmark(
                            config, args.mode, gendb_query_ids, queries, args.timeout)
                    except Exception as e:
                        print(f"  Umbra indexed error: {e}")
                    finally:
                        if umbra_container:
                            umbra_stop(umbra_container)

                # MonetDB with indexes
                if "MonetDB" in all_results and not config.skip_monetdb:
                    monetdb_farm = None
                    try:
                        monetdb_farm = monetdb_server_start(config)
                        print(f"\n=== MonetDB + Indexes ===")
                        create_indexes_monetdb(config, gendb_indexes)
                        monetdb_server_stop(monetdb_farm)
                        time.sleep(2)
                        monetdb_farm = monetdb_server_start(config)
                        print(f"  Running benchmark with indexes ({args.mode} mode)...")
                        indexed_results["MonetDB"] = monetdb_benchmark(
                            config, args.mode, gendb_query_ids, queries, args.timeout)
                    except Exception as e:
                        print(f"  MonetDB indexed error: {e}")
                    finally:
                        if monetdb_farm:
                            monetdb_server_stop(monetdb_farm)
            else:
                print("  No non-PK indexes found in storage_design.json.")
        else:
            print("\n  Warning: --with-indexes requires --gendb-run with storage_design.json")

    # Load indexed results from JSON in plot-only mode
    if args.plot_only:
        metrics_dir = benchmark_root / "results" / f"sf{config.scale_value}" / "metrics"
        idx_json_path = metrics_dir / "benchmark_indexed_results.json"
        if idx_json_path.exists():
            with open(idx_json_path) as f:
                saved_idx = json.load(f)
            for system, system_data in saved_idx.items():
                indexed_results[system] = {}
                for qid, qdata in system_data.items():
                    indexed_results[system][qid] = qdata["all_ms"]
            print(f"Loaded indexed results from: {idx_json_path}")

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
    if indexed_results:
        header += " | " + "".join(f"{s + ' (idx)':<18}" for s in indexed_results.keys())
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
        if indexed_results:
            row += " | "
            for system in indexed_results:
                times = indexed_results[system].get(q, [])
                if times:
                    row += f"{sum(times) / len(times):<18.2f}"
                else:
                    row += f"{'N/A':<18}"
        print(row)
    print("-" * len(header))
    row = f"{'Total':<8}"
    for system in all_results:
        total = sum(
            sum(all_results[system].get(q, [0])) / max(len(all_results[system].get(q, [1])), 1)
            for q in all_queries
        )
        row += f"{total:<15.2f}"
    if indexed_results:
        row += " | "
        for system in indexed_results:
            total = sum(
                sum(indexed_results[system].get(q, [0])) / max(len(indexed_results[system].get(q, [1])), 1)
                for q in all_queries
            )
            row += f"{total:<18.2f}"
    print(row)
    print()

    # --- Save JSON results ---
    if not args.plot_only:
        metrics_dir = benchmark_root / "results" / f"sf{config.scale_value}" / "metrics"
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

        if indexed_results:
            idx_json_path = metrics_dir / "benchmark_indexed_results.json"
            idx_json_data = {}
            for system, results in indexed_results.items():
                idx_json_data[system] = {
                    q: {
                        "all_ms": times,
                        "average_ms": sum(times) / len(times) if times else 0,
                        "min_ms": min(times) if times else 0,
                        "max_ms": max(times) if times else 0,
                    }
                    for q, times in results.items()
                }
            with open(idx_json_path, "w") as f:
                json.dump(idx_json_data, f, indent=2)
            print(f"Indexed results saved to: {idx_json_path}")

    # --- Plots ---
    min_systems = 1 if args.plot_only else 2
    all_query_ids = sorted(queries.keys(), key=lambda q: int(q[1:]))

    if skip_plot:
        return {
            "name": config.name,
            "scale_label": config.scale_label,
            "all_results": all_results,
            "gendb_history": gendb_history,
            "indexed_results": indexed_results if indexed_results else None,
            "all_query_ids": all_query_ids,
            "timeout_ms": args.timeout * 1000,
        }

    if len(all_results) >= min_systems:
        print()
        plot_results(all_results, args.output, scale_label=config.scale_label,
                     timeout_ms=args.timeout * 1000, all_query_ids=all_query_ids,
                     gendb_history=gendb_history,
                     indexed_results=indexed_results if indexed_results else None)


def run_combined_benchmark(workload_results, output_dir):
    """Generate a combined multi-workload figure from collected results.

    Args:
        workload_results: list of dicts returned by run_benchmark(skip_plot=True)
        output_dir: Path to output directory for the combined figure
    """
    plot_combined_results(workload_results, output_dir)
