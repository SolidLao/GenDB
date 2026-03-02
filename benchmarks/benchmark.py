#!/usr/bin/env python3
"""
Unified Benchmark Entry Point: GenDB vs PostgreSQL vs DuckDB vs ClickHouse vs Umbra vs MonetDB

Usage:
    python3 benchmarks/benchmark.py --benchmark tpc-h --sf <N> [options]
    python3 benchmarks/benchmark.py --benchmark sec-edgar --years <N> [options]
    python3 benchmarks/benchmark.py --benchmark all --sf <N> --years <N> [options]

Common options:
    --gendb-run <path>    Path to GenDB run directory
    --mode <hot|cold>     Benchmark mode (default: hot)
    --setup               Force database setup/reload
    --output <path>       Output plot path
    --gendb-only          Skip all baseline benchmarks
    --skip-clickhouse     Skip ClickHouse benchmark
    --skip-umbra          Skip Umbra benchmark
    --skip-monetdb        Skip MonetDB benchmark
    --timeout <N>         Per-query timeout in seconds (default: 300)
    --plot-only           Skip all benchmarks; re-plot from existing metrics
    --with-indexes        Also benchmark with GenDB-equivalent indexes
"""

import argparse
import importlib.util
import sys
from pathlib import Path

# Add the project root to sys.path so `from benchmarks.lib...` works
_project_root = Path(__file__).parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from benchmarks.lib.main_runner import run_benchmark, run_combined_benchmark


def _load_workload_module(workload_path):
    """Load a workload module from a file path (handles hyphenated directory names)."""
    spec = importlib.util.spec_from_file_location("workload", workload_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    parser = argparse.ArgumentParser(
        description="Unified Benchmark: GenDB vs PostgreSQL vs DuckDB vs ClickHouse vs Umbra vs MonetDB")
    parser.add_argument("--benchmark", choices=["tpc-h", "sec-edgar", "all"], required=True,
                        help="Workload to benchmark")

    # TPC-H specific
    parser.add_argument("--sf", type=int, default=None,
                        help="Scale factor for TPC-H (e.g., 1, 10)")
    parser.add_argument("--data-dir", type=Path, default=None,
                        help="Path to TPC-H .tbl data files")

    # SEC-EDGAR specific
    parser.add_argument("--years", type=int, default=None,
                        help="Number of years of data for SEC-EDGAR (e.g., 3)")

    # Common options
    parser.add_argument("--gendb-run", type=Path, default=None,
                        help="Path to GenDB run directory")
    parser.add_argument("--mode", type=str, default="hot", choices=["hot", "cold"],
                        help="Benchmark mode (default: hot)")
    parser.add_argument("--setup", action="store_true",
                        help="Force database setup/reload")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output plot path")
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
                        help="Skip all benchmarks; read existing metrics and re-plot")
    parser.add_argument("--with-indexes", action="store_true",
                        help="Also benchmark baseline systems with GenDB-equivalent indexes")

    args = parser.parse_args()

    benchmarks_dir = Path(__file__).parent

    if args.benchmark == "all":
        # Require both --sf and --years for combined mode
        if args.sf is None:
            parser.error("--sf is required for --benchmark all")
        if args.years is None:
            parser.error("--years is required for --benchmark all")

        # Run each workload individually, collecting results without plotting
        workload_results = []

        # TPC-H
        print("=" * 60)
        print("RUNNING TPC-H BENCHMARK")
        print("=" * 60)
        tpch_mod = _load_workload_module(benchmarks_dir / "tpc-h" / "workload.py")
        tpch_config = tpch_mod.get_config(args)
        result = run_benchmark(tpch_config, args, skip_plot=True)
        if result:
            workload_results.append(result)

        # SEC-EDGAR
        print()
        print("=" * 60)
        print("RUNNING SEC-EDGAR BENCHMARK")
        print("=" * 60)
        edgar_mod = _load_workload_module(benchmarks_dir / "sec-edgar" / "workload.py")
        edgar_config = edgar_mod.get_config(args)
        result = run_benchmark(edgar_config, args, skip_plot=True)
        if result:
            workload_results.append(result)

        # Generate combined figure
        if workload_results:
            print()
            print("=" * 60)
            print("GENERATING COMBINED FIGURE")
            print("=" * 60)
            output_dir = benchmarks_dir / "figures" / "all_benchmarks"
            run_combined_benchmark(workload_results, output_dir)

    else:
        # Single workload mode
        if args.benchmark == "tpc-h":
            if args.sf is None:
                parser.error("--sf is required for TPC-H benchmark")
            mod = _load_workload_module(benchmarks_dir / "tpc-h" / "workload.py")
        elif args.benchmark == "sec-edgar":
            if args.years is None:
                parser.error("--years is required for SEC-EDGAR benchmark")
            mod = _load_workload_module(benchmarks_dir / "sec-edgar" / "workload.py")

        config = mod.get_config(args)
        run_benchmark(config, args)


if __name__ == "__main__":
    main()
