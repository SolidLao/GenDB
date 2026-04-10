#!/usr/bin/env python3
"""
Benchmark figure for v3.19 runs (template-native generation).

Runs GenDB from output/tpc-h-sf10-3.19 and output/sec-edgar-sf3-3.19,
uses existing baseline indexed results, and plots a 2-row figure with
per-query time and total time panels only.

TPC-H: Q1, Q3, Q6, Q9, Q18
SEC-EDGAR: all queries

Usage:
    python3 benchmarks/benchmark_v319.py
    python3 benchmarks/benchmark_v319.py --plot-only   # skip GenDB re-run, use saved metrics
"""

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from matplotlib.patches import Patch

# Project root
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.lib.gendb import gendb_benchmark_best, find_best_binaries
from benchmarks.lib.utils import SYSTEM_COLORS

# --- Configuration ---
TPCH_QUERIES = ["Q1", "Q3", "Q6", "Q9", "Q18"]
TPCH_WORKLOAD_DIR = PROJECT_ROOT / "output" / "tpc-h-sf10-3.19"
TPCH_INDEXED_JSON = PROJECT_ROOT / "benchmarks" / "tpc-h" / "results" / "sf10" / "metrics" / "benchmark_indexed_results.json"

EDGAR_WORKLOAD_DIR = PROJECT_ROOT / "output" / "sec-edgar-sf3-3.19"
EDGAR_INDEXED_JSON = PROJECT_ROOT / "benchmarks" / "sec-edgar" / "results" / "sf3" / "metrics" / "benchmark_indexed_results.json"

OUTPUT_DIR = PROJECT_ROOT / "benchmarks" / "figures" / "v319"
METRICS_DIR = OUTPUT_DIR / "metrics"


def _apply_paper_rcparams():
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


def _style_log(ax, axis="y"):
    sci_fmt = ticker.LogFormatterSciNotation(base=10, labelOnlyBase=True)
    if axis == "y":
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(sci_fmt)
        ax.yaxis.set_minor_formatter(ticker.NullFormatter())
        ax.grid(axis="y", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
    else:
        ax.set_xscale("log")
        ax.xaxis.set_major_formatter(sci_fmt)
        ax.xaxis.set_minor_formatter(ticker.NullFormatter())
        ax.grid(axis="x", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
    ax.set_axisbelow(True)


def run_gendb(workload_dir, query_filter=None):
    """Run GenDB benchmarks for best binaries from a workload dir."""
    storage_dir = workload_dir / "storage"
    runs_latest = workload_dir / "runs" / "latest"
    run_dir = runs_latest.resolve() if runs_latest.exists() else None

    results = gendb_benchmark_best(run_dir, storage_dir, "hot", workload_dir=workload_dir)

    if query_filter:
        results = {q: v for q, v in results.items() if q in query_filter}

    return results


def load_indexed_baselines(json_path, query_filter=None):
    """Load baseline indexed results from JSON, filtering to specific queries."""
    with open(json_path) as f:
        data = json.load(f)

    results = {}
    for system, system_data in data.items():
        results[system] = {}
        for qid, qdata in system_data.items():
            if query_filter and qid not in query_filter:
                continue
            results[system][qid] = qdata["all_ms"]

    return results


def plot_v319_figure(workload_data, output_dir):
    """Plot a 2-row, 2-column figure (per-query + total) for both workloads."""
    _apply_paper_rcparams()

    n_rows = len(workload_data)
    fig_width = 8
    row_h = 2.4
    fig_height = row_h * n_rows
    fig = plt.figure(figsize=(fig_width, fig_height))

    norm_row = 1.0 / n_rows
    panel_h_frac = 0.78
    panel_y_frac = 0.10

    # Panel x-positions: per-query (wider) and total
    panel_specs = [
        (0.08, 0.52),   # per-query
        (0.63, 0.28),   # total
    ]

    all_systems_ordered = []
    seen = set()

    for row, wd in enumerate(workload_data):
        display_name = wd["display_name"]
        queries = wd["queries"]
        all_results = wd["all_results"]  # {system: {qid: [times]}}

        systems = list(all_results.keys())
        for s in systems:
            if s not in seen:
                all_systems_ordered.append(s)
                seen.add(s)

        # Compute averages
        data = {}
        for s in systems:
            data[s] = []
            for q in queries:
                times = all_results[s].get(q, [])
                if times:
                    data[s].append(sum(times) / len(times))
                else:
                    data[s].append(0)

        totals = {s: sum(data[s]) for s in systems}

        row_y_base = 1.0 - (row + 1) * norm_row

        def _add_panel(col_idx):
            px, pw = panel_specs[col_idx]
            py = row_y_base + panel_y_frac * norm_row
            ph = panel_h_frac * norm_row
            return fig.add_axes([px, py, pw, ph])

        # --- Per-query panel ---
        ax_pq = _add_panel(0)
        x = np.arange(len(queries))
        width = 0.92 / max(len(systems), 1)
        offsets = [(i - len(systems) / 2 + 0.5) * width for i in range(len(systems))]
        for i, system in enumerate(systems):
            color = SYSTEM_COLORS.get(system, "#999999")
            ax_pq.bar(x + offsets[i], data[system], width,
                       label=system, color=color,
                       edgecolor="white", linewidth=0.2, zorder=3)
        ax_pq.set_ylabel(f"$\\bf{{{display_name}}}$\nTime (ms)", fontsize=13)
        ax_pq.set_xticks(x)
        ax_pq.set_xticklabels(queries, fontsize=12)
        ax_pq.set_xlim(x[0] - 0.5, x[-1] + 0.5)
        ax_pq.tick_params(axis="x", length=0)
        ax_pq.tick_params(axis="y", labelsize=12)
        _style_log(ax_pq, "y")

        if row == 0:
            ax_pq.set_title("Per-Query Time", fontweight="bold", fontsize=13)

        # --- Total panel ---
        ax_tot = _add_panel(1)
        y_pos = np.arange(len(systems))
        bar_vals = [totals[s] for s in systems]
        max_val = max(bar_vals) if bar_vals else 1

        hbars = ax_tot.barh(y_pos, bar_vals, height=0.6,
                            color=[SYSTEM_COLORS.get(s, "#999999") for s in systems],
                            edgecolor="white", linewidth=0.2, zorder=3)
        for bar, system in zip(hbars, systems):
            val = totals[system]
            bw = bar.get_width()
            cy = bar.get_y() + bar.get_height() / 2
            if bw > max_val * 0.15:
                ax_tot.text(bw * 0.85, cy, f"{val:,.0f}",
                            ha="right", va="center", fontsize=12,
                            fontweight="bold", color="white", zorder=4)
            else:
                ax_tot.text(bw * 1.15, cy, f"{val:,.0f}",
                            ha="left", va="center", fontsize=12,
                            fontweight="bold", color="black", zorder=4)

        ax_tot.set_xlabel("(ms)", labelpad=6, fontsize=13)
        ax_tot.xaxis.set_label_coords(1.0, -0.12)
        ax_tot.xaxis.label.set_ha("right")
        ax_tot.tick_params(axis="x", labelsize=12)
        ax_tot.set_yticks([])
        ax_tot.set_ylim(-0.5, len(systems) - 0.5)
        ax_tot.set_xlim(left=100, right=max_val * 1.1)
        ax_tot.invert_yaxis()
        _style_log(ax_tot, "x")

        if row == 0:
            ax_tot.set_title("Total Time", fontweight="bold", fontsize=13)

    # Legend at top
    system_handles = [Patch(facecolor=SYSTEM_COLORS.get(s, "#999999"), label=s)
                      for s in all_systems_ordered]
    fig.legend(system_handles, all_systems_ordered, loc="upper center",
               ncol=len(all_systems_ordered),
               bbox_to_anchor=(0.48, 1.07), frameon=True, fontsize=13,
               columnspacing=1.0, handletextpad=0.4, handlelength=1.0,
               edgecolor="#CCCCCC", fancybox=False, framealpha=1.0)

    # Save
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    png_path = output_dir / "benchmark_v319_combined.png"
    pdf_path = output_dir / "benchmark_v319_combined.pdf"
    fig.savefig(png_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    fig.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
    plt.close()
    print(f"\nFigure saved to: {png_path}")
    print(f"Figure saved to: {pdf_path}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark v3.19 figure")
    parser.add_argument("--plot-only", action="store_true",
                        help="Skip GenDB re-run, use saved metrics")
    args = parser.parse_args()

    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    workload_data = []

    # ===== TPC-H =====
    print("=" * 60)
    print("TPC-H (Q1, Q3, Q6, Q9, Q18)")
    print("=" * 60)

    tpch_metrics_path = METRICS_DIR / "tpch_gendb.json"
    if args.plot_only and tpch_metrics_path.exists():
        with open(tpch_metrics_path) as f:
            tpch_gendb = json.load(f)
        print(f"Loaded GenDB results from {tpch_metrics_path}")
    else:
        print("\nRunning GenDB benchmarks...")
        tpch_gendb = run_gendb(TPCH_WORKLOAD_DIR, TPCH_QUERIES)
        with open(tpch_metrics_path, "w") as f:
            json.dump(tpch_gendb, f, indent=2)
        print(f"GenDB results saved to {tpch_metrics_path}")

    tpch_baselines = load_indexed_baselines(TPCH_INDEXED_JSON, TPCH_QUERIES)

    # Merge: GenDB first, then baselines
    tpch_all = {"GenDB": tpch_gendb}
    for system, results in tpch_baselines.items():
        tpch_all[system] = results

    workload_data.append({
        "display_name": "TPC-H (SF=10)",
        "queries": TPCH_QUERIES,
        "all_results": tpch_all,
    })

    # ===== SEC-EDGAR =====
    print()
    print("=" * 60)
    print("SEC-EDGAR")
    print("=" * 60)

    edgar_metrics_path = METRICS_DIR / "edgar_gendb.json"
    if args.plot_only and edgar_metrics_path.exists():
        with open(edgar_metrics_path) as f:
            edgar_gendb = json.load(f)
        print(f"Loaded GenDB results from {edgar_metrics_path}")
    else:
        print("\nRunning GenDB benchmarks...")
        edgar_gendb = run_gendb(EDGAR_WORKLOAD_DIR)
        with open(edgar_metrics_path, "w") as f:
            json.dump(edgar_gendb, f, indent=2)
        print(f"GenDB results saved to {edgar_metrics_path}")

    edgar_baselines = load_indexed_baselines(EDGAR_INDEXED_JSON)

    # Determine SEC-EDGAR query list from available data
    edgar_queries = sorted(
        set(list(edgar_gendb.keys()) + [q for s in edgar_baselines.values() for q in s]),
        key=lambda q: int(q[1:])
    )

    edgar_all = {"GenDB": edgar_gendb}
    for system, results in edgar_baselines.items():
        edgar_all[system] = results

    workload_data.append({
        "display_name": "SEC-EDGAR",
        "queries": edgar_queries,
        "all_results": edgar_all,
    })

    # ===== Plot =====
    print()
    print("=" * 60)
    print("GENERATING FIGURE")
    print("=" * 60)
    plot_v319_figure(workload_data, OUTPUT_DIR)


if __name__ == "__main__":
    main()
