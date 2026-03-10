#!/usr/bin/env python3
"""
Compare language implementations (Original C++, Optimized C++, Rust) on TPC-H.

Usage:
    python3 benchmarks/compare_languages.py [options]

Options:
    --mode <hot|cold>   Benchmark mode (default: hot)
                        hot:  8 runs per query, discard first, avg last 7
                        cold: clear OS cache before each query, 1 run only
    --plot-only         Skip benchmarks; re-plot from existing metrics
    --output-dir <path> Output directory (default: benchmarks/figures/language_comparison)
"""

import argparse
import json
import math
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ── Configuration ─────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent
LANG_COMPARISON_DIR = ROOT / "output" / "tpc-h" / "language-comparison"
ORIGINAL_RUN_DIR = ROOT / "output" / "tpc-h" / "2026-03-07T17-46-23"
GENDB_DIR = LANG_COMPARISON_DIR / "gendb"

QUERIES = ["Q1", "Q3", "Q6", "Q9", "Q18"]

IMPLEMENTATIONS = {
    "Original\nC++": {
        "type": "original_cpp",
    },
    "Optimized\nC++": {
        "type": "optimized_cpp",
        "binary_dir": LANG_COMPARISON_DIR / "optimized_cpp",
    },
    "Rust": {
        "type": "rust",
        "binary_dir": LANG_COMPARISON_DIR / "rust_impl" / "target" / "release",
    },
}

IMPL_COLORS = {
    "Original\nC++": "#92C5DE",
    "Optimized\nC++": "#2166AC",
    "Rust": "#B2182B",
}

GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
RESET = "\033[0m"

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_timing_output(stdout: str) -> dict:
    """Parse [TIMING] lines from binary output."""
    timings = {}
    for line in stdout.splitlines():
        m = re.match(r"\[TIMING\]\s+(\w+):\s+([\d.]+)\s*ms", line)
        if m:
            timings[m.group(1)] = float(m.group(2))
    if "total" in timings:
        timings["query_ms"] = timings["total"] - timings.get("output", 0)
    return timings


def drop_os_caches():
    try:
        subprocess.run(
            ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            check=True, capture_output=True, timeout=10,
        )
        print("  OS page cache cleared.")
    except Exception:
        print("  Warning: could not clear OS page cache.")


def find_original_binaries() -> dict:
    """Find best per-query binary from the original run."""
    run_json_path = ORIGINAL_RUN_DIR / "run.json"
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


def find_binaries(impl_name: str, impl_cfg: dict) -> dict:
    """Find binaries for an implementation."""
    if impl_cfg["type"] == "original_cpp":
        return find_original_binaries()

    binary_dir = impl_cfg["binary_dir"]
    binaries = {}
    for qid in QUERIES:
        binary = binary_dir / qid.lower()
        if binary.exists() and os.access(str(binary), os.X_OK):
            binaries[qid] = binary
    return binaries


def run_benchmark(impl_name: str, impl_cfg: dict, mode: str,
                  timeout: int = 600) -> dict:
    """Execute binaries and return timing results."""
    binaries = find_binaries(impl_name, impl_cfg)
    if not binaries:
        print(f"  {RED}No binaries found for {impl_name}{RESET}")
        return {}

    total_runs = 8 if mode == "hot" else 1
    results = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        for qid in sorted(binaries, key=lambda q: int(q[1:])):
            binary = binaries[qid]
            print(f"  {qid} ({binary.name})...", end="", flush=True)

            if mode == "cold":
                drop_os_caches()
            times = []
            ok = True
            for _ in range(total_runs):
                try:
                    proc = subprocess.run(
                        [str(binary), str(GENDB_DIR), tmpdir],
                        capture_output=True, text=True, timeout=timeout,
                    )
                except subprocess.TimeoutExpired:
                    print(f" {RED}TIMEOUT{RESET}")
                    ok = False
                    break
                if proc.returncode != 0:
                    # Some binaries use _exit(0) or exit(0) which may not be 0 on all platforms
                    pass
                output = proc.stdout + proc.stderr
                timings = parse_timing_output(output)
                if "query_ms" in timings:
                    times.append(timings["query_ms"])
                else:
                    print(f" {RED}no [TIMING] total in output{RESET}")
                    ok = False
                    break

            if ok and times:
                measured = times[1:] if mode == "hot" and len(times) > 1 else times
                results[qid] = measured
                avg = sum(measured) / len(measured)
                label = f"avg of {len(measured)} hot runs" if mode == "hot" else "cold run"
                print(f" {GREEN}{avg:.2f} ms{RESET} ({label})")
            elif not ok:
                results[qid] = []

    return results


# ── Plotting ──────────────────────────────────────────────────────────────────

def setup_matplotlib():
    matplotlib.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "DejaVu Serif"],
        "font.size": 11,
        "axes.labelsize": 12,
        "axes.titlesize": 12,
        "axes.titlepad": 6,
        "axes.labelpad": 2,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "figure.dpi": 300,
        "axes.linewidth": 0.5,
        "grid.linewidth": 0.3,
    })


def plot_perquery(all_results: dict, output_dir: Path):
    """Per-query execution time bar chart."""
    setup_matplotlib()
    matplotlib.rcParams.update({
        "font.size": 9,
        "axes.labelsize": 9,
        "axes.titlesize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 8,
        "axes.titlepad": 4,
        "axes.labelpad": 2,
    })

    ordered = list(IMPL_COLORS.keys())
    versions = [v for v in ordered if v in all_results]
    all_qs = sorted(
        set(q for res in all_results.values() for q in res if res[q]),
        key=lambda q: int(q[1:])
    )

    data = {}
    for ver in versions:
        data[ver] = []
        for q in all_qs:
            times = all_results[ver].get(q, [])
            data[ver].append(sum(times) / len(times) if times else 0)

    fig, ax = plt.subplots(figsize=(4.5, 2.8))
    x = np.arange(len(all_qs))
    n = len(versions)
    width = 0.85 / max(n, 1)
    offsets = [(i - n / 2 + 0.5) * width for i in range(n)]

    all_bars = []
    for i, ver in enumerate(versions):
        color = IMPL_COLORS.get(ver, "#999999")
        bars = ax.bar(x + offsets[i], data[ver], width,
                      label=ver.replace('\n', ' '), color=color,
                      edgecolor="white", linewidth=0.3, zorder=3)
        all_bars.append((bars, data[ver]))

    ax.set_xticks(x)
    ax.set_xticklabels(all_qs)
    ax.set_xlim(x[0] - 0.5, x[-1] + 0.5)
    ax.tick_params(axis="x", length=0)
    ax.set_ylabel("Time (ms)")
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.grid(axis="y", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
    ax.set_axisbelow(True)

    # Add bar labels
    all_vals = [v for _, vals in all_bars for v in vals if v and v > 0]
    if all_vals:
        for bars, vals in all_bars:
            for bar, val in zip(bars, vals):
                if not val or val <= 0:
                    continue
                bx = bar.get_x() + bar.get_width() / 2
                ax.text(bx, val + max(all_vals) * 0.02, f"{val:.1f}",
                        ha="center", va="bottom", fontsize=5.5,
                        fontweight="bold", rotation=90, zorder=4)

    # Title at very top, then legend below it, then chart
    fig.suptitle("TPC-H (SF=10) — Query Execution Time (ms)",
                 fontweight="bold", fontsize=10, x=0.05, ha="left", y=0.99)
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3,
               fontsize=7, frameon=True,
               bbox_to_anchor=(0.50, 0.93),
               columnspacing=1.0, handlelength=0.8, handletextpad=0.3)

    fig.subplots_adjust(top=0.77)
    output_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        out = output_dir / f"language_comparison_perquery.{ext}"
        fig.savefig(out, bbox_inches="tight", dpi=300, pad_inches=0.02)
        print(f"  Saved: {out}")
    plt.close(fig)


def plot_summary(all_results: dict, output_dir: Path):
    """Summary: total query time across all queries."""
    setup_matplotlib()
    matplotlib.rcParams.update({
        "font.size": 9,
        "axes.labelsize": 9,
        "axes.titlesize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 8,
    })

    ordered = list(IMPL_COLORS.keys())
    versions = [v for v in ordered if v in all_results]
    all_qs = sorted(
        set(q for res in all_results.values() for q in res if res[q]),
        key=lambda q: int(q[1:])
    )

    totals = {}
    for ver in versions:
        total = 0
        for q in all_qs:
            times = all_results[ver].get(q, [])
            if times:
                total += sum(times) / len(times)
        totals[ver] = total

    fig, ax = plt.subplots(figsize=(2.8, 2.0))
    x = np.arange(len(versions))
    colors = [IMPL_COLORS.get(v, "#999999") for v in versions]
    vals = [totals[v] for v in versions]
    bars = ax.bar(x, vals, 0.72, color=colors, edgecolor="white",
                  linewidth=0.3, zorder=3)

    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, val / 2,
                f"{val:.0f}", ha="center", va="center",
                fontsize=8, color="white", fontweight="bold")

    ax.set_title("Total Query Time (ms)", fontweight="bold", fontsize=10)
    ax.set_xticks(x)
    # Use line breaks to avoid overlap
    ax.set_xticklabels(["Original\nC++", "Optimized\nC++", "Rust"], fontsize=7)
    ax.tick_params(axis="x", length=0)
    ax.set_ylabel("Time (ms)")
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.grid(axis="y", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
    ax.set_axisbelow(True)

    fig.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        out = output_dir / f"language_comparison_summary.{ext}"
        fig.savefig(out, bbox_inches="tight", dpi=300, pad_inches=0.02)
        print(f"  Saved: {out}")
    plt.close(fig)


def print_summary_table(all_results: dict):
    """Print a text summary table."""
    ordered = list(IMPL_COLORS.keys())
    versions = [v for v in ordered if v in all_results]
    all_qs = sorted(
        set(q for res in all_results.values() for q in res if res[q]),
        key=lambda q: int(q[1:])
    )

    print(f"\n{'='*70}")
    print(f"  Language Implementation Comparison — TPC-H (SF=10)")
    print(f"{'='*70}")

    header = f"{'Query':<8}" + "".join(
        f"{v.replace(chr(10), ' '):<20}" for v in versions)
    print(f"\n{BOLD}Per-Query Execution Time (ms):{RESET}")
    print(f"  {header}")
    print(f"  {'─'*8}" + "─" * 20 * len(versions))
    for q in all_qs:
        row = f"  {q:<8}"
        for v in versions:
            times = all_results[v].get(q, [])
            if times:
                avg = sum(times) / len(times)
                row += f"{avg:>10.2f} ms" + " " * 8
            else:
                row += f"{'FAILED':>10}" + " " * 10
        print(row)

    print(f"\n{BOLD}Total Query Execution Time:{RESET}")
    for v in versions:
        total = sum(
            sum(all_results[v].get(q, [])) / max(len(all_results[v].get(q, [])), 1)
            for q in all_qs if all_results[v].get(q, []))
        print(f"  {v.replace(chr(10), ' '):<30} {total:>10.2f} ms")

    # Speedup comparison
    if len(versions) >= 2:
        baseline_ver = versions[0]
        baseline_total = sum(
            sum(all_results[baseline_ver].get(q, [])) /
            max(len(all_results[baseline_ver].get(q, [])), 1)
            for q in all_qs if all_results[baseline_ver].get(q, []))
        print(f"\n{BOLD}Speedup vs {baseline_ver.replace(chr(10), ' ')}:{RESET}")
        for v in versions[1:]:
            total = sum(
                sum(all_results[v].get(q, [])) /
                max(len(all_results[v].get(q, [])), 1)
                for q in all_qs if all_results[v].get(q, []))
            if total > 0:
                speedup = baseline_total / total
                print(f"  {v.replace(chr(10), ' '):<30} {speedup:>6.2f}x")

    print(f"{'='*70}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compare language implementations on TPC-H")
    parser.add_argument("--mode", default="hot", choices=["hot", "cold"])
    parser.add_argument("--plot-only", action="store_true")
    parser.add_argument("--output-dir",
                        default="benchmarks/figures/language_comparison")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    metrics_path = output_dir / "metrics.json"

    all_results = {}

    if not args.plot_only:
        for impl_name, impl_cfg in IMPLEMENTATIONS.items():
            clean_name = impl_name.replace('\n', ' ')
            print(f"\n{BOLD}▸ {clean_name}{RESET}")
            results = run_benchmark(impl_name, impl_cfg, args.mode)
            all_results[impl_name] = results

        output_dir.mkdir(parents=True, exist_ok=True)
        save_data = {
            "mode": args.mode,
            "results": {v: {q: times for q, times in res.items()}
                        for v, res in all_results.items()},
        }
        with open(metrics_path, "w") as f:
            json.dump(save_data, f, indent=2)
        print(f"\n  Metrics saved: {metrics_path}")
    else:
        if not metrics_path.exists():
            print(f"{RED}No metrics file: {metrics_path}{RESET}")
            return
        with open(metrics_path) as f:
            save_data = json.load(f)
        all_results = save_data["results"]

    print_summary_table(all_results)

    print(f"\n{BOLD}Generating figures...{RESET}")
    plot_perquery(all_results, output_dir)
    plot_summary(all_results, output_dir)


if __name__ == "__main__":
    main()
