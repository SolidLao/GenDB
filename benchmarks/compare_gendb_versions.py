#!/usr/bin/env python3
"""
Compare different GenDB versions/modes by running their best binaries.

Usage:
    python3 benchmarks/compare_gendb_versions.py --workload tpc-h [options]
    python3 benchmarks/compare_gendb_versions.py --workload sec-edgar [options]
    python3 benchmarks/compare_gendb_versions.py --workload all [options]

Options:
    --mode <hot|cold>   Benchmark mode (default: hot)
                        hot:  4 runs per query, discard first (warmup), avg last 3
                        cold: clear OS cache before each query, 1 run only
    --plot-only         Skip benchmarks; re-plot from existing metrics
    --output-dir <path> Output directory (default: benchmarks/compare_versions/results)
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

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Configuration ─────────────────────────────────────────────────────────────

TPCH_VERSIONS = {
    "Multi-Agent": {
        "run_dir": "output/tpc-h/2026-02-26T06-27-28",
    },
    "Single-Agent\n(High-Level)": {
        "run_dir": "output/tpc-h/2026-02-27T07-32-41",
    },
    "Single-Agent\n(Guided)": {
        "run_dir": "output/tpc-h/2026-02-27T11-32-42",
    },
}

EDGAR_VERSIONS = {
    "Multi-Agent": {
        "run_dir": "output/sec-edgar/2026-02-26T07-58-24",
    },
    "Single-Agent\n(High-Level)": {
        "run_dir": "output/sec-edgar/2026-02-27T09-32-42",
    },
    "Single-Agent\n(Guided)": {
        "run_dir": "output/sec-edgar/2026-02-27T13-32-42",
    },
}

VERSION_COLORS = {
    "Multi-Agent": "#E41A1C",
    "Single-Agent\n(High-Level)": "#377EB8",
    "Single-Agent\n(Guided)": "#4DAF4A",
}

GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def drop_os_caches():
    try:
        subprocess.run(
            ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            check=True, capture_output=True, timeout=10,
        )
        print("  OS page cache cleared.")
    except Exception:
        print("  Warning: could not clear OS page cache.")


def find_best_binaries(run_dir: Path) -> dict:
    """Find best per-query binary from run.json's bestCppPath,
    or by scanning execution_results.json in query dirs as fallback."""
    run_json_path = run_dir / "run.json"
    if not run_json_path.exists():
        return {}
    with open(run_json_path) as f:
        run_data = json.load(f)

    # Try bestCppPath from run.json first
    pipelines = run_data.get("phase2", {}).get("pipelines", {})
    binaries = {}
    for qid, info in pipelines.items():
        cpp_path = info.get("bestCppPath")
        if cpp_path:
            iter_dir = Path(cpp_path).parent
            binary = iter_dir / qid.lower()
            if binary.exists() and os.access(str(binary), os.X_OK):
                binaries[qid] = binary

    if binaries:
        return binaries

    # Fallback: scan query dirs for best passing iteration
    queries_dir = run_dir / "queries"
    if not queries_dir.exists():
        return {}
    for qdir in sorted(queries_dir.iterdir()):
        if not qdir.is_dir():
            continue
        qid = qdir.name  # e.g. "Q1"
        best_time = None
        best_binary = None
        for iter_dir in sorted(qdir.glob("iter_*")):
            exec_path = iter_dir / "execution_results.json"
            if not exec_path.exists():
                continue
            with open(exec_path) as f:
                exec_data = json.load(f)
            val = exec_data.get("validation", {})
            if isinstance(val, dict):
                status = val.get("status", "")
            else:
                status = val
            if status != "pass":
                continue
            t = exec_data.get("timing_ms")
            if t is None:
                continue
            binary = iter_dir / qid.lower()
            if not (binary.exists() and os.access(str(binary), os.X_OK)):
                continue
            if best_time is None or t < best_time:
                best_time = t
                best_binary = binary
        if best_binary:
            binaries[qid] = best_binary

    return binaries


def run_benchmark(run_dir: Path, gendb_dir: Path, mode: str, timeout: int = 600) -> dict:
    """Execute best per-query binaries and return timing results."""
    best_binaries = find_best_binaries(run_dir)
    if not best_binaries:
        print(f"  {RED}No best binaries found{RESET}")
        return {}

    total_runs = 4 if mode == "hot" else 1
    results = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        for qid in sorted(best_binaries, key=lambda q: int(q[1:])):
            binary = best_binaries[qid]
            print(f"  {qid} ({binary.parent.name}/{binary.name})...", end="", flush=True)

            if mode == "cold":
                drop_os_caches()
            times = []
            ok = True
            for _ in range(total_runs):
                try:
                    proc = subprocess.run(
                        [str(binary), str(gendb_dir), tmpdir],
                        capture_output=True, text=True, timeout=timeout,
                    )
                except subprocess.TimeoutExpired:
                    print(f" {RED}TIMEOUT{RESET}")
                    ok = False
                    break
                if proc.returncode != 0:
                    print(f" {RED}ERROR: {proc.stderr[:200]}{RESET}")
                    ok = False
                    break
                timings = parse_timing_output(proc.stdout)
                if "query_ms" in timings:
                    times.append(timings["query_ms"])
                else:
                    print(f" {RED}no [TIMING] total{RESET}")
                    ok = False
                    break

            if ok and times:
                measured = times[1:] if mode == "hot" and len(times) > 1 else times
                results[qid] = measured
                avg = sum(measured) / len(measured)
                label = f"avg of {len(measured)} hot runs" if mode == "hot" else "cold run"
                print(f" {GREEN}{avg:.2f} ms{RESET} ({label})")
            elif not ok:
                results[qid] = []  # mark as failed

    return results


def read_telemetry(run_dir: Path) -> dict:
    """Read telemetry.json for cost/time info."""
    tel_path = run_dir / "telemetry.json"
    if not tel_path.exists():
        return {}
    with open(tel_path) as f:
        return json.load(f)




def read_run_json(run_dir: Path) -> dict:
    """Read run.json for metadata."""
    rj = run_dir / "run.json"
    if not rj.exists():
        return {}
    with open(rj) as f:
        return json.load(f)


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


def _prepare_workload_data(all_results: dict):
    """Compute per-query averages and totals from raw results."""
    versions = list(all_results.keys())
    all_qs = sorted(
        set(q for res in all_results.values() for q in res if res[q]),
        key=lambda q: int(q[1:])
    )
    data = {}
    for ver in versions:
        data[ver] = []
        for q in all_qs:
            times = all_results[ver].get(q, [])
            data[ver].append(sum(times) / len(times) if times else None)

    common_idx = [j for j in range(len(all_qs))
                  if sum(1 for v in versions if data[v][j] is not None) >= 2]
    queries = [all_qs[j] for j in common_idx]
    fdata = {v: [data[v][j] if data[v][j] is not None else 0
                 for j in common_idx] for v in versions}
    totals = {v: sum(fdata[v]) for v in versions}
    return versions, queries, fdata, totals


def plot_perquery(workloads_data: dict, output_dir: Path):
    """Figure 1: Per-query execution time — stacked vertically, shared legend on top.
    Sized for one column of a SIGMOD double-column paper.
    """
    setup_matplotlib()
    matplotlib.rcParams.update({
        "font.size": 8,
        "axes.labelsize": 8,
        "axes.titlesize": 8,
        "xtick.labelsize": 8,
        "ytick.labelsize": 7,
        "axes.titlepad": 3,
        "axes.labelpad": 2,
    })
    sci_fmt = ticker.LogFormatterSciNotation(base=10, labelOnlyBase=True)

    n_wl = len(workloads_data)
    col_w = 3.6
    row_h = 1.35
    fig, axes = plt.subplots(n_wl, 1, figsize=(col_w, row_h * n_wl + 0.3),
                             squeeze=False)
    axes = [a[0] for a in axes]

    panel_labels = iter("abcdefgh")
    legend_handles = None

    for ax, (wl_key, wd) in zip(axes, workloads_data.items()):
        versions, queries, fdata, totals = _prepare_workload_data(wd["results"])
        if not queries:
            continue

        wl_name = wd["label"]
        sf = wd["sf"]
        label = next(panel_labels)

        x = np.arange(len(queries))
        n = len(versions)
        width = 0.88 / max(n, 1)
        offsets = [(i - n / 2 + 0.5) * width for i in range(n)]

        all_bars = []
        for i, ver in enumerate(versions):
            color = VERSION_COLORS.get(ver, "#999999")
            bars = ax.bar(x + offsets[i], fdata[ver], width,
                          label=ver.replace('\n', ' '), color=color,
                          edgecolor="white", linewidth=0.15, zorder=3)
            all_bars.append((bars, fdata[ver]))

        if legend_handles is None:
            legend_handles = ax.get_legend_handles_labels()

        ax.set_title(f"({label}) {wl_name} (SF={sf}) — Query Time (ms)",
                     fontweight="bold", fontsize=8, loc="left")
        ax.set_xticks(x)
        ax.set_xticklabels(queries)
        ax.set_xlim(x[0] - 0.5, x[-1] + 0.5)
        ax.tick_params(axis="x", length=0)
        ax.set_yscale("log")
        ax.yaxis.set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["top"].set_visible(False)
        ax.grid(axis="y", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
        ax.set_axisbelow(True)

        # Add bar labels after log scale is set
        all_vals = [v for _, vals in all_bars for v in vals if v and v > 0]
        max_val = max(all_vals)
        min_val = min(all_vals)
        for bars, vals in all_bars:
            for bar, val in zip(bars, vals):
                if not val or val <= 0:
                    continue
                bx = bar.get_x() + bar.get_width() / 2
                # Short bar threshold: if bar is <15% of max in log space
                import math
                log_range = math.log10(max_val) - math.log10(min_val)
                log_frac = (math.log10(val) - math.log10(min_val)) / log_range if log_range > 0 else 1
                if log_frac < 0.25:
                    # Above bar
                    ax.text(bx, val * 1.15, f"{val:.0f}",
                            ha="center", va="bottom", fontsize=5,
                            fontweight="bold", rotation=90, zorder=4)
                else:
                    # Inside bar
                    ax.text(bx, val * 0.55, f"{val:.0f}",
                            ha="center", va="center", fontsize=5,
                            color="white", fontweight="bold",
                            rotation=90, zorder=4)

    # Shared legend at top
    if legend_handles:
        fig.legend(*legend_handles, loc="upper center", ncol=3,
                   fontsize=7, frameon=True,
                   bbox_to_anchor=(0.52, 1.04),
                   columnspacing=1.2, handlelength=0.8, handletextpad=0.3)

    fig.subplots_adjust(hspace=0.50, left=0.02, right=0.98)
    output_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        out = output_dir / f"gendb_version_perquery.{ext}"
        fig.savefig(out, bbox_inches="tight", dpi=300, pad_inches=0.02)
        print(f"  Saved: {out}")
    plt.close(fig)


def plot_summary(workloads_data: dict, output_dir: Path):
    """Figure 2: compact 1-row layout for SIGMOD single column.
    (a) total query time | (b) generation time | (c) generation cost.
    Shared legend centered between panels.
    """
    setup_matplotlib()
    matplotlib.rcParams.update({
        "font.size": 5,
        "axes.labelsize": 5,
        "axes.titlesize": 6,
        "xtick.labelsize": 4.5,
        "ytick.labelsize": 4.5,
        "axes.titlepad": 2,
        "axes.labelpad": 1,
        "xtick.major.pad": 1,
        "ytick.major.pad": 1,
        "xtick.major.size": 1.5,
        "ytick.major.size": 1.5,
    })

    wl_keys = list(workloads_data.keys())
    versions = list(workloads_data[wl_keys[0]]["results"].keys())
    n_ver = len(versions)
    n_wl = len(wl_keys)

    wl_totals = {}
    wl_gentimes = {}
    wl_gencosts = {}
    wl_labels = {}

    for wl_key, wd in workloads_data.items():
        versions_w, queries, fdata, totals = _prepare_workload_data(wd["results"])
        wl_totals[wl_key] = totals
        wl_labels[wl_key] = wd["label"]
        gt, gc = {}, {}
        for ver in versions_w:
            tel = wd["telemetry"].get(ver, {})
            wc_ms = tel.get("total_wall_clock_ms") or tel.get("agent_duration_ms", 0)
            gt[ver] = wc_ms / 60000
            gc[ver] = tel.get("total_cost_usd") or tel.get("cost_usd", 0)
        wl_gentimes[wl_key] = gt
        wl_gencosts[wl_key] = gc

    col_w = 3.6
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(col_w, 1.4))

    # Bars and groups
    group_gap = 2.2
    group_positions = np.arange(n_wl) * group_gap
    group_width = 2.0
    bar_width = group_width / n_ver

    def _style_ax(ax, title):
        """Common axis styling: hide y-axis, spines, set title and x-ticks."""
        ax.set_title(title, fontweight="bold", fontsize=7, pad=1)
        ax.set_xticks(group_positions)
        ax.set_xticklabels([wl_labels[wk] for wk in wl_keys], fontsize=7)
        ax.tick_params(axis="x", length=0, pad=1)
        margin = group_width / 2 + 0.05
        ax.set_xlim(group_positions[0] - margin, group_positions[-1] + margin)
        ax.yaxis.set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["top"].set_visible(False)

    def _bar_center_y(ax, val):
        """Get the visual vertical center of a bar in data coords."""
        ymin = ax.get_ylim()[0]
        if ax.get_yscale() == "log":
            if ymin <= 0:
                ymin = 1
            return (ymin * val) ** 0.5
        else:
            return val / 2

    def draw_inside(ax, value_fn, title, label_fmt, val_fontsize=6.5):
        """Bars with inside labels (rotated 90). For linear-scale panels."""
        for i, ver in enumerate(versions):
            color = VERSION_COLORS.get(ver, "#999999")
            offset = (i - n_ver / 2 + 0.5) * bar_width
            vals = [value_fn(wl_key, ver) for wl_key in wl_keys]
            bars = ax.bar(group_positions + offset, vals, bar_width,
                          label=ver.replace('\n', ' '), color=color,
                          edgecolor="white", linewidth=0.2, zorder=3)
            for bar, val in zip(bars, vals):
                y_pos = _bar_center_y(ax, val)
                ax.text(bar.get_x() + bar.get_width() / 2, y_pos,
                        label_fmt(val), ha="center", va="center",
                        fontsize=val_fontsize, color="white",
                        fontweight="bold", rotation=90)
        _style_ax(ax, title)

    def draw_hybrid(ax, value_fn, title, label_fmt, val_fontsize=6.5):
        """Bars with hybrid labels: inside (rotated 90) for tall bars,
        above (rotated 90, centered) for short bars. Log scale."""
        max_val = max(max(value_fn(wk, v) for v in versions) for wk in wl_keys)
        ax.set_yscale("log")
        all_vals = []
        for i, ver in enumerate(versions):
            offset = (i - n_ver / 2 + 0.5) * bar_width
            vals = [value_fn(wl_key, ver) for wl_key in wl_keys]
            all_vals.extend(vals)
        ax.set_ylim(min(all_vals) * 0.8, max(all_vals) * 1.15)

        for i, ver in enumerate(versions):
            color = VERSION_COLORS.get(ver, "#999999")
            offset = (i - n_ver / 2 + 0.5) * bar_width
            vals = [value_fn(wl_key, ver) for wl_key in wl_keys]
            bars = ax.bar(group_positions + offset, vals, bar_width,
                          label=ver.replace('\n', ' '), color=color,
                          edgecolor="white", linewidth=0.2, zorder=3)
            for bar, val in zip(bars, vals):
                bar_frac = val / max_val if max_val > 0 else 0
                if bar_frac > 0.35:
                    y_pos = _bar_center_y(ax, val)
                    ax.text(bar.get_x() + bar.get_width() / 2, y_pos,
                            label_fmt(val), ha="center", va="center",
                            fontsize=val_fontsize, color="white",
                            fontweight="bold", rotation=90)
                else:
                    y_pos = val * 1.12
                    ax.text(bar.get_x() + bar.get_width() / 2, y_pos,
                            label_fmt(val), ha="center", va="bottom",
                            fontsize=val_fontsize, rotation=90)
        _style_ax(ax, title)

    # (a) Query time — all labels inside bars
    draw_inside(ax1,
                lambda wk, v: wl_totals[wk].get(v, 0),
                "(a) Query Time (ms)",
                lambda v: f"{v:.0f}")

    # (b) Generation time — inside labels
    draw_inside(ax2,
                lambda wk, v: wl_gentimes[wk].get(v, 0),
                "(b) Gen. Time (min)",
                lambda v: f"{v:.0f}")

    # (c) Generation cost — inside labels
    draw_inside(ax3,
                lambda wk, v: wl_gencosts[wk].get(v, 0),
                "(c) Gen. Cost ($)",
                lambda v: f"{v:.0f}")

    fig.subplots_adjust(wspace=0.08, left=0.02, right=0.98)

    # Place legend centered — nudge right to compensate for tight bbox crop
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3,
               fontsize=6.5, frameon=True,
               bbox_to_anchor=(0.52, 1.12),
               columnspacing=1.2, handlelength=0.8, handletextpad=0.3)

    output_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        out = output_dir / f"gendb_version_summary.{ext}"
        fig.savefig(out, bbox_inches="tight", dpi=300, pad_inches=0.02)
        print(f"  Saved: {out}")
    plt.close(fig)


def print_summary_table(all_results: dict, telemetry: dict, workload: str):
    """Print a text summary table."""
    versions = list(all_results.keys())
    # Common queries
    all_qs = sorted(
        set(q for res in all_results.values() for q in res if res[q]),
        key=lambda q: int(q[1:])
    )
    common_qs = [q for q in all_qs
                 if sum(1 for v in versions if all_results[v].get(q, [])) >= 2]

    print(f"\n{'='*80}")
    print(f"  GenDB Version Comparison — {workload.upper()}")
    print(f"{'='*80}")

    # Per-query times
    header = f"{'Query':<8}" + "".join(f"{v.replace(chr(10), ' '):<25}" for v in versions)
    print(f"\n{BOLD}Per-Query Execution Time (ms):{RESET}")
    print(f"  {header}")
    print(f"  {'─'*8}" + "─" * 25 * len(versions))
    for q in common_qs:
        row = f"  {q:<8}"
        for v in versions:
            times = all_results[v].get(q, [])
            if times:
                avg = sum(times) / len(times)
                row += f"{avg:>10.2f} ms" + " " * 13
            else:
                row += f"{'FAILED':>10}" + " " * 15
        print(row)

    # Totals
    print(f"\n{BOLD}Total Query Execution Time:{RESET}")
    for v in versions:
        total = sum(sum(all_results[v].get(q, [])) / max(len(all_results[v].get(q, [])), 1)
                    for q in common_qs if all_results[v].get(q, []))
        print(f"  {v.replace(chr(10), ' '):<30} {total:>10.2f} ms")

    # Generation metrics
    print(f"\n{BOLD}Generation Metrics:{RESET}")
    print(f"  {'Version':<30} {'Wall Clock':>12} {'Cost (USD)':>12} {'Output Tokens':>15}")
    print(f"  {'─'*30} {'─'*12} {'─'*12} {'─'*15}")
    for v in versions:
        tel = telemetry.get(v, {})
        wc_ms = tel.get("total_wall_clock_ms") or tel.get("agent_duration_ms", 0)
        cost = tel.get("total_cost_usd") or tel.get("cost_usd", 0)
        tokens = tel.get("total_tokens", tel.get("tokens", {}))
        out_tok = tokens.get("output", 0)
        wc_min = wc_ms / 60000
        print(f"  {v.replace(chr(10), ' '):<30} {wc_min:>9.1f} min ${cost:>10.2f} {out_tok:>15,}")

    print(f"{'='*80}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_workload(workload: str, versions_config: dict, root: Path,
                 mode: str, output_dir: Path, plot_only: bool) -> dict:
    """Run benchmarks for one workload. Returns collected data dict."""
    metrics_path = output_dir / f"metrics_{workload}.json"

    all_results = {}
    telemetry_data = {}

    if not plot_only:
        for ver_name, ver_cfg in versions_config.items():
            run_dir = root / ver_cfg["run_dir"]
            if not run_dir.exists():
                print(f"{RED}Run dir not found: {run_dir}{RESET}")
                continue

            run_data = read_run_json(run_dir)
            gendb_dir = Path(run_data.get("gendbDir", str(run_dir / "gendb")))
            if not gendb_dir.exists():
                print(f"{RED}GenDB dir not found: {gendb_dir}{RESET}")
                continue

            print(f"\n{BOLD}▸ {ver_name.replace(chr(10), ' ')} — {workload}{RESET}")
            print(f"  Run dir: {run_dir}")
            print(f"  GenDB dir: {gendb_dir}")

            results = run_benchmark(run_dir, gendb_dir, mode)
            all_results[ver_name] = results

            tel = read_telemetry(run_dir)
            telemetry_data[ver_name] = tel

        # Save metrics
        output_dir.mkdir(parents=True, exist_ok=True)
        save_data = {
            "workload": workload,
            "mode": mode,
            "results": {v: {q: times for q, times in res.items()} for v, res in all_results.items()},
            "telemetry": telemetry_data,
        }
        with open(metrics_path, "w") as f:
            json.dump(save_data, f, indent=2)
        print(f"\n  Metrics saved: {metrics_path}")
    else:
        if not metrics_path.exists():
            print(f"{RED}No metrics file found: {metrics_path}{RESET}")
            return None
        with open(metrics_path) as f:
            save_data = json.load(f)
        all_results = save_data["results"]
        telemetry_data = save_data["telemetry"]
        mode = save_data.get("mode", mode)

    # Determine scale factor from first run
    first_ver = list(versions_config.values())[0]
    first_run = read_run_json(root / first_ver["run_dir"])
    sf = first_run.get("scaleFactor", "?")

    # Print summary
    print_summary_table(all_results, telemetry_data, workload)

    # Display label
    wl_label = "TPC-H" if "tpc" in workload.lower() else "SEC-EDGAR"

    return {
        "results": all_results,
        "telemetry": telemetry_data,
        "sf": sf,
        "label": wl_label,
    }


WORKLOAD_DISPLAY = {
    "tpc-h": "TPC-H",
    "sec-edgar": "SEC-EDGAR",
}


def main():
    parser = argparse.ArgumentParser(description="Compare GenDB versions")
    parser.add_argument("--workload", required=True,
                        choices=["tpc-h", "sec-edgar", "all"],
                        help="Workload to compare")
    parser.add_argument("--mode", default="hot", choices=["hot", "cold"])
    parser.add_argument("--plot-only", action="store_true")
    parser.add_argument("--output-dir", default="benchmarks/compare_versions/results")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    output_dir = Path(args.output_dir)

    workloads = []
    if args.workload in ("tpc-h", "all"):
        workloads.append(("tpc-h", TPCH_VERSIONS))
    if args.workload in ("sec-edgar", "all"):
        workloads.append(("sec-edgar", EDGAR_VERSIONS))

    # Collect data from all workloads first
    all_wl_data = {}  # wl_name -> data dict
    for wl_name, wl_versions in workloads:
        data = run_workload(wl_name, wl_versions, root, args.mode, output_dir, args.plot_only)
        if data:
            all_wl_data[wl_name] = data

    if not all_wl_data:
        print(f"{RED}No workload data collected.{RESET}")
        return

    # Plot combined figures
    fig_dir = output_dir / "figures"
    print(f"\n{BOLD}Generating combined figures...{RESET}")
    plot_perquery(all_wl_data, fig_dir)
    plot_summary(all_wl_data, fig_dir)


if __name__ == "__main__":
    main()
