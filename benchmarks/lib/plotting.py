"""Plotting: 4-panel combined figure + GenDB iteration plots."""

import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from matplotlib.patches import Patch

from .utils import SYSTEM_COLORS


# Display name mapping for workloads
WORKLOAD_DISPLAY_NAMES = {
    "tpc-h": "TPC-H",
    "sec-edgar": "SEC-EDGAR",
}


def _apply_paper_rcparams():
    """Apply consistent matplotlib rcParams for paper-quality figures."""
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


def _style_log(a, axis="y"):
    """Apply log scale styling to an axis."""
    sci_fmt = ticker.LogFormatterSciNotation(base=10, labelOnlyBase=True)
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


def _prepare_data(all_results, all_query_ids, timeout_ms):
    """Prepare per-query and total data from raw results.

    Returns: (queries, systems, data, totals, data_all, is_timeout_all)
    """
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

    return queries, systems, data, totals, data_all, is_timeout_all, all_qs, common_idx


def _prepare_indexed_data(indexed_results, systems, data, totals, all_qs, common_idx):
    """Prepare indexed benchmark data.

    Returns: (idx_data, idx_totals, idx_benefit, has_indexed)
    """
    idx_data = {}
    idx_totals = {}
    has_indexed = indexed_results is not None and len(indexed_results) > 0
    if has_indexed:
        for system in systems:
            if system in indexed_results:
                idx_data_all = []
                for q in all_qs:
                    times = indexed_results[system].get(q, [])
                    if times:
                        idx_data_all.append(sum(times) / len(times))
                    else:
                        idx_data_all.append(None)
                idx_data[system] = [idx_data_all[j] if idx_data_all[j] is not None
                                    else data[system][ci]
                                    for ci, j in enumerate(common_idx)]
                idx_totals[system] = sum(idx_data[system])
    idx_benefit = set()
    if has_indexed:
        for system in idx_totals:
            if system != "GenDB" and idx_totals[system] < totals[system] * 0.97:
                idx_benefit.add(system)

    return idx_data, idx_totals, idx_benefit, has_indexed


def _plot_perquery_panel(ax, queries, systems, data, idx_data, idx_benefit, title):
    """Draw grouped bar chart for per-query times on the given axes."""
    x = np.arange(len(queries))
    width = 0.92 / max(len(systems), 1)
    offsets = [(i - len(systems) / 2 + 0.5) * width for i in range(len(systems))]
    for i, system in enumerate(systems):
        color = SYSTEM_COLORS.get(system, "#999999")
        if system in idx_benefit:
            idx_vals = idx_data[system]
            no_idx_vals = data[system]
            ax.bar(x + offsets[i], idx_vals, width,
                   label=system, color=color,
                   edgecolor="white", linewidth=0.2, zorder=3)
            overhead = [max(0, n - w) for n, w in zip(no_idx_vals, idx_vals)]
            ax.bar(x + offsets[i], overhead, width, bottom=idx_vals,
                   label="_nolegend_", color=color, alpha=0.3, hatch="//",
                   edgecolor="white", linewidth=0.2, zorder=3)
        else:
            ax.bar(x + offsets[i], data[system], width,
                   label=system, color=color,
                   edgecolor="white", linewidth=0.2, zorder=3)
    ax.set_ylabel("Time (ms)", fontsize=13)
    ax.set_title(title, fontweight="bold", fontsize=13)
    ax.set_xticks(x)
    ax.set_xticklabels(queries, fontsize=12)
    ax.set_xlim(x[0] - 0.5, x[-1] + 0.5)
    ax.tick_params(axis="x", length=0)
    ax.tick_params(axis="y", labelsize=12)
    _style_log(ax, "y")


def _plot_total_panel(ax, systems, totals, idx_totals, idx_benefit, has_indexed, title):
    """Draw horizontal bar chart for total times on the given axes."""
    y_pos = np.arange(len(systems))
    bar_vals = [totals[s] for s in systems]
    max_val = max(bar_vals)
    if has_indexed and idx_benefit:
        idx_bar_vals = []
        overhead_vals = []
        for s in systems:
            if s in idx_benefit:
                idx_bar_vals.append(idx_totals[s])
                overhead_vals.append(max(0, totals[s] - idx_totals[s]))
            else:
                idx_bar_vals.append(totals[s])
                overhead_vals.append(0)
        hbars = ax.barh(y_pos, idx_bar_vals, height=0.6,
                        color=[SYSTEM_COLORS.get(s, "#999999") for s in systems],
                        edgecolor="white", linewidth=0.2, zorder=3)
        ax.barh(y_pos, overhead_vals, height=0.6, left=idx_bar_vals,
                color=[SYSTEM_COLORS.get(s, "#999999") for s in systems],
                alpha=0.3, hatch="//",
                edgecolor="white", linewidth=0.2, zorder=3)
        for bar, system in zip(hbars, systems):
            total_val = totals[system]
            cy = bar.get_y() + bar.get_height() / 2
            if system in idx_benefit:
                idx_val = idx_totals[system]
                label = f"{idx_val:,.0f}/{total_val:,.0f}"
                if idx_val > max_val * 0.15:
                    ax.text(idx_val * 0.85, cy, label,
                            ha="right", va="center", fontsize=11,
                            fontweight="bold", color="white", zorder=4)
                else:
                    ax.text(idx_val * 1.15, cy, label,
                            ha="left", va="center", fontsize=11,
                            fontweight="bold", color="black", zorder=4)
            else:
                label = f"{total_val:,.0f}"
                bw = total_val
                if bw > max_val * 0.15:
                    ax.text(bw * 0.85, cy, label,
                            ha="right", va="center", fontsize=11,
                            fontweight="bold", color="white", zorder=4)
                else:
                    ax.text(bw * 1.15, cy, label,
                            ha="left", va="center", fontsize=11,
                            fontweight="bold", color="black", zorder=4)
    else:
        hbars = ax.barh(y_pos, bar_vals, height=0.6,
                        color=[SYSTEM_COLORS.get(s, "#999999") for s in systems],
                        edgecolor="white", linewidth=0.2, zorder=3)
        for bar, system in zip(hbars, systems):
            val = totals[system]
            bw = bar.get_width()
            cy = bar.get_y() + bar.get_height() / 2
            if bw > max_val * 0.15:
                ax.text(bw * 0.85, cy, f"{val:,.0f}",
                        ha="right", va="center", fontsize=12,
                        fontweight="bold", color="white", zorder=4)
            else:
                ax.text(bw * 1.15, cy, f"{val:,.0f}",
                        ha="left", va="center", fontsize=12,
                        fontweight="bold", color="black", zorder=4)
    ax.set_xlabel("(ms)", labelpad=6, fontsize=13)
    ax.xaxis.set_label_coords(1.0, -0.12)
    ax.xaxis.label.set_ha("right")
    ax.set_title(title, fontweight="bold", fontsize=13)
    ax.tick_params(axis="x", labelsize=12)
    ax.set_yticks([])
    ax.set_ylim(-0.5, len(systems) - 0.5)
    ax.set_xlim(left=100, right=max_val * 1.1)
    ax.invert_yaxis()
    _style_log(ax, "x")


def _plot_iter_panels(ax_total, ax_perq, gendb_history, systems, totals,
                      title_total, title_perq):
    """Draw GenDB iteration total and per-query panels."""
    iter_query_ids = gendb_history["query_ids"]
    iter_data = gendb_history["data"]
    num_iters = gendb_history["num_iters"]
    best_so_far = _compute_best_so_far(iter_data, iter_query_ids, num_iters)
    cmap = matplotlib.colormaps.get_cmap("tab10")
    q_colors = {q: cmap(i) for i, q in enumerate(iter_query_ids)}
    x_iters = list(range(num_iters))

    # --- Total execution time across iterations ---
    valid_qids = [qid for qid in iter_query_ids
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
        ax_total.plot(total_x, total_y, marker="o", linewidth=2.5, markersize=6,
                      color="#9C27B0")

    hline_markers = ["+", "x", "*", "s", "D"]
    non_gendb = [s for s in systems if s != "GenDB"]
    n_pts = 8
    for idx_i, sys_name in enumerate(non_gendb):
        sys_total = totals.get(sys_name, 0)
        if sys_total > 0:
            offset = (idx_i - len(non_gendb) / 2) * 0.15
            hline_x = np.linspace(-0.3 + offset, num_iters - 0.7 + offset, n_pts)
            ax_total.plot(hline_x, [sys_total] * n_pts, linestyle=":",
                          linewidth=2.0, marker=hline_markers[idx_i % len(hline_markers)],
                          markersize=7,
                          color=SYSTEM_COLORS.get(sys_name, "#999999"), alpha=0.7, zorder=2)

    ax_total.set_xlabel("Iteration", fontsize=13)
    ax_total.set_title(title_total, fontweight="bold", fontsize=13)
    ax_total.set_xticks(x_iters)
    ax_total.set_xticklabels([str(i) for i in x_iters], fontsize=12)
    ax_total.set_xlim(-0.3, num_iters - 0.7)
    ax_total.set_yscale("log")
    ax_total.yaxis.set_major_formatter(ticker.NullFormatter())
    ax_total.yaxis.set_minor_formatter(ticker.NullFormatter())
    ax_total.grid(axis="y", alpha=0.25, linestyle="-", linewidth=0.3, zorder=0)
    ax_total.set_axisbelow(True)
    ax_total.text(0.04, 0.04, "(ms)", transform=ax_total.transAxes, fontsize=12,
                  va="bottom", ha="left")

    # --- Per-query execution time across iterations ---
    sorted_qids = sorted(iter_query_ids, key=lambda q: int(q[1:]))
    for qid in sorted_qids:
        series = best_so_far.get(qid, [])
        if not series:
            continue
        color = q_colors[qid]
        attempted = [(xi, yi) for xi, yi, s in series if s in ("valid", "fail")]
        if attempted:
            ax_perq.plot([p[0] for p in attempted], [p[1] for p in attempted],
                         marker="o", linewidth=2.5, markersize=6, label=qid, color=color)
        skipped = [(xi, yi) for xi, yi, s in series if s == "skipped"]
        if skipped and attempted:
            dash_x = [attempted[-1][0]] + [p[0] for p in skipped]
            dash_y = [attempted[-1][1]] + [p[1] for p in skipped]
            ax_perq.plot(dash_x, dash_y, linestyle="--", linewidth=1.5, markersize=0,
                         color=color, alpha=0.4)
        fail_x = [xi for xi, yi, s in series if s == "fail"]
        fail_y = [yi for xi, yi, s in series if s == "fail"]
        if fail_x:
            ax_perq.scatter(fail_x, fail_y, marker="x", s=50, linewidths=1.5,
                            color="red", zorder=5)

    ax_perq.set_xlabel("Iteration", fontsize=13)
    ax_perq.set_title(title_perq, fontweight="bold", fontsize=13)
    ax_perq.set_xticks(x_iters)
    ax_perq.set_xticklabels([str(i) for i in x_iters], fontsize=12)
    ax_perq.set_xlim(-0.3, num_iters - 0.7)
    ax_perq.tick_params(axis="y", direction="out", pad=2, labelsize=12)
    ax_perq.text(-0.09, 0.96, "(ms)", transform=ax_perq.transAxes, fontsize=11,
                 va="top", ha="center")
    all_vals = []
    for qid in sorted_qids:
        series = best_so_far.get(qid, [])
        all_vals.extend([yi for _, yi, _ in series if yi > 0])
    _style_log(ax_perq, "y")
    if all_vals:
        y_bottom = min(all_vals) * 0.5
        y_top = max(all_vals) * 5
        ax_perq.set_ylim(bottom=y_bottom, top=y_top)
        ax_perq.set_autoscale_on(False)
        low_pow = math.ceil(math.log10(min(all_vals)))
        high_pow = math.floor(math.log10(max(all_vals)))
        ax_perq.set_yticks([10**p for p in range(low_pow, high_pow + 1)])
    ax_perq.legend(fontsize=10, loc="upper center", framealpha=0.9,
                   handlelength=1.2, handletextpad=0.3, borderpad=0.15,
                   labelspacing=0.15, ncol=3)


def _compute_best_so_far(data, query_ids, num_iters):
    """For each query, compute the running best (minimum) valid timing up to each iteration.

    Returns:
        {qid: [(iter, timing_ms, status), ...]}
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
                series.append((i, best_t, "fail"))
            elif entry is None and best_t is not None:
                series.append((i, best_t, "skipped"))
        result[qid] = series
    return result


def plot_gendb_iterations(history, output_path, scale_label="?"):
    """Plot GenDB performance evolution across iterations."""
    query_ids = history["query_ids"]
    data = history["data"]
    num_iters = history["num_iters"]
    if not query_ids or num_iters == 0:
        return

    cmap = plt.cm.get_cmap("tab20", max(len(query_ids), 20))
    q_colors = {q: cmap(i) for i, q in enumerate(query_ids)}
    best_so_far = _compute_best_so_far(data, query_ids, num_iters)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 7))

    iter_labels = [f"Iter {i}" for i in range(num_iters)]
    x_pos = list(range(num_iters))

    # Left: Total execution time
    valid_qids = [qid for qid in query_ids if any(s == "valid" for _, _, s in best_so_far.get(qid, []))]
    total_x, total_y, total_n = [], [], []
    for i in range(num_iters):
        total = 0
        n_queries = 0
        for qid in valid_qids:
            for (si, st, _) in best_so_far.get(qid, []):
                if si == i:
                    total += st
                    n_queries += 1
                    break
        if n_queries > 0:
            total_x.append(i)
            total_y.append(total)
            total_n.append(n_queries)

    if total_y:
        ax1.plot(total_x, total_y, marker="o", linewidth=2.5, markersize=10,
                 color="#9C27B0", label=f"Total ({len(valid_qids)} queries)")
        for x, y, n in zip(total_x, total_y, total_n):
            label = f"{y:.0f}"
            if n < len(valid_qids):
                label += f"\n({n}/{len(valid_qids)})"
            ax1.text(x, y * 1.02, label, ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax1.set_xlabel("Iteration", fontsize=12)
    ax1.set_ylabel("Total Execution Time (ms, log scale)", fontsize=12)
    ax1.set_title(f"Total Execution Time ({scale_label})", fontsize=13, fontweight="bold")
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(iter_labels, fontsize=9, rotation=45 if num_iters > 6 else 0)
    if total_y:
        ax1.set_yscale("log")
    ax1.grid(axis="y", alpha=0.3)

    # Right: Per-query execution time
    for qid in query_ids:
        series = best_so_far.get(qid, [])
        if not series:
            continue
        color = q_colors[qid]

        attempted = [(x, y) for x, y, s in series if s in ("valid", "fail")]
        if attempted:
            ax2.plot([p[0] for p in attempted], [p[1] for p in attempted],
                     marker="o", linewidth=2, markersize=8, label=qid, color=color)

        skipped = [(x, y) for x, y, s in series if s == "skipped"]
        if skipped and attempted:
            dash_x = [attempted[-1][0]] + [p[0] for p in skipped]
            dash_y = [attempted[-1][1]] + [p[1] for p in skipped]
            ax2.plot(dash_x, dash_y, linestyle="--", linewidth=1.5, markersize=0,
                     color=color, alpha=0.4)

        fail_x = [x for x, y, s in series if s == "fail"]
        fail_y = [y for x, y, s in series if s == "fail"]
        if fail_x:
            ax2.scatter(fail_x, fail_y, marker="x", s=120, linewidths=2.5,
                        color="red", zorder=5)

    ax2.scatter([], [], marker="x", s=120, linewidths=2.5, color="red", label="Failed iteration")
    ax2.plot([], [], linestyle="--", linewidth=1.5, color="gray", alpha=0.4, label="Not optimized")

    ax2.set_xlabel("Iteration", fontsize=12)
    ax2.set_ylabel("Execution Time (ms, log scale)", fontsize=12)
    ax2.set_title(f"Per-Query Execution Time ({scale_label})", fontsize=13, fontweight="bold")
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(iter_labels, fontsize=9, rotation=45 if num_iters > 6 else 0)
    ax2.set_yscale("log")
    ax2.legend(fontsize=8, bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0)
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout(rect=[0, 0, 0.88, 1])
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  GenDB iteration plot saved to: {output_path}")


def plot_results(all_results, output_path, scale_label="?",
                 timeout_ms=300000, all_query_ids=None,
                 gendb_history=None, indexed_results=None):
    """Create a single 4-panel figure (figure* for SIGMOD two-column layout).

    Panels: (a) per-query time, (b) total time, (c) GenDB iteration total,
            (d) GenDB iteration per-query.

    If indexed_results is provided, baseline systems show stacked bars.
    """
    _apply_paper_rcparams()

    queries, systems, data, totals, data_all, is_timeout_all, all_qs, common_idx = \
        _prepare_data(all_results, all_query_ids, timeout_ms)
    idx_data, idx_totals, idx_benefit, has_indexed = \
        _prepare_indexed_data(indexed_results, systems, data, totals, all_qs, common_idx)

    has_iter = gendb_history and gendb_history["num_iters"] > 0

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

    # ===== (a) Per-query grouped bar chart =====
    _plot_perquery_panel(ax, queries, systems, data, idx_data, idx_benefit,
                         f"(a) Per-Query Time ({scale_label})")

    # ===== (b) Total time horizontal bar chart =====
    _plot_total_panel(ax2, systems, totals, idx_totals, idx_benefit, has_indexed,
                      f"(b) Total Time ({scale_label})")

    # ===== (c) & (d) GenDB iteration panels =====
    if has_iter:
        _plot_iter_panels(ax3, ax4, gendb_history, systems, totals,
                          "(c) GenDB Total / Iter.", "(d) GenDB Per-Query / Iter.")

    # Legend
    system_handles = [Patch(facecolor=SYSTEM_COLORS.get(s, "#999999"), label=s)
                      for s in systems]
    all_handles = list(system_handles)
    all_labels = list(systems)
    if idx_benefit:
        all_handles.append(Patch(facecolor="#888888", label="w/ index"))
        all_labels.append("default + GenDB index")
        all_handles.append(Patch(facecolor="#888888", alpha=0.3, hatch="//",
                                 label="w/o index"))
        all_labels.append("default index")
    fig.legend(all_handles, all_labels, loc="upper center",
               ncol=len(all_labels),
               bbox_to_anchor=(0.46, 1.08), frameon=True, fontsize=13,
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


def plot_combined_results(workload_results, output_dir):
    """Create a multi-row combined figure with all workloads.

    Uses the same manual add_axes layout as the single-benchmark plot_results,
    stacking one row per workload with shared column titles at the top.

    Args:
        workload_results: list of dicts, each with keys:
            - name: workload name (e.g. "tpc-h")
            - scale_label: e.g. "SF=10"
            - all_results: {system: {qid: [times]}}
            - gendb_history: iteration history dict or None
            - indexed_results: indexed results dict or None
            - all_query_ids: list of query ids
            - timeout_ms: timeout in ms
        output_dir: Path to output directory
    """
    _apply_paper_rcparams()

    n_workloads = len(workload_results)
    any_has_iter = any(
        w.get("gendb_history") and w["gendb_history"]["num_iters"] > 0
        for w in workload_results
    )

    # Use same dimensions as single-benchmark: 13 x 3.0 per row
    fig_width = 13
    row_h = 3.0
    gap = -0.15  # vertical gap between rows (in figure fraction)
    fig_height = row_h * n_workloads + row_h * gap * (n_workloads - 1)
    fig = plt.figure(figsize=(fig_width, fig_height))

    # Normalized row height in figure coordinates
    norm_row = row_h / fig_height
    norm_gap = (row_h * gap) / fig_height

    # Panel x-positions and widths (same as single-benchmark plot_results)
    # With iteration panels: 4 panels
    sq = 0.17
    if any_has_iter:
        panel_specs = [
            (0.08, 0.27),   # per-query
            (0.37, sq),      # total
            (0.56, sq),      # iter total
            (0.76, sq),      # iter per-query
        ]
    else:
        panel_specs = [
            (0.05, 0.55),   # per-query
            (0.68, 0.28),   # total
        ]

    # Inner panel height as fraction of row
    panel_h_frac = 0.68
    panel_y_frac = 0.20  # bottom padding within row

    # Collect all systems across workloads for shared legend
    all_systems_ordered = []
    all_idx_benefit = set()
    seen_systems = set()

    for row, w in enumerate(workload_results):
        display_name = WORKLOAD_DISPLAY_NAMES.get(w["name"], w["name"].upper())
        scale_label = w["scale_label"]
        timeout_ms = w.get("timeout_ms", 300000)

        queries, systems, data, totals, data_all, is_timeout_all, all_qs, common_idx = \
            _prepare_data(w["all_results"], w.get("all_query_ids"), timeout_ms)
        idx_data, idx_totals, idx_benefit, has_indexed = \
            _prepare_indexed_data(w.get("indexed_results"), systems, data, totals, all_qs, common_idx)
        has_iter = w.get("gendb_history") and w["gendb_history"]["num_iters"] > 0

        for s in systems:
            if s not in seen_systems:
                all_systems_ordered.append(s)
                seen_systems.add(s)
        all_idx_benefit.update(idx_benefit)

        # Row base y (top row first)
        row_y_base = 1.0 - (row + 1) * norm_row - row * norm_gap

        def _add_panel(col_idx):
            px, pw = panel_specs[col_idx]
            py = row_y_base + panel_y_frac * norm_row
            ph = panel_h_frac * norm_row
            return fig.add_axes([px, py, pw, ph])

        # Build row label: "TPC-H (SF = 10)" or just "SEC-EDGAR"
        if w["name"] == "tpc-h":
            row_label = f"{display_name} ({scale_label})"
        else:
            row_label = display_name

        # Per-query panel
        ax_pq = _add_panel(0)
        _plot_perquery_panel(
            ax_pq, queries, systems, data, idx_data, idx_benefit, "")
        # Set ylabel to bold workload name + Time (ms)
        ax_pq.set_ylabel(f"$\\bf{{{row_label}}}$\nTime (ms)", fontsize=13)

        # Total panel
        ax_tot = _add_panel(1)
        _plot_total_panel(
            ax_tot, systems, totals, idx_totals, idx_benefit, has_indexed, "")

        # Iteration panels
        if any_has_iter:
            if has_iter:
                ax_it = _add_panel(2)
                ax_iq = _add_panel(3)
                _plot_iter_panels(
                    ax_it, ax_iq, w["gendb_history"], systems, totals, "", "")

        # Column titles — only on the first row
        if row == 0:
            ax_pq.set_title("Per-Query Time", fontweight="bold", fontsize=13)
            ax_tot.set_title("Total Time", fontweight="bold", fontsize=13)
            if any_has_iter and has_iter:
                ax_it.set_title("GenDB Total / Iter.", fontweight="bold", fontsize=13)
                ax_iq.set_title("GenDB Per-Query / Iter.", fontweight="bold", fontsize=13)

    # Shared legend at top
    system_handles = [Patch(facecolor=SYSTEM_COLORS.get(s, "#999999"), label=s)
                      for s in all_systems_ordered]
    all_handles = list(system_handles)
    all_labels = list(all_systems_ordered)
    if all_idx_benefit:
        all_handles.append(Patch(facecolor="#888888", label="w/ index"))
        all_labels.append("default + GenDB index")
        all_handles.append(Patch(facecolor="#888888", alpha=0.3, hatch="//",
                                 label="w/o index"))
        all_labels.append("default index")
    fig.legend(all_handles, all_labels, loc="upper center",
               ncol=len(all_labels),
               bbox_to_anchor=(0.48, 1.06), frameon=True, fontsize=13,
               columnspacing=1.0, handletextpad=0.4, handlelength=1.0,
               edgecolor="#CCCCCC", fancybox=False, framealpha=1.0)

    # Save
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    png_path = output_dir / "benchmark_results_combined.png"
    pdf_path = output_dir / "benchmark_results_combined.pdf"
    fig.savefig(png_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    fig.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
    plt.close()
    print(f"  Combined multi-benchmark plot saved to: {png_path}")
    print(f"  Combined multi-benchmark plot (PDF) saved to: {pdf_path}")
