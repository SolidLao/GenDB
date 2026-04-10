"""GenDB MQO (Multiple Query Optimization) mode benchmarking.

Invoked from `benchmark.py --mqo-run <path>` when the run directory contains
an `mqo/` subdirectory with a compiled `./mqo` binary. Measures:
  - Batch mode:  ./mqo --all  (one process, amortized shared work)
  - Single mode: ./mqo --query Qi (for each query, subset execution)

Writes metrics to the same `benchmarks/results/<name>/sf<N>/metrics/` location
as the single-query-mode benchmarks, under a new `mqo_results.json` file.
"""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path


def is_mqo_run(run_dir):
    """True if the given run dir contains an MQO artifact (compiled binary)."""
    if run_dir is None:
        return False
    run_dir = Path(run_dir)
    mqo_bin = run_dir / "mqo" / "mqo"
    return mqo_bin.exists()


def _drop_os_caches():
    """Best-effort OS cache clear for cold mode. Requires root or NOPASSWD sudo."""
    try:
        subprocess.run(["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
                       check=False, timeout=30, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def _run_binary(binary_path, argv, cwd, timeout_s=600):
    """Run a subprocess and return (stdout, stderr, duration_ms, exit_code)."""
    start = time.perf_counter_ns()
    try:
        proc = subprocess.run(
            [str(binary_path)] + list(argv),
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        elapsed_ms = (time.perf_counter_ns() - start) / 1e6
        return proc.stdout, proc.stderr, elapsed_ms, proc.returncode
    except subprocess.TimeoutExpired as e:
        elapsed_ms = (time.perf_counter_ns() - start) / 1e6
        return e.stdout or "", e.stderr or f"Timeout after {timeout_s}s", elapsed_ms, -1


def _parse_dispatcher_total_ms(profile_path):
    """Pull dispatcher_total_ms from profile.json if available."""
    if not profile_path.exists():
        return None
    try:
        data = json.loads(profile_path.read_text())
        return data.get("regions", {}).get("dispatcher_total", {}).get("total_ms")
    except Exception:
        return None


def run_mqo_benchmark(run_dir, query_ids, mode="hot", runs_per_query=3, timeout_s=600, compare_tool=None, ground_truth_dir=None):
    """Benchmark an MQO artifact.

    Args:
        run_dir: path containing `mqo/mqo` binary
        query_ids: list of query IDs expected in the batch (e.g., ['Q1', 'Q3', ...])
        mode: 'hot' or 'cold'
        runs_per_query: number of runs to average (hot mode)
        timeout_s: per-invocation timeout
        compare_tool: optional path to src/gendb/tools/compare_results.py for validation
        ground_truth_dir: optional ground-truth dir for validation

    Returns:
        dict with:
          batch_mode: { total_ms (wall), dispatcher_total_ms, per_query_csvs, validated }
          single_mode: { [qid]: { total_ms, validated } }
    """
    run_dir = Path(run_dir)
    mqo_dir = run_dir / "mqo"
    binary = mqo_dir / "mqo"
    if not binary.exists():
        raise FileNotFoundError(f"MQO binary not found: {binary}")

    results_root = mqo_dir / "bench_results"
    if results_root.exists():
        shutil.rmtree(results_root)
    results_root.mkdir(parents=True, exist_ok=True)
    batch_out = results_root / "batch"
    batch_out.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Batch mode: ./mqo --all
    # -----------------------------------------------------------------------
    batch_wall_ms = []
    batch_dispatcher_ms = []
    for i in range(runs_per_query):
        if mode == "cold":
            _drop_os_caches()
        print(f"[MQO bench] Batch run {i + 1}/{runs_per_query} ({mode})")
        stdout, stderr, elapsed, rc = _run_binary(
            binary, ["--all", "--output-dir", str(batch_out)], cwd=mqo_dir, timeout_s=timeout_s,
        )
        if rc != 0:
            print(f"[MQO bench] ./mqo --all FAILED (exit {rc}): {stderr.strip().splitlines()[-3:]}")
            continue
        batch_wall_ms.append(elapsed)
        dispatcher_ms = _parse_dispatcher_total_ms(mqo_dir / "profile.json")
        if dispatcher_ms is not None:
            batch_dispatcher_ms.append(dispatcher_ms)

    batch_summary = {
        "mode": mode,
        "runs": len(batch_wall_ms),
        "wall_ms_mean": sum(batch_wall_ms) / len(batch_wall_ms) if batch_wall_ms else None,
        "wall_ms_min": min(batch_wall_ms) if batch_wall_ms else None,
        "dispatcher_total_ms_mean": (
            sum(batch_dispatcher_ms) / len(batch_dispatcher_ms) if batch_dispatcher_ms else None
        ),
        "dispatcher_total_ms_min": min(batch_dispatcher_ms) if batch_dispatcher_ms else None,
    }

    # Optional validation of batch CSVs against ground truth
    if compare_tool and ground_truth_dir:
        batch_summary["validated"] = _validate_results(compare_tool, ground_truth_dir, batch_out)

    # -----------------------------------------------------------------------
    # Single-query mode: ./mqo --query Qi, averaged
    # -----------------------------------------------------------------------
    single_mode = {}
    for qid in query_ids:
        single_out = results_root / "single" / qid
        single_out.mkdir(parents=True, exist_ok=True)
        per_query_wall = []
        for i in range(runs_per_query):
            if mode == "cold":
                _drop_os_caches()
            stdout, stderr, elapsed, rc = _run_binary(
                binary, ["--query", qid, "--output-dir", str(single_out)], cwd=mqo_dir, timeout_s=timeout_s,
            )
            if rc == 0:
                per_query_wall.append(elapsed)
        entry = {
            "runs": len(per_query_wall),
            "wall_ms_mean": sum(per_query_wall) / len(per_query_wall) if per_query_wall else None,
            "wall_ms_min": min(per_query_wall) if per_query_wall else None,
        }
        if compare_tool and ground_truth_dir:
            entry["validated"] = _validate_results(compare_tool, ground_truth_dir, single_out)
        single_mode[qid] = entry
        print(f"[MQO bench] {qid}: {entry['wall_ms_mean']} ms mean")

    return {"batch_mode": batch_summary, "single_mode": single_mode}


def _validate_results(compare_tool, ground_truth_dir, results_dir):
    try:
        proc = subprocess.run(
            ["python3", str(compare_tool), str(ground_truth_dir), str(results_dir)],
            capture_output=True, text=True, timeout=120, check=False,
        )
        return proc.returncode == 0
    except Exception:
        return False


def write_mqo_metrics(metrics_dir, mqo_results):
    """Write mqo_results.json into the benchmark metrics directory."""
    metrics_dir = Path(metrics_dir)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    out = metrics_dir / "mqo_results.json"
    out.write_text(json.dumps(mqo_results, indent=2))
    return out
