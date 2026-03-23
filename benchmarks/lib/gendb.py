"""GenDB iteration history & benchmarking — shared across workloads.

Supports two output structures:
  - Legacy (v36-): output/<benchmark>/<timestamp>/ with run.json, queries/Q1/iter_*/
  - Workload (v37+): output/<benchmark>-sf<N>/ with workload.json, queries/Q1/best->vN/,
    runs/<timestamp>/run.json, runs/<timestamp>/queries/Q1/iter_*/
"""

import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

from .utils import GREEN, RED, BOLD, RESET, drop_os_caches


def is_workload_dir(path):
    """Check if a path is a v37+ persistent workload directory."""
    return (path / "workload.json").exists()


def resolve_gendb_paths(gendb_path):
    """Resolve a user-provided GenDB path to (workload_dir, run_dir).

    Accepts:
      - A workload dir (has workload.json) -> uses runs/latest/ as run_dir
      - A legacy run dir (has run.json at top level)
      - A run dir inside a workload dir (runs/<timestamp>/)

    Returns (workload_dir_or_None, run_dir_or_None).
    """
    gendb_path = Path(gendb_path).resolve()

    if is_workload_dir(gendb_path):
        # Workload dir — find latest run
        latest = gendb_path / "runs" / "latest"
        run_dir = latest.resolve() if latest.exists() else None
        return gendb_path, run_dir

    # Check if it's a run dir (has run.json)
    if (gendb_path / "run.json").exists():
        # Could be inside a workload dir (workload_dir/runs/<timestamp>/)
        # or a legacy standalone run dir
        parent = gendb_path.parent
        if parent.name == "runs" and is_workload_dir(parent.parent):
            return parent.parent, gendb_path
        return None, gendb_path

    return None, None


def read_gendb_iteration_history(run_dir):
    """Read iteration timing/validation from execution_results.json files.

    Returns:
    {
      "query_ids": ["Q1", "Q3", "Q6"],
      "num_iters": 11,
      "data": {
        "Q1": {0: {"timing_ms": 65.13, "validation": "pass"}, 1: {...}, ...},
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


def print_iteration_table(history):
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

    # Total row
    if best:
        total = sum(b["timing_ms"] for b in best.values())
        print(sep)
        cells = [f"{'Total':<{Q_COL_W}}"]
        for _ in range(num_iters):
            cells.append(f"{'':>{COL_W}}")
        total_cell = f"{int(round(total))} ms"
        cells.append(f"{BOLD}{total_cell:>{COL_W}}{RESET}")
        print(" | ".join(cells))


def parse_timing_output(stdout):
    """Parse [TIMING] lines from GenDB binary output."""
    timings = {}
    for line in stdout.splitlines():
        m = re.match(r"\[TIMING\]\s+(\w+):\s+([\d.]+)\s*ms", line)
        if m:
            timings[m.group(1)] = float(m.group(2))
    if "total" in timings:
        timings["query_ms"] = timings["total"] - timings.get("output", 0)
    return timings


def find_best_binaries(run_dir, workload_dir=None):
    """Find the best per-query binary.

    Supports two modes:
    1. Workload dir (v37+): queries/Q1/best/ symlink -> vN/ with binary inside
    2. Legacy run dir: run.json bestCppPath -> iter_N/ with binary inside

    Returns {"Q1": Path("/path/to/q1"), ...}
    """
    binaries = {}

    # v37+ workload dir: use queries/Q1/best/ symlinks
    if workload_dir and is_workload_dir(workload_dir):
        queries_dir = workload_dir / "queries"
        if queries_dir.exists():
            for qdir in sorted(queries_dir.iterdir()):
                if not qdir.is_dir() or not qdir.name.startswith("Q"):
                    continue
                qid = qdir.name
                best_link = qdir / "best"
                if best_link.exists():
                    best_dir = best_link.resolve()
                    binary = best_dir / qid.lower()
                    if binary.exists() and os.access(str(binary), os.X_OK):
                        binaries[qid] = binary
        if binaries:
            return binaries

    # Legacy: read bestCppPath from run.json
    if run_dir is None:
        return {}
    run_json_path = run_dir / "run.json"
    if not run_json_path.exists():
        return {}

    with open(run_json_path) as f:
        run_data = json.load(f)

    pipelines = run_data.get("phase2", {}).get("pipelines", {})
    for qid, info in pipelines.items():
        cpp_path = info.get("bestCppPath")
        if cpp_path:
            bin_dir = Path(cpp_path).parent
            binary = bin_dir / qid.lower()
            if binary.exists() and os.access(str(binary), os.X_OK):
                binaries[qid] = binary

    return binaries


def _read_param_cli_args(workload_dir, qid):
    """Read params.json for a query and return CLI args list."""
    if workload_dir is None:
        return []
    params_path = workload_dir / "queries" / qid / "params.json"
    if not params_path.exists():
        return []
    with open(params_path) as f:
        params_data = json.load(f)
    args = []
    for p in params_data.get("parameters", []):
        args.append(f"--{p['name']}")
        args.append(str(p["default"]))
    return args


def gendb_benchmark_best(run_dir, gendb_dir, mode, workload_dir=None):
    """Execute the best per-query GenDB binaries and return timing results.
    Returns {"Q1": [times], "Q3": [times], ...}
    """
    best_binaries = find_best_binaries(run_dir, workload_dir)
    if not best_binaries:
        print("  No best binaries found")
        return {}

    total_runs = 4 if mode == "hot" else 1
    results = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        for qid in sorted(best_binaries, key=lambda q: int(q[1:])):
            binary = best_binaries[qid]
            print(f"  Running {qid} ({binary.parent.name}/{binary.name})...", end="", flush=True)

            # Build command: binary <gendb_dir> <results_dir> [--param value ...]
            cmd = [str(binary), str(gendb_dir), tmpdir]
            param_args = _read_param_cli_args(workload_dir, qid)
            cmd.extend(param_args)

            if mode == "cold":
                drop_os_caches()
            times = []
            ok = True
            for _ in range(total_runs):
                proc = subprocess.run(
                    cmd,
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
                pass  # error already printed

    return results
