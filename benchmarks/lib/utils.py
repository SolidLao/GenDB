"""Shared utilities: OS cache management, FK stripping, ANSI colors, thread count."""

import os
import re
import subprocess
import time

# ANSI color codes
GREEN = "\033[32m"
RED = "\033[31m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

# Number of threads for all systems (set to core count for fair comparison)
BENCHMARK_THREADS = os.cpu_count() or 64

# System colors for plotting
SYSTEM_COLORS = {
    "GenDB": "#E41A1C",
    "PostgreSQL": "#377EB8",
    "DuckDB": "#FF7F00",
    "ClickHouse": "#4DAF4A",
    "Umbra": "#984EA3",
    "MonetDB": "#A65628",
}


def drop_os_caches():
    """Clear OS page cache so the first query run starts cold."""
    try:
        subprocess.run(
            ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            check=True, capture_output=True, timeout=10,
        )
        print("  OS page cache cleared.")
    except Exception:
        print("  Warning: could not clear OS page cache (need passwordless sudo).")
        print("  Run: sudo bash benchmarks/setup_drop_caches.sh")


def restart_postgresql():
    """Restart PostgreSQL to clear shared_buffers (internal buffer pool)."""
    try:
        subprocess.run(
            ["sudo", "-n", "pg_ctlcluster", "18", "tpch", "restart"],
            check=True, capture_output=True, timeout=30,
        )
        time.sleep(2)
        print("  PostgreSQL restarted (buffer pool cleared).")
    except Exception:
        print("  Warning: could not restart PostgreSQL (need passwordless sudo).")


def strip_fk_constraints(schema_sql: str, extra_fn=None) -> str:
    """Remove FK constraints from schema SQL. Optionally apply extra transformations."""
    result = re.sub(r"--[^\n]*", "", schema_sql)
    result = re.sub(r",?\s*FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s+\w+\([^)]+\)", "", result)
    result = re.sub(r"\s*REFERENCES\s+\w+\([^)]+\)", "", result)
    result = re.sub(r",\s*\)", "\n)", result)
    if extra_fn:
        result = extra_fn(result)
    return result
