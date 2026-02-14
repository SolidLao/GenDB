#!/usr/bin/env python3
"""Compare query result CSV files between expected (ground truth) and actual directories.

Handles: floating-point precision, NULL variants, row ordering (sorts both sides).
Outputs JSON summary to stdout.

Usage:
    python3 compare_results.py <expected_dir> <actual_dir> [--precision 2]
"""

import argparse
import csv
import json
import math
import os
import sys


def normalize_value(val, precision):
    """Normalize a cell value for comparison."""
    if val is None:
        return ""
    val = str(val).strip()
    # Treat NULL variants as empty
    if val.lower() in ("null", "nan", "none", ""):
        return ""
    # Try to parse as float and round
    try:
        f = float(val)
        if math.isnan(f):
            return ""
        return f"{round(f, precision):.{precision}f}"
    except (ValueError, OverflowError):
        return val


def read_csv_file(path, precision):
    """Read a CSV file and return (headers, sorted_rows) with normalized values."""
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        rows = []
        for row in reader:
            normalized = tuple(normalize_value(v, precision) for v in row)
            rows.append(normalized)
    # Sort rows for order-independent comparison
    rows.sort()
    return [h.strip().lower() for h in headers], rows


def compare_query(expected_path, actual_path, precision):
    """Compare two CSV files. Returns dict with match status and details."""
    if not os.path.exists(expected_path):
        return {"match": False, "error": "expected file not found", "rows_expected": 0, "rows_actual": 0}
    if not os.path.exists(actual_path):
        return {"match": False, "error": "actual file not found", "rows_expected": 0, "rows_actual": 0}

    exp_headers, exp_rows = read_csv_file(expected_path, precision)
    act_headers, act_rows = read_csv_file(actual_path, precision)

    result = {
        "rows_expected": len(exp_rows),
        "rows_actual": len(act_rows),
    }

    # Check row count
    if len(exp_rows) != len(act_rows):
        result["match"] = False
        result["error"] = f"row count mismatch: expected {len(exp_rows)}, got {len(act_rows)}"
        return result

    # Compare rows (already sorted)
    mismatches = []
    for i, (exp_row, act_row) in enumerate(zip(exp_rows, act_rows)):
        # Pad shorter row with empty strings
        max_cols = max(len(exp_row), len(act_row))
        exp_padded = exp_row + ("",) * (max_cols - len(exp_row))
        act_padded = act_row + ("",) * (max_cols - len(act_row))
        if exp_padded != act_padded:
            if len(mismatches) < 3:  # Only report first 3 mismatches
                mismatches.append({
                    "row": i,
                    "expected": list(exp_padded),
                    "actual": list(act_padded),
                })

    if mismatches:
        result["match"] = False
        result["error"] = f"{len(mismatches)} row(s) differ"
        result["sample_mismatches"] = mismatches
    else:
        result["match"] = True

    return result


def main():
    parser = argparse.ArgumentParser(description="Compare query result CSV files")
    parser.add_argument("expected_dir", help="Directory with expected (ground truth) CSV files")
    parser.add_argument("actual_dir", help="Directory with actual CSV files")
    parser.add_argument("--precision", type=int, default=2, help="Decimal places for float comparison (default: 2)")
    args = parser.parse_args()

    if not os.path.isdir(args.expected_dir):
        print(json.dumps({"match": False, "error": f"expected directory not found: {args.expected_dir}"}))
        sys.exit(1)

    if not os.path.isdir(args.actual_dir):
        print(json.dumps({"match": False, "error": f"actual directory not found: {args.actual_dir}"}))
        sys.exit(1)

    # Find all Q*.csv files in actual directory (only validate what was actually produced)
    actual_files = sorted(f for f in os.listdir(args.actual_dir) if f.startswith("Q") and f.endswith(".csv"))

    if not actual_files:
        print(json.dumps({"match": False, "error": "no Q*.csv files in actual directory"}))
        sys.exit(1)

    all_match = True
    queries = {}

    for filename in actual_files:
        query_name = filename.replace(".csv", "")
        expected_path = os.path.join(args.expected_dir, filename)
        actual_path = os.path.join(args.actual_dir, filename)

        result = compare_query(expected_path, actual_path, args.precision)
        queries[query_name] = result
        if not result["match"]:
            all_match = False

    output = {
        "match": all_match,
        "queries": queries,
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
