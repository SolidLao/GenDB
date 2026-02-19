#!/usr/bin/env python3
"""Compare query result CSV files between expected (ground truth) and actual directories.

Handles: floating-point precision, NULL variants.
Non-TPC-H mode: sorts both sides for order-independent comparison.
TPC-H mode: positional comparison (validates ORDER BY correctness).
Outputs JSON summary to stdout.

Usage:
    python3 compare_results.py <expected_dir> <actual_dir> [--precision 2] [--tpch]

With --tpch, uses TPC-H validation rules (TPC-H spec 2.1.4.4):
  a) Singleton column values and COUNT aggregates: exact match
  b) Ratios: 0.99*v <= round(r,2) <= 1.01*v (1% relative, rounded to 2 decimals)
  c) SUM aggregates: abs diff <= $100
  d) AVG aggregates: 0.99*v <= round(r,2) <= 1.01*v (1% relative, rounded to 2 decimals)
  Comment 1: SUM+ratio combinations (Q8,Q14,Q17): must satisfy BOTH b) and c)
  Comment 2: SUM of 0/1 resembling row count (Q12): exact match per a)
  Comment 3: Values selected from views without computation (Q15 total_revenue): $100 per c)
  Comment 4: SUM(l_quantity) (Q1,Q18): exact match
"""

import argparse
import csv
import json
import math
import os
import re
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


def read_csv_raw(path, sort=False):
    """Read a CSV file and return (headers, rows) with raw string values.

    If sort=True, rows are sorted lexicographically for order-independent comparison.
    If sort=False (default), rows preserve original order for positional comparison.
    """
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        rows = []
        for row in reader:
            cleaned = tuple(str(v).strip() for v in row)
            rows.append(cleaned)
    if sort:
        rows.sort()
    return [h.strip().lower() for h in headers], rows


def classify_tpch_column(header):
    """Classify a column header into a TPC-H tolerance category.

    Returns one of:
      'exact_count'  — COUNT aggregates and row-count SUMs: exact match (rules a, Comment 2)
      'exact_sum_qty' — SUM(l_quantity): exact match (Comment 4)
      'sum_money'    — SUM aggregates of monetary values: abs diff <= $100 (rule c)
      'avg'          — AVG aggregates: 1% relative, rounded to 2 decimals (rule d)
      'ratio'        — Ratios: 1% relative, rounded to 2 decimals (rule b)
      'sum_ratio'    — SUM+ratio combinations (Comment 1): must satisfy BOTH $100 AND 1%
      'exact'        — Singleton column values: exact match (rule a)
    """
    h = header.strip().lower()
    # Count columns and row-count SUMs — exact (rules a, Comment 2)
    if h == "count" or h.startswith("count_") or h.endswith("_count") or h in (
        "order_count", "high_line_count", "low_line_count",
        "supplier_cnt", "custdist", "numcust", "numwait",
    ):
        return "exact_count"
    # SUM(l_quantity) — exact (Comment 4)
    if h in ("sum_qty", "sum_quantity"):
        return "exact_sum_qty"
    # SUM+ratio combinations — must satisfy BOTH $100 abs AND 1% relative (Comment 1)
    # Q8: mkt_share, Q14: promo_revenue, Q17: avg_yearly
    if h in ("promo_revenue", "mkt_share", "avg_yearly"):
        return "sum_ratio"
    # SUM aggregates of monetary values — $100 tolerance (rule c, Comment 3)
    if h.startswith("sum_") or h in ("revenue", "total_revenue", "value", "totacctbal"):
        return "sum_money"
    # AVG aggregates — 1% relative tolerance (rule d)
    if h.startswith("avg_"):
        return "avg"
    # Everything else — singleton column values, exact match (rule a)
    return "exact"


def _check_1pct_tolerance(exp_f, act_f):
    """Check TPC-H 1% relative tolerance: 0.99*v <= round(r,2) <= 1.01*v.

    v = expected value (validation output), r = actual result.
    Per spec, round the result to nearest 1/100th, compare against unrounded expected.
    """
    act_r = round(act_f, 2)
    if exp_f == 0:
        return act_r == 0
    lower = min(0.99 * exp_f, 1.01 * exp_f)  # handles negative v
    upper = max(0.99 * exp_f, 1.01 * exp_f)
    return lower <= act_r <= upper


def compare_tpch_value(exp_val, act_val, category):
    """Compare two values under TPC-H rules.

    Returns (match: bool, detail: dict or None).
    detail includes: expected, actual, abs_diff, rel_diff_pct (when applicable).
    """
    # Both empty/null — match
    exp_s = str(exp_val).strip()
    act_s = str(act_val).strip()

    # Normalize NULL variants
    if exp_s.lower() in ("null", "nan", "none", ""):
        exp_s = ""
    if act_s.lower() in ("null", "nan", "none", ""):
        act_s = ""

    if exp_s == "" and act_s == "":
        return True, None

    # For exact categories (rule a), try numeric comparison first, then string
    if category in ("exact_count", "exact_sum_qty", "exact"):
        # Try numeric comparison
        try:
            exp_f = float(exp_s)
            act_f = float(act_s)
            # If expected has no decimal point, treat as integer comparison
            if "." not in exp_s:
                if int(exp_f) == int(act_f):
                    return True, None
                abs_diff = abs(exp_f - act_f)
                rel_diff = (abs_diff / abs(exp_f) * 100) if exp_f != 0 else float('inf')
                return False, {
                    "expected": exp_s, "actual": act_s,
                    "abs_diff": abs_diff, "rel_diff_pct": round(rel_diff, 4),
                }
            # Float exact: compare string representations rounded to expected precision
            # Determine precision from expected value's decimal places
            dec_places = len(exp_s.split(".")[-1]) if "." in exp_s else 0
            if round(exp_f, dec_places) == round(act_f, dec_places):
                return True, None
            abs_diff = abs(exp_f - act_f)
            rel_diff = (abs_diff / abs(exp_f) * 100) if exp_f != 0 else float('inf')
            return False, {
                "expected": exp_s, "actual": act_s,
                "abs_diff": abs_diff, "rel_diff_pct": round(rel_diff, 4),
            }
        except (ValueError, OverflowError):
            # String comparison
            if exp_s == act_s:
                return True, None
            return False, {"expected": exp_s, "actual": act_s}

    # Rule c: SUM aggregates — abs diff <= $100
    if category == "sum_money":
        try:
            exp_f = float(exp_s)
            act_f = float(act_s)
            abs_diff = abs(exp_f - act_f)
            if abs_diff <= 100.0:
                return True, None
            rel_diff = (abs_diff / abs(exp_f) * 100) if exp_f != 0 else float('inf')
            return False, {
                "expected": exp_s, "actual": act_s,
                "abs_diff": round(abs_diff, 2), "rel_diff_pct": round(rel_diff, 4),
                "tolerance": "$100",
            }
        except (ValueError, OverflowError):
            if exp_s == act_s:
                return True, None
            return False, {"expected": exp_s, "actual": act_s}

    # Rules b/d: AVG aggregates and ratios — 0.99*v <= round(r,2) <= 1.01*v
    if category in ("avg", "ratio"):
        try:
            exp_f = float(exp_s)
            act_f = float(act_s)
            if _check_1pct_tolerance(exp_f, act_f):
                return True, None
            act_r = round(act_f, 2)
            abs_diff = abs(act_r - exp_f)
            rel_diff = (abs_diff / abs(exp_f) * 100) if exp_f != 0 else float('inf')
            return False, {
                "expected": exp_s, "actual": act_s,
                "abs_diff": round(abs_diff, 6), "rel_diff_pct": round(rel_diff, 4),
                "tolerance": "1%",
            }
        except (ValueError, OverflowError):
            if exp_s == act_s:
                return True, None
            return False, {"expected": exp_s, "actual": act_s}

    # Comment 1: SUM+ratio combinations — must satisfy BOTH $100 abs AND 1% relative
    if category == "sum_ratio":
        try:
            exp_f = float(exp_s)
            act_f = float(act_s)
            abs_diff = abs(exp_f - act_f)
            passes_sum = abs_diff <= 100.0
            passes_ratio = _check_1pct_tolerance(exp_f, act_f)
            if passes_sum and passes_ratio:
                return True, None
            rel_diff = (abs_diff / abs(exp_f) * 100) if exp_f != 0 else float('inf')
            violations = []
            if not passes_sum:
                violations.append(f"$100 (diff=${abs_diff:.2f})")
            if not passes_ratio:
                violations.append(f"1% (diff={rel_diff:.4f}%)")
            return False, {
                "expected": exp_s, "actual": act_s,
                "abs_diff": round(abs_diff, 2), "rel_diff_pct": round(rel_diff, 4),
                "tolerance": "BOTH $100 AND 1%",
                "violated": ", ".join(violations),
            }
        except (ValueError, OverflowError):
            if exp_s == act_s:
                return True, None
            return False, {"expected": exp_s, "actual": act_s}

    # Fallback: exact string match
    if exp_s == act_s:
        return True, None
    return False, {"expected": exp_s, "actual": act_s}


def compare_query_tpch(expected_path, actual_path):
    """Compare two CSV files using TPC-H tolerance rules.

    Compares rows positionally (no sorting) to validate ORDER BY correctness.
    TPC-H queries specify ORDER BY; the ground truth is in the correct order,
    and the generated code must produce matching order.
    """
    if not os.path.exists(expected_path):
        return {"match": False, "error": "expected file not found", "rows_expected": 0, "rows_actual": 0}
    if not os.path.exists(actual_path):
        return {"match": False, "error": "actual file not found", "rows_expected": 0, "rows_actual": 0}

    # No sorting — compare positionally to validate ORDER BY
    exp_headers, exp_rows = read_csv_raw(expected_path, sort=False)
    act_headers, act_rows = read_csv_raw(actual_path, sort=False)

    result = {
        "rows_expected": len(exp_rows),
        "rows_actual": len(act_rows),
    }

    # Check row count
    if len(exp_rows) != len(act_rows):
        result["match"] = False
        result["error"] = f"row count mismatch: expected {len(exp_rows)}, got {len(act_rows)}"
        return result

    # Classify columns
    # Use expected headers if available, otherwise actual
    headers = exp_headers if exp_headers else act_headers
    categories = [classify_tpch_column(h) for h in headers]

    # Compare rows
    column_failures = {}  # col_name -> list of failure details
    row_mismatches = 0

    for i, (exp_row, act_row) in enumerate(zip(exp_rows, act_rows)):
        max_cols = max(len(exp_row), len(act_row), len(headers))
        row_has_mismatch = False

        for col_idx in range(max_cols):
            exp_val = exp_row[col_idx] if col_idx < len(exp_row) else ""
            act_val = act_row[col_idx] if col_idx < len(act_row) else ""
            cat = categories[col_idx] if col_idx < len(categories) else "exact"
            col_name = headers[col_idx] if col_idx < len(headers) else f"col_{col_idx}"

            match, detail = compare_tpch_value(exp_val, act_val, cat)
            if not match:
                row_has_mismatch = True
                if col_name not in column_failures:
                    column_failures[col_name] = {
                        "category": cat,
                        "failure_count": 0,
                        "samples": [],
                    }
                column_failures[col_name]["failure_count"] += 1
                if len(column_failures[col_name]["samples"]) < 3:
                    if detail:
                        detail["row"] = i
                    column_failures[col_name]["samples"].append(detail or {"row": i})

        if row_has_mismatch:
            row_mismatches += 1

    if column_failures:
        result["match"] = False
        result["error"] = f"{row_mismatches} row(s) differ across {len(column_failures)} column(s)"
        result["column_failures"] = column_failures
    else:
        result["match"] = True

    return result


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
    parser.add_argument("--tpch", action="store_true", help="Use TPC-H precision rules (per-column tolerance)")
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

        if args.tpch:
            result = compare_query_tpch(expected_path, actual_path)
        else:
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
