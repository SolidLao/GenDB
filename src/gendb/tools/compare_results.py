#!/usr/bin/env python3
"""Compare query result CSV files between expected (ground truth) and actual directories.

Handles: floating-point precision, NULL variants.
Non-TPC-H mode: sorts both sides for order-independent comparison.
TPC-H mode: positional comparison (validates ORDER BY correctness).
Outputs JSON summary to stdout.

Usage:
    python3 compare_results.py <expected_dir> <actual_dir> [--precision 2] [--tpch] [--financial]

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


def classify_financial_column(header):
    """Classify a column header into a financial tolerance category using heuristics.

    General-purpose: works for any financial workload (SEC-EDGAR, etc.).
    Reuses TPC-H tolerance categories so compare_tpch_value() handles the math.

    Returns one of:
      'exact_count'  — count-like columns: exact match
      'sum_money'    — monetary sums/totals: abs diff <= $100
      'avg'          — averages: 1% relative tolerance
      'exact'        — everything else: exact match
    """
    h = header.strip().lower()
    # Count columns — exact
    if h in ("cnt", "count") or h.endswith("_count") or h.endswith("_cnt"):
        return "exact_count"
    # num_* columns (but NOT num_value which is a monetary amount)
    if h.startswith("num_") and h != "num_value":
        return "exact_count"
    # Monetary sums/totals — $100 tolerance
    if (h.startswith("total_") or h == "total" or h.startswith("sum_")
            or h in ("revenue", "total_revenue", "value")):
        return "sum_money"
    # Averages — 1% relative tolerance
    if h.startswith("avg_"):
        return "avg"
    # Everything else — exact
    return "exact"


def _detect_order_column(headers, categories, rows):
    """Detect the ORDER BY column by finding a monotonically non-increasing numeric column.

    Scans numeric columns (sum_money, avg, exact_count, exact) and returns the index
    of the first column whose values are monotonically non-increasing in the expected rows.
    Returns None if no such column is found.
    """
    numeric_cats = {"sum_money", "avg", "exact_count", "exact"}
    for col_idx in range(len(headers)):
        cat = categories[col_idx] if col_idx < len(categories) else "exact"
        if cat not in numeric_cats:
            continue
        # Check if this column is monotonically non-increasing
        prev_val = None
        is_monotonic = True
        has_values = False
        for row in rows:
            if col_idx >= len(row):
                is_monotonic = False
                break
            val_s = row[col_idx].strip()
            if val_s == "" or val_s.lower() in ("null", "nan", "none"):
                continue
            try:
                val_f = float(val_s)
            except (ValueError, OverflowError):
                is_monotonic = False
                break
            has_values = True
            if prev_val is not None and val_f > prev_val:
                is_monotonic = False
                break
            prev_val = val_f
        if is_monotonic and has_values and prev_val is not None:
            # Also require at least some variation (not all same value)
            # to avoid false positives on constant columns
            first_val = None
            for row in rows:
                if col_idx < len(row):
                    try:
                        first_val = float(row[col_idx].strip())
                        break
                    except (ValueError, OverflowError):
                        continue
            if first_val is not None and first_val != prev_val:
                return col_idx
    return None


def _group_by_order_value(rows, col_idx):
    """Group consecutive rows by their value at col_idx.

    Returns list of (order_value_str, [row_tuples]) groups.
    """
    groups = []
    current_val = None
    current_group = []
    for row in rows:
        val = row[col_idx] if col_idx < len(row) else ""
        if val != current_val:
            if current_group:
                groups.append((current_val, current_group))
            current_val = val
            current_group = [row]
        else:
            current_group.append(row)
    if current_group:
        groups.append((current_val, current_group))
    return groups


def _compare_rows_financial(exp_rows, act_rows, headers, categories, column_failures, row_offset=0):
    """Compare rows positionally using financial tolerance rules. Returns number of mismatched rows."""
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
                        detail["row"] = row_offset + i
                    column_failures[col_name]["samples"].append(detail or {"row": row_offset + i})

        if row_has_mismatch:
            row_mismatches += 1
    return row_mismatches


def compare_query_financial(expected_path, actual_path):
    """Compare two CSV files using financial tolerance rules.

    Positional comparison (validates ORDER BY correctness), with tie-aware handling:
    when consecutive rows share the same ORDER BY value, their relative order is
    undefined per SQL spec, so they are compared as sets rather than positionally.
    Uses heuristic column classification with tolerances matching TPC-H rules.
    """
    if not os.path.exists(expected_path):
        return {"match": False, "error": "expected file not found", "rows_expected": 0, "rows_actual": 0}
    if not os.path.exists(actual_path):
        return {"match": False, "error": "actual file not found", "rows_expected": 0, "rows_actual": 0}

    exp_headers, exp_rows = read_csv_raw(expected_path, sort=False)
    act_headers, act_rows = read_csv_raw(actual_path, sort=False)

    result = {
        "rows_expected": len(exp_rows),
        "rows_actual": len(act_rows),
    }

    if len(exp_rows) != len(act_rows):
        result["match"] = False
        result["error"] = f"row count mismatch: expected {len(exp_rows)}, got {len(act_rows)}"
        return result

    headers = exp_headers if exp_headers else act_headers
    categories = [classify_financial_column(h) for h in headers]

    # Detect ORDER BY column for tie-aware comparison
    order_col = _detect_order_column(headers, categories, exp_rows)

    column_failures = {}
    row_mismatches = 0

    if order_col is None:
        # No ORDER BY column detected — strict positional comparison
        row_mismatches = _compare_rows_financial(exp_rows, act_rows, headers, categories, column_failures)
    else:
        # Tie-aware comparison: group by ORDER BY value, compare tied groups as sets
        exp_groups = _group_by_order_value(exp_rows, order_col)
        act_groups = _group_by_order_value(act_rows, order_col)

        if len(exp_groups) != len(act_groups):
            # Different group structure — fall back to strict positional
            row_mismatches = _compare_rows_financial(exp_rows, act_rows, headers, categories, column_failures)
        else:
            row_offset = 0
            for (exp_val, exp_group), (act_val, act_group) in zip(exp_groups, act_groups):
                if exp_val != act_val or len(exp_group) != len(act_group):
                    # Group mismatch — compare positionally
                    row_mismatches += _compare_rows_financial(
                        exp_group, act_group, headers, categories, column_failures, row_offset)
                elif len(exp_group) == 1:
                    # Single-row group — positional comparison
                    row_mismatches += _compare_rows_financial(
                        exp_group, act_group, headers, categories, column_failures, row_offset)
                else:
                    # Multi-row tie group — sort both sides by all columns, then compare
                    sorted_exp = sorted(exp_group)
                    sorted_act = sorted(act_group)
                    row_mismatches += _compare_rows_financial(
                        sorted_exp, sorted_act, headers, categories, column_failures, row_offset)
                row_offset += len(exp_group)

    if column_failures:
        result["match"] = False
        result["error"] = f"{row_mismatches} row(s) differ across {len(column_failures)} column(s)"
        result["column_failures"] = column_failures
        if order_col is not None:
            result["tie_aware"] = True
            result["order_column"] = headers[order_col] if order_col < len(headers) else f"col_{order_col}"
        pattern_counts = {}
        for col_name, cf in column_failures.items():
            for sample in cf.get("samples", []):
                exp_s = sample.get("expected", "")
                act_s = sample.get("actual", "")
                if exp_s and act_s:
                    p = _classify_mismatch_pattern(exp_s, act_s)
                    sample["pattern"] = p
                    pattern_counts[p] = pattern_counts.get(p, 0) + 1
        if pattern_counts:
            result["mismatch_patterns"] = pattern_counts
    else:
        result["match"] = True
        if order_col is not None:
            result["tie_aware"] = True
            result["order_column"] = headers[order_col] if order_col < len(headers) else f"col_{order_col}"

    return result


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
        # Auto-classify mismatch patterns across all column failures
        pattern_counts = {}
        for col_name, cf in column_failures.items():
            for sample in cf.get("samples", []):
                exp_s = sample.get("expected", "")
                act_s = sample.get("actual", "")
                if exp_s and act_s:
                    p = _classify_mismatch_pattern(exp_s, act_s)
                    sample["pattern"] = p
                    pattern_counts[p] = pattern_counts.get(p, 0) + 1
        if pattern_counts:
            result["mismatch_patterns"] = pattern_counts
    else:
        result["match"] = True

    return result


def _classify_mismatch_pattern(exp_s, act_s):
    """Auto-classify a mismatch into a pattern category for diagnostic feedback."""
    try:
        exp_f = float(exp_s)
        act_f = float(act_s)
    except (ValueError, OverflowError):
        if act_s == "" or act_s.lower() in ("null", "nan", "none"):
            return "zero_output"
        return "string_mismatch"

    abs_diff = abs(exp_f - act_f)
    magnitude = abs(exp_f) if exp_f != 0 else 0

    if act_f == 0 and exp_f != 0:
        return "zero_output"
    if exp_f != 0 and act_f != 0 and (exp_f > 0) != (act_f > 0):
        return "sign_flip"
    if magnitude > 0:
        ratio = act_f / exp_f if exp_f != 0 else float('inf')
        if 90 < ratio < 110 or 0.009 < ratio < 0.011:
            return "scale_error"
    if magnitude > 1e10 and abs_diff < 1.0:
        return "precision_loss"
    return "value_mismatch"


def _enrich_cell_detail(exp_s, act_s):
    """Produce enriched per-cell mismatch detail with abs_diff, magnitude, relative_diff, pattern."""
    detail = {"expected": exp_s, "actual": act_s}
    try:
        exp_f = float(exp_s)
        act_f = float(act_s)
        abs_diff = abs(exp_f - act_f)
        magnitude = abs(exp_f)
        detail["abs_diff"] = round(abs_diff, 6)
        detail["magnitude"] = round(magnitude, 2)
        if magnitude > 0:
            detail["relative_diff"] = round(abs_diff / magnitude, 8)
    except (ValueError, OverflowError):
        pass
    detail["pattern"] = _classify_mismatch_pattern(exp_s, act_s)
    return detail


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
        result["pattern"] = "row_count_mismatch"
        return result

    # Compare rows (already sorted)
    mismatches = []
    pattern_counts = {}
    for i, (exp_row, act_row) in enumerate(zip(exp_rows, act_rows)):
        # Pad shorter row with empty strings
        max_cols = max(len(exp_row), len(act_row))
        exp_padded = exp_row + ("",) * (max_cols - len(exp_row))
        act_padded = act_row + ("",) * (max_cols - len(act_row))
        if exp_padded != act_padded:
            if len(mismatches) < 3:  # Only report first 3 mismatches
                enriched_cells = []
                for col_idx in range(max_cols):
                    e = exp_padded[col_idx] if col_idx < len(exp_padded) else ""
                    a = act_padded[col_idx] if col_idx < len(act_padded) else ""
                    if e != a:
                        cell_detail = _enrich_cell_detail(e, a)
                        cell_detail["col"] = col_idx
                        enriched_cells.append(cell_detail)
                        p = cell_detail.get("pattern", "unknown")
                        pattern_counts[p] = pattern_counts.get(p, 0) + 1
                mismatches.append({
                    "row": i,
                    "expected": list(exp_padded),
                    "actual": list(act_padded),
                    "cell_details": enriched_cells,
                })
            else:
                # Still count patterns for diagnostic even beyond 3 sample mismatches
                for col_idx in range(max_cols):
                    e = exp_padded[col_idx] if col_idx < len(exp_padded) else ""
                    a = act_padded[col_idx] if col_idx < len(act_padded) else ""
                    if e != a:
                        p = _classify_mismatch_pattern(e, a)
                        pattern_counts[p] = pattern_counts.get(p, 0) + 1

    if mismatches:
        result["match"] = False
        result["error"] = f"{len(mismatches)} row(s) differ"
        result["sample_mismatches"] = mismatches
        if pattern_counts:
            result["mismatch_patterns"] = pattern_counts
    else:
        result["match"] = True

    return result


def main():
    parser = argparse.ArgumentParser(description="Compare query result CSV files")
    parser.add_argument("expected_dir", help="Directory with expected (ground truth) CSV files")
    parser.add_argument("actual_dir", help="Directory with actual CSV files")
    parser.add_argument("--precision", type=int, default=2, help="Decimal places for float comparison (default: 2)")
    parser.add_argument("--tpch", action="store_true", help="Use TPC-H precision rules (per-column tolerance)")
    parser.add_argument("--financial", action="store_true", help="Use financial tolerance rules (heuristic column classification)")
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
        elif args.financial:
            result = compare_query_financial(expected_path, actual_path)
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
