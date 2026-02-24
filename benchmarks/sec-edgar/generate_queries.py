#!/usr/bin/env python3
"""Generate random SQL queries for SEC EDGAR benchmark.

Uses template-based random SQL generation to produce standard, cross-system
compatible analytical queries with diverse features (JOINs, GROUP BY,
subqueries, HAVING, DISTINCT, etc.).

Pipeline:
1. Generate random queries from parameterized templates
2. Filter: valid execution, non-trivial results, reasonable runtime
3. Extract features (JOINs, GROUP BY, subqueries, etc.)
4. Diversity-based sampling via greedy set-cover
5. Output labeled queries to queries.sql

Usage:
    python3 benchmarks/sec-edgar/generate_queries.py [--num-generate 2000] [--num-select 25]
"""

import argparse
import random
import re
import sys
import time
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Template-based random SQL query generator for SEC EDGAR schema
# ---------------------------------------------------------------------------

# Schema reference:
#   sub(adsh PK, cik, name, sic, countryba, stprba, cityba, countryinc,
#       form, period, fy, fp, filed, accepted, prevrpt, nciks, afs, wksi, fye, instance)
#   num(adsh, tag, version, ddate, qtrs, uom, coreg, value, footnote)
#   tag(tag PK, version PK, custom, abstract, datatype, iord, crdr, tlabel, doc)
#   pre(adsh, report, line, stmt, inpth, rfile, tag, version, plabel, negating)

# Common filter values observed in the data
FORMS = ["10-K", "10-Q", "8-K", "10-K/A", "10-Q/A"]
STMTS = ["BS", "IS", "CF", "EQ", "CI"]
COUNTRIES = ["US", "CA", "GB", "IE", "IL", "CN", "JP", "DE", "CH", "KY"]
UOMS = ["USD", "shares", "pure", "USD/shares"]
FPS = ["FY", "Q1", "Q2", "Q3", "Q4"]
CRDR_VALUES = ["C", "D"]
IORD_VALUES = ["I", "D"]

# SIC code ranges for industry sectors
SIC_RANGES = [
    (100, 999),     # Agriculture/Mining
    (1000, 1499),   # Mining
    (1500, 1799),   # Construction
    (2000, 3999),   # Manufacturing
    (4000, 4999),   # Transportation/Utilities
    (5000, 5199),   # Wholesale Trade
    (5200, 5999),   # Retail Trade
    (6000, 6799),   # Finance/Insurance/Real Estate
    (7000, 8999),   # Services
]

# Period/date ranges for filtering (YYYYMMDD format)
PERIOD_RANGES = [
    (20220101, 20221231),
    (20230101, 20231231),
    (20240101, 20241231),
    (20220101, 20241231),
]


def rand_form():
    return random.choice(FORMS)

def rand_stmt():
    return random.choice(STMTS)

def rand_country():
    return random.choice(COUNTRIES)

def rand_uom():
    return random.choice(UOMS)

def rand_fp():
    return random.choice(FPS)

def rand_sic_range():
    return random.choice(SIC_RANGES)

def rand_period_range():
    return random.choice(PERIOD_RANGES)

def rand_limit():
    return random.choice([50, 100, 200, 500, 1000])

def rand_value_threshold():
    return random.choice([0, 1000, 10000, 100000, 1000000, 1000000000])

def rand_fy():
    return random.choice([2022, 2023, 2024])

def rand_qtrs():
    return random.choice([0, 1, 4])


def generate_templates():
    """Return a list of (template_fn, feature_tags) tuples.
    Each template_fn() returns a SQL string when called.
    """
    templates = []

    # --- Single-table queries ---

    # T1: Simple aggregation on num
    def t1():
        uom = rand_uom()
        lo, hi = rand_period_range()
        return f"""
SELECT tag, COUNT(*) AS cnt, SUM(value) AS total_value,
       AVG(value) AS avg_value, MIN(value) AS min_value, MAX(value) AS max_value
FROM num
WHERE uom = '{uom}' AND ddate BETWEEN {lo} AND {hi} AND value IS NOT NULL
GROUP BY tag
HAVING COUNT(*) > 10
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t1, {"aggregation", "group_by", "having", "order_by", "uses:num", "0_joins"}))

    # T2: Distinct filers on sub
    def t2():
        form = rand_form()
        return f"""
SELECT DISTINCT countryba, stprba, COUNT(*) AS num_filings
FROM sub
WHERE form = '{form}' AND countryba IS NOT NULL AND stprba IS NOT NULL
GROUP BY countryba, stprba
ORDER BY num_filings DESC
LIMIT {rand_limit()}
"""
    templates.append((t2, {"distinct", "aggregation", "group_by", "order_by", "uses:sub", "0_joins"}))

    # T3: Tag statistics
    def t3():
        return """
SELECT datatype, iord, crdr, COUNT(*) AS tag_count,
       SUM(custom) AS custom_count,
       SUM(abstract) AS abstract_count
FROM tag
WHERE datatype IS NOT NULL
GROUP BY datatype, iord, crdr
ORDER BY tag_count DESC
LIMIT 100
"""
    templates.append((t3, {"aggregation", "group_by", "order_by", "uses:tag", "0_joins"}))

    # T4: Pre statement distribution
    def t4():
        return """
SELECT stmt, rfile, COUNT(*) AS cnt,
       COUNT(DISTINCT adsh) AS num_filings,
       AVG(line) AS avg_line_num
FROM pre
WHERE stmt IS NOT NULL
GROUP BY stmt, rfile
ORDER BY cnt DESC
"""
    templates.append((t4, {"aggregation", "group_by", "order_by", "distinct", "uses:pre", "0_joins"}))

    # --- Two-table JOIN queries ---

    # T5: num JOIN sub — values by form type
    def t5():
        uom = rand_uom()
        fy = rand_fy()
        return f"""
SELECT s.form, COUNT(*) AS num_values,
       SUM(n.value) AS total_value, AVG(n.value) AS avg_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = '{uom}' AND s.fy = {fy} AND n.value IS NOT NULL
GROUP BY s.form
ORDER BY total_value DESC
"""
    templates.append((t5, {"aggregation", "group_by", "order_by", "uses:num", "uses:sub", "1_join"}))

    # T6: num JOIN sub — top companies by total value
    def t6():
        uom = rand_uom()
        lo, hi = rand_period_range()
        return f"""
SELECT s.name, s.cik, s.sic, COUNT(*) AS num_entries,
       SUM(n.value) AS total_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = '{uom}' AND n.ddate BETWEEN {lo} AND {hi}
      AND n.value > 0
GROUP BY s.name, s.cik, s.sic
HAVING SUM(n.value) > {rand_value_threshold()}
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t6, {"aggregation", "group_by", "having", "order_by", "uses:num", "uses:sub", "1_join"}))

    # T7: pre JOIN sub — presentation by form
    def t7():
        stmt = rand_stmt()
        return f"""
SELECT s.form, s.fy, COUNT(*) AS num_lines,
       COUNT(DISTINCT p.tag) AS distinct_tags,
       AVG(p.line) AS avg_line
FROM pre p
JOIN sub s ON p.adsh = s.adsh
WHERE p.stmt = '{stmt}'
GROUP BY s.form, s.fy
ORDER BY num_lines DESC
LIMIT {rand_limit()}
"""
    templates.append((t7, {"aggregation", "group_by", "order_by", "distinct", "uses:pre", "uses:sub", "1_join"}))

    # T8: num JOIN tag — values by tag metadata
    def t8():
        uom = rand_uom()
        crdr = random.choice(CRDR_VALUES)
        return f"""
SELECT t.crdr, t.iord, t.datatype,
       COUNT(*) AS cnt, SUM(n.value) AS total,
       AVG(n.value) AS avg_val
FROM num n
JOIN tag t ON n.tag = t.tag AND n.version = t.version
WHERE n.uom = '{uom}' AND t.crdr = '{crdr}' AND n.value IS NOT NULL
GROUP BY t.crdr, t.iord, t.datatype
ORDER BY total DESC
LIMIT {rand_limit()}
"""
    templates.append((t8, {"aggregation", "group_by", "order_by", "uses:num", "uses:tag", "1_join"}))

    # T9: pre JOIN tag — tag labels in presentation
    def t9():
        stmt = rand_stmt()
        return f"""
SELECT t.tlabel, t.datatype, COUNT(*) AS usage_count,
       COUNT(DISTINCT p.adsh) AS filing_count
FROM pre p
JOIN tag t ON p.tag = t.tag AND p.version = t.version
WHERE p.stmt = '{stmt}' AND t.tlabel IS NOT NULL
GROUP BY t.tlabel, t.datatype
HAVING COUNT(*) > 5
ORDER BY usage_count DESC
LIMIT {rand_limit()}
"""
    templates.append((t9, {"aggregation", "group_by", "having", "order_by", "distinct", "uses:pre", "uses:tag", "1_join"}))

    # --- Three-table JOIN queries ---

    # T10: num JOIN sub JOIN tag — values with company and tag info
    def t10():
        uom = rand_uom()
        fy = rand_fy()
        sic_lo, sic_hi = rand_sic_range()
        return f"""
SELECT s.name, t.tlabel, SUM(n.value) AS total_value,
       COUNT(*) AS cnt
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN tag t ON n.tag = t.tag AND n.version = t.version
WHERE n.uom = '{uom}' AND s.fy = {fy}
      AND s.sic BETWEEN {sic_lo} AND {sic_hi}
      AND n.value IS NOT NULL AND t.abstract = 0
GROUP BY s.name, t.tlabel
HAVING SUM(n.value) > {rand_value_threshold()}
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t10, {"aggregation", "group_by", "having", "order_by", "uses:num", "uses:sub", "uses:tag", "2_joins"}))

    # T11: pre JOIN sub JOIN tag — presentation with metadata
    def t11():
        form = rand_form()
        stmt = rand_stmt()
        return f"""
SELECT s.name, p.stmt, t.tlabel, p.line, p.plabel
FROM pre p
JOIN sub s ON p.adsh = s.adsh
JOIN tag t ON p.tag = t.tag AND p.version = t.version
WHERE s.form = '{form}' AND p.stmt = '{stmt}'
      AND t.custom = 0
ORDER BY s.name, p.line
LIMIT {rand_limit()}
"""
    templates.append((t11, {"order_by", "uses:pre", "uses:sub", "uses:tag", "2_joins"}))

    # T12: num JOIN sub JOIN pre — cross-referencing values and presentation
    def t12():
        uom = rand_uom()
        stmt = rand_stmt()
        fy = rand_fy()
        return f"""
SELECT s.name, p.stmt, n.tag, p.plabel,
       SUM(n.value) AS total_value, COUNT(*) AS cnt
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = '{uom}' AND p.stmt = '{stmt}' AND s.fy = {fy}
      AND n.value IS NOT NULL
GROUP BY s.name, p.stmt, n.tag, p.plabel
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t12, {"aggregation", "group_by", "order_by", "uses:num", "uses:sub", "uses:pre", "2_joins"}))

    # --- Four-table JOIN queries ---

    # T13: All four tables joined
    def t13():
        uom = rand_uom()
        form = rand_form()
        stmt = rand_stmt()
        return f"""
SELECT s.name, s.sic, t.tlabel, p.stmt, p.plabel,
       SUM(n.value) AS total_value, COUNT(*) AS cnt
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN tag t ON n.tag = t.tag AND n.version = t.version
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = '{uom}' AND s.form = '{form}' AND p.stmt = '{stmt}'
      AND n.value IS NOT NULL AND t.abstract = 0
GROUP BY s.name, s.sic, t.tlabel, p.stmt, p.plabel
HAVING COUNT(*) > 1
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t13, {"aggregation", "group_by", "having", "order_by", "uses:num", "uses:sub", "uses:tag", "uses:pre", "3plus_joins"}))

    # --- Subquery queries ---

    # T14: Subquery — companies with above-average values
    def t14():
        uom = rand_uom()
        fy = rand_fy()
        return f"""
SELECT s.name, s.cik, SUM(n.value) AS total_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = '{uom}' AND s.fy = {fy} AND n.value IS NOT NULL
GROUP BY s.name, s.cik
HAVING SUM(n.value) > (
    SELECT AVG(sub_total) FROM (
        SELECT SUM(n2.value) AS sub_total
        FROM num n2
        JOIN sub s2 ON n2.adsh = s2.adsh
        WHERE n2.uom = '{uom}' AND s2.fy = {fy} AND n2.value IS NOT NULL
        GROUP BY s2.cik
    ) avg_sub
)
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t14, {"aggregation", "group_by", "having", "order_by", "subquery", "uses:num", "uses:sub", "1_join"}))

    # T15: Subquery — tags used more than average
    def t15():
        uom = rand_uom()
        return f"""
SELECT n.tag, COUNT(*) AS usage_count, SUM(n.value) AS total
FROM num n
WHERE n.uom = '{uom}' AND n.value IS NOT NULL
GROUP BY n.tag
HAVING COUNT(*) > (
    SELECT AVG(cnt) FROM (
        SELECT COUNT(*) AS cnt FROM num WHERE uom = '{uom}' GROUP BY tag
    ) sub
)
ORDER BY usage_count DESC
LIMIT {rand_limit()}
"""
    templates.append((t15, {"aggregation", "group_by", "having", "order_by", "subquery", "uses:num", "0_joins"}))

    # T16: IN subquery — filings from companies with high activity
    def t16():
        form = rand_form()
        return f"""
SELECT s.name, s.cik, s.countryba, s.sic
FROM sub s
WHERE s.form = '{form}'
      AND s.cik IN (
          SELECT cik FROM sub
          WHERE form = '{form}'
          GROUP BY cik
          HAVING COUNT(*) > 3
      )
ORDER BY s.name
LIMIT {rand_limit()}
"""
    templates.append((t16, {"subquery", "order_by", "uses:sub", "0_joins"}))

    # T17: EXISTS subquery — tags that appear in both num and pre
    def t17():
        uom = rand_uom()
        stmt = rand_stmt()
        return f"""
SELECT DISTINCT n.tag, n.version, COUNT(*) AS cnt
FROM num n
WHERE n.uom = '{uom}' AND n.value IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM pre p
          WHERE p.tag = n.tag AND p.version = n.version
                AND p.stmt = '{stmt}'
      )
GROUP BY n.tag, n.version
ORDER BY cnt DESC
LIMIT {rand_limit()}
"""
    templates.append((t17, {"distinct", "aggregation", "group_by", "order_by", "subquery", "uses:num", "uses:pre", "0_joins"}))

    # T18: Correlated subquery — max value per tag per company
    def t18():
        uom = rand_uom()
        fy = rand_fy()
        return f"""
SELECT s.name, n.tag, n.value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = '{uom}' AND s.fy = {fy} AND n.value IS NOT NULL
      AND n.value = (
          SELECT MAX(n2.value)
          FROM num n2
          WHERE n2.tag = n.tag AND n2.adsh = n.adsh AND n2.uom = '{uom}'
      )
ORDER BY n.value DESC
LIMIT {rand_limit()}
"""
    templates.append((t18, {"subquery", "order_by", "uses:num", "uses:sub", "1_join"}))

    # --- Window / analytical-style queries (without WINDOW, using standard SQL) ---

    # T19: Year-over-year comparison
    def t19():
        uom = rand_uom()
        tag_filter = random.choice([
            "Revenues", "NetIncomeLoss", "Assets", "StockholdersEquity",
            "EarningsPerShareBasic", "CashAndCashEquivalentsAtCarryingValue",
        ])
        return f"""
SELECT s.cik, s.name, s.fy, SUM(n.value) AS total_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.tag = '{tag_filter}' AND n.uom = '{uom}'
      AND n.qtrs = 0 AND s.fp = 'FY' AND n.value IS NOT NULL
GROUP BY s.cik, s.name, s.fy
ORDER BY s.cik, s.fy
LIMIT {rand_limit()}
"""
    templates.append((t19, {"aggregation", "group_by", "order_by", "uses:num", "uses:sub", "1_join"}))

    # T20: Industry sector comparison
    def t20():
        uom = rand_uom()
        fy = rand_fy()
        return f"""
SELECT s.sic, COUNT(DISTINCT s.cik) AS num_companies,
       COUNT(*) AS num_values,
       SUM(n.value) AS total_value,
       AVG(n.value) AS avg_value,
       MIN(n.value) AS min_value,
       MAX(n.value) AS max_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = '{uom}' AND s.fy = {fy}
      AND s.sic IS NOT NULL AND n.value IS NOT NULL AND n.value > 0
GROUP BY s.sic
HAVING COUNT(DISTINCT s.cik) >= 3
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t20, {"aggregation", "group_by", "having", "order_by", "distinct", "uses:num", "uses:sub", "1_join"}))

    # T21: Filing count by country and form
    def t21():
        fy = rand_fy()
        return f"""
SELECT countryba, form, fy, COUNT(*) AS filing_count,
       COUNT(DISTINCT cik) AS company_count
FROM sub
WHERE fy = {fy} AND countryba IS NOT NULL
GROUP BY countryba, form, fy
ORDER BY filing_count DESC
LIMIT {rand_limit()}
"""
    templates.append((t21, {"aggregation", "group_by", "order_by", "distinct", "uses:sub", "0_joins"}))

    # T22: Complex multi-join with aggregation across industry
    def t22():
        uom = rand_uom()
        stmt = rand_stmt()
        sic_lo, sic_hi = rand_sic_range()
        return f"""
SELECT s.sic, t.tlabel, p.stmt,
       COUNT(DISTINCT s.cik) AS num_companies,
       SUM(n.value) AS total_value,
       AVG(n.value) AS avg_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN tag t ON n.tag = t.tag AND n.version = t.version
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = '{uom}' AND p.stmt = '{stmt}'
      AND s.sic BETWEEN {sic_lo} AND {sic_hi}
      AND n.value IS NOT NULL AND t.abstract = 0
GROUP BY s.sic, t.tlabel, p.stmt
HAVING COUNT(DISTINCT s.cik) >= 2
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t22, {"aggregation", "group_by", "having", "order_by", "distinct",
                            "uses:num", "uses:sub", "uses:tag", "uses:pre", "3plus_joins"}))

    # T23: Quarterly trend — value by quarter
    def t23():
        uom = rand_uom()
        tag_filter = random.choice([
            "Revenues", "NetIncomeLoss", "Assets", "Liabilities",
            "OperatingIncomeLoss", "CostOfRevenue",
        ])
        return f"""
SELECT s.fy, s.fp, COUNT(DISTINCT s.cik) AS num_companies,
       SUM(n.value) AS total_value, AVG(n.value) AS avg_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.tag = '{tag_filter}' AND n.uom = '{uom}'
      AND n.value IS NOT NULL AND s.fp IN ('Q1', 'Q2', 'Q3', 'Q4', 'FY')
GROUP BY s.fy, s.fp
ORDER BY s.fy, s.fp
"""
    templates.append((t23, {"aggregation", "group_by", "order_by", "distinct", "uses:num", "uses:sub", "1_join"}))

    # T24: Tag popularity — most used tags by number of filings
    def t24():
        lo, hi = rand_period_range()
        return f"""
SELECT n.tag, t.tlabel, t.datatype,
       COUNT(DISTINCT n.adsh) AS num_filings,
       COUNT(*) AS total_entries,
       SUM(CASE WHEN n.value > 0 THEN 1 ELSE 0 END) AS positive_count,
       SUM(CASE WHEN n.value < 0 THEN 1 ELSE 0 END) AS negative_count
FROM num n
JOIN tag t ON n.tag = t.tag AND n.version = t.version
WHERE n.ddate BETWEEN {lo} AND {hi} AND n.value IS NOT NULL
      AND t.custom = 0
GROUP BY n.tag, t.tlabel, t.datatype
HAVING COUNT(DISTINCT n.adsh) > 100
ORDER BY num_filings DESC
LIMIT {rand_limit()}
"""
    templates.append((t24, {"aggregation", "group_by", "having", "order_by", "distinct", "uses:num", "uses:tag", "1_join"}))

    # T25: Companies with multiple forms
    def t25():
        fy = rand_fy()
        country = rand_country()
        return f"""
SELECT s1.cik, s1.name, s1.countryba,
       COUNT(DISTINCT s1.form) AS form_types,
       COUNT(*) AS total_filings
FROM sub s1
WHERE s1.fy = {fy}
      AND s1.countryba = '{country}'
      AND s1.cik IN (
          SELECT cik FROM sub WHERE fy = {fy}
          GROUP BY cik HAVING COUNT(DISTINCT form) > 1
      )
GROUP BY s1.cik, s1.name, s1.countryba
ORDER BY total_filings DESC
LIMIT {rand_limit()}
"""
    templates.append((t25, {"aggregation", "group_by", "order_by", "distinct", "subquery", "uses:sub", "0_joins"}))

    # T26: Value distribution by tag and form
    def t26():
        uom = rand_uom()
        qtrs = rand_qtrs()
        return f"""
SELECT s.form, n.tag,
       COUNT(*) AS cnt,
       MIN(n.value) AS min_val,
       MAX(n.value) AS max_val,
       AVG(n.value) AS avg_val
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = '{uom}' AND n.qtrs = {qtrs}
      AND n.value IS NOT NULL AND n.value != 0
GROUP BY s.form, n.tag
HAVING COUNT(*) > 50
ORDER BY cnt DESC
LIMIT {rand_limit()}
"""
    templates.append((t26, {"aggregation", "group_by", "having", "order_by", "uses:num", "uses:sub", "1_join"}))

    # T27: Geographic distribution of filings with values
    def t27():
        uom = rand_uom()
        form = rand_form()
        return f"""
SELECT s.countryba, s.stprba,
       COUNT(DISTINCT s.adsh) AS num_filings,
       COUNT(DISTINCT s.cik) AS num_companies,
       SUM(n.value) AS total_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = '{uom}' AND s.form = '{form}'
      AND s.countryba IS NOT NULL AND n.value > 0
GROUP BY s.countryba, s.stprba
ORDER BY total_value DESC
LIMIT {rand_limit()}
"""
    templates.append((t27, {"aggregation", "group_by", "order_by", "distinct", "uses:num", "uses:sub", "1_join"}))

    # T28: NOT EXISTS — tags in num that have no presentation entry
    def t28():
        uom = rand_uom()
        lo, hi = rand_period_range()
        return f"""
SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total
FROM num n
WHERE n.uom = '{uom}' AND n.ddate BETWEEN {lo} AND {hi}
      AND n.value IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM pre p
          WHERE p.tag = n.tag AND p.version = n.version AND p.adsh = n.adsh
      )
GROUP BY n.tag, n.version
HAVING COUNT(*) > 10
ORDER BY cnt DESC
LIMIT {rand_limit()}
"""
    templates.append((t28, {"aggregation", "group_by", "having", "order_by", "subquery", "uses:num", "uses:pre", "0_joins"}))

    # T29: Self-join on sub — companies filing in multiple years
    def t29():
        form = rand_form()
        return f"""
SELECT s1.cik, s1.name,
       COUNT(DISTINCT s1.fy) AS years_filed,
       MIN(s1.fy) AS first_year, MAX(s1.fy) AS last_year,
       COUNT(*) AS total_filings
FROM sub s1
WHERE s1.form = '{form}' AND s1.fy IS NOT NULL
GROUP BY s1.cik, s1.name
HAVING COUNT(DISTINCT s1.fy) >= 2
ORDER BY total_filings DESC
LIMIT {rand_limit()}
"""
    templates.append((t29, {"aggregation", "group_by", "having", "order_by", "distinct", "uses:sub", "0_joins"}))

    # T30: CASE-based aggregation
    def t30():
        uom = rand_uom()
        fy = rand_fy()
        return f"""
SELECT s.sic,
       COUNT(*) AS total_entries,
       SUM(CASE WHEN n.value > 0 THEN n.value ELSE 0 END) AS positive_total,
       SUM(CASE WHEN n.value < 0 THEN n.value ELSE 0 END) AS negative_total,
       SUM(CASE WHEN n.value = 0 THEN 1 ELSE 0 END) AS zero_count,
       COUNT(DISTINCT s.cik) AS num_companies
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = '{uom}' AND s.fy = {fy}
      AND s.sic IS NOT NULL AND n.value IS NOT NULL
GROUP BY s.sic
HAVING COUNT(*) > 100
ORDER BY positive_total DESC
LIMIT {rand_limit()}
"""
    templates.append((t30, {"aggregation", "group_by", "having", "order_by", "distinct", "uses:num", "uses:sub", "1_join"}))

    return templates


def extract_features(sql: str, exec_time_ms: float, row_count: int, col_count: int) -> dict:
    """Extract structural features from a SQL query."""
    sql_upper = sql.upper()

    join_count = len(re.findall(r'\bJOIN\b', sql_upper))

    if exec_time_ms < 100:
        time_bucket = "fast"
    elif exec_time_ms < 1000:
        time_bucket = "medium"
    elif exec_time_ms < 10000:
        time_bucket = "slow"
    else:
        time_bucket = "very_slow"

    if join_count == 0:
        join_bucket = "0_joins"
    elif join_count == 1:
        join_bucket = "1_join"
    elif join_count == 2:
        join_bucket = "2_joins"
    else:
        join_bucket = "3plus_joins"

    tables = set()
    for t in ["sub", "num", "tag", "pre"]:
        if re.search(r'\b' + t + r'\b', sql, re.IGNORECASE):
            tables.add(t)

    features = {
        "join_count": join_count,
        "join_bucket": join_bucket,
        "has_group_by": bool(re.search(r'\bGROUP\s+BY\b', sql_upper)),
        "has_subquery": sql_upper.count("SELECT") > 1,
        "has_order_by": bool(re.search(r'\bORDER\s+BY\b', sql_upper)),
        "has_having": bool(re.search(r'\bHAVING\b', sql_upper)),
        "has_distinct": bool(re.search(r'\bDISTINCT\b', sql_upper)),
        "has_aggregation": bool(re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', sql_upper)),
        "num_tables": len(tables),
        "tables": tables,
        "time_bucket": time_bucket,
        "exec_time_ms": exec_time_ms,
        "row_count": row_count,
        "col_count": col_count,
    }

    feature_set = set()
    feature_set.add(f"time:{time_bucket}")
    feature_set.add(f"joins:{join_bucket}")
    feature_set.add(f"tables:{len(tables)}")
    if features["has_group_by"]:
        feature_set.add("group_by")
    if features["has_subquery"]:
        feature_set.add("subquery")
    if features["has_order_by"]:
        feature_set.add("order_by")
    if features["has_having"]:
        feature_set.add("having")
    if features["has_distinct"]:
        feature_set.add("distinct")
    if features["has_aggregation"]:
        feature_set.add("aggregation")
    for t in tables:
        feature_set.add(f"uses:{t}")

    features["feature_set"] = feature_set
    return features


def greedy_diversity_sample(candidates: list, num_select: int) -> list:
    """Select queries maximizing feature diversity via greedy set-cover."""
    if len(candidates) <= num_select:
        return list(range(len(candidates)))

    selected = []
    covered = set()
    remaining = set(range(len(candidates)))

    while len(selected) < num_select and remaining:
        best_idx = None
        best_new = -1
        best_time = float("inf")

        for idx in remaining:
            _, feat = candidates[idx]
            new_features = feat["feature_set"] - covered
            n_new = len(new_features)
            if n_new > best_new or (n_new == best_new and
                                     abs(feat["exec_time_ms"] - 500) < abs(best_time - 500)):
                best_idx = idx
                best_new = n_new
                best_time = feat["exec_time_ms"]

        if best_idx is not None:
            selected.append(best_idx)
            _, feat = candidates[best_idx]
            covered.update(feat["feature_set"])
            remaining.discard(best_idx)
        else:
            break

    return selected


def main():
    parser = argparse.ArgumentParser(description="Generate SEC EDGAR benchmark queries")
    parser.add_argument("--num-generate", type=int, default=2000,
                        help="Number of random queries to generate (default: 2000)")
    parser.add_argument("--num-select", type=int, default=25,
                        help="Number of queries to select (default: 25)")
    parser.add_argument("--query-timeout", type=int, default=60,
                        help="Per-query execution timeout in seconds (default: 60)")
    parser.add_argument("--db-path", type=Path, default=None,
                        help="Path to DuckDB database")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    db_path = args.db_path or (script_dir / "duckdb" / "sec_edgar.duckdb")

    if not db_path.exists():
        print(f"Error: database not found at {db_path}")
        print("Run: python3 benchmarks/sec-edgar/load_data.py")
        sys.exit(1)

    random.seed(args.seed)

    print(f"Database: {db_path}")
    print(f"Generating {args.num_generate} random queries...")
    print(f"Selecting {args.num_select} diverse queries...")
    print(f"Random seed: {args.seed}")
    print()

    # Generate queries from templates
    templates = generate_templates()
    print(f"  {len(templates)} template types available")

    raw_queries = []
    for _ in range(args.num_generate):
        template_fn, _ = random.choice(templates)
        sql = template_fn().strip()
        raw_queries.append(sql)

    print(f"  Generated {len(raw_queries)} raw queries")

    # Deduplicate
    seen = set()
    unique_queries = []
    for sql in raw_queries:
        normalized = " ".join(sql.split())
        if normalized not in seen:
            seen.add(normalized)
            unique_queries.append(sql)
    print(f"  {len(unique_queries)} unique queries after dedup")

    # Filter and evaluate queries
    print(f"\nFiltering queries (timeout={args.query_timeout}s per query)...")
    candidates = []
    errors = 0
    timeouts = 0
    empty = 0
    too_large = 0

    con = duckdb.connect(str(db_path), read_only=True)

    for i, sql in enumerate(unique_queries):
        if (i + 1) % 200 == 0:
            print(f"  Evaluated {i + 1}/{len(unique_queries)} "
                  f"(valid: {len(candidates)}, errors: {errors}, "
                  f"empty: {empty}, too_large: {too_large})")

        try:
            start = time.perf_counter()
            result = con.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            elapsed_ms = (time.perf_counter() - start) * 1000

            if elapsed_ms > args.query_timeout * 1000:
                timeouts += 1
                continue

            row_count = len(rows)
            col_count = len(columns)

            if row_count == 0:
                empty += 1
                continue
            if row_count > 100000:
                too_large += 1
                continue

            features = extract_features(sql, elapsed_ms, row_count, col_count)
            candidates.append((sql, features))

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            break
        except Exception:
            errors += 1
            continue

    con.close()

    print(f"\nFiltering complete:")
    print(f"  Valid candidates: {len(candidates)}")
    print(f"  Errors: {errors}")
    print(f"  Timeouts: {timeouts}")
    print(f"  Empty results: {empty}")
    print(f"  Too many rows (>100K): {too_large}")

    if len(candidates) == 0:
        print("Error: no valid queries found!")
        sys.exit(1)

    # Diversity-based sampling
    print(f"\nSelecting {args.num_select} diverse queries...")
    selected_indices = greedy_diversity_sample(candidates, args.num_select)

    # Sort selected by execution time for consistent ordering
    selected = [(candidates[i][0], candidates[i][1]) for i in selected_indices]
    selected.sort(key=lambda x: x[1]["exec_time_ms"])

    # Print feature coverage summary
    all_covered = set()
    for sql, feat in selected:
        all_covered.update(feat["feature_set"])
    print(f"  Feature coverage: {len(all_covered)} unique features")

    # Print selected query summary
    print(f"\n{'Q#':<5} {'Joins':<6} {'Agg':<5} {'Sub':<5} {'Tables':<8} "
          f"{'Rows':<8} {'Time(ms)':<10}")
    print("-" * 60)
    for i, (sql, feat) in enumerate(selected, 1):
        print(f"Q{i:<4} {feat['join_count']:<6} "
              f"{'Y' if feat['has_aggregation'] else 'N':<5} "
              f"{'Y' if feat['has_subquery'] else 'N':<5} "
              f"{feat['num_tables']:<8} "
              f"{feat['row_count']:<8} "
              f"{feat['exec_time_ms']:<10.1f}")

    # Write queries to queries.sql
    queries_path = script_dir / "queries.sql"
    with open(queries_path, "w") as f:
        f.write("-- SEC EDGAR Benchmark Queries\n")
        f.write(f"-- Generated by generate_queries.py (template-based random generation)\n")
        f.write(f"-- {len(selected)} queries selected from {len(unique_queries)} generated\n\n")
        for i, (sql, feat) in enumerate(selected, 1):
            f.write(f"-- Q{i}: joins={feat['join_count']}, "
                    f"agg={feat['has_aggregation']}, "
                    f"sub={feat['has_subquery']}, "
                    f"tables={feat['num_tables']}, "
                    f"rows={feat['row_count']}, "
                    f"time={feat['exec_time_ms']:.1f}ms\n")
            # Clean up whitespace
            clean_sql = "\n".join(line for line in sql.strip().split("\n"))
            f.write(clean_sql + ";\n\n")

    print(f"\nQueries written to: {queries_path}")
    print("Done.")


if __name__ == "__main__":
    main()
