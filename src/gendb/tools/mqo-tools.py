#!/usr/bin/env python3
"""
MQO Tool Layer — SQLGlot-backed structural analysis for Multiple Query Optimization.

This is the Python CLI that GenDB's MQO agents call to get deterministic
structural facts about a query batch. Semantic equivalence is NEVER attempted
here — the LLM does semantic reasoning; this script provides lookups and
canonical *structural* forms.

Usage (CLI — all subcommands take --queries-file, a SQL file with queries
separated by comment lines starting with '--' and containing a query ID):

  mqo-tools.py list-queries --queries-file queries.sql
      → JSON array [{qid, template_sql, tables, join_edges, group_keys, filters}]

  mqo-tools.py get-query --queries-file queries.sql --qid Q3
      → {qid, sql}

  mqo-tools.py canonical-signature --queries-file queries.sql --qid Q3 --scope filter
      → {qid, scope, signature}

  mqo-tools.py find-queries-touching --queries-file queries.sql --table lineitem
      → {"table": "lineitem", "queries": ["Q1","Q3","Q6",...]}

  mqo-tools.py find-queries-with-join --queries-file queries.sql --table-a customer --table-b orders
      → {"edge": ["customer","orders"], "queries": ["Q3","Q5","Q10"]}

  mqo-tools.py predicate-overlap --queries-file queries.sql --qid-a Q1 --qid-b Q3 --table lineitem --column l_shipdate
      → {"overlap": "range" | "set" | "none", "details": {...}}

  mqo-tools.py agg-signature --queries-file queries.sql --qid Q3
      → {"qid": "Q3", "group_by": [...], "aggs": [...], "signature": "..."}

  mqo-tools.py registry-read --registry-path <path>
      → {...} contents of the registry JSON
  mqo-tools.py registry-add-candidate --registry-path <path> --spec-json '<json>'
      → {"ok": true, "component_id": "..."}
  mqo-tools.py registry-update-consumers --registry-path <path> --component-id <id> --qids Q1,Q2,Q3
      → {"ok": true}

The registry is just a JSON file of shape:
  { "candidates": [ {component_id, kind, canonical_signature, source_tables,
                     consumers, rationale, notes}, ... ] }

All commands write JSON to stdout. Exit code 0 on success.
"""

import argparse
import json
import re
import sys
import hashlib
from pathlib import Path

try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    print(json.dumps({"error": "sqlglot not installed"}), file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Query file parsing (same format as src/gendb/shared.mjs parseQueryFile)
# Queries are separated by `--` comment lines; a comment of form
# "-- Q<id>: ..." or "-- Query <id>" marks a new query.
# ---------------------------------------------------------------------------

QID_RE = re.compile(r"^--\s*(?:Query\s+)?([Qq]?\w+)[:.\s]", re.IGNORECASE)


def parse_query_file(path: Path):
    """Parse a SQL batch file → list of (qid, sql)."""
    text = path.read_text()
    lines = text.splitlines()
    queries = []
    current_id = None
    current_sql_lines = []
    counter = 0

    def flush():
        nonlocal current_id, current_sql_lines, counter
        sql = "\n".join(current_sql_lines).strip()
        if sql:
            qid = current_id or f"Q{counter}"
            queries.append((qid, sql))
            counter += 1
        current_id = None
        current_sql_lines = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current_sql_lines:
                current_sql_lines.append(line)
            continue
        if stripped.startswith("--"):
            # Header comment — check if it's a query ID marker
            m = QID_RE.match(stripped)
            if m:
                flush()
                current_id = m.group(1).upper()
                if not current_id.startswith("Q"):
                    current_id = "Q" + current_id
            # plain comments (inside a query body) stay attached
            continue
        current_sql_lines.append(line)

    flush()
    return queries


# ---------------------------------------------------------------------------
# SQLGlot parsing helpers
# ---------------------------------------------------------------------------

def safe_parse(sql: str):
    try:
        parsed = sqlglot.parse(sql)
        if parsed and parsed[0]:
            return parsed[0]
    except Exception:
        pass
    # Try fallback dialects
    for dialect in ["postgres", "duckdb", "mysql"]:
        try:
            parsed = sqlglot.parse(sql, read=dialect)
            if parsed and parsed[0]:
                return parsed[0]
        except Exception:
            continue
    return None


def extract_tables(ast):
    tables = set()
    if ast is None:
        return []
    for t in ast.find_all(exp.Table):
        name = t.name
        if name:
            tables.add(name.lower())
    return sorted(tables)


def extract_join_edges(ast):
    """Return list of (tA, tB) normalized pairs with tA < tB.

    Handles:
      - Explicit JOIN ... ON with aliased columns
      - Implicit comma-joins whose join predicate lives in WHERE
      - TPC-H-style unaliased columns (c_custkey = o_custkey): when a column
        has no explicit table qualifier, infer its table from the FROM list
        via column-name prefix matching (c_* → customer, l_* → lineitem, etc.)
    """
    edges = set()
    if ast is None:
        return []
    for select in ast.find_all(exp.Select):
        # Gather all tables in scope of this SELECT
        tables_in_select = set()
        from_clause = select.args.get("from")
        if from_clause:
            for t in from_clause.find_all(exp.Table):
                if t.name:
                    tables_in_select.add(t.name.lower())
        for j in select.find_all(exp.Join):
            jt = j.find(exp.Table)
            if jt and jt.name:
                tables_in_select.add(jt.name.lower())

        def resolve_column_table(col):
            """Try explicit alias first; fall back to column-name prefix."""
            if not isinstance(col, exp.Column):
                return None
            if col.table:
                return col.table.lower()
            cname = (col.name or "").lower()
            if not cname:
                return None
            # Prefix match: e.g., "c_custkey" matches "customer"
            candidates = [t for t in tables_in_select if t and t.startswith(cname.split("_")[0])]
            if len(candidates) == 1:
                return candidates[0]
            return None

        def record_eqs(scope_node):
            if scope_node is None:
                return
            for eq in scope_node.find_all(exp.EQ):
                l, r = eq.args.get("this"), eq.args.get("expression")
                lt = resolve_column_table(l)
                rt = resolve_column_table(r)
                if lt and rt and lt != rt and lt in tables_in_select and rt in tables_in_select:
                    a, b = sorted([lt, rt])
                    edges.add((a, b))

        # Explicit JOIN ... ON
        for j in select.find_all(exp.Join):
            record_eqs(j.args.get("on"))
        # Implicit join predicates in WHERE
        record_eqs(select.args.get("where"))

    return [list(e) for e in sorted(edges)]


def _column_table(node):
    if isinstance(node, exp.Column):
        t = node.table
        return t.lower() if t else None
    return None


def extract_group_keys(ast):
    keys = []
    if ast is None:
        return []
    for g in ast.find_all(exp.Group):
        for e in g.expressions or []:
            keys.append(e.sql())
    return keys


def extract_filters(ast):
    """Return a list of {column, op, value, table} for WHERE predicates (literal comparisons).

    If a column has no explicit table qualifier AND the query has exactly one
    table in its FROM clause, the filter's table is defaulted to that single
    table — without this fallback, unaliased TPC-H queries (Q1, Q6, etc.)
    would lose predicate-overlap information.
    """
    filters = []
    if ast is None:
        return []
    tables = extract_tables(ast)
    default_table = tables[0] if len(tables) == 1 else None
    for where in ast.find_all(exp.Where):
        _walk_predicate(where.this, filters)
    if default_table:
        for f in filters:
            if not f.get("table"):
                f["table"] = default_table
    return filters


_OP_CLASSES = {
    exp.EQ: "=", exp.LT: "<", exp.GT: ">", exp.LTE: "<=", exp.GTE: ">=", exp.NEQ: "!=",
}


def _walk_predicate(node, out):
    if node is None:
        return
    if isinstance(node, (exp.And, exp.Or)):
        _walk_predicate(node.args.get("this"), out)
        _walk_predicate(node.args.get("expression"), out)
        return
    if isinstance(node, exp.Paren):
        _walk_predicate(node.args.get("this"), out)
        return
    if isinstance(node, exp.Between):
        col = node.args.get("this")
        low = node.args.get("low")
        high = node.args.get("high")
        if isinstance(col, exp.Column):
            out.append({
                "table": (col.table or "").lower() or None,
                "column": col.name.lower() if col.name else None,
                "op": "BETWEEN",
                "value": [_literal_value(low), _literal_value(high)],
            })
        return
    if isinstance(node, exp.In):
        col = node.args.get("this")
        if isinstance(col, exp.Column):
            vals = [_literal_value(e) for e in (node.args.get("expressions") or [])]
            out.append({
                "table": (col.table or "").lower() or None,
                "column": col.name.lower() if col.name else None,
                "op": "IN",
                "value": vals,
            })
        return
    for cls, op in _OP_CLASSES.items():
        if isinstance(node, cls):
            l = node.args.get("this")
            r = node.args.get("expression")
            if isinstance(l, exp.Column):
                out.append({
                    "table": (l.table or "").lower() or None,
                    "column": l.name.lower() if l.name else None,
                    "op": op,
                    "value": _literal_value(r),
                })
            elif isinstance(r, exp.Column):
                out.append({
                    "table": (r.table or "").lower() or None,
                    "column": r.name.lower() if r.name else None,
                    "op": _flip_op(op),
                    "value": _literal_value(l),
                })
            return
    if isinstance(node, exp.Like):
        col = node.args.get("this")
        if isinstance(col, exp.Column):
            out.append({
                "table": (col.table or "").lower() or None,
                "column": col.name.lower() if col.name else None,
                "op": "LIKE",
                "value": _literal_value(node.args.get("expression")),
            })
        return


_FLIP = {"<": ">", ">": "<", "<=": ">=", ">=": "<="}
def _flip_op(op): return _FLIP.get(op, op)


def _literal_value(node):
    if node is None:
        return None
    if isinstance(node, exp.Literal):
        return node.args.get("this")
    if isinstance(node, exp.Paren):
        return _literal_value(node.args.get("this"))
    if isinstance(node, exp.Cast):
        return _literal_value(node.args.get("this"))
    if isinstance(node, exp.DateStrToDate):
        return _literal_value(node.args.get("this"))
    return node.sql()


def extract_agg_signature(ast):
    """Return normalized agg signature: group_by columns + aggregate calls."""
    if ast is None:
        return {"group_by": [], "aggs": [], "signature": ""}
    group_by = extract_group_keys(ast)
    aggs = []
    for a in ast.find_all(exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max):
        aggs.append(a.sql().lower())
    sig_raw = "GB:" + "|".join(sorted(group_by)) + ";AGG:" + "|".join(sorted(aggs))
    return {
        "group_by": group_by,
        "aggs": sorted(aggs),
        "signature": sig_raw,
    }


def canonical_signature_for_scope(ast, scope: str):
    """Return a canonical string signature for a given scope of the query."""
    if ast is None:
        return ""
    if scope == "filter":
        filters = extract_filters(ast)
        parts = [f"{f.get('table')}.{f.get('column')}:{f.get('op')}:{f.get('value')}" for f in filters]
        return "FILTER(" + ",".join(sorted(parts)) + ")"
    if scope == "agg":
        return extract_agg_signature(ast)["signature"]
    if scope.startswith("scan:"):
        table = scope.split(":", 1)[1]
        # include any WHERE predicates on that table
        filters = [f for f in extract_filters(ast) if (f.get("table") or "") == table.lower()]
        parts = [f"{f.get('column')}:{f.get('op')}:{f.get('value')}" for f in filters]
        return f"SCAN({table})[" + ",".join(sorted(parts)) + "]"
    if scope.startswith("join:"):
        # Expected scope: "join:tA,tB"
        pair = scope.split(":", 1)[1]
        return f"JOIN({pair})"
    return ""


# ---------------------------------------------------------------------------
# Registry helpers (file-backed JSON)
# ---------------------------------------------------------------------------

def _registry_load(path: Path):
    if not path.exists():
        return {"candidates": []}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {"candidates": []}


def _registry_save(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def _component_id_from_signature(sig: str):
    h = hashlib.sha1(sig.encode()).hexdigest()[:8]
    return f"c_{h}"


# ---------------------------------------------------------------------------
# Predicate overlap (range / set / none)
# ---------------------------------------------------------------------------

def predicate_overlap(filters_a, filters_b, table: str, column: str):
    table = (table or "").lower()
    column = (column or "").lower()
    a = [f for f in filters_a if (f.get("table") or "") == table and (f.get("column") or "") == column]
    b = [f for f in filters_b if (f.get("table") or "") == table and (f.get("column") or "") == column]
    if not a and not b:
        return {"overlap": "none", "details": "neither query constrains this column"}
    if not a or not b:
        return {"overlap": "partial", "details": "only one query constrains this column"}

    def range_of(fs):
        lo, hi = None, None
        for f in fs:
            op = f.get("op")
            v = f.get("value")
            if op == "BETWEEN" and isinstance(v, list) and len(v) == 2:
                lo = v[0]; hi = v[1]
            elif op == ">=": lo = v if lo is None else max(lo, v)
            elif op == ">":  lo = v if lo is None else max(lo, v)
            elif op == "<=": hi = v if hi is None else min(hi, v)
            elif op == "<":  hi = v if hi is None else min(hi, v)
        return lo, hi

    loA, hiA = range_of(a)
    loB, hiB = range_of(b)
    if (loA is not None or hiA is not None) and (loB is not None or hiB is not None):
        # naive overlap test (string-compatible for dates and numbers alike)
        try:
            overlaps = (loA is None or hiB is None or str(loA) <= str(hiB)) and \
                       (loB is None or hiA is None or str(loB) <= str(hiA))
        except Exception:
            overlaps = True
        return {
            "overlap": "range" if overlaps else "none",
            "details": {"a": {"lo": loA, "hi": hiA}, "b": {"lo": loB, "hi": hiB}},
        }

    # Set overlap (IN / = / LIKE)
    def values_of(fs):
        vals = set()
        for f in fs:
            op = f.get("op")
            v = f.get("value")
            if op == "IN" and isinstance(v, list):
                vals.update(str(x) for x in v)
            elif op == "=":
                vals.add(str(v))
        return vals

    vA = values_of(a); vB = values_of(b)
    inter = vA & vB
    if vA and vB:
        return {
            "overlap": "set" if inter else "none",
            "details": {"intersection_size": len(inter), "a_size": len(vA), "b_size": len(vB)},
        }
    return {"overlap": "partial", "details": "mixed operators"}


# ---------------------------------------------------------------------------
# CLI dispatch
# ---------------------------------------------------------------------------

def _load_batch(args):
    qfile = Path(args.queries_file)
    queries = parse_query_file(qfile)
    return queries


def _query_summary(qid: str, sql: str):
    ast = safe_parse(sql)
    return {
        "qid": qid,
        "template_sql": sql[:200] + ("..." if len(sql) > 200 else ""),
        "tables": extract_tables(ast),
        "join_edges": extract_join_edges(ast),
        "group_keys": extract_group_keys(ast),
        "filters": extract_filters(ast),
    }


def cmd_list_queries(args):
    queries = _load_batch(args)
    out = [_query_summary(qid, sql) for qid, sql in queries]
    print(json.dumps(out, indent=2, default=str))


def cmd_get_query(args):
    queries = _load_batch(args)
    for qid, sql in queries:
        if qid.upper() == args.qid.upper():
            print(json.dumps({"qid": qid, "sql": sql}))
            return
    print(json.dumps({"error": f"query not found: {args.qid}"}))
    sys.exit(2)


def cmd_canonical_signature(args):
    queries = _load_batch(args)
    for qid, sql in queries:
        if qid.upper() == args.qid.upper():
            ast = safe_parse(sql)
            sig = canonical_signature_for_scope(ast, args.scope)
            print(json.dumps({"qid": qid, "scope": args.scope, "signature": sig}, default=str))
            return
    print(json.dumps({"error": f"query not found: {args.qid}"}))
    sys.exit(2)


def cmd_find_queries_touching(args):
    queries = _load_batch(args)
    target = args.table.lower()
    hits = []
    for qid, sql in queries:
        ast = safe_parse(sql)
        if target in extract_tables(ast):
            hits.append(qid)
    print(json.dumps({"table": target, "queries": hits}))


def cmd_find_queries_with_join(args):
    queries = _load_batch(args)
    tA, tB = sorted([args.table_a.lower(), args.table_b.lower()])
    hits = []
    for qid, sql in queries:
        ast = safe_parse(sql)
        edges = extract_join_edges(ast)
        if [tA, tB] in edges:
            hits.append(qid)
    print(json.dumps({"edge": [tA, tB], "queries": hits}))


def cmd_predicate_overlap(args):
    queries = _load_batch(args)
    sql_a = sql_b = None
    for qid, sql in queries:
        if qid.upper() == args.qid_a.upper(): sql_a = sql
        if qid.upper() == args.qid_b.upper(): sql_b = sql
    if sql_a is None or sql_b is None:
        print(json.dumps({"error": "query not found"}))
        sys.exit(2)
    fa = extract_filters(safe_parse(sql_a))
    fb = extract_filters(safe_parse(sql_b))
    result = predicate_overlap(fa, fb, args.table, args.column)
    print(json.dumps({
        "qid_a": args.qid_a, "qid_b": args.qid_b,
        "table": args.table, "column": args.column,
        **result,
    }, default=str))


def cmd_agg_signature(args):
    queries = _load_batch(args)
    for qid, sql in queries:
        if qid.upper() == args.qid.upper():
            sig = extract_agg_signature(safe_parse(sql))
            print(json.dumps({"qid": qid, **sig}, default=str))
            return
    print(json.dumps({"error": f"query not found: {args.qid}"}))
    sys.exit(2)


def cmd_registry_read(args):
    data = _registry_load(Path(args.registry_path))
    print(json.dumps(data, indent=2, default=str))


def cmd_registry_add_candidate(args):
    path = Path(args.registry_path)
    data = _registry_load(path)
    spec = json.loads(args.spec_json)
    sig = spec.get("canonical_signature") or ""
    if not spec.get("component_id"):
        spec["component_id"] = _component_id_from_signature(sig or json.dumps(spec, sort_keys=True))
    # Dedup by component_id (append if new, union consumers if exists)
    existing = next((c for c in data["candidates"] if c.get("component_id") == spec["component_id"]), None)
    if existing:
        cons = set(existing.get("consumers", [])) | set(spec.get("consumers", []))
        existing["consumers"] = sorted(cons)
        for k, v in spec.items():
            if k not in ("component_id", "consumers"):
                existing.setdefault(k, v)
    else:
        data["candidates"].append(spec)
    _registry_save(path, data)
    print(json.dumps({"ok": True, "component_id": spec["component_id"]}))


def cmd_registry_update_consumers(args):
    path = Path(args.registry_path)
    data = _registry_load(path)
    qids = [q.strip() for q in args.qids.split(",") if q.strip()]
    for c in data["candidates"]:
        if c.get("component_id") == args.component_id:
            cons = set(c.get("consumers", [])) | set(qids)
            c["consumers"] = sorted(cons)
            _registry_save(path, data)
            print(json.dumps({"ok": True}))
            return
    print(json.dumps({"ok": False, "error": f"component_id not found: {args.component_id}"}))
    sys.exit(2)


def main():
    p = argparse.ArgumentParser(description="MQO tool layer")
    sub = p.add_subparsers(dest="cmd", required=True)

    def qf(sp):
        sp.add_argument("--queries-file", required=True)

    sp = sub.add_parser("list-queries"); qf(sp); sp.set_defaults(func=cmd_list_queries)
    sp = sub.add_parser("get-query"); qf(sp); sp.add_argument("--qid", required=True); sp.set_defaults(func=cmd_get_query)
    sp = sub.add_parser("canonical-signature"); qf(sp); sp.add_argument("--qid", required=True); sp.add_argument("--scope", required=True); sp.set_defaults(func=cmd_canonical_signature)
    sp = sub.add_parser("find-queries-touching"); qf(sp); sp.add_argument("--table", required=True); sp.set_defaults(func=cmd_find_queries_touching)
    sp = sub.add_parser("find-queries-with-join"); qf(sp); sp.add_argument("--table-a", required=True); sp.add_argument("--table-b", required=True); sp.set_defaults(func=cmd_find_queries_with_join)
    sp = sub.add_parser("predicate-overlap"); qf(sp); sp.add_argument("--qid-a", required=True); sp.add_argument("--qid-b", required=True); sp.add_argument("--table", required=True); sp.add_argument("--column", required=True); sp.set_defaults(func=cmd_predicate_overlap)
    sp = sub.add_parser("agg-signature"); qf(sp); sp.add_argument("--qid", required=True); sp.set_defaults(func=cmd_agg_signature)

    sp = sub.add_parser("registry-read"); sp.add_argument("--registry-path", required=True); sp.set_defaults(func=cmd_registry_read)
    sp = sub.add_parser("registry-add-candidate"); sp.add_argument("--registry-path", required=True); sp.add_argument("--spec-json", required=True); sp.set_defaults(func=cmd_registry_add_candidate)
    sp = sub.add_parser("registry-update-consumers"); sp.add_argument("--registry-path", required=True); sp.add_argument("--component-id", required=True); sp.add_argument("--qids", required=True); sp.set_defaults(func=cmd_registry_update_consumers)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
