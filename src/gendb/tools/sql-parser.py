#!/usr/bin/env python3
"""
SQL structural feature extractor using SQLGlot.

Parses SQL and extracts structural features for memory system L1 template matching.
Can be used as CLI tool (by programs or LLM agents) or imported as a Python module.

Usage:
  echo "SELECT * FROM t" | python3 sql-parser.py
  python3 sql-parser.py --sql "SELECT * FROM t"
  python3 sql-parser.py --file query.sql
"""

import sys
import json
import argparse

import sqlglot
from sqlglot import exp


def extract_structural_features(sql: str) -> dict:
    """Extract structural features from a SQL query using SQLGlot AST."""
    features = {
        "join_count": 0,
        "join_types": [],
        "has_aggregation": False,
        "aggregate_functions": [],
        "has_group_by": False,
        "has_order_by": False,
        "has_subquery": False,
        "has_like": False,
        "has_in": False,
        "has_between": False,
        "has_case": False,
        "has_distinct": False,
        "has_cte": False,
        "has_window_function": False,
        "has_union": False,
        "has_limit": False,
        "has_having": False,
        "filter_types": [],
        "tables": [],
    }

    try:
        parsed = sqlglot.parse(sql)
    except sqlglot.errors.ParseError:
        # Fallback: try different dialects
        for dialect in ["postgres", "mysql", "duckdb"]:
            try:
                parsed = sqlglot.parse(sql, read=dialect)
                break
            except sqlglot.errors.ParseError:
                continue
        else:
            return features  # Unparseable SQL, return defaults

    if not parsed:
        return features

    ast = parsed[0]
    if ast is None:
        return features

    # --- Tables ---
    tables = set()
    for table in ast.find_all(exp.Table):
        name = table.name
        if name:
            tables.add(name.lower())
    features["tables"] = sorted(tables)

    # --- Joins ---
    joins = list(ast.find_all(exp.Join))
    features["join_count"] = len(joins)
    for join in joins:
        join_type = "INNER"  # default
        if join.args.get("side"):
            join_type = join.args["side"].upper()
        elif join.args.get("kind"):
            join_type = join.args["kind"].upper()
        features["join_types"].append(join_type)

    # Count implicit comma-joins: FROM a, b, c = 2 implicit joins
    for select in ast.find_all(exp.Select):
        from_clause = select.args.get("from")
        if from_clause:
            # The FROM clause has one expression; additional tables come from joins
            # But implicit joins (comma-separated) appear as multiple Table expressions
            # within a single FROM clause via the "expressions" of the From node
            pass

        # Check for implicit joins in FROM via comma-separated tables
        # SQLGlot represents "FROM a, b, c" with Join nodes of kind ""
        # But we need to check for cross joins that are implicit
        # Actually, SQLGlot may parse "FROM a, b" as a cross join
        # Let's count tables in FROM that aren't in explicit JOIN
        joined_tables = set()
        for join in select.find_all(exp.Join):
            join_table = join.find(exp.Table)
            if join_table:
                joined_tables.add(join_table.name.lower() if join_table.name else "")

        from_clause = select.args.get("from")
        if from_clause:
            from_tables = []
            for table in from_clause.find_all(exp.Table):
                name = table.name
                if name and name.lower() not in joined_tables:
                    from_tables.append(name.lower())
            # Implicit comma-joins: N tables in FROM = N-1 implicit joins
            if len(from_tables) > 1:
                implicit = len(from_tables) - 1
                features["join_count"] += implicit

    # --- Aggregation ---
    agg_types = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)
    agg_names = {"Sum": "SUM", "Count": "COUNT", "Avg": "AVG", "Min": "MIN", "Max": "MAX"}
    seen_aggs = set()
    for agg in ast.find_all(*agg_types):
        agg_name = agg_names.get(type(agg).__name__, type(agg).__name__.upper())
        seen_aggs.add(agg_name)
    if seen_aggs:
        features["has_aggregation"] = True
        features["aggregate_functions"] = sorted(seen_aggs)

    # --- GROUP BY ---
    features["has_group_by"] = bool(ast.find(exp.Group))

    # --- ORDER BY ---
    features["has_order_by"] = bool(ast.find(exp.Order))

    # --- Subqueries ---
    # Count SELECT statements; more than 1 = has subquery
    selects = list(ast.find_all(exp.Select))
    features["has_subquery"] = len(selects) > 1

    # --- LIKE ---
    features["has_like"] = bool(ast.find(exp.Like))

    # --- IN ---
    features["has_in"] = bool(ast.find(exp.In))

    # --- BETWEEN ---
    features["has_between"] = bool(ast.find(exp.Between))

    # --- CASE ---
    features["has_case"] = bool(ast.find(exp.Case))

    # --- DISTINCT ---
    features["has_distinct"] = bool(ast.find(exp.Distinct))

    # --- CTE ---
    features["has_cte"] = bool(ast.find(exp.CTE))

    # --- Window functions ---
    features["has_window_function"] = bool(ast.find(exp.Window))

    # --- UNION/INTERSECT/EXCEPT ---
    features["has_union"] = bool(ast.find(exp.Union)) or bool(ast.find(exp.Intersect)) or bool(ast.find(exp.Except))

    # --- LIMIT ---
    features["has_limit"] = bool(ast.find(exp.Limit))

    # --- HAVING ---
    features["has_having"] = bool(ast.find(exp.Having))

    # --- Filter types ---
    filter_types = set()
    if features["has_like"]:
        filter_types.add("LIKE")
    if features["has_in"]:
        filter_types.add("IN")
    if features["has_between"]:
        filter_types.add("BETWEEN")
    # Check for comparison operators (=, <, >, <=, >=, !=)
    comparison_types = (exp.EQ, exp.LT, exp.GT, exp.LTE, exp.GTE, exp.NEQ)
    if any(ast.find(ct) for ct in comparison_types):
        filter_types.add("comparison")
    features["filter_types"] = sorted(filter_types)

    return features


# ---------------------------------------------------------------------------
# Template extraction: replace predicate value expressions with named placeholders
# ---------------------------------------------------------------------------

import re as _re


def _in_predicate_clause(node):
    """Check if a node is inside WHERE, HAVING, or LIMIT (not SELECT/JOIN ON)."""
    parent = node.parent
    while parent:
        if isinstance(parent, exp.Join):
            return False
        if isinstance(parent, (exp.Where, exp.Having, exp.Limit)):
            return True
        parent = parent.parent
    return False


def _contains_column(node):
    """Check if an expression tree contains any Column references."""
    if isinstance(node, exp.Column):
        return True
    for child in node.args.values():
        if isinstance(child, exp.Expression) and _contains_column(child):
            return True
    return False


def _eval_literal_expr(node):
    """Try to evaluate a constant arithmetic expression to a numeric value."""
    if isinstance(node, exp.Literal):
        val = str(node.args.get("this", ""))
        try:
            return float(val)
        except ValueError:
            return None
    if isinstance(node, exp.Paren):
        inner = node.args.get("this")
        if inner:
            return _eval_literal_expr(inner)
    if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
        left = _eval_literal_expr(node.args.get("this"))
        right = _eval_literal_expr(node.args.get("expression"))
        if left is not None and right is not None:
            if isinstance(node, exp.Add): return left + right
            if isinstance(node, exp.Sub): return left - right
            if isinstance(node, exp.Mul): return left * right
            if isinstance(node, exp.Div) and right != 0: return left / right
    if isinstance(node, exp.Neg):
        inner = _eval_literal_expr(node.args.get("this"))
        if inner is not None:
            return -inner
    return None


def _get_date_literal(node):
    """Extract date string from DATE '...' expressions and CAST(... AS DATE)."""
    if isinstance(node, exp.DateStrToDate):
        inner = node.args.get("this")
        if isinstance(inner, exp.Literal):
            return str(inner.args.get("this", ""))
    if isinstance(node, exp.Cast):
        to_type = node.args.get("to")
        if to_type and "date" in str(to_type).lower():
            inner = node.args.get("this")
            if isinstance(inner, exp.Literal):
                return str(inner.args.get("this", ""))
    if isinstance(node, exp.Literal) and node.is_string:
        val = str(node.args.get("this", ""))
        if _re.match(r'^\d{4}-\d{2}-\d{2}$', val):
            return val
    return None


def _is_date_interval_expr(node):
    """Check if expression is DATE + INTERVAL or DATE - INTERVAL."""
    if isinstance(node, (exp.Add, exp.Sub)):
        for child in [node.args.get("this"), node.args.get("expression")]:
            if isinstance(child, exp.Interval):
                return True
    # TsOrDsAdd, DateAdd, etc.
    if hasattr(exp, 'TsOrDsAdd') and isinstance(node, exp.TsOrDsAdd):
        return True
    if hasattr(exp, 'DateAdd') and isinstance(node, exp.DateAdd):
        return True
    return False


def _make_param_name(col_name, suffix, idx, seen_names):
    """Generate a descriptive parameter name."""
    parts = []
    if col_name:
        parts.append(col_name.replace('.', '_'))
    if suffix:
        parts.append(suffix)
    name = '_'.join(parts) if parts else f'param_{idx}'
    base = name
    counter = 2
    while name in seen_names:
        name = f'{base}_{counter}'
        counter += 1
    seen_names.add(name)
    return name


def _collect_predicate_targets(ast):
    """Walk the AST and collect predicate value expressions to parameterize.

    Returns a list of (expression_node, col_name, suffix, param_type, default_value).
    Each entry represents one predicate value to replace with a :param placeholder.
    """
    targets = []

    for node in ast.walk():
        # --- Comparisons: col op value ---
        if isinstance(node, (exp.EQ, exp.LT, exp.GT, exp.LTE, exp.GTE, exp.NEQ)):
            if not _in_predicate_clause(node):
                continue
            lhs = node.args.get("this")
            rhs = node.args.get("expression")
            # Determine which side is column and which is value
            if _contains_column(lhs) and not _contains_column(rhs):
                col_node, val_node = lhs, rhs
            elif _contains_column(rhs) and not _contains_column(lhs):
                col_node, val_node = rhs, lhs
            else:
                continue  # Both sides have columns (join condition) or neither

            col_name = col_node.name if isinstance(col_node, exp.Column) else None
            op_suffix = {exp.EQ: 'eq', exp.LT: 'upper', exp.GT: 'lower',
                         exp.LTE: 'upper', exp.GTE: 'lower', exp.NEQ: 'neq'}.get(type(node), '')

            # Handle DATE + INTERVAL as a date expression
            date_str = _get_date_literal(val_node)
            if date_str:
                targets.append((val_node, col_name, op_suffix, "date", date_str))
                continue

            # Handle date + interval expressions — parameterize the whole expression
            if _is_date_interval_expr(val_node):
                # Extract date base from the expression
                base_node = val_node.args.get("this")
                date_str = _get_date_literal(base_node) if base_node else None
                targets.append((val_node, col_name, op_suffix, "date_expr", date_str or ""))
                continue

            # Numeric: try to evaluate the expression
            num_val = _eval_literal_expr(val_node)
            if num_val is not None:
                is_float = '.' in str(val_node.sql()) or isinstance(num_val, float) and num_val != int(num_val)
                ptype = "double" if is_float else "int"
                default = num_val if is_float else int(num_val)
                targets.append((val_node, col_name, op_suffix, ptype, default))
                continue

            # String literal
            if isinstance(val_node, exp.Literal) and val_node.is_string:
                targets.append((val_node, col_name, op_suffix, "string",
                                str(val_node.args.get("this", ""))))
                continue

        # --- BETWEEN: col BETWEEN low AND high ---
        if isinstance(node, exp.Between):
            if not _in_predicate_clause(node):
                continue
            this = node.args.get("this")
            low = node.args.get("low")
            high = node.args.get("high")
            if not (this and low and high):
                continue
            col_name = this.name if isinstance(this, exp.Column) else None

            # Low bound
            date_str = _get_date_literal(low)
            if date_str:
                targets.append((low, col_name, "lower", "date", date_str))
            elif _is_date_interval_expr(low):
                targets.append((low, col_name, "lower", "date_expr", ""))
            else:
                num_val = _eval_literal_expr(low)
                if num_val is not None:
                    is_float = isinstance(num_val, float) and num_val != int(num_val)
                    targets.append((low, col_name, "lower", "double" if is_float else "int",
                                    num_val if is_float else int(num_val)))

            # High bound
            date_str = _get_date_literal(high)
            if date_str:
                targets.append((high, col_name, "upper", "date", date_str))
            elif _is_date_interval_expr(high):
                targets.append((high, col_name, "upper", "date_expr", ""))
            else:
                num_val = _eval_literal_expr(high)
                if num_val is not None:
                    is_float = isinstance(num_val, float) and num_val != int(num_val)
                    targets.append((high, col_name, "upper", "double" if is_float else "int",
                                    num_val if is_float else int(num_val)))

        # --- LIKE: col LIKE 'pattern' ---
        if isinstance(node, exp.Like):
            if not _in_predicate_clause(node):
                continue
            this = node.args.get("this")
            expr = node.args.get("expression")
            if isinstance(this, exp.Column) and isinstance(expr, exp.Literal):
                col_name = this.name
                targets.append((expr, col_name, "pattern", "string",
                                str(expr.args.get("this", ""))))

        # --- LIMIT N ---
        if isinstance(node, exp.Limit):
            expr = node.args.get("expression")
            if isinstance(expr, exp.Literal) and expr.is_number:
                val = str(expr.args.get("this", ""))
                try:
                    targets.append((expr, None, "limit", "int", int(val)))
                except ValueError:
                    pass

    return targets


def extract_template(sql: str) -> dict:
    """Extract a parameterized template from SQL.

    Works at the predicate level: replaces entire predicate value expressions
    (including arithmetic like 0.06 - 0.01) with single :param_name placeholders.
    Returns {template_sql, parameters: [{name, type, default, description}]}.
    """
    try:
        parsed = sqlglot.parse(sql)
    except sqlglot.errors.ParseError:
        for dialect in ["postgres", "mysql", "duckdb"]:
            try:
                parsed = sqlglot.parse(sql, read=dialect)
                break
            except sqlglot.errors.ParseError:
                continue
        else:
            return {"template_sql": sql, "parameters": []}

    if not parsed or parsed[0] is None:
        return {"template_sql": sql, "parameters": []}

    ast = parsed[0]
    targets = _collect_predicate_targets(ast)

    # Deduplicate by node identity (same node may be visited multiple times)
    seen_nodes = set()
    unique_targets = []
    for target in targets:
        node_id = id(target[0])
        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            unique_targets.append(target)

    parameters = []
    seen_names = set()

    for expr_node, col_name, suffix, param_type, default_val in unique_targets:
        param_name = _make_param_name(col_name, suffix, len(parameters), seen_names)

        # For date_expr type (DATE + INTERVAL), store as "date" with computed default
        display_type = "date" if param_type == "date_expr" else param_type

        desc_parts = []
        if col_name:
            desc_parts.append(f"Filter on {col_name}")
        if suffix:
            desc_parts.append(f"({suffix})")
        description = " ".join(desc_parts) if desc_parts else f"Parameter {param_name}"

        parameters.append({
            "name": param_name,
            "type": display_type,
            "default": default_val,
            "description": description,
        })

        # Replace the expression node with a placeholder in the AST
        placeholder = exp.Placeholder(this=param_name)
        expr_node.replace(placeholder)

    template_sql = ast.sql(dialect="postgres", pretty=True) if parameters else ast.sql(dialect="postgres", pretty=True)

    return {
        "template_sql": template_sql,
        "parameters": parameters,
    }


def main():
    parser = argparse.ArgumentParser(description="Extract structural features from SQL using SQLGlot")
    parser.add_argument("--sql", type=str, help="SQL query string")
    parser.add_argument("--file", type=str, help="Path to SQL file")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument("--extract-template", action="store_true",
                        help="Extract parameterized template instead of structural features")
    args = parser.parse_args()

    if args.sql:
        sql = args.sql
    elif args.file:
        with open(args.file, "r") as f:
            sql = f.read()
    elif not sys.stdin.isatty():
        sql = sys.stdin.read()
    else:
        parser.print_help()
        sys.exit(1)

    if args.extract_template:
        result = extract_template(sql.strip())
    else:
        result = extract_structural_features(sql.strip())

    indent = 2 if args.pretty else None
    print(json.dumps(result, indent=indent))


if __name__ == "__main__":
    main()
