"""Index management: parse, create, drop indexes across all systems."""

import json
import subprocess

import duckdb as duckdb_mod
import psycopg2

from .runners import get_pg_conn_params


def parse_gendb_indexes(storage_design_path, pk_columns, filter_fn=None):
    """Parse GenDB storage_design.json to extract index definitions for SQL systems.

    Args:
        storage_design_path: Path to storage_design.json
        pk_columns: dict {table: (col,...)} for PK dedup
        filter_fn: optional callable(table_name, raw_columns) -> filtered_columns tuple
                   Used by EDGAR to remove row_id and deduplicate.

    Returns list of {table, columns, name} for non-PK indexes.
    """
    with open(storage_design_path) as f:
        design = json.load(f)
    indexes = []
    seen = set()
    for table_name, table_info in design.get("tables", {}).items():
        for idx in table_info.get("indexes", []):
            raw_columns = idx.get("columns", [])
            if filter_fn:
                columns = filter_fn(table_name, raw_columns)
            else:
                columns = tuple(raw_columns)
            if not columns:
                continue
            # Skip if this matches the table's PK
            if columns == pk_columns.get(table_name, ()):
                continue
            # Deduplicate
            key = (table_name, columns)
            if key in seen:
                continue
            seen.add(key)
            indexes.append({
                "table": table_name,
                "columns": list(columns),
                "name": idx.get("name", f"idx_{table_name}_{'_'.join(columns)}"),
            })
    return indexes


# ---------------------------------------------------------------------------
# Create indexes
# ---------------------------------------------------------------------------

def create_indexes_pg(config, indexes):
    """Create indexes on PostgreSQL and run ANALYZE."""
    conn = psycopg2.connect(**get_pg_conn_params(config))
    conn.autocommit = True
    cur = conn.cursor()
    for idx in indexes:
        cols = ", ".join(idx["columns"])
        sql = f"CREATE INDEX IF NOT EXISTS {idx['name']} ON {idx['table']}({cols})"
        print(f"    {idx['name']} ON {idx['table']}({cols})")
        cur.execute(sql)
    print("    Running ANALYZE...")
    cur.execute("ANALYZE")
    cur.close()
    conn.close()


def create_indexes_duckdb(config, indexes):
    """Create indexes on DuckDB (ART indexes)."""
    conn = duckdb_mod.connect(str(config.duckdb_path))
    for idx in indexes:
        cols = ", ".join(idx["columns"])
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS {idx['name']} ON {idx['table']}({cols})")
            print(f"    {idx['name']} ON {idx['table']}({cols})")
        except Exception as e:
            print(f"    Warning: could not create {idx['name']}: {e}")
    conn.close()


def create_indexes_clickhouse(config, indexes):
    """Create skipping indexes on ClickHouse (minmax type)."""
    ch_dir = config.benchmark_root / "clickhouse"
    binary = ch_dir / "clickhouse"
    for idx in indexes:
        cols = ", ".join(idx["columns"])
        sql = (f"ALTER TABLE {idx['table']} ADD INDEX IF NOT EXISTS "
               f"{idx['name']} ({cols}) TYPE minmax GRANULARITY 1")
        try:
            subprocess.run(
                [str(binary), "client", "--port", "9000", "-q", sql],
                check=True, capture_output=True, text=True, timeout=60,
            )
            print(f"    {idx['name']} ON {idx['table']}({cols})")
        except Exception as e:
            print(f"    Warning: could not create {idx['name']}: {e}")
    # Materialize indexes
    try:
        for table in set(idx["table"] for idx in indexes):
            subprocess.run(
                [str(binary), "client", "--port", "9000", "-q",
                 f"OPTIMIZE TABLE {table} FINAL"],
                capture_output=True, text=True, timeout=300,
            )
    except Exception:
        pass


def create_indexes_umbra(config, indexes):
    """Create indexes on Umbra (PostgreSQL-compatible)."""
    conn = psycopg2.connect(
        host="127.0.0.1", port=config.umbra_port, user="postgres",
        password="postgres", dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()
    for idx in indexes:
        cols = ", ".join(idx["columns"])
        sql = f"CREATE INDEX IF NOT EXISTS {idx['name']} ON {idx['table']}({cols})"
        try:
            cur.execute(sql)
            print(f"    {idx['name']} ON {idx['table']}({cols})")
        except Exception as e:
            conn.rollback()
            print(f"    Warning: could not create {idx['name']}: {e}")
    cur.close()
    conn.close()


def create_indexes_monetdb(config, indexes):
    """Create indexes on MonetDB."""
    try:
        import pymonetdb
    except ImportError:
        print("    pymonetdb not installed, skipping indexes.")
        return
    conn = pymonetdb.connect(database=config.db_name, hostname="localhost",
                             port=50000, username="monetdb", password="monetdb")
    cur = conn.cursor()
    for idx in indexes:
        cols = ", ".join(idx["columns"])
        try:
            cur.execute(f"CREATE INDEX {idx['name']} ON {idx['table']}({cols})")
            conn.commit()
            print(f"    {idx['name']} ON {idx['table']}({cols})")
        except Exception as e:
            conn.rollback()
            if "already exists" not in str(e).lower():
                print(f"    Warning: could not create {idx['name']}: {e}")
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# Drop indexes
# ---------------------------------------------------------------------------

def drop_indexes_pg(config, indexes):
    """Drop previously created indexes on PostgreSQL."""
    conn = psycopg2.connect(**get_pg_conn_params(config))
    conn.autocommit = True
    cur = conn.cursor()
    for idx in indexes:
        try:
            cur.execute(f"DROP INDEX IF EXISTS {idx['name']}")
        except Exception:
            pass
    cur.execute("ANALYZE")
    cur.close()
    conn.close()


def drop_indexes_duckdb(config, indexes):
    """Drop previously created indexes on DuckDB."""
    conn = duckdb_mod.connect(str(config.duckdb_path))
    for idx in indexes:
        try:
            conn.execute(f"DROP INDEX IF EXISTS {idx['name']}")
        except Exception:
            pass
    conn.close()


def drop_indexes_clickhouse(config, indexes):
    """Drop skipping indexes on ClickHouse."""
    ch_dir = config.benchmark_root / "clickhouse"
    binary = ch_dir / "clickhouse"
    for idx in indexes:
        try:
            sql = f"ALTER TABLE {idx['table']} DROP INDEX IF EXISTS {idx['name']}"
            subprocess.run(
                [str(binary), "client", "--port", "9000", "-q", sql],
                capture_output=True, text=True, timeout=60,
            )
        except Exception:
            pass


def drop_indexes_umbra(config, indexes):
    """Drop previously created indexes on Umbra."""
    conn = psycopg2.connect(
        host="127.0.0.1", port=config.umbra_port, user="postgres",
        password="postgres", dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()
    for idx in indexes:
        try:
            cur.execute(f"DROP INDEX IF EXISTS {idx['name']}")
        except Exception:
            pass
    cur.close()
    conn.close()


def drop_indexes_monetdb(config, indexes):
    """Drop previously created indexes on MonetDB."""
    try:
        import pymonetdb
    except ImportError:
        return
    conn = pymonetdb.connect(database=config.db_name, hostname="localhost",
                             port=50000, username="monetdb", password="monetdb")
    cur = conn.cursor()
    for idx in indexes:
        try:
            cur.execute(f"DROP INDEX {idx['name']}")
            conn.commit()
        except Exception:
            conn.rollback()
    cur.close()
    conn.close()
