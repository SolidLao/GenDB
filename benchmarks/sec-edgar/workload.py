"""SEC-EDGAR workload configuration and setup functions."""

import re
import subprocess
from pathlib import Path

import duckdb
import psycopg2

from benchmarks.lib.config import WorkloadConfig
from benchmarks.lib.indexes import parse_gendb_indexes
from benchmarks.lib.runners import get_pg_conn_params
from benchmarks.lib.systems import mclient
from benchmarks.lib.utils import strip_fk_constraints


# ---------------------------------------------------------------------------
# Query loading
# ---------------------------------------------------------------------------

def load_queries(queries_path: Path) -> dict:
    """Load queries from queries.sql file."""
    with open(queries_path, "r") as f:
        content = f.read()

    queries = {}
    parts = re.split(r"--\s*(Q\d+):\s*([^\n]*)\n", content)
    i = 1
    while i + 2 <= len(parts):
        name = parts[i].strip()
        sql = parts[i + 2].strip()
        sql = re.sub(r"--[^\n]*$", "", sql, flags=re.MULTILINE).strip()
        sql = sql.rstrip(";").strip()
        if sql:
            queries[name] = sql
        i += 3

    return queries


# ---------------------------------------------------------------------------
# ClickHouse table definitions
# ---------------------------------------------------------------------------

CLICKHOUSE_TABLES = {
    "sub": ("adsh String, cik Nullable(Int32), name Nullable(String), "
            "sic Nullable(Int32), countryba Nullable(String), stprba Nullable(String), "
            "cityba Nullable(String), countryinc Nullable(String), form Nullable(String), "
            "period Nullable(Int32), fy Nullable(Int32), fp Nullable(String), "
            "filed Nullable(Int32), accepted Nullable(String), prevrpt Nullable(Int32), "
            "nciks Nullable(Int32), afs Nullable(String), wksi Nullable(Int32), "
            "fye Nullable(String), instance Nullable(String)"),
    "num": ("adsh String, tag String, version String, "
            "ddate Nullable(Int32), qtrs Nullable(Int32), uom Nullable(String), "
            "coreg Nullable(String), value Nullable(Float64), footnote Nullable(String)"),
    "tag": ("tag String, version String, custom Nullable(Int32), "
            "abstract Nullable(Int32), datatype Nullable(String), "
            "iord Nullable(String), crdr Nullable(String), "
            "tlabel Nullable(String), doc Nullable(String)"),
    "pre": ("adsh String, report Nullable(Int32), line Nullable(Int32), "
            "stmt Nullable(String), inpth Nullable(Int32), rfile Nullable(String), "
            "tag String, version String, plabel Nullable(String), "
            "negating Nullable(Int32)"),
}

CLICKHOUSE_ORDER_KEYS = {
    "sub": "adsh",
    "num": "(adsh, tag, version)",
    "tag": "(tag, version)",
    "pre": "(adsh, tag, version)",
}

EDGAR_PK_COLUMNS = {
    "sub": ("adsh",),
    "tag": ("tag", "version"),
    "num": (),
    "pre": (),
}


def _strip_fk_extra(schema_sql):
    """Extra transform for EDGAR: DOUBLE -> DOUBLE PRECISION for PostgreSQL."""
    return re.sub(r'\bDOUBLE\b', 'DOUBLE PRECISION', schema_sql, flags=re.IGNORECASE)


def _edgar_index_filter(table_name, raw_columns):
    """Filter internal GenDB columns and return tuple for index dedup."""
    internal_cols = {"row_id"}
    return tuple(c for c in raw_columns if c not in internal_cols)


def _edgar_parse_indexes(storage_design_path):
    """Parse indexes with EDGAR-specific filtering (remove row_id, deduplicate)."""
    return parse_gendb_indexes(
        storage_design_path, EDGAR_PK_COLUMNS, filter_fn=_edgar_index_filter)


# ---------------------------------------------------------------------------
# Setup functions
# ---------------------------------------------------------------------------

def pg_setup(config, force_setup=False):
    dbname = config.db_name

    # Check if exists
    try:
        conn = psycopg2.connect(**get_pg_conn_params(config))
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {config.check_table}")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        if not force_setup and count > 0:
            print(f"  Database '{dbname}' already exists with data, skipping setup.")
            print(f"  Use --setup flag to force data reload.")
            return
    except Exception:
        pass

    benchmark_root = config.benchmark_root
    schema_path = benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    db_path = config.duckdb_path

    conn_default = psycopg2.connect(
        host="/var/run/postgresql", port=5435, user="postgres", dbname="postgres",
    )
    conn_default.autocommit = True
    cur_default = conn_default.cursor()
    print(f"  Creating database '{dbname}'...")
    cur_default.execute(f"DROP DATABASE IF EXISTS {dbname}")
    cur_default.execute(f"CREATE DATABASE {dbname}")
    cur_default.close()
    conn_default.close()

    conn = psycopg2.connect(**get_pg_conn_params(config))
    conn.autocommit = True
    cur = conn.cursor()

    schema_no_fk = strip_fk_constraints(schema_sql, extra_fn=_strip_fk_extra)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            cur.execute(stmt)

    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        cur.close()
        conn.close()
        return

    duck_con = duckdb.connect(str(db_path), read_only=True)
    tmp_dir = benchmark_root / "pg_tmp"
    tmp_dir.mkdir(exist_ok=True)
    for table in ["sub", "tag", "num", "pre"]:
        print(f"  Loading {table}...", end="", flush=True)
        tmp_csv = tmp_dir / f"{table}.csv"
        duck_con.execute(f"COPY {table} TO '{tmp_csv}' (FORMAT CSV, HEADER true)")
        psql_cmd = (
            f"\\copy {table} FROM '{tmp_csv.resolve()}' WITH (FORMAT CSV, HEADER true)"
        )
        subprocess.run(
            ["psql", "-h", "/var/run/postgresql", "-p", "5435", "-U", "postgres",
             "-d", dbname, "-c", psql_cmd],
            check=True, capture_output=True, timeout=600,
        )
        tmp_csv.unlink()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")
    duck_con.close()
    tmp_dir.rmdir()

    cur.close()
    conn.close()


def clickhouse_setup(config, force_setup=False):
    ch_dir = config.benchmark_root / "clickhouse"
    binary = ch_dir / "clickhouse"
    db_path = config.duckdb_path

    def ch_query(sql):
        subprocess.run(
            [str(binary), "client", "--port", "9000", "-q", sql],
            check=True, capture_output=True, text=True, timeout=300,
        )

    if not force_setup:
        try:
            result = subprocess.run(
                [str(binary), "client", "--port", "9000", "-q",
                 f"SELECT count() FROM {config.check_table}"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0 and int(result.stdout.strip()) > 0:
                print("  ClickHouse already has data, skipping setup.")
                return
        except Exception:
            pass

    for table, cols in CLICKHOUSE_TABLES.items():
        order_key = CLICKHOUSE_ORDER_KEYS[table]
        ch_query(f"DROP TABLE IF EXISTS {table}")
        ch_query(f"CREATE TABLE {table} ({cols}) ENGINE = MergeTree() ORDER BY {order_key}")

    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return

    duck_con = duckdb.connect(str(db_path), read_only=True)
    for table in ["sub", "tag", "num", "pre"]:
        print(f"  Loading {table}...", end="", flush=True)
        tmp_csv = ch_dir / f"{table}_export.csv"
        duck_con.execute(f"COPY {table} TO '{tmp_csv}' (FORMAT CSV, HEADER false)")
        subprocess.run(
            f"'{binary}' client --port 9000 "
            f"--query='INSERT INTO {table} FORMAT CSV' < '{tmp_csv}'",
            shell=True, check=True, capture_output=True, timeout=600,
        )
        tmp_csv.unlink()
        result = subprocess.run(
            [str(binary), "client", "--port", "9000", "-q",
             f"SELECT count() FROM {table}"],
            capture_output=True, text=True, timeout=30,
        )
        count = result.stdout.strip()
        print(f" {int(count):,} rows")
    duck_con.close()


def umbra_setup(config, force_setup=False):
    benchmark_root = config.benchmark_root
    schema_path = benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    db_path = config.duckdb_path

    conn = psycopg2.connect(
        host="127.0.0.1", port=config.umbra_port, user="postgres",
        password="postgres", dbname="postgres",
    )
    conn.autocommit = True
    cur = conn.cursor()

    if not force_setup:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {config.check_table}")
            count = cur.fetchone()[0]
            if count > 0:
                print("  Umbra already has data, skipping setup.")
                cur.close()
                conn.close()
                return
        except Exception:
            conn.rollback()

    schema_no_fk = strip_fk_constraints(schema_sql, extra_fn=_strip_fk_extra)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            try:
                cur.execute(f"DROP TABLE IF EXISTS {stmt.split()[2]}")
            except Exception:
                conn.rollback()
            cur.execute(stmt)

    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        cur.close()
        conn.close()
        return

    duck_con = duckdb.connect(str(db_path), read_only=True)
    tmp_dir = benchmark_root / "umbra_tmp"
    tmp_dir.mkdir(exist_ok=True)
    for table in ["sub", "tag", "num", "pre"]:
        print(f"  Loading {table}...", end="", flush=True)
        tmp_csv = tmp_dir / f"{table}.csv"
        duck_con.execute(f"COPY {table} TO '{tmp_csv}' (FORMAT CSV, HEADER true)")
        try:
            cur.execute(
                f"COPY {table} FROM '/data/umbra_tmp/{table}.csv' WITH (FORMAT CSV, HEADER true)"
            )
        except Exception:
            conn.rollback()
            with open(tmp_csv, "r") as f:
                header = f.readline().strip()
                col_names = header.split(",")
                cur.copy_expert(
                    f"COPY {table} ({', '.join(col_names)}) FROM STDIN WITH (FORMAT CSV)",
                    f,
                )
        tmp_csv.unlink()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")
    duck_con.close()
    tmp_dir.rmdir()

    cur.close()
    conn.close()


def monetdb_install():
    """MonetDB must be installed via system package manager for EDGAR."""
    print("  MonetDB must be installed via system package manager.")
    return False


def monetdb_setup(config, force_setup=False):
    benchmark_root = config.benchmark_root
    schema_path = benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    db_path = config.duckdb_path
    dbname = config.db_name

    if not force_setup:
        try:
            import pymonetdb
            conn = pymonetdb.connect(database=dbname, hostname="localhost",
                                     port=50000, username="monetdb", password="monetdb")
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {config.check_table}")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            if count > 0:
                print("  MonetDB already has data, skipping setup.")
                return
        except Exception:
            pass

    # Build MonetDB-compatible schema DDL
    schema_no_fk = strip_fk_constraints(schema_sql, extra_fn=_strip_fk_extra)
    schema_no_fk = re.sub(r'\bTEXT\b', 'CLOB', schema_no_fk, flags=re.IGNORECASE)
    monetdb_varchar_overrides = {
        "256": "210",
        "1024": "540",
        "512": "400",
    }
    for old_sz, new_sz in monetdb_varchar_overrides.items():
        schema_no_fk = schema_no_fk.replace(
            f"VARCHAR({old_sz})", f"VARCHAR({new_sz})")

    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            table_name = stmt.split()[2]
            try:
                mclient(dbname, f"DROP TABLE {table_name};", timeout=10)
            except Exception:
                pass
            mclient(dbname, stmt + ";")

    if not db_path.exists():
        print(f"  Error: DuckDB database not found at {db_path}")
        return

    duck_con = duckdb.connect(str(db_path), read_only=True)
    tmp_dir = benchmark_root / "monetdb_tmp"
    tmp_dir.mkdir(exist_ok=True)
    for table in ["sub", "tag", "num", "pre"]:
        print(f"  Loading {table}...", end="", flush=True)
        tmp_csv = tmp_dir / f"{table}.tbl"
        col_info = duck_con.execute(f"DESCRIBE {table}").fetchall()
        select_exprs = []
        for row in col_info:
            c, ctype = row[0], row[1].upper()
            if "VARCHAR" in ctype or "TEXT" in ctype:
                select_exprs.append(
                    f"REPLACE({c}, '\\', '') AS {c}")
            else:
                select_exprs.append(c)
        duck_con.execute(
            f"COPY (SELECT {', '.join(select_exprs)} FROM {table}) "
            f"TO '{tmp_csv}' (FORMAT CSV, HEADER false, DELIMITER '|')"
        )
        abs_path = str(tmp_csv.resolve())
        mclient(dbname,
                 f"COPY INTO {table} FROM '{abs_path}' "
                 f"USING DELIMITERS '|',E'\\n','\"' NULL AS '';")
        tmp_csv.unlink()
        result = mclient(dbname, f"SELECT COUNT(*) FROM {table};")
        for line in result.stdout.splitlines():
            line = line.strip().strip("|").strip()
            if line.isdigit():
                print(f" {int(line):,} rows")
                break
    duck_con.close()
    try:
        tmp_dir.rmdir()
    except OSError:
        pass


# ---------------------------------------------------------------------------
# get_config — builds WorkloadConfig from CLI args
# ---------------------------------------------------------------------------

def get_config(args) -> WorkloadConfig:
    benchmark_root = Path(__file__).parent
    years = args.years

    queries_path = benchmark_root / "queries.sql"
    if not queries_path.exists():
        import sys
        print(f"Error: queries file not found at {queries_path}")
        print("Run: python3 benchmarks/sec-edgar/generate_queries.py")
        sys.exit(1)
    queries = load_queries(queries_path)

    if args.output is None:
        figures_dir = benchmark_root.parent / "figures" / "sec-edgar"
        figures_dir.mkdir(parents=True, exist_ok=True)
        args.output = figures_dir / "benchmark_results_per_query.png"

    return WorkloadConfig(
        name="sec-edgar",
        scale_label=f"Years={years}",
        scale_value=years,
        db_name=f"sec_edgar_{years}y",
        check_table="num",
        benchmark_root=benchmark_root,
        queries=queries,
        duckdb_path=benchmark_root / "duckdb" / "sec_edgar.duckdb",
        umbra_port=5441,
        pk_columns=EDGAR_PK_COLUMNS,
        pg_setup_fn=pg_setup,
        duckdb_setup_fn=None,  # DuckDB already exists for EDGAR
        clickhouse_setup_fn=clickhouse_setup,
        umbra_setup_fn=umbra_setup,
        monetdb_setup_fn=monetdb_setup,
        monetdb_install_fn=monetdb_install,
        clickhouse_tables=CLICKHOUSE_TABLES,
        clickhouse_order_keys=CLICKHOUSE_ORDER_KEYS,
        clickhouse_settings={"join_use_nulls": 1},
        parse_indexes_fn=_edgar_parse_indexes,
        strip_fk_extra_fn=_strip_fk_extra,
        skip_postgres=getattr(args, 'skip_postgres', False),
        skip_duckdb=getattr(args, 'skip_duckdb', False),
        skip_clickhouse=args.skip_clickhouse,
        skip_umbra=args.skip_umbra,
        skip_monetdb=args.skip_monetdb,
    )
