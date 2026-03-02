"""TPC-H workload configuration and setup functions."""

import io
import os
import re
import subprocess
from pathlib import Path

import duckdb
import psycopg2

from benchmarks.lib.config import WorkloadConfig
from benchmarks.lib.indexes import parse_gendb_indexes
from benchmarks.lib.runners import get_pg_conn_params
from benchmarks.lib.utils import strip_fk_constraints


# ---------------------------------------------------------------------------
# TPC-H queries (standard SQL, cross-system compatible)
# ---------------------------------------------------------------------------

def get_queries(scale_factor: int = 1) -> dict:
    """Return all 22 TPC-H queries with default parameter substitutions."""
    q11_threshold = 0.0001 / scale_factor

    return {
        "Q1": """
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) AS sum_qty,
                SUM(l_extendedprice) AS sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                AVG(l_quantity) AS avg_qty,
                AVG(l_extendedprice) AS avg_price,
                AVG(l_discount) AS avg_disc,
                COUNT(*) AS count_order
            FROM lineitem
            WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """,
        "Q2": """
            SELECT
                s_acctbal, s_name, n_name, p_partkey, p_mfgr,
                s_address, s_phone, s_comment
            FROM part, supplier, partsupp, nation, region
            WHERE
                p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND p_size = 15
                AND p_type LIKE '%BRASS'
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE'
                AND ps_supplycost = (
                    SELECT MIN(ps_supplycost)
                    FROM partsupp, supplier, nation, region
                    WHERE
                        p_partkey = ps_partkey
                        AND s_suppkey = ps_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE'
                )
            ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
            LIMIT 100
        """,
        "Q3": """
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM customer, orders, lineitem
            WHERE
                c_mktsegment = 'BUILDING'
                AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate < DATE '1995-03-15'
                AND l_shipdate > DATE '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate
            LIMIT 10
        """,
        "Q4": """
            SELECT
                o_orderpriority,
                COUNT(*) AS order_count
            FROM orders
            WHERE
                o_orderdate >= DATE '1993-07-01'
                AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
                AND EXISTS (
                    SELECT * FROM lineitem
                    WHERE l_orderkey = o_orderkey
                      AND l_commitdate < l_receiptdate
                )
            GROUP BY o_orderpriority
            ORDER BY o_orderpriority
        """,
        "Q5": """
            SELECT
                n_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM customer, orders, lineitem, supplier, nation, region
            WHERE
                c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey
                AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'ASIA'
                AND o_orderdate >= DATE '1994-01-01'
                AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY n_name
            ORDER BY revenue DESC
        """,
        "Q6": """
            SELECT
                SUM(l_extendedprice * l_discount) AS revenue
            FROM lineitem
            WHERE
                l_shipdate >= DATE '1994-01-01'
                AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
                AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
                AND l_quantity < 24
        """,
        "Q7": """
            SELECT
                supp_nation, cust_nation, l_year,
                SUM(volume) AS revenue
            FROM (
                SELECT
                    n1.n_name AS supp_nation,
                    n2.n_name AS cust_nation,
                    EXTRACT(YEAR FROM l_shipdate) AS l_year,
                    l_extendedprice * (1 - l_discount) AS volume
                FROM supplier, lineitem, orders, customer, nation n1, nation n2
                WHERE
                    s_suppkey = l_suppkey
                    AND o_orderkey = l_orderkey
                    AND c_custkey = o_custkey
                    AND s_nationkey = n1.n_nationkey
                    AND c_nationkey = n2.n_nationkey
                    AND (
                        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                    )
                    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            ) AS shipping
            GROUP BY supp_nation, cust_nation, l_year
            ORDER BY supp_nation, cust_nation, l_year
        """,
        "Q8": """
            SELECT
                o_year,
                SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share
            FROM (
                SELECT
                    EXTRACT(YEAR FROM o_orderdate) AS o_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    n2.n_name AS nation
                FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
                WHERE
                    p_partkey = l_partkey
                    AND s_suppkey = l_suppkey
                    AND l_orderkey = o_orderkey
                    AND o_custkey = c_custkey
                    AND c_nationkey = n1.n_nationkey
                    AND n1.n_regionkey = r_regionkey
                    AND r_name = 'AMERICA'
                    AND s_nationkey = n2.n_nationkey
                    AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                    AND p_type = 'ECONOMY ANODIZED STEEL'
            ) AS all_nations
            GROUP BY o_year
            ORDER BY o_year
        """,
        "Q9": """
            SELECT
                nation, o_year,
                SUM(amount) AS sum_profit
            FROM (
                SELECT
                    n_name AS nation,
                    EXTRACT(YEAR FROM o_orderdate) AS o_year,
                    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                FROM part, supplier, lineitem, partsupp, orders, nation
                WHERE
                    s_suppkey = l_suppkey
                    AND ps_suppkey = l_suppkey
                    AND ps_partkey = l_partkey
                    AND p_partkey = l_partkey
                    AND o_orderkey = l_orderkey
                    AND s_nationkey = n_nationkey
                    AND p_name LIKE '%green%'
            ) AS profit
            GROUP BY nation, o_year
            ORDER BY nation, o_year DESC
        """,
        "Q10": """
            SELECT
                c_custkey, c_name,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                c_acctbal, n_name, c_address, c_phone, c_comment
            FROM customer, orders, lineitem, nation
            WHERE
                c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate >= DATE '1993-10-01'
                AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
                AND l_returnflag = 'R'
                AND c_nationkey = n_nationkey
            GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
            ORDER BY revenue DESC
            LIMIT 20
        """,
        "Q11": f"""
            SELECT
                ps_partkey,
                SUM(ps_supplycost * ps_availqty) AS value
            FROM partsupp, supplier, nation
            WHERE
                ps_suppkey = s_suppkey
                AND s_nationkey = n_nationkey
                AND n_name = 'GERMANY'
            GROUP BY ps_partkey
            HAVING SUM(ps_supplycost * ps_availqty) > (
                SELECT SUM(ps_supplycost * ps_availqty) * {q11_threshold:.10f}
                FROM partsupp, supplier, nation
                WHERE
                    ps_suppkey = s_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_name = 'GERMANY'
            )
            ORDER BY value DESC
        """,
        "Q12": """
            SELECT
                l_shipmode,
                SUM(CASE
                    WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1
                    ELSE 0
                END) AS high_line_count,
                SUM(CASE
                    WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1
                    ELSE 0
                END) AS low_line_count
            FROM orders, lineitem
            WHERE
                o_orderkey = l_orderkey
                AND l_shipmode IN ('MAIL', 'SHIP')
                AND l_commitdate < l_receiptdate
                AND l_shipdate < l_commitdate
                AND l_receiptdate >= DATE '1994-01-01'
                AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY l_shipmode
            ORDER BY l_shipmode
        """,
        "Q13": """
            SELECT
                c_count,
                COUNT(*) AS custdist
            FROM (
                SELECT
                    c_custkey,
                    COUNT(o_orderkey) AS c_count
                FROM customer LEFT OUTER JOIN orders ON
                    c_custkey = o_custkey
                    AND o_comment NOT LIKE '%special%requests%'
                GROUP BY c_custkey
            ) AS c_orders
            GROUP BY c_count
            ORDER BY custdist DESC, c_count DESC
        """,
        "Q14": """
            SELECT
                100.00 * SUM(CASE
                    WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
                    ELSE 0
                END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM lineitem, part
            WHERE
                l_partkey = p_partkey
                AND l_shipdate >= DATE '1995-09-01'
                AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH
        """,
        "Q15": """
            WITH revenue0 AS (
                SELECT
                    l_suppkey AS supplier_no,
                    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM lineitem
                WHERE
                    l_shipdate >= DATE '1996-01-01'
                    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
                GROUP BY l_suppkey
            )
            SELECT
                s_suppkey, s_name, s_address, s_phone, total_revenue
            FROM supplier, revenue0
            WHERE
                s_suppkey = supplier_no
                AND total_revenue = (
                    SELECT MAX(total_revenue) FROM revenue0
                )
            ORDER BY s_suppkey
        """,
        "Q16": """
            SELECT
                p_brand, p_type, p_size,
                COUNT(DISTINCT ps_suppkey) AS supplier_cnt
            FROM partsupp, part
            WHERE
                p_partkey = ps_partkey
                AND p_brand <> 'Brand#45'
                AND p_type NOT LIKE 'MEDIUM POLISHED%'
                AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
                AND ps_suppkey NOT IN (
                    SELECT s_suppkey FROM supplier
                    WHERE s_comment LIKE '%Customer%Complaints%'
                )
            GROUP BY p_brand, p_type, p_size
            ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
        """,
        "Q17": """
            SELECT
                SUM(l_extendedprice) / 7.0 AS avg_yearly
            FROM lineitem, part
            WHERE
                p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container = 'MED BOX'
                AND l_quantity < (
                    SELECT 0.2 * AVG(l_quantity)
                    FROM lineitem
                    WHERE l_partkey = p_partkey
                )
        """,
        "Q18": """
            SELECT
                c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
                SUM(l_quantity) AS sum_qty
            FROM customer, orders, lineitem
            WHERE
                o_orderkey IN (
                    SELECT l_orderkey FROM lineitem
                    GROUP BY l_orderkey
                    HAVING SUM(l_quantity) > 300
                )
                AND c_custkey = o_custkey
                AND o_orderkey = l_orderkey
            GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
            ORDER BY o_totalprice DESC, o_orderdate
            LIMIT 100
        """,
        "Q19": """
            SELECT
                SUM(l_extendedprice * (1 - l_discount)) AS revenue
            FROM lineitem, part
            WHERE
                (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#12'
                    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    AND l_quantity >= 1 AND l_quantity <= 1 + 10
                    AND p_size BETWEEN 1 AND 5
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
                OR (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#23'
                    AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    AND l_quantity >= 10 AND l_quantity <= 10 + 10
                    AND p_size BETWEEN 1 AND 10
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
                OR (
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#34'
                    AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    AND l_quantity >= 20 AND l_quantity <= 20 + 10
                    AND p_size BETWEEN 1 AND 15
                    AND l_shipmode IN ('AIR', 'AIR REG')
                    AND l_shipinstruct = 'DELIVER IN PERSON'
                )
        """,
        "Q20": """
            SELECT s_name, s_address
            FROM supplier, nation
            WHERE
                s_suppkey IN (
                    SELECT ps_suppkey FROM partsupp
                    WHERE
                        ps_partkey IN (
                            SELECT p_partkey FROM part WHERE p_name LIKE 'forest%'
                        )
                        AND ps_availqty > (
                            SELECT 0.5 * SUM(l_quantity)
                            FROM lineitem
                            WHERE
                                l_partkey = ps_partkey
                                AND l_suppkey = ps_suppkey
                                AND l_shipdate >= DATE '1994-01-01'
                                AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
                        )
                )
                AND s_nationkey = n_nationkey
                AND n_name = 'CANADA'
            ORDER BY s_name
        """,
        "Q21": """
            SELECT
                s_name,
                COUNT(*) AS numwait
            FROM supplier, lineitem l1, orders, nation
            WHERE
                s_suppkey = l1.l_suppkey
                AND o_orderkey = l1.l_orderkey
                AND o_orderstatus = 'F'
                AND l1.l_receiptdate > l1.l_commitdate
                AND EXISTS (
                    SELECT * FROM lineitem l2
                    WHERE l2.l_orderkey = l1.l_orderkey
                      AND l2.l_suppkey <> l1.l_suppkey
                )
                AND NOT EXISTS (
                    SELECT * FROM lineitem l3
                    WHERE l3.l_orderkey = l1.l_orderkey
                      AND l3.l_suppkey <> l1.l_suppkey
                      AND l3.l_receiptdate > l3.l_commitdate
                )
                AND s_nationkey = n_nationkey
                AND n_name = 'SAUDI ARABIA'
            GROUP BY s_name
            ORDER BY numwait DESC, s_name
            LIMIT 100
        """,
        "Q22": """
            SELECT
                cntrycode,
                COUNT(*) AS numcust,
                SUM(c_acctbal) AS totacctbal
            FROM (
                SELECT
                    SUBSTRING(c_phone, 1, 2) AS cntrycode,
                    c_acctbal
                FROM customer
                WHERE
                    SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                    AND c_acctbal > (
                        SELECT AVG(c_acctbal) FROM customer
                        WHERE
                            c_acctbal > 0.00
                            AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
                    )
                    AND NOT EXISTS (
                        SELECT * FROM orders WHERE o_custkey = c_custkey
                    )
            ) AS custsale
            GROUP BY cntrycode
            ORDER BY cntrycode
        """,
    }


# ---------------------------------------------------------------------------
# ClickHouse table definitions
# ---------------------------------------------------------------------------

CLICKHOUSE_TABLES = {
    "nation": "n_nationkey Int32, n_name String, n_regionkey Int32, n_comment String",
    "region": "r_regionkey Int32, r_name String, r_comment String",
    "supplier": "s_suppkey Int32, s_name String, s_address String, s_nationkey Int32, s_phone String, s_acctbal Decimal(15,2), s_comment String",
    "part": "p_partkey Int32, p_name String, p_mfgr String, p_brand String, p_type String, p_size Int32, p_container String, p_retailprice Decimal(15,2), p_comment String",
    "partsupp": "ps_partkey Int32, ps_suppkey Int32, ps_availqty Int32, ps_supplycost Decimal(15,2), ps_comment String",
    "customer": "c_custkey Int32, c_name String, c_address String, c_nationkey Int32, c_phone String, c_acctbal Decimal(15,2), c_mktsegment String, c_comment String",
    "orders": "o_orderkey Int32, o_custkey Int32, o_orderstatus String, o_totalprice Decimal(15,2), o_orderdate Date, o_orderpriority String, o_clerk String, o_shippriority Int32, o_comment String",
    "lineitem": "l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32, l_quantity Decimal(15,2), l_extendedprice Decimal(15,2), l_discount Decimal(15,2), l_tax Decimal(15,2), l_returnflag String, l_linestatus String, l_shipdate Date, l_commitdate Date, l_receiptdate Date, l_shipinstruct String, l_shipmode String, l_comment String",
}

CLICKHOUSE_ORDER_KEYS = {
    "nation": "n_nationkey",
    "region": "r_regionkey",
    "supplier": "s_suppkey",
    "part": "p_partkey",
    "partsupp": "(ps_partkey, ps_suppkey)",
    "customer": "c_custkey",
    "orders": "o_orderkey",
    "lineitem": "(l_orderkey, l_linenumber)",
}

TPCH_PK_COLUMNS = {
    "nation": ("n_nationkey",),
    "region": ("r_regionkey",),
    "supplier": ("s_suppkey",),
    "part": ("p_partkey",),
    "partsupp": ("ps_partkey", "ps_suppkey"),
    "customer": ("c_custkey",),
    "orders": ("o_orderkey",),
    "lineitem": ("l_orderkey", "l_linenumber"),
}


def adapt_query_for_clickhouse(sql: str) -> str:
    """Adapt standard SQL for ClickHouse compatibility."""
    sql = re.sub(
        r"INTERVAL\s+'(\d+)'\s+(DAY|MONTH|YEAR)",
        r"INTERVAL \1 \2",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


# ---------------------------------------------------------------------------
# Setup functions
# ---------------------------------------------------------------------------

def pg_setup(config, force_setup=False):
    dbname = config.db_name
    data_dir = config.data_dir

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

    schema_path = config.benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()

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

    schema_no_fk = strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            cur.execute(stmt)

    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        proc = subprocess.Popen(
            ["sed", "s/|$//", str(tbl_file)],
            stdout=subprocess.PIPE,
        )
        cur.copy_expert(f"COPY {table} FROM STDIN WITH (DELIMITER '|')", proc.stdout)
        proc.wait()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")

    cur.close()
    conn.close()


def duckdb_setup(config, force_setup=False):
    db_path = config.duckdb_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    data_dir = config.data_dir

    # Check if exists
    if not force_setup and db_path.exists():
        try:
            conn = duckdb.connect(str(db_path), read_only=True)
            count = conn.execute(f"SELECT COUNT(*) FROM {config.check_table}").fetchone()[0]
            conn.close()
            if count > 0:
                print(f"  Database '{db_path.name}' already exists with data, skipping setup.")
                print(f"  Use --setup flag to force data reload.")
                return
        except Exception:
            pass

    if force_setup and db_path.exists():
        print(f"  Removing existing database '{db_path.name}'...")
        db_path.unlink()

    print(f"  Creating persistent database '{db_path.name}'...")
    conn = duckdb.connect(str(db_path))

    schema_path = config.benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    schema_no_fk = strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            conn.execute(stmt)

    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        conn.execute(f"COPY {table} FROM '{tbl_file}' (DELIMITER '|')")
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f" {count:,} rows")

    conn.close()


def clickhouse_setup(config, force_setup=False):
    ch_dir = config.benchmark_root / "clickhouse"
    binary = ch_dir / "clickhouse"
    data_dir = config.data_dir

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
                print("  Use --setup flag to force data reload.")
                return
        except Exception:
            pass

    for table, cols in CLICKHOUSE_TABLES.items():
        order_key = CLICKHOUSE_ORDER_KEYS[table]
        ch_query(f"DROP TABLE IF EXISTS {table}")
        ch_query(f"CREATE TABLE {table} ({cols}) ENGINE = MergeTree() ORDER BY {order_key}")

    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        subprocess.run(
            f"sed 's/|$//' '{tbl_file}' | '{binary}' client --port 9000 "
            f"--query='INSERT INTO {table} FORMAT CSV' "
            f"--format_csv_delimiter='|'",
            shell=True, check=True, capture_output=True, timeout=600,
        )
        result = subprocess.run(
            [str(binary), "client", "--port", "9000", "-q",
             f"SELECT count() FROM {table}"],
            capture_output=True, text=True, timeout=30,
        )
        count = result.stdout.strip()
        print(f" {int(count):,} rows")


def umbra_setup(config, force_setup=False):
    data_dir = config.data_dir

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
                print("  Use --setup flag to force data reload.")
                cur.close()
                conn.close()
                return
        except Exception:
            conn.rollback()

    schema_path = config.benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    schema_no_fk = strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            try:
                cur.execute(f"DROP TABLE IF EXISTS {stmt.split()[2]}")
            except Exception:
                conn.rollback()
            cur.execute(stmt)

    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)
        try:
            cur.execute(
                f"COPY {table} FROM '/data/{table}.tbl' WITH (DELIMITER '|')"
            )
        except Exception:
            conn.rollback()
            with open(tbl_file, "r") as f:
                cleaned = io.StringIO()
                for line in f:
                    line = line.rstrip("\n")
                    if line.endswith("|"):
                        line = line[:-1]
                    cleaned.write(line + "\n")
                cleaned.seek(0)
                cur.copy_expert(f"COPY {table} FROM STDIN WITH (DELIMITER '|')", cleaned)
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")

    cur.close()
    conn.close()


def monetdb_install():
    """Install MonetDB via apt."""
    import shutil
    if shutil.which("monetdbd") or shutil.which("mserver5"):
        print("  MonetDB already installed.")
        return True
    print("  Installing MonetDB...")
    try:
        result = subprocess.run(
            ["lsb_release", "-cs"], capture_output=True, text=True, timeout=10,
        )
        suite = result.stdout.strip() if result.returncode == 0 else "focal"

        list_content = f"deb https://dev.monetdb.org/downloads/deb/ {suite} monetdb\n"
        subprocess.run(
            ["sudo", "tee", "/etc/apt/sources.list.d/monetdb.list"],
            input=list_content, capture_output=True, text=True, check=True, timeout=10,
        )
        subprocess.run(
            ["sudo", "bash", "-c",
             "wget -qO /etc/apt/trusted.gpg.d/monetdb.gpg "
             "https://dev.monetdb.org/downloads/MonetDB-GPG-KEY.gpg"],
            check=True, capture_output=True, timeout=30,
        )
        subprocess.run(
            ["sudo", "apt-get", "update", "-y"],
            check=True, capture_output=True, timeout=60,
        )
        subprocess.run(
            ["sudo", "apt-get", "install", "-y", "monetdb5-sql", "monetdb-client"],
            check=True, capture_output=True, timeout=120,
        )
        print("  MonetDB installed successfully.")
        return True
    except Exception as e:
        print(f"  MonetDB installation failed: {e}")
        return False


def monetdb_setup(config, force_setup=False):
    try:
        import pymonetdb
    except ImportError:
        print("  pymonetdb not installed. Run: pip install pymonetdb")
        return

    dbname = config.db_name
    data_dir = config.data_dir
    conn = pymonetdb.connect(database=dbname, hostname="localhost",
                             port=50000, username="monetdb", password="monetdb")
    conn.autocommit = False
    cur = conn.cursor()

    if not force_setup:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {config.check_table}")
            count = cur.fetchone()[0]
            if count > 0:
                print("  MonetDB already has data, skipping setup.")
                print("  Use --setup flag to force data reload.")
                cur.close()
                conn.close()
                return
        except Exception:
            conn.rollback()

    schema_path = config.benchmark_root / "schema.sql"
    schema_sql = schema_path.read_text()
    schema_no_fk = strip_fk_constraints(schema_sql)
    for stmt in schema_no_fk.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            table_name = stmt.split()[2]
            try:
                cur.execute(f"DROP TABLE {table_name}")
                conn.commit()
            except Exception:
                conn.rollback()
            cur.execute(stmt)
            conn.commit()

    tables_to_load = ["nation", "region", "supplier", "part", "partsupp", "customer", "orders", "lineitem"]
    tmp_dir = data_dir / "_monetdb_clean"
    tmp_dir.mkdir(exist_ok=True)

    for table in tables_to_load:
        tbl_file = data_dir / f"{table}.tbl"
        if not tbl_file.exists():
            print(f"  Warning: {tbl_file} not found, skipping")
            continue
        print(f"  Loading {table}...", end="", flush=True)

        clean_file = tmp_dir / f"{table}.tbl"
        if not clean_file.exists():
            subprocess.run(
                ["sed", "s/|$//", str(tbl_file)],
                stdout=open(str(clean_file), "w"),
                check=True, timeout=120,
            )

        abs_path = str(clean_file.resolve())
        cur.execute(
            f"COPY INTO {table} FROM '{abs_path}' "
            f"USING DELIMITERS '|','\\n','\"' NULL AS ''"
        )
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f" {count:,} rows")

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# get_config — builds WorkloadConfig from CLI args
# ---------------------------------------------------------------------------

def get_config(args) -> WorkloadConfig:
    benchmark_root = Path(__file__).parent
    sf = args.sf

    data_dir = args.data_dir
    if data_dir is None:
        data_dir = benchmark_root / "data" / f"sf{sf}"
    data_dir = data_dir.resolve()

    queries = get_queries(sf)

    if args.output is None:
        figures_dir = benchmark_root.parent / "figures" / "tpc-h"
        figures_dir.mkdir(parents=True, exist_ok=True)
        args.output = figures_dir / "benchmark_results_per_query.png"

    duckdb_dir = benchmark_root / "duckdb"
    duckdb_dir.mkdir(exist_ok=True)

    return WorkloadConfig(
        name="tpc-h",
        scale_label=f"SF={sf}",
        scale_value=sf,
        db_name=f"tpch_sf{sf}",
        check_table="lineitem",
        benchmark_root=benchmark_root,
        queries=queries,
        duckdb_path=duckdb_dir / f"tpch_sf{sf}.duckdb",
        umbra_port=5440,
        pk_columns=TPCH_PK_COLUMNS,
        pg_setup_fn=pg_setup,
        duckdb_setup_fn=duckdb_setup,
        clickhouse_setup_fn=clickhouse_setup,
        umbra_setup_fn=umbra_setup,
        monetdb_setup_fn=monetdb_setup,
        monetdb_install_fn=monetdb_install,
        clickhouse_tables=CLICKHOUSE_TABLES,
        clickhouse_order_keys=CLICKHOUSE_ORDER_KEYS,
        adapt_query_fn=adapt_query_for_clickhouse,
        data_dir=data_dir,
        skip_clickhouse=args.skip_clickhouse,
        skip_umbra=args.skip_umbra,
        skip_monetdb=args.skip_monetdb,
    )
