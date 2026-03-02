"""WorkloadConfig dataclass — shared configuration for all benchmark modules."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional


@dataclass
class WorkloadConfig:
    name: str                    # "tpc-h" or "sec-edgar"
    scale_label: str             # "SF=10" or "Years=3"
    scale_value: int             # 10 or 3
    db_name: str                 # "tpch_sf10" or "sec_edgar_3y"
    check_table: str             # "lineitem" or "num"
    benchmark_root: Path         # benchmarks/tpc-h/ or benchmarks/sec-edgar/
    queries: dict                # {"Q1": "SELECT...", ...}
    duckdb_path: Path            # path to .duckdb file
    umbra_port: int              # 5440 or 5441
    pk_columns: dict             # {table: (col,...)} for index dedup

    # Callbacks — workload-specific setup functions
    pg_setup_fn: Callable        # pg_setup(config, force_setup)
    clickhouse_setup_fn: Callable  # clickhouse_setup(config, force_setup)
    umbra_setup_fn: Callable     # umbra_setup(config, force_setup)
    monetdb_setup_fn: Callable   # monetdb_setup(config, force_setup)
    monetdb_install_fn: Callable # monetdb_install()

    # ClickHouse specifics
    clickhouse_tables: dict      # {table: col_defs_string}
    clickhouse_order_keys: dict
    clickhouse_settings: dict = field(default_factory=dict)  # extra CH client settings

    # Query adaptation for ClickHouse (identity for EDGAR)
    adapt_query_fn: Callable = field(default=lambda sql: sql)

    # Custom index parsing (EDGAR filters row_id, deduplicates)
    parse_indexes_fn: Optional[Callable] = None

    # DuckDB setup (TPC-H has one, EDGAR doesn't need it)
    duckdb_setup_fn: Optional[Callable] = None

    # TPC-H only
    data_dir: Optional[Path] = None  # path to .tbl files (None for EDGAR)

    # Extra _strip_fk_constraints transform (EDGAR adds DOUBLE->DOUBLE PRECISION)
    strip_fk_extra_fn: Optional[Callable] = None

    # Skip flags (EDGAR has --skip-postgres, --skip-duckdb)
    skip_postgres: bool = False
    skip_duckdb: bool = False
    skip_clickhouse: bool = False
    skip_umbra: bool = False
    skip_monetdb: bool = False
