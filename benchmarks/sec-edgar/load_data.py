#!/usr/bin/env python3
"""Load SEC EDGAR data into DuckDB.

Reads quarterly TSV files from data/ directory, deduplicates tags, and produces a persistent DuckDB database.

Usage:
    python3 benchmarks/sec-edgar/load_data.py [--years 3] [--data-dir PATH]
"""

import argparse
import re
import sys
from pathlib import Path

import duckdb

# Columns to SELECT from each CSV file (subset of the full file columns).
# These must match the CREATE TABLE columns in schema.sql exactly.
TABLE_COLUMNS = {
    "sub": "adsh, cik, name, sic, countryba, stprba, cityba, countryinc, "
           "form, period, fy, fp, filed, accepted, prevrpt, nciks, afs, wksi, fye, instance",
    "num": "adsh, tag, version, ddate, qtrs, uom, coreg, value, footnote",
    "pre": "adsh, report, line, stmt, inpth, rfile, tag, version, plabel, negating",
    "tag": "tag, version, custom, abstract, datatype, iord, crdr, tlabel, doc",
}


def export_csv(con, data_dir, years):
    """Export merged CSV files for GenDB consumption."""
    export_dir = data_dir / f"sf{years}"
    export_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n=== Exporting CSV files to {export_dir} ===")
    for table in ["sub", "num", "tag", "pre"]:
        csv_path = export_dir / f"{table}.csv"
        con.execute(f"COPY {table} TO '{csv_path}' (FORMAT CSV, HEADER true)")
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        print(f"  {table}.csv: {csv_size:.1f} MB")
    print(f"  Export directory: {export_dir}")


def main():
    parser = argparse.ArgumentParser(description="Load SEC EDGAR data into DuckDB")
    parser.add_argument("--years", type=int, default=3, help="Number of years (default: 3)")
    parser.add_argument("--data-dir", type=Path, default=None, help="Path to extracted data")
    parser.add_argument("--force", action="store_true", help="Force reload (drop existing DB)")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    data_dir = args.data_dir or (script_dir / "data")
    db_dir = script_dir / "duckdb"
    db_dir.mkdir(exist_ok=True)
    db_path = db_dir / "sec_edgar.duckdb"

    if not data_dir.exists():
        print(f"Error: data directory not found: {data_dir}")
        sys.exit(1)

    # Check for existing database
    if db_path.exists() and not args.force:
        try:
            con = duckdb.connect(str(db_path), read_only=True)
            count = con.execute("SELECT COUNT(*) FROM num").fetchone()[0]
            if count > 0:
                print(f"Database already exists at {db_path} with {count:,} num rows.")
                print("Use --force to rebuild.")
                # Still export CSV files if needed
                export_csv(con, data_dir, args.years)
                con.close()
                return
            con.close()
        except Exception:
            pass

    if db_path.exists() and args.force:
        print(f"Removing existing database: {db_path}")
        db_path.unlink()

    # Find quarter directories
    quarter_dirs = sorted([
        d for d in data_dir.iterdir()
        if d.is_dir() and d.name[0:4].isdigit() and "q" in d.name
    ])
    if not quarter_dirs:
        print(f"Error: no quarter directories found in {data_dir}")
        print("Run: bash benchmarks/sec-edgar/setup_data.sh")
        sys.exit(1)

    print(f"Found {len(quarter_dirs)} quarters: {', '.join(d.name for d in quarter_dirs)}")
    print(f"Database: {db_path}")

    con = duckdb.connect(str(db_path))

    # Create tables from schema.sql (strip FK constraints)
    schema_path = script_dir / "schema.sql"
    schema_sql = schema_path.read_text()
    # Strip comments, FK constraints, and dangling commas
    schema_clean = re.sub(r"--[^\n]*", "", schema_sql)
    schema_clean = re.sub(r",?\s*FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s+\w+\([^)]+\)", "", schema_clean)
    schema_clean = re.sub(r"\s*REFERENCES\s+\w+\([^)]+\)", "", schema_clean)
    schema_clean = re.sub(r",\s*\)", "\n)", schema_clean)
    for stmt in schema_clean.split(";"):
        stmt = stmt.strip()
        if stmt and stmt.upper().startswith("CREATE"):
            con.execute(stmt)

    # Helper to read a TSV file as a DuckDB relation (all columns)
    def read_tsv(path):
        return f"read_csv('{path}', delim='\\t', header=true, quote='', ignore_errors=true, null_padding=true)"

    # Load sub, num, pre tables (append across quarters, selecting only schema columns)
    for table in ["sub", "num", "pre"]:
        cols = TABLE_COLUMNS[table]
        print(f"\nLoading {table}...")
        total_rows = 0
        for qdir in quarter_dirs:
            txt_file = qdir / f"{table}.txt"
            if not txt_file.exists():
                print(f"  Warning: {txt_file} not found, skipping")
                continue
            try:
                con.execute(f"""
                    INSERT INTO {table}
                    SELECT {cols} FROM {read_tsv(txt_file)}
                """)
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                added = count - total_rows
                total_rows = count
                print(f"  {qdir.name}: +{added:,} rows (total: {total_rows:,})")
            except Exception as e:
                print(f"  {qdir.name}: ERROR - {e}")
        print(f"  {table} total: {total_rows:,} rows")

    # Load tag table with deduplication (standard tags repeat across quarters)
    print(f"\nLoading tag (with deduplication)...")
    cols = TABLE_COLUMNS["tag"]
    con.execute(f"CREATE TEMP TABLE tag_staging AS SELECT * FROM tag LIMIT 0")
    for qdir in quarter_dirs:
        txt_file = qdir / "tag.txt"
        if not txt_file.exists():
            continue
        try:
            con.execute(f"""
                INSERT INTO tag_staging
                SELECT {cols} FROM {read_tsv(txt_file)}
            """)
        except Exception as e:
            print(f"  {qdir.name}: ERROR - {e}")
    con.execute("""
        INSERT INTO tag
        SELECT DISTINCT ON (tag, version) *
        FROM tag_staging
    """)
    tag_count = con.execute("SELECT COUNT(*) FROM tag").fetchone()[0]
    con.execute("DROP TABLE tag_staging")
    print(f"  tag total: {tag_count:,} unique (tag, version) pairs")

    # Drop rows with NULL values in critical columns
    print("\nCleaning data...")
    before = con.execute("SELECT COUNT(*) FROM num").fetchone()[0]
    con.execute("DELETE FROM num WHERE value IS NULL")
    after = con.execute("SELECT COUNT(*) FROM num").fetchone()[0]
    print(f"  num: removed {before - after:,} rows with NULL value ({after:,} remaining)")

    # Final summary
    print("\n=== Database Summary ===")
    for table in ["sub", "num", "tag", "pre"]:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    db_size = db_path.stat().st_size / (1024 * 1024)
    print(f"\n  Database size: {db_size:.1f} MB")
    print(f"  Database path: {db_path}")

    export_csv(con, data_dir, args.years)

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
