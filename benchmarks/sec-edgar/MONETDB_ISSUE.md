## MonetDB Excluded from SEC EDGAR Benchmark

**Status:** MonetDB is excluded from this benchmark due to memory explosion on correlated subqueries.

**MonetDB version tested:** v11.55.1 (Dec2025) — the latest release.

**What works:** Data loading completes correctly. All 4 tables load with correct row counts (sub: 86K, tag: 1.07M, num: 39.4M, pre: 9.6M). Farm size is 3.4 GB — reasonable.

**What fails:** Query execution. Many SEC EDGAR queries use `EXISTS` subqueries joining `num` (39.4M rows) with `pre` (9.6M rows) on `(tag, version)` without indexes. MonetDB's optimizer fails to flatten these correlated subqueries into efficient joins, instead materializing huge intermediates. Memory grows to 200+ GB and the server eventually crashes.

**Root cause:** This is a known MonetDB limitation:
- MonetDB's query planner cannot always push filters down to source tables (https://github.com/MonetDB/MonetDB/issues/7301), causing subqueries to be fully materialized.
- `gdk_mem_maxsize` is only a soft limit — MonetDB can exceed it (https://www.monetdb.org/documentation/admin-guide/system-resources/memory-footprint/).
- MonetDB's performance tips recommend rewriting complex subqueries using CTEs (https://www.monetdb.org/documentation-Mar2025/admin-guide/performance-tips/), but the benchmark uses identical SQL across all systems for fairness.

**Other systems handle these queries fine:** PostgreSQL, DuckDB, ClickHouse, and Umbra all complete the full 25-query benchmark without memory issues.
