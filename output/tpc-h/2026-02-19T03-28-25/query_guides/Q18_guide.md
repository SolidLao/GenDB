# Q18 Guide

## Column Reference

### c_name (VARCHAR, std::string)
- File: customer/c_name.bin (1500000 rows)
- This query: Output projection; retrieve customer name for qualifying rows
- Cardinality: 1.5M unique (high cardinality, NOT dictionary-encoded)
- Load strings for qualified customers only (late materialization)

### c_custkey (INTEGER, int32_t)
- File: customer/c_custkey.bin (1500000 rows)
- This query: Join key with orders (c_custkey = o_custkey)
- Index: customer/c_custkey_hash.idx (not heavily used, can linear scan for small result set)

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15000000 rows)
- This query: Primary key; used to filter by subquery result set
- Subquery generates set of ~50K qualified order keys
- Main query: Filter orders.o_orderkey IN (subquery_result)

### o_orderdate (DATE, int32_t)
- File: orders/o_orderdate.bin (15000000 rows)
- This query: Output projection and ORDER BY key
- For qualified orders: retrieve o_orderdate

### o_totalprice (DECIMAL, int64_t, scale_factor=100)
- File: orders/o_totalprice.bin (15000000 rows)
- Stored values = SQL_value × 100
- This query: Output projection and ORDER BY key (o_totalprice DESC)
- For ~50K qualified orders: retrieve and sort by price

### o_custkey (INTEGER, int32_t)
- File: orders/o_custkey.bin (15000000 rows)
- This query: Join key with customer (o_custkey = c_custkey)
- Index: orders/o_custkey_hash.idx (multi-value hash, 999982 unique keys)
- Usage: For each order in subquery result (~50K), find customer via o_custkey

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59986052 rows)
- This query: Join key with orders (o_orderkey = l_orderkey)
- Subquery: GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
- Index: lineitem/l_orderkey_hash.idx (multi-value hash, 15M unique keys)
- Two-phase: (1) Subquery evaluates all lineitem; (2) Main query probes with subquery result

### l_quantity (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_quantity.bin (59986052 rows)
- Stored values = SQL_value × 100 (e.g., SQL 5 → stored 500)
- This query: (1) Subquery: SUM(l_quantity) with HAVING SUM > 300 → filter to thresholded quantity 30000
  - (2) Main query: SUM(l_quantity) for output; divide by 100 for display
- Formula: `sum_qty_raw = sum(l_quantity_code[i])` for final output

## Table Stats
| Table   | Rows      | Role      | Sort Order | Block Size |
|---------|-----------|-----------|------------|------------|
| customer| 1500000   | dimension | (none)     | 100000     |
| orders  | 15000000  | fact      | o_orderdate| 100000     |
| lineitem| 59986052  | fact      | l_shipdate | 100000     |

## Query Analysis
- Two-phase execution: (1) Subquery materialization; (2) Main query join + aggregation
- Subquery: GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
  - Scan all 60M lineitem rows
  - Hash aggregation: GROUP BY l_orderkey (15M unique groups)
  - Filter: HAVING SUM > 300 (scaled: > 30000) → ~50K qualifying orders (~0.33% selectivity)
  - Materialize to hash set or vector of orderkeys
- Main query: Filter orders by subquery result (~50K of 15M)
  - Hash semi-join: Use subquery result set as filter
  - Join customer-orders-lineitem
  - Final aggregation: GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
- Output: ~50K rows, sorted by o_totalprice DESC, o_orderdate ASC, LIMIT 100

## Indexes

### Hash Index: orders/o_custkey_hash.idx
- Multi-value hash: Maps o_custkey → order row positions
- Cardinality: 999982 unique keys in 15M rows
- This query: For each subquery-qualified order, lookup customer via o_custkey

### Hash Index: orders/o_orderkey_hash.idx
- Multi-value hash: Maps o_orderkey → order row positions
- Cardinality: 15M unique keys (most orders are unique)
- This query: Used implicitly to validate/filter orders against subquery result

### Hash Index: lineitem/l_orderkey_hash.idx
- Multi-value hash: Maps l_orderkey → lineitem row positions
- Cardinality: 15M unique keys
- Subquery: Used to aggregate l_quantity per orderkey
- Main query: For each qualified order, find lineitem rows

## Performance Notes
- Subquery optimization: Two-phase approach avoids nested subquery re-evaluation
  - Phase 1: Scan lineitem (60M), partial aggregation per thread, merge to get ~50K qualified orders
  - Phase 2: Use semi-join to filter 15M orders down to ~50K, then join with customer/lineitem
- Materialization: Subquery result (~50K orderkeys) stored in compact set (e.g., sorted vector or hash set) for O(log N) or O(1) lookup
- Semi-join: Test `o_orderkey IN subquery_result` to eliminate orders before main joins
- Hash join: Build customer hash (1.5M rows), probe with ~50K qualified orders
- Late materialization: Load customer names only for ~50K qualifying rows (vs all 1.5M)
- Sort & Top-K: Sort ~50K rows by o_totalprice DESC, o_orderdate ASC, extract top 100
- Parallelism:
  - Subquery: Partial aggregation per thread on lineitem scan (60M / 64 cores ≈ 1M per thread)
  - Main query: Scans are small enough (50K orders) that serial execution may be faster than parallel overhead
- Memory: Subquery result (50K orderkeys) fits in L2/L3 cache
