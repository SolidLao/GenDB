# Q9 Guide

## Column Reference

### p_name (VARCHAR, std::string)
- File: part/p_name.bin (2000000 rows)
- This query: Filter `p_name LIKE '%green%'` → substring match
- Selectivity: ~5% (estimated from workload_analysis.json → ~100K rows)
- Implementation: Load strings for part table, scan for substring; mark qualifying p_partkey values
- Semi-join optimization: Pre-filter part table by p_name, then join with lineitem via p_partkey

### p_partkey (INTEGER, int32_t)
- File: part/p_partkey.bin (2000000 rows)
- This query: Join key with lineitem and partsupp
- Index: part/p_partkey_hash.idx (multi-value hash, 2M unique keys)
- After p_name filter: ~100K qualified partkeys

### l_partkey (INTEGER, int32_t)
- File: lineitem/l_partkey.bin (59986052 rows)
- This query: Join key with part (l_partkey = p_partkey) and partsupp (ps_partkey)
- After filtering via part semi-join: only lineitem rows with qualified p_partkey (~5% → ~3M rows)

### l_suppkey (INTEGER, int32_t)
- File: lineitem/l_suppkey.bin (59986052 rows)
- This query: Join key with supplier and partsupp
- After joining with filtered part: ~3M rows to join with supplier

### l_quantity (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_quantity.bin (59986052 rows)
- Stored values = SQL_value × 100
- This query: Expression `l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`
- Formula: `(l_ext_raw × (100 - l_disc_raw)) / 10000 - (ps_cost_raw × l_qty_raw) / 10000`

### l_extendedprice (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_extendedprice.bin (59986052 rows)
- Stored values = SQL_value × 100
- This query: Used in revenue expression `l_extendedprice * (1 - l_discount)`

### l_discount (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_discount.bin (59986052 rows)
- Stored values = SQL_value × 100
- This query: Used in revenue expression `(1 - l_discount / 100)`

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59986052 rows)
- This query: Join key with orders (o_orderkey = l_orderkey)
- Index: lineitem/l_orderkey_hash.idx (multi-value hash, 15M unique keys)
- Purpose: Retrieve o_orderdate for EXTRACT(YEAR FROM o_orderdate) computation

### s_suppkey (INTEGER, int32_t)
- File: supplier/s_suppkey.bin (100000 rows)
- This query: Join key with lineitem (l_suppkey = s_suppkey)
- Cardinality: 100K unique supplier keys

### s_nationkey (INTEGER, int32_t)
- File: supplier/s_nationkey.bin (100000 rows)
- This query: Join key with nation (s_nationkey = n_nationkey)
- Index: supplier/s_nationkey_hash.idx (multi-value hash, 25 unique keys)
- Used for GROUP BY projection (need nation name)

### ps_partkey (INTEGER, int32_t)
- File: partsupp/ps_partkey.bin (8000000 rows)
- This query: Join key with part (ps_partkey = p_partkey) and lineitem (l_partkey)
- Index: partsupp/ps_partkey_hash.idx (multi-value hash, 2M unique keys)
- After filtering: ~100K qualified partkeys (from p_name filter)

### ps_suppkey (INTEGER, int32_t)
- File: partsupp/ps_suppkey.bin (8000000 rows)
- This query: Join key with supplier (ps_suppkey = s_suppkey) and lineitem (l_suppkey)
- Index: partsupp/ps_suppkey_hash.idx (multi-value hash, 100K unique keys)

### ps_supplycost (DECIMAL, int64_t, scale_factor=100)
- File: partsupp/ps_supplycost.bin (8000000 rows)
- Stored values = SQL_value × 100
- This query: Expression `ps_supplycost * l_quantity` for cost computation

### n_nationkey (INTEGER, int32_t)
- File: nation/n_nationkey.bin (25 rows)
- This query: Join key with supplier (s_nationkey = n_nationkey)
- Cardinality: 25 unique nations

### n_name (STRING, int16_t, dictionary-encoded)
- File: nation/n_name.bin (25 rows, encoded as int16_t codes)
- Dictionary: nation/n_name_dict.txt (25 nation names)
- This query: GROUP BY projection; output nation names in result
- Load dict at startup; decode codes to strings for output

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15000000 rows)
- This query: Join key with lineitem (l_orderkey = o_orderkey)
- Index: orders/o_orderkey_hash.idx (hash index, 15M unique keys)

### o_orderdate (DATE, int32_t)
- File: orders/o_orderdate.bin (15000000 rows)
- This query: Extract YEAR from o_orderdate for GROUP BY
- SQL: EXTRACT(YEAR FROM DATE) → C++ computation: `(epoch_days / 365.25) + 1970` or use lookup table

## Table Stats
| Table   | Rows      | Role      | Sort Order | Block Size |
|---------|-----------|-----------|------------|------------|
| nation  | 25        | dimension | (none)     | 25         |
| region  | 5         | dimension | (none)     | 5          |
| part    | 2000000   | dimension | (none)     | 100000     |
| supplier| 100000    | dimension | (none)     | 10000      |
| partsupp| 8000000   | bridge    | (none)     | 100000     |
| orders  | 15000000  | fact      | o_orderdate| 100000     |
| lineitem| 59986052  | fact      | l_shipdate | 100000     |

## Query Analysis
- Join ordering (smallest first): nation (25) → supplier (100K) → part (2M filtered to 100K by p_name) → partsupp (8M) → lineitem (60M) → orders (15M)
- Filter p_name LIKE '%green%': ~5% selectivity → ~100K part rows
- Semi-join on part: Use part as build side, probe lineitem to find matching line items (~5% → ~3M rows)
- Multi-way join: part-lineitem-supplier-partsupp-orders-nation (6 tables)
- Aggregation: GROUP BY (nation, o_year) → ~25 nations × ~7-8 years ≈ ~200 groups
- Output: ~200 rows, sorted by nation ASC, o_year DESC

## Indexes

### Hash Index: part/p_partkey_hash.idx
- Multi-value hash: Maps p_partkey → row positions
- Cardinality: 2M unique keys (most part table is unique by partkey)
- This query: After p_name filter, use to rapidly find partsupp matches

### Hash Index: partsupp/ps_partkey_hash.idx
- Multi-value hash: Maps ps_partkey → partsupp row positions
- Cardinality: 2M unique keys
- This query: For each qualified partkey from part, find matching partsupp rows

### Hash Index: partsupp/ps_suppkey_hash.idx
- Multi-value hash: Maps ps_suppkey → partsupp row positions
- Cardinality: 100K unique keys
- This query: For each supplier, find matching partsupp rows

### Hash Index: supplier/s_nationkey_hash.idx
- Multi-value hash: Maps s_nationkey → supplier row positions
- Cardinality: 25 unique keys (nations)
- This query: For each nation, find matching suppliers (small table, hash may not be optimal)

## Performance Notes
- Join strategy: Semi-join on part by p_name (cheapest filter), then hash joins for FK columns
- Join order: part (filtered) → partsupp → supplier → orders → nation
- Materialization: For each part row matching p_name filter, scan lineitem for l_partkey matches, then probe partsupp/supplier/orders
- Parallelism: Partition lineitem rows across threads, each thread independently traces through join graph
- Aggregation: Hash aggregation with 200 groups (small enough for single hash table in memory)
- Output: Sort by nation, o_year DESC (only 200 rows, external sort optional)
