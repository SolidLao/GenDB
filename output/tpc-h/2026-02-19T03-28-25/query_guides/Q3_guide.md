# Q3 Guide

## Column Reference

### c_mktsegment (STRING, int16_t, dictionary-encoded)
- File: customer/c_mktsegment.bin (1500000 rows)
- Dictionary: customer/c_mktsegment_dict.txt (5 values: AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY)
- This query: Filter `c_mktsegment = 'BUILDING'` → find code for 'BUILDING' in dict, scan codes, keep positions where code matches
- Selectivity: ~20% of customers (estimated 300K rows)
- Example: If BUILDING → code 1, filter codes == 1

### c_custkey (INTEGER, int32_t)
- File: customer/c_custkey.bin (1500000 rows)
- This query: Join key with orders table (c_custkey = o_custkey)
- Index: customer/c_custkey_hash.idx (multi-value hash index for probe side)
- Build side: customer filtered rows (~300K)

### o_custkey (INTEGER, int32_t)
- File: orders/o_custkey.bin (15000000 rows)
- This query: Join key with customer table (o_custkey = c_custkey)
- Index: orders/o_custkey_hash.idx (hash index, 999982 unique keys)
- Hash lookup: Given c_custkey, find all orders with matching o_custkey
- Probe side: orders filtered rows (~8% selectivity → ~1.2M rows)

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15000000 rows)
- This query: Join key with lineitem (l_orderkey = o_orderkey)
- Index: orders/o_orderkey_hash.idx (multi-value hash index, 15M unique keys)
- After filtering on o_orderdate: ~1.2M qualified order keys

### o_orderdate (DATE, int32_t)
- File: orders/o_orderdate.bin (15000000 rows)
- This query: Filter `o_orderdate < DATE '1995-03-15'` → `raw < gendb::date_str_to_epoch_days("1995-03-15")`
- Epoch days for 1995-03-15: 9247 (days from 1970-01-01 to 1995-03-15)
- Selectivity: ~8% of orders (estimated 1.2M rows)
- Zone map: orders/o_orderdate_zone.idx (150 blocks, block_size=100K) can skip blocks where min >= 9247

### o_shippriority (INTEGER, int32_t)
- File: orders/o_shippriority.bin (15000000 rows)
- This query: Project l_orderkey, o_orderdate, o_shippriority in output (GROUP BY includes these)
- No filter; read for qualified orders only

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59986052 rows)
- This query: Join key with orders (l_orderkey = o_orderkey)
- Index: lineitem/l_orderkey_hash.idx (multi-value hash index, 15M unique keys)
- Probe side: lineitem rows matching qualified order keys (~85% selectivity of lineitem → ~51M rows)

### l_shipdate (DATE, int32_t)
- File: lineitem/l_shipdate.bin (59986052 rows)
- This query: Filter `l_shipdate > DATE '1995-03-15'` → `raw > 9247`
- Selectivity: ~85% of lineitem (estimated 51M rows)
- Zone map: lineitem/l_shipdate_zone.idx (600 blocks) can skip blocks where max <= 9247

### l_extendedprice (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_extendedprice.bin (59986052 rows)
- Stored values = SQL_value × 100
- This query: SUM(l_extendedprice * (1 - l_discount)) accumulates in scaled arithmetic
- Formula: `sum_raw = sum(l_extendedprice_code[i] * (100 - l_discount_code[i]) / 100)` (integer division)
- Output: Divide final sum by 100 for display

### l_discount (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_discount.bin (59986052 rows)
- Stored values = SQL_value × 100
- This query: Used in `(1 - l_discount / 100)` computation for revenue calculation
- Formula: `l_extendedprice * (100 - l_discount) / 100` keeps precision in scaled arithmetic

## Table Stats
| Table   | Rows      | Role      | Sort Order   | Block Size |
|---------|-----------|-----------|--------------|------------|
| customer| 1500000   | dimension | (none)       | 100000     |
| orders  | 15000000  | fact      | o_orderdate  | 100000     |
| lineitem| 59986052  | fact      | l_shipdate   | 100000     |

## Query Analysis
- Join pattern: customer (filtered 20%) → orders (filtered 8%) → lineitem (filtered 85%)
- Filter pushdown: Apply c_mktsegment and o_orderdate filters before probing lineitem
- Estimated qualifying rows: 1.5M × 0.20 × 0.08 × 59.9M × 0.85 ≈ ~15M lineitem rows qualify
- Aggregation: GROUP BY (l_orderkey, o_orderdate, o_shippriority) → ~50K-150K groups (from workload_analysis)
- Ordering: ORDER BY revenue DESC (external sort on computed aggregate)
- LIMIT 10: Top-K optimization; use partial sort or heap to get first 10 rows only

## Indexes

### Hash Index: customer/c_custkey_hash.idx
- Multi-value hash: Maps c_custkey → positions of rows in filtered result
- Usage: Build from customer after filtering on c_mktsegment; ~300K rows
- Cardinality: ~300K unique customer keys (high selectivity join)
- This query: Build customer hash table from filtered rows, probe with o_custkey values

### Hash Index: orders/o_custkey_hash.idx
- Multi-value hash: Maps o_custkey → list of order row positions
- Capacity: 2097152 (for 999982 unique keys, load factor ~0.5)
- Cardinality: 999982 unique values in 15M rows
- This query: Probe this index for each filtered customer key to find matching orders
- Estimated hits: ~1.2M order rows (average ~1.2 orders per customer in filtered set)

### Hash Index: orders/o_orderkey_hash.idx
- Multi-value hash: Maps o_orderkey → list of order row positions
- Capacity: 33554432 (for 15M unique keys)
- This query: After filtering orders (8% selectivity), probe lineitem with these orderkeys
- Probe selectivity: ~1.2M / 15M = 8%

### Hash Index: lineitem/l_orderkey_hash.idx
- Multi-value hash: Maps l_orderkey → list of lineitem row positions
- Capacity: 33554432 (for 15M unique keys, since many orders have multiple lineitems)
- This query: Final probe; for each qualified o_orderkey, find all matching lineitem rows
- Expected result: ~51M lineitem rows (85% × 59.9M) join to ~1.2M orders → ~12-15M lineitem rows output

### Zone Map: orders/o_orderdate_zone.idx
- 150 blocks, block_size=100K
- This query: Filter blocks where all values >= 1995-03-15 (epoch 9247); skip these blocks entirely
- Expected skip: ~50% of blocks (orders from 1995-03-15 onward)

### Zone Map: lineitem/l_shipdate_zone.idx
- 600 blocks, block_size=100K
- This query: Filter blocks where all values <= 1995-03-15; skip these blocks
- Expected skip: ~20-30% of blocks (lineitem from 1992-1995 early part)

## Performance Notes
- Multi-table hash join: Build customer hash from filtered set (~300K rows), probe with orders (1.2M matches)
- Then build filtered orders hash, probe lineitem with ~1.2M order keys
- Parallelism: Split large scans (orders, lineitem) across 64 cores
- Top-K: Use min-heap to track top 10 by revenue; avoid full external sort
