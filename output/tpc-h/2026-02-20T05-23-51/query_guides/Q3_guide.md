# Q3 Guide — Shipping Priority

## Query
```sql
SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate  > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
```

## Column Reference

### c_mktsegment (STRING, int8_t, dictionary-encoded)
- File: `customer/c_mktsegment.bin` (1,500,000 × 1 byte)
- Dictionary file: `customer/c_mktsegment_dict.txt` (5 entries, one per line)
  - Code 0 = "AUTOMOBILE", Code 1 = "BUILDING", Code 2 = "FURNITURE"
  - Code 3 = "HOUSEHOLD",  Code 4 = "MACHINERY"
- **This query**: `c_mktsegment = 'BUILDING'` → `col[row] == 1`
- Load dict at runtime: `std::vector<std::string> dict; read lines from dict file`
- Filter: `c_mktsegment[row] == 1` (code for "BUILDING")
- Selectivity: ~22% (1 in 5 segments) → ~330,000 qualifying customers

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 × 4 bytes)
- Row i corresponds to the same customer as c_mktsegment[i], c_name[i], etc.
- **This query**: Used as join key: `c_custkey = o_custkey`
- After filtering BUILDING customers, collect their custkeys into a hash set for semi-join with orders.

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 × 4 bytes)
- **This query**: Join `o_custkey = c_custkey` (filter orders whose customer is BUILDING)
- Index available: `indexes/orders_custkey_hash.bin` (999,982 unique customer keys in orders)

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes)
- Encoding: epoch days since 1970-01-01.
- **This query**: `o_orderdate < DATE '1995-03-15'`
  - Epoch(1995-03-15): 1995-01-01=9131, +31(Jan)+28(Feb)+14(Mar-1)=73 → **9204**
  - C++ predicate: `o_orderdate[row] < 9204`
  - Selectivity: ~49% of orders (dates range 1992-1998; 1995-03-15 is ~midpoint)
  - Workload analysis estimate: 31% selectivity

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes)
- **This query**: Join key `l_orderkey = o_orderkey`. Output column.
- Index: `indexes/orders_orderkey_hash.bin` (PK, 15M unique keys, cap=33,554,432)

### o_shippriority (INTEGER, int32_t)
- File: `orders/o_shippriority.bin` (15,000,000 × 4 bytes)
- **This query**: Output column only (GROUP BY, SELECT).

### o_totalprice — NOT USED in Q3. Do not load.

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes)
- **This query**: Join key `l_orderkey = o_orderkey`. Also output + GROUP BY column.
- Index: `indexes/lineitem_orderkey_hash.bin` (15M unique keys among 60M rows)
  - Use: for qualifying order keys, look up lineitem rows via this hash

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes)
- **This query**: `l_shipdate > DATE '1995-03-15'`
  - Epoch(1995-03-15) = **9204** (same date as o_orderdate cutoff)
  - C++ predicate: `l_shipdate[row] > 9204`
  - Selectivity: ~68% of lineitem rows have shipdate > 1995-03-15

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes)
- **This query**: `SUM(l_extendedprice * (1 - l_discount))` → revenue per group

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes)
- **This query**: Used in `(1 - l_discount)` expression. Range 0.00–0.10.

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| lineitem | 59,986,052 | fact      | none       | 100,000    |
| orders   | 15,000,000 | fact      | none       | 100,000    |
| customer | 1,500,000  | dimension | none       | 100,000    |

## Query Analysis
- **Join pattern**: customer → orders → lineitem (dimension-to-fact)
  - Build: hash set of qualifying c_custkey (after BUILDING filter: ~330K keys)
  - Probe: scan orders, filter `o_orderdate < 9204` AND `o_custkey IN hash_set`
  - Qualifying orders: ~22% × 31% × 15M ≈ ~1M orders
  - For qualifying orders, look up lineitem rows via lineitem_orderkey_hash and filter `l_shipdate > 9204`
- **Filter selectivities** (from workload analysis):
  - `c_mktsegment = 'BUILDING'`: 22% of customers
  - `o_orderdate < 1995-03-15`: 31% of orders
  - `l_shipdate > 1995-03-15`: 68% of lineitem rows
  - Combined after joins: ~0.22 × 0.31 × 0.68 × 60M ≈ ~2.8M lineitem rows qualify
- **Aggregation**: GROUP BY (l_orderkey, o_orderdate, o_shippriority) → ~3,000 groups
  - Use hash map keyed by l_orderkey (since o_orderdate and o_shippriority are determined by l_orderkey via the join)
- **Output**: Top 10 by revenue DESC, o_orderdate ASC → partial sort (top-10 heap)
- **Subquery**: None
- **Recommended execution order**:
  1. Scan customer (1.5M), collect BUILDING custkeys into hash set (~330K entries)
  2. Scan orders (15M), filter by date AND custkey hash set → qualifying order set (~1M)
  3. For each qualifying order, use lineitem_orderkey_hash to find its lineitem rows
  4. Filter lineitems by l_shipdate > 9204, compute revenue contribution
  5. Top-10 heap on (revenue DESC, o_orderdate ASC)

## Indexes

### lineitem_orderkey_hash (hash on l_orderkey, multi-value)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout:
  ```
  [uint32_t num_positions = 59,986,052]
  [uint32_t positions[59986052]]          // row indices grouped by l_orderkey value
  [uint32_t capacity = 33,554,432]
  [struct HEntry32 {int32_t key; uint32_t offset; uint32_t count;} × 33554432]
  // Empty slot: key == INT32_MIN (-2147483648)
  ```
- Hash function: `uint32_t h = (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (cap-1)`
- Lookup for order_key k:
  ```cpp
  uint32_t h = hash32(k) & (cap-1);
  while (ht[h].key != INT32_MIN && ht[h].key != k) h = (h+1) & (cap-1);
  if (ht[h].key == k) {
      // rows: positions[ht[h].offset .. ht[h].offset + ht[h].count - 1]
  }
  ```
- row_offset is a ROW index. Byte offset into l_extendedprice.bin = row_idx × 8.

### orders_orderkey_hash (hash on o_orderkey, single-value PK)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: same as hash_int32 format but count=1 for every entry (PK)
  ```
  [uint32_t num_positions = 15,000,000]
  [uint32_t positions[15000000]]
  [uint32_t capacity = 33,554,432]
  [struct HEntry32 {int32_t key; uint32_t offset; uint32_t count=1;} × 33554432]
  ```
- Use to map o_orderkey → row index in orders table
- row_offset → byte offset in orders columns: row × 4 (for int32 cols) or row × 8 (for double cols)

### orders_orderdate_zonemap (zone_map on o_orderdate)
- File: `indexes/orders_orderdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=150][{int32_t min, int32_t max, uint32_t block_size} × 150]`
- **This query**: Skip block if `zm[b].min >= 9204` (entire block has orderdate >= cutoff, skip)
- Since orders are in natural (o_orderkey) order, dates are roughly sorted → zone maps may prune ~50% of blocks

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][{int32_t min, int32_t max, uint32_t block_size} × 600]`
- **This query**: Skip block if `zm[b].max <= 9204` (all dates ≤ cutoff, none qualify)
- Less useful here since lineitem is accessed via the orderkey hash index (not full scan)
