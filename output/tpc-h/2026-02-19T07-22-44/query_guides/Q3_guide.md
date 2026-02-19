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
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
```

## Column Reference

### c_mktsegment (STRING, int8_t, dictionary-encoded)
- File: `customer/c_mktsegment.bin` (1,500,000 rows × 1 byte)
- Dictionary: `customer/c_mktsegment_dict.txt` — load as `std::vector<std::string>`
- Actual dict contents: `0="BUILDING"`, `1="FURNITURE"`, `2="AUTOMOBILE"`, `3="MACHINERY"`, `4="HOUSEHOLD"`
- This query: `c_mktsegment = 'BUILDING'` → load dict, find code where entry == "BUILDING" → code=0; filter rows where `c_mktsegment[i] == 0`
- Selectivity: 20% → ~300,000 qualifying customers

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 rows × 4 bytes)
- This query: used as join key `c_custkey = o_custkey`. Build an `unordered_set<int32_t>` of qualifying c_custkey values after filtering on c_mktsegment.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- **orders is physically sorted by o_orderdate ascending**
- This query: `o_orderdate < DATE '1995-03-15'` → `raw < 9204`
  - Derivation: YEAR_START[1995-1970=25]=9131; months Jan-Feb = 31+28=59; day offset = 15-1 = 14; total = 9131+59+14 = **9204**
- Selectivity: 48% → ~7,200,000 qualifying orders
- Zone map: skip blocks with `block_min >= 9204` (trailing blocks in the sorted file)

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- Used as join key `l_orderkey = o_orderkey`; output column.
- Hash index available: `orders_orderkey_hash.bin` maps o_orderkey → row position in orders.

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 rows × 4 bytes)
- Join key with customer: `c_custkey = o_custkey`.

### o_shippriority (INTEGER, int32_t)
- File: `orders/o_shippriority.bin` (15,000,000 rows × 4 bytes)
- Output column. In TPC-H all values = 0, but store/output as-is.

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes)
- **lineitem is physically sorted by l_shipdate ascending**
- This query: `l_shipdate > DATE '1995-03-15'` → `raw > 9204`
  - Same epoch constant as o_orderdate threshold: **9204**
- Selectivity: 52% → ~31,192,955 qualifying lineitems
- Zone map: skip blocks with `block_max <= 9204` (leading blocks before cutoff)

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes)
- Join key with orders: `l_orderkey = o_orderkey`; output column.
- Multi-value hash index available: `lineitem_orderkey_hash.bin`.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. No scaling needed.
- Used in: `SUM(l_extendedprice*(1-l_discount))`

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. No scaling needed.

## Table Stats
| Table    | Rows       | Role      | Sort Order   | Block Size |
|----------|------------|-----------|--------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate↑  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate↑ | 100,000    |
| customer | 1,500,000  | dimension | none         | 100,000    |

## Query Analysis
- **Join pattern**: 3-way join `customer ⋈ orders ⋈ lineitem`
  - customer (1.5M) → orders (15M) → lineitem (60M)
  - Cardinality ratios: customer:orders = 1:10, orders:lineitem = 1:4
  - Build sides (smaller): customer builds hash → probe orders; filtered orders build hash → probe lineitem
- **Filter selectivities** (from workload analysis):
  - `c_mktsegment = 'BUILDING'`: 20% → 300,000 customers
  - `o_orderdate < 9204`: 48% of orders = 7,200,000 orders (zone map skips ~52% of blocks)
  - `l_shipdate > 9204`: 52% of lineitem = 31,200,000 rows (zone map skips ~48% of blocks)
  - After all three filters and joins: estimated ~2,000,000 unique (l_orderkey, o_orderdate, o_shippriority) groups
- **Execution plan** (recommended):
  1. Scan `customer/c_mktsegment.bin` + `customer/c_custkey.bin`, collect qualifying custkeys into `unordered_set<int32_t>` (300K entries, ~2.5MB — fits in L3)
  2. Scan orders with zone map on o_orderdate (skip blocks with `block_min >= 9204`). For each qualifying row: probe custkey set. Build `unordered_map<int32_t, pair<int32_t,int32_t>>` mapping o_orderkey → (o_orderdate, o_shippriority) for qualifying orders (~7.2M entries, ~115MB)
  3. Scan lineitem with zone map on l_shipdate (skip blocks with `block_max <= 9204`). For each qualifying row: probe order map on l_orderkey. Accumulate SUM(l_extendedprice*(1-l_discount)) per (l_orderkey, o_orderdate, o_shippriority) group.
  4. Top-K with k=10 by revenue DESC, o_orderdate ASC.
- **Aggregation**: ~2M groups but LIMIT 10 → use a partial sort / heap for top-K.

## Indexes

### orders_orderdate_zonemap (zone_map on o_orderdate) — USEFUL FOR Q3
- File: `indexes/orders_orderdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=150]` then per block: `[int32_t min, int32_t max, uint32_t block_size]` (12 bytes each)
- Total file size: 1,804 bytes
- **Skip logic**: skip block if `block_min >= 9204` — removes ~52% of orders blocks (trailing date blocks)
- `row_offset` is the ROW index (not byte offset). Access as `col[row_offset + i]`.

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then per block: `[int32_t min, int32_t max, uint32_t block_size]` (12 bytes)
- **Skip logic**: skip block if `block_max <= 9204` — removes ~48% of lineitem blocks (leading date blocks)
- `row_offset` is the ROW index. Access as `col[row_offset + i]`.

### orders_orderkey_hash (single-value hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t capacity=33554432]` then `[capacity × {int32_t key, uint32_t pos}]` (8 bytes/bucket)
- Empty bucket marker: `key == INT32_MIN`
- Lookup: `bucket = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (capacity-1)`; linear probe on collision
- Returns `pos` = row index in orders column files
- This query: look up orders row for a given o_orderkey — useful if probing orders from lineitem side

### lineitem_orderkey_hash (multi-value hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout: `[uint32_t capacity=33554432]` `[uint32_t num_positions=59986052]` `[capacity × {int32_t key, uint32_t offset, uint32_t count}]` `[59986052 × uint32_t positions]`
- Empty bucket marker: `key == INT32_MIN`
- Lookup: hash key → find `(offset, count)` → `positions[offset..offset+count-1]` are lineitem row indices
- Bucket size: 12 bytes. Positions array starts at byte offset: `4 + 4 + capacity × 12`
- This query: given a qualifying o_orderkey, look up all lineitem rows for that order
