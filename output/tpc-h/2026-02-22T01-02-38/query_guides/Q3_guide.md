# Q3 Guide — Shipping Priority

## SQL
```sql
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'    -- raw < 9204
  AND l_shipdate  > DATE '1995-03-15'    -- raw > 9204
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate ASC
LIMIT 10;
```

## Column Reference

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 rows × 4 bytes)
- This query: used as join key (build side after filter); also see `customer_custkey_hash.bin`

### c_mktsegment (STRING, uint8_t, dictionary-encoded)
- File: `customer/c_mktsegment.bin` (1,500,000 rows × 1 byte)
- Dictionary: `customer/c_mktsegment_dict.txt` — load at runtime as `std::vector<std::string>`
  - Code 0 = "BUILDING", Code 1 = "AUTOMOBILE", Code 2 = "MACHINERY", Code 3 = "HOUSEHOLD", Code 4 = "FURNITURE"
- Filtering: `c_mktsegment = 'BUILDING'` → filter rows where `code == 0` (uint8_t)
- Do NOT hardcode 0 — load dict, find index where `dict[i] == "BUILDING"`, then filter by code
- Selectivity: ~20% of 1.5M customers → ~300,000 qualifying customers
- Output: not projected (only used for filtering)

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key with lineitem; also used as output/group-by column
- Hash index: `indexes/orders_orderkey_hash.bin` (unique: 1 row per key)

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key → `c_custkey = o_custkey`
- Hash index: `indexes/orders_custkey_hash.bin` (multi-value: ~10 orders per customer)
  - Use this to probe orders given qualifying customer keys

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- orders is **sorted by o_orderdate**
- This query: `o_orderdate < '1995-03-15'` → `raw < 9204`
- Selectivity: ~49% of 15M orders → ~7.35M qualifying orders (before customer join)
- Zone map: `indexes/orders_orderdate_zonemap.bin`
  - Layout: `[uint32_t num_blocks=150][int32_t min, int32_t max per block]`
  - Skip block if `block_min >= 9204`
  - Since sorted, binary search to find last qualifying block (~51% of blocks pruned)

### o_shippriority (INTEGER, int32_t)
- File: `orders/o_shippriority.bin` (15,000,000 rows × 4 bytes)
- This query: output/group-by column; no predicate

### o_totalprice (DECIMAL, double)
- File: `orders/o_totalprice.bin` (15,000,000 rows × 8 bytes)
- This query: NOT referenced in Q3 SQL

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes)
- This query: join key with orders; output/group-by column
- Hash index: `indexes/lineitem_orderkey_hash.bin` (multi-value: ~4 rows per key)

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes)
- lineitem sorted by l_shipdate
- This query: `l_shipdate > '1995-03-15'` → `raw > 9204`
- Selectivity: ~55% of 60M rows → ~33M qualifying lineitem rows
- Zone map: `indexes/lineitem_shipdate_zonemap.bin`
  - Layout: `[uint32_t num_blocks=600][int32_t min, int32_t max per block]`
  - Skip block if `block_max <= 9204` — first ~45% of blocks (pre-1995-03-15 rows) pruned

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values match SQL directly.
- This query: `SUM(l_extendedprice * (1 - l_discount))` as revenue

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values 0.00..0.10 match SQL directly.
- This query: used in revenue formula `(1 - l_discount)`

## Table Stats
| Table    | Rows       | Role      | Sort Order   | Block Size |
|----------|------------|-----------|--------------|------------|
| customer | 1,500,000  | dimension | none         | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate  | 100,000    |
| lineitem | 59,986,052 | fact      | l_shipdate   | 100,000    |

## Query Analysis
- **Join pattern**: 3-way join: customer ⋈ orders ⋈ lineitem
  - Build side 1: customer filtered on `c_mktsegment='BUILDING'` (300K rows) → hash table on `c_custkey`
  - Probe side 1 / Build side 2: orders filtered on `o_orderdate < 9204` (7.35M) → probe customer hash → qualifying orders (~7.35M × 0.2 = ~1.47M) → build hash table on `o_orderkey`
    - Actually: all orders with `o_orderdate < 9204` probe customer. ~49% of orders × 20% customer selectivity → ~735K orders match both. Build hash table keyed by `o_orderkey` with (o_orderdate, o_shippriority) payload.
  - Probe side 2: lineitem filtered on `l_shipdate > 9204` (33M rows) → probe orders hash table → emit (l_orderkey, ep*(1-disc), o_orderdate, o_shippriority) for matches
- **Filters**:
  - customer: `c_mktsegment=BUILDING` → 20% selectivity → 300K rows
  - orders: `o_orderdate < 9204` → 49% selectivity → 7.35M rows; after customer join: ~735K
  - lineitem: `l_shipdate > 9204` → 55% selectivity → 33M rows; after orders join: ~2.94M
- **Combined flow**: scan 300K customers → build custkey hash → scan/filter 7.35M orders → probe → build orderkey hash (735K keys) → scan/filter 33M lineitem → probe → hash aggregate by (l_orderkey, o_orderdate, o_shippriority)
- **Aggregation**: SUM(revenue) per group; estimate ~5M groups initially but after join filtering ~few hundred K
- **Output**: Top-10 by (revenue DESC, o_orderdate ASC) — use heap of size 10, no full sort needed

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][int32_t min, int32_t max per block]`
- Block b covers rows `[b*100000 .. min((b+1)*100000, 59986052))`
- `row_offset` is ROW index, not byte offset
- Usage: skip block if `block_max <= 9204`. Sorted data → binary search for first qualifying block (~45% pruning). Process remaining ~55% (33M rows).

### orders_orderdate_zonemap (zone_map on o_orderdate)
- File: `indexes/orders_orderdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=150][int32_t min, int32_t max per block]`
- Block b covers rows `[b*100000 .. min((b+1)*100000, 15000000))`
- `row_offset` is ROW index, not byte offset
- Usage: skip block if `block_min >= 9204`. Sorted data → find last qualifying block. ~51% pruning.

### customer_custkey_hash (hash on c_custkey)
- File: `indexes/customer_custkey_hash.bin`
- Layout: `[uint32_t ht_size=4194304][uint32_t num_positions=1500000]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- Hash function: `(uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (ht_size-1)`
- This query: NOT the primary access path for customer. Prefer scanning customer columns directly (1.5M rows is small), filtering on mktsegment, collecting custkeys into a hash set.

### orders_custkey_hash (hash on o_custkey)
- File: `indexes/orders_custkey_hash.bin`
- Layout: `[uint32_t ht_size=2097152][uint32_t num_positions=15000000]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- This query: can probe orders by custkey (build hash set of 300K qualifying custkeys, then use this index to retrieve their orders). Alternative: scan all orders with date filter (7.35M pass date filter anyway).

### orders_orderkey_hash (hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t ht_size=33554432][uint32_t num_positions=15000000]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- This query: probe with qualifying l_orderkey values from lineitem scan to find matching order rows. Or build in-memory hash table from 735K qualifying orders.

### lineitem_orderkey_hash (hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout: `[uint32_t ht_size=33554432][uint32_t num_positions=59986052]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- This query: NOT recommended as primary path (33M lineitem rows pass shipdate filter anyway — scanning is faster than index lookup).
