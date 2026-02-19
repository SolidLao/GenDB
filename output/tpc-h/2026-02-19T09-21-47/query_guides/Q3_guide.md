# Q3 Guide — Shipping Priority

## Column Reference

### c_mktsegment (STRING, int8_t, dictionary-encoded)
- File: `customer/c_mktsegment.bin` (1,500,000 rows, 1 byte/row)
- Dictionary: `customer/c_mktsegment_dict.txt` → `AUTOMOBILE=0, BUILDING=1, FURNITURE=2, HOUSEHOLD=3, MACHINERY=4`
- This query: `c_mktsegment = 'BUILDING'` → C++ filter: `raw == 1`

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 rows)
- This query: join key `c_custkey = o_custkey`. Used as lookup key in hash join (build side).

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows, **sorted by o_orderdate**)
- This query: join key `o_orderkey = l_orderkey`. Also in SELECT output.
- After filtering orders by o_orderdate, pass o_orderkey to probe lineitem.

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 rows)
- This query: join key `o_custkey = c_custkey` → probe customer hash index.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows, **sorted ascending**)
- This query: `o_orderdate < DATE '1995-03-15'`
  - Epoch: `YEAR_DAYS[25] + MONTH_STARTS[0][2] + 14` = `9131 + 59 + 14` = **9204**
  - C++ filter: `raw < 9204`

### o_shippriority (INTEGER, int32_t)
- File: `orders/o_shippriority.bin` (15,000,000 rows)
- This query: in SELECT output and GROUP BY. All values are 0 in TPC-H SF10.

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: join key `l_orderkey = o_orderkey`. Probe against a hash set of qualifying order keys.
- Also in SELECT output and GROUP BY.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows)
- Stored as native double. No scaling needed.
- This query: `SUM(l_extendedprice * (1 - l_discount)) AS revenue`

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows)
- Stored as native double. Values in `[0.00, 0.10]`.
- This query: used in revenue calculation `l_extendedprice * (1 - l_discount)`.

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows, **sorted ascending**)
- This query: `l_shipdate > DATE '1995-03-15'`
  - Epoch: **9204** (same as o_orderdate threshold)
  - C++ filter: `raw > 9204`

---

## Table Stats
| Table    | Rows       | Role      | Sort Order     | Block Size |
|----------|------------|-----------|----------------|------------|
| customer | 1,500,000  | dimension | none           | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate ↑  | 100,000    |
| lineitem | 59,986,052 | fact      | l_shipdate ↑   | 100,000    |

---

## Query Analysis
- **Join pattern**: Three-way join: `customer ⨝ orders ⨝ lineitem`
  - customer → orders: FK c_custkey = o_custkey (M:1 from orders perspective)
  - orders → lineitem: FK l_orderkey = o_orderkey (1:M from orders perspective)
- **Recommended join order** (smallest filtered set drives):
  1. Filter customer: c_mktsegment=BUILDING → ~150K rows (10% selectivity)
  2. Build hash table on qualifying c_custkey → small 150K-entry hash
  3. Scan orders with o_orderdate zone map (skip ~51% blocks). Probe customer hash.
     Qualifying orders: ~15M × 42% date × (only those with BUILDING customers) ≈ 630K orders
  4. Build small hash set of qualifying o_orderkey values
  5. Scan lineitem with l_shipdate zone map (skip ~46% blocks). Filter l_shipdate > 9204.
     Probe o_orderkey hash set. Compute revenue per qualifying lineitem row.
- **Filters and selectivities** (from workload analysis):
  - `c_mktsegment = 'BUILDING'`: 10% of customers → ~150K qualifying customers
  - `o_orderdate < 9204`: 42% of orders qualify
  - `l_shipdate > 9204`: 65% of lineitems qualify (after date sort skip: ~35% skippable blocks)
- **Combined selectivity after joins**: ~0.42 × 0.65 × join_fraction ≈ very selective
- **Aggregation**: GROUP BY (l_orderkey, o_orderdate, o_shippriority), ~150K groups estimated
- **Output**: ORDER BY revenue DESC, o_orderdate ASC, LIMIT 10 → use top-K heap

---

## Indexes

### o_orderdate_zonemap (zone_map on o_orderdate)
- File: `orders/indexes/o_orderdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=150]` then `[double min_val, double max_val, uint32_t row_count, uint32_t _pad]` per block = 24 bytes/entry
- `row_offset` of block `i` = `i * 100000` (row index, not byte offset). Access as `col_ptr[row_offset]`.
- This query: `o_orderdate < 9204` → skip block if `entry[i].min_val >= 9204.0`
  - Since data sorted ascending, binary search for last qualifying block. ~51% of blocks skippable.

### l_shipdate_zonemap (zone_map on l_shipdate)
- File: `lineitem/indexes/l_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then `[double min_val, double max_val, uint32_t row_count, uint32_t _pad]` per block
- This query: `l_shipdate > 9204` → skip block if `entry[i].max_val <= 9204.0`
  - ~46% of lineitem blocks skippable (those fully before 1995-03-15).

### c_custkey_hash (hash on c_custkey → row_pos)
- File: `customer/indexes/c_custkey_hash.bin`
- Layout: `[uint32_t capacity=4194304][uint32_t num_entries=1500000][HashSlot × capacity]`
- `HashSlot = {int32_t key, uint32_t row_pos}` = 8 bytes. Empty: `key == INT32_MIN`.
- Hash: `h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64-22)) & (cap-1)`
- This query: build a boolean filter `is_building_customer[c_custkey]` by scanning customer once.
  Then probe: for each qualifying order, check if o_custkey maps to a BUILDING customer.
  Alternative: use hash index to look up c_mktsegment code for a given c_custkey.

### o_orderkey_hash (hash on o_orderkey → row_pos)
- File: `orders/indexes/o_orderkey_hash.bin`
- Layout: `[uint32_t capacity=33554432][uint32_t num_entries=15000000][HashSlot × capacity]`
- `HashSlot = {int32_t key, uint32_t row_pos}` = 8 bytes. Empty: `key == INT32_MIN`.
- Hash: `h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64-25)) & (cap-1)`
- This query: after collecting qualifying o_orderkey set, use to probe during lineitem scan.
  OR: scan lineitem first (with l_shipdate zone map), then look up each l_orderkey in the qualifying orders hash set. Build a **runtime** hash set of qualifying orderkeys (not this pre-built index).

### l_discount_zonemap, l_quantity_zonemap
- Not useful for Q3 (no filter on these columns).
