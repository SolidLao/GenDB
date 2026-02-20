# Q3 Guide — Shipping Priority

## Column Reference

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 rows × 4 bytes)
- This query: used as build-side key for customer→orders hash join and equi-join `c_custkey = o_custkey`

### c_mktsegment (STRING, uint8_t, dictionary-encoded)
- File: `customer/c_mktsegment.bin` (1,500,000 rows × 1 byte)
- Dictionary: `customer/c_mktsegment_dict.txt`
  - Format: `0=AUTOMOBILE\n1=BUILDING\n2=FURNITURE\n3=HOUSEHOLD\n4=MACHINERY\n`
  - Load at runtime: `std::vector<std::string> dict; parse code=value pairs`
- This query: `c_mktsegment = 'BUILDING'` → find code where dict[code]=="BUILDING" = code **1**
  - Filter: `c_mktsegment[i] == 1` (direct integer comparison on encoded values)
  - Do NOT compare `c_mktsegment[i] == 'B'` — that would be wrong; compare to the code value

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key `l_orderkey = o_orderkey`; also output column

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- This query: `o_orderdate < DATE '1995-03-15'` → `o_orderdate < 9204`
  - epoch('1995-03-15') = 9204
- Also used as output column and secondary ORDER BY key

### o_shippriority (INTEGER, int32_t)
- File: `orders/o_shippriority.bin` (15,000,000 rows × 4 bytes)
- In TPC-H, o_shippriority = 0 for all rows (uniform); output column for Q3

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes, sorted by l_shipdate)
- This query: join key `l_orderkey = o_orderkey`

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → accumulate `price * (1 - disc)` as double

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- This query: used in revenue = `l_extendedprice * (1 - l_discount)`

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes, sorted ascending)
- This query: `l_shipdate > DATE '1995-03-15'` → `l_shipdate > 9204`
  - epoch('1995-03-15') = 9204
  - Zone map: skip blocks with `zone.max_val <= 9204` (those with all dates ≤ 1995-03-15)

## Table Stats
| Table    | Rows       | Role      | Sort Order   | Block Size |
|----------|------------|-----------|--------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate ↑ | 100,000    |
| orders   | 15,000,000 | fact      | none         | 100,000    |
| customer | 1,500,000  | dimension | none         | 100,000    |

## Query Analysis
- **Join pattern**: `customer ⋈ orders ⋈ lineitem` (PK-FK)
  - Build side 1: customer (filter to BUILDING segment ~300K rows) → hash set on c_custkey
  - Probe: orders filtered by `o_orderdate < 9204` (~25% = 3.75M rows) → probe customer hash set
    → produces qualifying (o_orderkey, o_orderdate, o_shippriority) set (~750K rows)
  - Build side 2: qualifying o_orderkey set → hash table
  - Probe: lineitem filtered by `l_shipdate > 9204` (~65% = 39M rows) → probe orders hash table
- **Filters and selectivities**:
  - `c_mktsegment = 'BUILDING'`: 20% selectivity → 300K customers
  - `o_orderdate < 9204`: 25% selectivity → 3.75M orders (use orders_orderdate_zonemap for pruning)
  - `l_shipdate > 9204`: 65% selectivity → 39M lineitem rows (use lineitem_shipdate_zonemap)
  - Combined lineitem pass-through: ~13% (0.65 × orders reduction)
- **Aggregation**: GROUP BY (l_orderkey, o_orderdate, o_shippriority) → ~10 output groups (LIMIT 10)
- **Output**: TOP-10 by revenue DESC, o_orderdate ASC → use a partial sort / bounded priority queue

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then 600 × `[int32_t min_val, int32_t max_val, uint32_t row_count]`
- Skip logic for this query: skip block b if `zone.max_val <= 9204` (all dates ≤ 1995-03-15)
  - Since data is sorted ascending, early blocks have small dates and get skipped
  - ~35% of blocks (those before 1995-03-15) can be skipped → read ~65% of lineitem
- row_offset for block b = b × 100,000 (row index, not byte offset)

### orders_orderdate_zonemap (zone_map on o_orderdate)
- File: `indexes/orders_orderdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=150]` then 150 × `[int32_t min_val, int32_t max_val, uint32_t row_count]`
- Skip logic: skip block b if `zone.min_val >= 9204` (all dates ≥ 1995-03-15 → don't qualify)
- orders is NOT sorted by orderdate, so pruning effectiveness is limited

### customer_custkey_hash (unique hash on c_custkey)
- File: `indexes/customer_custkey_hash.bin`
- Layout: `[uint32_t cap=4194304]` then 4194304 × `[int32_t key, uint32_t row_idx]` (8 bytes each)
- Empty slot: `key == INT32_MIN` (0x80000000 = -2147483648)
- Hash function: `slot = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & (cap-1)`
- Linear probe: advance slot = (slot+1) & (cap-1) until key matches or empty
- Usage in Q3: After filtering customer to BUILDING (code=1), build a hash set of qualifying c_custkey values for probing o_custkey during orders scan

### orders_orderkey_hash (unique hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t cap=33554432]` then 33554432 × `[int32_t key, uint32_t row_idx]` (8 bytes each)
- Empty slot: `key == INT32_MIN`
- Usage in Q3: After filtering qualifying orders (o_orderdate < 9204 AND custkey matched), build hash set of qualifying o_orderkey. Then probe from lineitem l_orderkey on filtered rows.

### lineitem_orderkey_hash (multivalue hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout: `[uint32_t cap=33554432][uint32_t n_pos=59986052]`
  then 33554432 × `[int32_t key, uint32_t offset, uint32_t count]` (12 bytes each)
  then 59986052 × `uint32_t positions`
- Empty slot: `key == INT32_MIN`
- Lookup: hash(l_orderkey) → slot → (offset, count) → positions[offset..offset+count)
- Each position is a row index into sorted lineitem arrays
- Usage in Q3: Given qualifying o_orderkey, look up all lineitem positions for that orderkey and check `l_shipdate > 9204`

## Execution Strategy
1. Scan customer (1.5M rows), filter `c_mktsegment == 1` → build hash set of qualifying c_custkey (~300K)
2. Scan orders with zone-map pruning on o_orderdate, filter `o_orderdate < 9204`, probe c_custkey hash set → collect qualifying (o_orderkey, o_orderdate, o_shippriority) → ~750K entries
3. Build hash table on qualifying o_orderkey → orders_meta[o_orderkey] = {o_orderdate, o_shippriority}
4. Scan lineitem with zone-map on l_shipdate (skip blocks with max ≤ 9204), filter `l_shipdate > 9204`, probe o_orderkey → aggregate revenue per l_orderkey
5. Top-10 by revenue DESC using partial sort (priority queue of size 10)
