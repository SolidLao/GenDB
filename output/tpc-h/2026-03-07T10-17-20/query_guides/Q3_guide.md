# Q3 Guide — Shipping Priority

## Query
```sql
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate  > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate ASC
LIMIT 10;
```

## Table Stats
| Table    | Rows       | Role      | Sort Order    | Block Size |
|----------|------------|-----------|---------------|------------|
| lineitem | 59,986,052 | fact/probe | l_shipdate ↑  | 100,000    |
| orders   | 15,000,000 | build     | o_orderdate ↑ | 100,000    |
| customer | 1,500,000  | build     | c_custkey ↑   | 100,000    |

## Column Reference

### c_mktsegment (categorical_5, int8_t — dict encoded)
- File: `customer/c_mktsegment.bin` (1,500,000 × 1 byte)
- Encoding from `ingest.cpp::encode_mktsegment` (first character dispatch):
  ```cpp
  case 'A': return 0; // AUTOMOBILE
  case 'B': return 1; // BUILDING
  case 'F': return 2; // FURNITURE
  case 'H': return 3; // HOUSEHOLD
  case 'M': return 4; // MACHINERY
  ```
- This query: filter `c_mktsegment == 1` (BUILDING). Load code at startup:
  ```cpp
  const int8_t kBuilding = 1; // from encode_mktsegment('B')
  ```
  **Do not hardcode 1 — derive from the switch above.**
- Selectivity: ~20% of customers (300,000 qualifying)

### c_custkey (pk, int32_t — raw)
- File: `customer/c_custkey.bin` (1,500,000 × 4 bytes)
- This query: build qualifying-customer set via scan; used as FK lookup key

### o_custkey (fk_customer_pk, int32_t — raw)
- File: `orders/o_custkey.bin` (15,000,000 × 4 bytes)
- This query: join predicate `o_custkey = c_custkey` via `c_custkey_hash`

### o_orderdate (date, int32_t — days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes, sorted ascending)
- Filter: `o_orderdate < DATE '1995-03-15'`
- Compute threshold at query startup:
  ```cpp
  // parse_date("1995-03-15")
  int y=1995, m=3, d=15;
  // m > 2: no adjustment
  int A = y/100;                        // 19
  int B = 2 - A + A/4;                 // -13
  int jdn = (int)(365.25*(y+4716))     // (int)(365.25*6711) = 2450003
           + (int)(30.6001*(m+1))      // (int)(30.6001*4)   = 122
           + d + B - 1524;             // +15 -13 -1524
  // jdn = 2448603  →  days = 2448603 - 2440588 = 8015
  const int32_t kOrderDateMax = 8015;  // exclusive upper bound
  ```
- Selectivity: ~49% of orders pass (date range 1992-01-01 to 1998-07-30)
- This query: GROUP BY key and output column

### o_orderkey (pk, int32_t — raw)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes, sorted by o_orderdate)
- This query: FK join target from lineitem.l_orderkey; also GROUP BY / output key

### o_shippriority (integer, int32_t — raw)
- File: `orders/o_shippriority.bin` (15,000,000 × 4 bytes)
- This query: GROUP BY key and output column

### l_orderkey (fk_orders_pk, int32_t — raw)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes, sorted by l_shipdate)
- This query: join predicate `l_orderkey = o_orderkey` via `o_orderkey_hash`
- Also GROUP BY / output key

### l_shipdate (date, int32_t — days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes, sorted ascending)
- Filter: `l_shipdate > DATE '1995-03-15'` → `l_shipdate > 8015`
- Same threshold date as o_orderdate; `kShipdateMin = 8015` (exclusive lower bound)
- Selectivity: ~51% of lineitem rows pass

### l_extendedprice (decimal_15_2, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes)
- This query: revenue = SUM(l_extendedprice * (1 - l_discount))

### l_discount (decimal_15_2, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes)
- This query: multiplied into revenue

## Indexes

### o_orderdate_zone_map (zone_map on o_orderdate)
- File: `orders/o_orderdate_zone_map.bin`
- Layout (all int32_t, packed):
  ```
  [0]           int32_t  num_blocks        // = ceil(15000000 / 100000) = 150
  [1]           int32_t  block_size        // = 100000
  [2 .. N+1]    int32_t  min[num_blocks]
  [N+2 .. 2N+1] int32_t  max[num_blocks]
  ```
  `orders` is sorted by o_orderdate ascending, so `min[b]=first`, `max[b]=last` in block.
- Usage: skip block b if `zone_min[b] >= kOrderDateMax` (all rows in block ≥ threshold).
  ~49% pass → approximately the first 74 blocks (out of 150) will be fully included;
  blocks in the middle may be partially filtered; trailing ~51% blocks skipped.
  ```cpp
  if (zone_min[b] >= kOrderDateMax) break; // sorted: all later blocks also fail
  ```

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/l_shipdate_zone_map.bin`
- Layout: same structure as o_orderdate_zone_map with 600 blocks (59,986,052 rows).
- Usage: skip blocks where `zone_max[b] <= kShipdateMin` (all rows ≤ threshold).
  ~49% of blocks (roughly the first 294 of 600) can be skipped entirely.
  ```cpp
  if (zone_max[b] <= kShipdateMin) continue; // block is entirely before threshold
  if (zone_min[b] >  kShipdateMin) {
      // entire block passes: no per-row shipdate check needed
  }
  ```

### c_custkey_hash (hash int32→int32 on customer.c_custkey)
- File: `customer/c_custkey_hash.bin`
- Capacity: 4,194,304 (= 2²²); load factor ≈ 1,500,000 / 4,194,304 ≈ 0.36
- Binary layout:
  ```
  Bytes  0..7    int64_t  capacity        // = 4194304
  Bytes  8..15   int64_t  num_entries     // = 1500000
  Bytes  16 ..   int32_t  keys[4194304]   // sentinel = -1
  Bytes  16 + 4194304*4 ..
                 int32_t  values[4194304] // row index into customer columns
  ```
- Hash function (verbatim from `build_indexes.cpp::hash32`):
  ```cpp
  static inline uint32_t hash32(uint32_t x) {
      x = ((x >> 16) ^ x) * 0x45d9f3bu;
      x = ((x >> 16) ^ x) * 0x45d9f3bu;
      x = (x >> 16) ^ x;
      return x;
  }
  ```
- Probe sequence: open-addressing, linear probing with mask:
  ```cpp
  int64_t mask = capacity - 1;   // = 4194303
  uint64_t h = (uint64_t)hash32((uint32_t)o_custkey) & (uint64_t)mask;
  while (keys[h] != -1) {
      if (keys[h] == o_custkey) { cust_row = values[h]; break; }
      h = (h + 1) & (uint64_t)mask;
  }
  // if keys[h] == -1: no match (should not happen for valid FK)
  ```
- Usage: look up `orders.o_custkey` → `customer row index` to check `c_mktsegment`

### o_orderkey_hash (hash int32→int32 on orders.o_orderkey)
- File: `orders/o_orderkey_hash.bin`
- Capacity: 33,554,432 (= 2²⁵); load factor ≈ 15,000,000 / 33,554,432 ≈ 0.45
- Binary layout:
  ```
  Bytes  0..7    int64_t  capacity          // = 33554432
  Bytes  8..15   int64_t  num_entries       // = 15000000
  Bytes  16 ..   int32_t  keys[33554432]    // sentinel = -1
  Bytes  16 + 33554432*4 ..
                 int32_t  values[33554432]  // row index into orders columns
  ```
- Hash function: same `hash32` as above.
- Probe sequence: same linear probing pattern.
  ```cpp
  int64_t mask = 33554432LL - 1;  // = 33554431
  uint64_t h = (uint64_t)hash32((uint32_t)l_orderkey) & (uint64_t)mask;
  while (keys[h] != -1) {
      if (keys[h] == l_orderkey) { orders_row = values[h]; break; }
      h = (h + 1) & (uint64_t)mask;
  }
  ```
- Usage: look up `lineitem.l_orderkey` → `orders row index` to fetch
  `o_orderdate`, `o_shippriority`, and to join with pre-filtered order set.

## Query Analysis

### Join Strategy (recommended: hash-join build/probe)
1. **Phase 1 — Build qualifying customer bitset:**
   Scan `customer/c_mktsegment.bin`. For each row i where `c_mktsegment[i] == 1`:
   record `c_custkey[i]` in a hash set (or bitset indexed by custkey value).
   ~300,000 qualifying customers (20% of 1.5M).

2. **Phase 2 — Build qualifying orders map:**
   Scan `orders/o_custkey.bin` (with zone map to skip blocks where
   `zone_min[b] >= kOrderDateMax`). For each row i where:
   - `o_orderdate[i] < kOrderDateMax` AND
   - `o_custkey[i]` is in the qualifying customer set:
   → record `o_orderkey[i]` → (o_orderdate[i], o_shippriority[i]) in a hash map.
   ~49% date pass × ~20% segment pass ≈ ~1.47M qualifying orders.

3. **Phase 3 — Scan lineitem with zone map:**
   Use `l_shipdate_zone_map` to skip blocks where `zone_max[b] <= kShipdateMin`.
   For each row i where `l_shipdate[i] > kShipdateMin`:
   - Look up `l_orderkey[i]` in qualifying orders map.
   - If found: accumulate `revenue += l_extendedprice[i] * (1 - l_discount[i])`
     into group `(l_orderkey, o_orderdate, o_shippriority)`.
   ~51% lineitem rows pass date filter; of those, ~1.47M/15M × avg_lineitem_per_order
   will match qualifying orders.

4. **Output:** partial_sort top-10 by (revenue DESC, o_orderdate ASC).

### Alternative: probe o_orderkey_hash from lineitem
Instead of building a qualifying-orders map in Phase 2, scan lineitem (Phase 3)
first, use `o_orderkey_hash` to get the orders row, then check `o_orderdate` and
`o_custkey` directly. This avoids building the intermediate orders map but does
a hash probe per lineitem row instead of per qualifying row only.

### Aggregation
- Estimated groups: ~3,000,000 (one per qualifying (l_orderkey, o_orderdate, o_shippriority) tuple)
- Use unordered_map keyed by (int32_t l_orderkey) — orderkey alone uniquely identifies the group
  since each order has one orderdate and one shippriority.
- After aggregation: extract top-10 by (revenue DESC, o_orderdate ASC) using partial_sort or
  a running top-10 heap.
