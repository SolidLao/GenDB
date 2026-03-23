# Q3 Guide — Shipping Priority

## Query
```sql
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
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
| Table    | Rows       | Role       | Sort Order     | Block Size |
|----------|------------|------------|----------------|------------|
| lineitem | 59,986,052 | fact/probe | l_shipdate ASC | 100,000    |
| orders   | 15,000,000 | build      | (none)         | 100,000    |
| customer | 1,500,000  | filter     | (none)         | 100,000    |

## Column Reference

### c_mktsegment (`CHAR(10)`, `int8_t`, dict_int8)
- File: `customer/c_mktsegment.bin` (1,500,000 × 1 byte = ~1.4 MB)
- Dict sidecar: `customer/c_mktsegment_dict.bin`
  - Format: 5 entries × 16 bytes each (null-padded strings)
  - Entry order (by code): 0=AUTOMOBILE, 1=BUILDING, 2=FURNITURE, 3=HOUSEHOLD, 4=MACHINERY
- **Load pattern** (do NOT hardcode code for 'BUILDING'):
  ```cpp
  char mkt_dict[5][16];
  FILE* f = fopen(".../customer/c_mktsegment_dict.bin", "rb");
  fread(mkt_dict, 16, 5, f); fclose(f);
  int8_t building_code = -1;
  for (int i = 0; i < 5; i++)
      if (strncmp(mkt_dict[i], "BUILDING", 8) == 0) { building_code = (int8_t)i; break; }
  ```
- This query: `c_mktsegment = 'BUILDING'` → `c_mktsegment[i] == building_code`
- Selectivity: ~20% of customers (300,000 rows pass)

### c_custkey (`INTEGER`, `int32_t`, plain)
- File: `customer/c_custkey.bin` (1,500,000 × 4 bytes = ~5.7 MB)
- This query: join key — used to build a bitset/hash set of qualifying custkeys

### o_custkey (`INTEGER`, `int32_t`, plain)
- File: `orders/o_custkey.bin` (15,000,000 × 4 bytes = ~57 MB)
- This query: join probe — filter orders where `o_custkey` is in the BUILDING-customer set

### o_orderdate (`DATE`, `int32_t`, days_since_epoch)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes = ~57 MB)
- Encoding: days since 1970-01-01
- This query: `o_orderdate < DATE '1995-03-15'`
  - Threshold: **1995-03-15 = 9204** (days since epoch)
    - y=1995, m=3, d=15 (m>2, no year adjustment)
    - era=4, yoe=395, doy=(153×0+2)/5+14=14, doe=144284
    - result = 4×146097 + 144284 − 719468 = **9204**
  - C++ comparison: `o_orderdate[i] < 9204`
- Selectivity: ~48.7% of orders pass
- Also GROUP BY and output column — store in group key

### o_orderkey (`INTEGER`, `int32_t`, plain)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes = ~57 MB)
- This query: join key (lineitem FK → orders PK), GROUP BY key, output column

### o_shippriority (`INTEGER`, `int32_t`, plain)
- File: `orders/o_shippriority.bin` (15,000,000 × 4 bytes = ~57 MB)
- This query: GROUP BY key and output column

### l_orderkey (`INTEGER`, `int32_t`, plain)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes = ~229 MB)
- This query: join probe key (lineitem → orders), GROUP BY key, output column

### l_shipdate (`DATE`, `int32_t`, days_since_epoch)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes = ~229 MB)
- Table is sorted by this column ASC — zone map is available
- This query: `l_shipdate > DATE '1995-03-15'`
  - Threshold: **1995-03-15 = 9204** (same value as o_orderdate threshold)
  - C++ comparison: `l_shipdate[i] > 9204`
- Selectivity: ~54.5% of lineitem rows pass

### l_extendedprice (`DECIMAL(15,2)`, `double`, plain_double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes = ~458 MB)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → revenue aggregation

### l_discount (`DECIMAL(15,2)`, `int8_t`, int8_hundredths)
- File: `lineitem/l_discount.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: value × 100 as int8_t (range 0–10)
- This query: decode inline: `double disc = l_discount[i] * 0.01;`
  - Revenue contribution: `l_extendedprice[i] * (1.0 - disc)`

## Query Analysis

### Recommended Join Strategy
Three-table join with selectivity-based ordering:

**Phase 1 — Build customer filter set**
- Scan `customer/c_mktsegment.bin` + `customer/c_custkey.bin` in parallel
- Collect custkeys where mktsegment code == building_code
- Build a dense bitset: `std::vector<bool> cust_ok(1500001, false); cust_ok[c_custkey[i]] = true;`
- Result: ~300,000 qualifying custkeys

**Phase 2 — Filter and hash-build from orders**
- Scan orders columns: o_custkey, o_orderdate, o_orderkey, o_shippriority
- Keep rows where: `cust_ok[o_custkey[i]] && o_orderdate[i] < 9204`
- Build hash map: `unordered_map<int32_t, OrderInfo>` keyed by o_orderkey
  ```cpp
  struct OrderInfo { int32_t orderdate; int32_t shippriority; };
  ```
- Expected ~15M × 0.2 (cust filter) × 0.487 (date filter) ≈ **1.46M qualifying orders**
- Use `orders/orders_by_orderkey.bin` dense array for lineitem→orders lookup (see Indexes)

**Phase 3 — Probe lineitem with zone map**
- Use zone map to skip lineitem blocks where `zone.max_date <= 9204`
- For each qualifying lineitem row: `l_shipdate[i] > 9204`
- Look up order: `row_idx = orders_by_orderkey[l_orderkey[i]]; if (row_idx == -1) continue;`
- Check order passed Phase 2 filter using the hash map or a qualifying-orders bitset
- Accumulate revenue into per-group map keyed by (l_orderkey, o_orderdate, o_shippriority)

### Aggregation
- ~4,000,000 estimated output groups (before LIMIT 10)
- Use `unordered_map<int64_t, double>` keyed by a composite of (l_orderkey):
  - Since l_orderkey uniquely determines (o_orderdate, o_shippriority) after the join,
    key = l_orderkey is sufficient; store orderdate + shippriority alongside
- Final step: partial_sort top 10 by (revenue DESC, o_orderdate ASC)

### Date Threshold (both filters use the same date)
```cpp
const int32_t DATE_1995_03_15 = 9204;
// o_orderdate < DATE_1995_03_15
// l_shipdate  > DATE_1995_03_15
```

## Indexes

### orders_by_orderkey (dense_array on o_orderkey)
- File: `orders/orders_by_orderkey.bin`
- Layout: flat `int32_t` array, **60,000,001 entries** (indices 0..60,000,000)
  - `array[o_orderkey] = row_index` into orders column files
  - Sentinel: `-1` for unused slots
  - File size: 60,000,001 × 4 = **240 MB** (fits in RAM; 376 GB available)
- Built by `build_dense_index()` with `max_key = 60000000`
- **Usage for Q3**:
  ```cpp
  // Load at startup:
  int32_t* ord_idx = ...; // mmap or read orders_by_orderkey.bin
  // Per lineitem row i:
  int32_t okey = l_orderkey[i];
  int32_t row  = ord_idx[okey];          // O(1) lookup
  if (row == -1) continue;               // key not in orders
  // Access orders columns:
  int32_t odate = o_orderdate[row];
  int32_t oprio = o_shippriority[row];
  ```
- Enables O(1) lineitem→orders join; no hash map construction at query time

### customer_by_custkey (dense_array on c_custkey)
- File: `customer/customer_by_custkey.bin`
- Layout: flat `int32_t` array, **1,500,001 entries** (indices 0..1,500,000)
  - `array[c_custkey] = row_index` into customer column files
  - Sentinel: `-1` for unused slots
  - File size: 1,500,001 × 4 = **~5.7 MB**
- Built by `build_dense_index()` with `max_key = 1500000`
- **Usage for Q3**: Build o_custkey → customer filter
  - Option A (preferred): scan customer directly to build cust_ok bitset (cheaper than join)
  - Option B: use dense array to go from o_custkey to customer row, then check mktsegment
  ```cpp
  // Option B per orders row:
  int32_t cust_row = cust_idx[o_custkey[j]];
  if (cust_row == -1 || c_mktsegment[cust_row] != building_code) continue;
  ```

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/l_shipdate_zone_map.bin`
- Layout:
  ```
  [0..3]  uint32_t num_blocks  (= 600)
  [4..7]  uint32_t block_size  (= 100,000)
  [8..]   ZoneEntry[600]: struct { int32_t min_date; int32_t max_date; }  // 8 bytes each
  ```
- **Usage for Q3**: Skip blocks where `zone.max_date <= 9204` (all rows have shipdate ≤ 1995-03-15)
  ```cpp
  const int32_t THRESHOLD = 9204;
  for (uint32_t b = 0; b < num_blocks; b++) {
      if (zones[b].max_date <= THRESHOLD) continue;  // all rows fail l_shipdate > threshold
      size_t row_start = (size_t)b * block_size;
      size_t row_end   = std::min(row_start + block_size, (size_t)total_rows);
      for (size_t i = row_start; i < row_end; i++) {
          if (l_shipdate[i] <= THRESHOLD) continue;
          // process row i
      }
  }
  ```
- Selectivity: ~54.5% pass → zone map skips the early ~45.5% of blocks entirely
  (since lineitem is sorted ASC, early blocks have small shipdates — all fail)
