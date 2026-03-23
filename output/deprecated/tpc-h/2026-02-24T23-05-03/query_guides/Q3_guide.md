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
ORDER BY revenue DESC, o_orderdate ASC
LIMIT 10;
```

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| customer | 1,500,000  | dimension | (none)     | 65536      |
| orders   | 15,000,000 | fact      | (none)     | 65536      |
| lineitem | 59,986,052 | fact      | (none)     | 65536      |

## Column Reference

### c_custkey (key, int32_t — direct)
- File: `customer/c_custkey.bin` (1,500,000 × 4 bytes = 6,000,000 bytes)
- This query: build semi-join filter set; join key for c_custkey = o_custkey
- Values: sequential integers in TPC-H [1, 1,500,000]

### c_mktsegment (category, int16_t — dict-encoded)
- File: `customer/c_mktsegment.bin` (1,500,000 × 2 bytes = 3,000,000 bytes)
- Dict file: `customer/c_mktsegment_dict.txt` (5 distinct values)
- This query: `c_mktsegment = 'BUILDING'` → ~300,276 rows pass (~20% selectivity)
- **C2**: Load dict at runtime, scan for 'BUILDING':
  ```cpp
  std::vector<std::string> ms_dict;
  // read customer/c_mktsegment_dict.txt line by line into ms_dict
  int16_t building_code = -1;
  for (int16_t i = 0; i < (int16_t)ms_dict.size(); i++)
      if (ms_dict[i] == "BUILDING") { building_code = i; break; }
  // filter: c_mktsegment[row] == building_code
  ```

### o_orderkey (key, int32_t — direct)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- This query: GROUP BY key; join key for l_orderkey = o_orderkey

### o_custkey (foreign key, int32_t — direct)
- File: `orders/o_custkey.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- This query: join predicate c_custkey = o_custkey

### o_orderdate (date, int32_t — days_since_epoch_1970)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- Encoding: Howard Hinnant algorithm; values confirmed > 3000; range 1992-01-01 to 1998-08-02
- This query: `o_orderdate < DATE '1995-03-15'` → ~48.6% of orders pass
- Also GROUP BY output column; also used in ORDER BY
- **C1/C7**:
  ```cpp
  gendb::init_date_tables();  // C11
  int32_t date_threshold = gendb::date_to_epoch("1995-03-15");
  // filter: o_orderdate[row] < date_threshold
  ```

### o_shippriority (attribute, int32_t — direct)
- File: `orders/o_shippriority.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- This query: GROUP BY key and output column

### o_totalprice (measure, double) — NOT used in Q3
- Not accessed in Q3.

### l_orderkey (key, int32_t — direct)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes = 239,944,208 bytes)
- This query: GROUP BY key; join key for l_orderkey = o_orderkey; probe orders hash

### l_extendedprice (measure, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; max individual value ~104,949
- This query: SUM(ep*(1-disc)) — multi-column derived expression
- **C35**: Use `long double` accumulation:
  ```cpp
  long double revenue = 0.0L;
  revenue += (long double)ep * (1.0L - disc);
  ```

### l_discount (measure, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; values ∈ [0.00, 0.10]
- This query: used in SUM(ep*(1-disc))

### l_shipdate (date, int32_t — days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes = 239,944,208 bytes)
- Encoding: Howard Hinnant algorithm; values confirmed > 3000
- This query: `l_shipdate > DATE '1995-03-15'` → ~54.0% pass (selectivity 0.54)
- **C1/C7**:
  ```cpp
  int32_t ship_threshold = gendb::date_to_epoch("1995-03-15"); // same date as o_orderdate filter
  // filter: l_shipdate[row] > ship_threshold
  ```

## Query Analysis

### Join Pattern
```
customer (1.5M) --[c_custkey=o_custkey]--> orders (15M) --[o_orderkey=l_orderkey]--> lineitem (60M)
```

### Recommended Execution Plan
1. **Build customer filter** (20% selectivity → ~300K qualifying custkeys):
   - Scan `c_mktsegment.bin`, find building_code at runtime
   - Collect qualifying `c_custkey` values into a bitmap or hash set
   - c_custkey ∈ [1, 1.5M] → dense bitmap: 1.5M/8 = 187.5KB (fits L2 cache)

2. **Scan orders** with o_orderdate zone-map skip:
   - Filter: `o_custkey ∈ customer_bitmap AND o_orderdate < threshold`
   - ~48.6% pass o_orderdate filter; ~20% pass custkey filter → ~9.7% overall
   - Build runtime hash map: `o_orderkey → (o_orderdate, o_shippriority)` for qualifying rows
   - ~1.45M qualifying orders

3. **Scan lineitem** with l_shipdate zone-map (marginal at 54% selectivity):
   - Filter: `l_shipdate > ship_threshold`
   - Probe runtime orders hash map on `l_orderkey`
   - Accumulate SUM(ep*(1-disc)) per (l_orderkey, o_orderdate, o_shippriority) group

4. **Final**: sort by revenue DESC, o_orderdate ASC; emit top-10

### Selectivities
| Filter                      | Selectivity | Qualifying Rows |
|-----------------------------|-------------|-----------------|
| c_mktsegment = 'BUILDING'   | 20%         | ~300,276        |
| o_orderdate < 1995-03-15    | 48.6%       | ~7,290,000      |
| combined customer+order join | ~9.7%      | ~1,455,000      |
| l_shipdate > 1995-03-15     | 54.0%       | ~32,392,468     |
| lineitem join hit           | ~2.4%       | ~3,500,000 groups |

### Aggregation
- ~3.5M groups estimated (post-filter join)
- GROUP BY key: (l_orderkey, o_orderdate, o_shippriority) — C15: all three required
- Top-10 final output: partial_sort or priority queue

## Indexes

### customer_pk_hash (hash on c_custkey) — NOT needed for Q3
- For Q3, customer is the smallest table and we build a compact bitmap from c_custkey values.
- The pre-built `customer_pk_hash` is useful for PK lookup (orders → customer direction).
- In Q3 the join direction is customer → orders (scan customer first, then probe orders),
  so a simple bitmap of qualifying custkeys suffices.
- File: `customer/indexes/customer_pk_hash.bin` (not required for Q3)

### orders_pk_hash (hash on o_orderkey) — used for lineitem→orders lookup
- File: `orders/indexes/orders_pk_hash.bin`
- File size: 4 + 33,554,432 × 8 = 268,435,460 bytes (~256MB)
- Layout: `[uint32_t cap][PKSlot[cap]]`
  ```cpp
  struct PKSlot { int32_t key; uint32_t row_idx; };
  ```
- Cap: 33,554,432 (2^25 = next_power_of_2(15,000,000 × 2))
- Mask: 33,554,431 (cap - 1)
- Empty sentinel: `key == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  static uint32_t pk_hash(int32_t key, uint32_t mask) {
      return ((uint32_t)key * 2654435761u) & mask;
  }
  ```
- Probe pattern (C24 bounded):
  ```cpp
  uint32_t h = ((uint32_t)l_orderkey * 2654435761u) & mask;
  for (uint32_t pr = 0; pr < cap; pr++) {
      uint32_t idx = (h + pr) & mask;
      if (ht[idx].key == INT32_MIN) break;       // empty — not found
      if (ht[idx].key == l_orderkey) {
          uint32_t row = ht[idx].row_idx;
          // access o_orderdate[row], o_shippriority[row]
          break;
      }
  }
  ```
- **Note**: For Q3, it may be faster to build a RUNTIME hash map over the ~1.45M filtered orders
  (rather than probing the full 256MB pre-built index) to keep it L3-resident.
  See P30: compact pre-filter HT build pattern.

### o_orderdate_zone_map (zone_map on o_orderdate)
- File: `orders/indexes/o_orderdate_zone_map.bin`
- Layout: `[uint32_t num_blocks][Block*]` where `struct Block { int32_t mn, mx; uint32_t cnt; }`
- Block size: 65536 rows → `num_blocks = ceil(15,000,000 / 65536) = 229` blocks
- File size: 4 + 229×12 = 2,752 bytes
- Usage: skip block b if `blocks[b].mn >= date_threshold` (all dates ≥ 1995-03-15 → none pass)
  ```cpp
  if (blocks[b].mn >= date_threshold) continue; // entire block fails o_orderdate < threshold
  ```

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/indexes/l_shipdate_zone_map.bin`
- Layout: `[uint32_t num_blocks][Block*]` where `struct Block { int32_t mn, mx; uint32_t cnt; }`
- Block size: 65536 rows → `num_blocks = ceil(59,986,052 / 65536) = 916` blocks
- File size: 4 + 916×12 = 10,996 bytes
- Usage: skip block b if `blocks[b].mx <= ship_threshold` (all dates ≤ 1995-03-15 → none pass)
  ```cpp
  if (blocks[b].mx <= ship_threshold) continue; // entire block fails l_shipdate > threshold
  ```
- At 54% selectivity, benefit is marginal — roughly half of blocks can be skipped

## Date Constant Summary
| SQL Expression              | C++ Pattern                                         |
|-----------------------------|-----------------------------------------------------|
| DATE '1995-03-15'           | `gendb::date_to_epoch("1995-03-15")`                |
| o_orderdate < DATE '1995-03-15' | `o_orderdate[row] < date_threshold`             |
| l_shipdate > DATE '1995-03-15'  | `l_shipdate[row] > ship_threshold`  (same value)|
