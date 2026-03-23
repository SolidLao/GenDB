---
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

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| customer | 1,500,000  | dimension | (none)      | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate | 100,000    |
| lineitem | 59,986,052 | fact      | l_shipdate  | 100,000    |

## Query Analysis
- **Join order (recommended)**: customer → orders → lineitem
  1. Filter customer: `c_mktsegment = BUILDING_CODE` (20% selectivity → ~300,000 rows)
  2. For each qualifying customer, look up orders via `orders_custkey_sorted` FK index
  3. Filter orders: `o_orderdate < 9204` (49% selectivity)
  4. For qualifying orders, look up lineitem rows via `lineitem_orderkey_sorted` FK index
  5. Filter lineitem: `l_shipdate > 9204` (51% selectivity)
- **GROUP BY**: `(l_orderkey, o_orderdate, o_shippriority)` — up to ~10M groups but limited to rows passing all filters; expect far fewer in practice.
- **Output**: top 10 by revenue DESC, o_orderdate ASC.

## Date Thresholds
```
DATE '1995-03-15' → date_to_days(1995, 3, 15) = 9204
```
C++ predicates:
```cpp
o_orderdate[i] < 9204    // orders filter
l_shipdate[j]  > 9204    // lineitem filter
```

## Column Reference

### c_mktsegment (filter_low_card, int8_t, dict)
- File: `customer/c_mktsegment.bin` — int8_t[1,500,000]
- Dict file: `customer/c_mktsegment.dict` — 5 lines (0-indexed)
- Encoding (from ingest.cpp `enc_mktsegment`):
  ```cpp
  switch (s[0]) {
      case 'A': return 0; // AUTOMOBILE
      case 'B': return 1; // BUILDING
      case 'F': return 2; // FURNITURE
      case 'H': return 3; // HOUSEHOLD
      default:  return 4; // MACHINERY
  }
  ```
- Load BUILDING code at runtime:
  ```cpp
  // Read c_mktsegment.dict, find line matching "BUILDING" → that line number is the code
  // Do NOT hardcode: use the dict file for portability
  int8_t building_code = -1;
  FILE* df = fopen("customer/c_mktsegment.dict", "r");
  char buf[64]; int8_t code = 0;
  while (fgets(buf, sizeof(buf), df)) {
      buf[strcspn(buf, "\n")] = 0;
      if (strcmp(buf, "BUILDING") == 0) { building_code = code; break; }
      code++;
  }
  fclose(df);
  ```
- Predicate: `c_mktsegment[i] == building_code`

### c_custkey (PK_join_key, int32_t, raw)
- File: `customer/c_custkey.bin` — int32_t[1,500,000]
- Used as join key: matched against `o_custkey` via `orders_custkey_sorted` FK index

### o_custkey (FK_join_key, int32_t, raw)
- File: `orders/o_custkey.bin` — int32_t[15,000,000]
- Join side: looked up via `orders_custkey_sorted` index

### o_orderdate (date_filter, int32_t, days_since_epoch_1970)
- File: `orders/o_orderdate.bin` — int32_t[15,000,000]
- Sorted ascending (table sort order = o_orderdate)
- Predicate: `o_orderdate[i] < 9204`
- Also output column

### o_orderkey (PK_join_key, int32_t, raw)
- File: `orders/o_orderkey.bin` — int32_t[15,000,000]
- Used as join key matched against `l_orderkey` via `lineitem_orderkey_sorted`
- Also GROUP BY / output key

### o_shippriority (group_by_output, int32_t, raw)
- File: `orders/o_shippriority.bin` — int32_t[15,000,000]
- GROUP BY and output column; fetched for qualifying orders rows

### l_orderkey (FK_join_key, int32_t, raw)
- File: `lineitem/l_orderkey.bin` — int32_t[59,986,052]
- Join side: looked up via `lineitem_orderkey_sorted`
- Also GROUP BY / output key

### l_shipdate (date_filter, int32_t, days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` — int32_t[59,986,052]
- Sorted ascending (table sort order = l_shipdate)
- Predicate: `l_shipdate[j] > 9204`

### l_extendedprice (measure, double, raw)
- File: `lineitem/l_extendedprice.bin` — double[59,986,052]
- Used in: `SUM(l_extendedprice * (1.0 - l_discount))`

### l_discount (measure_filter, double, raw)
- File: `lineitem/l_discount.bin` — double[59,986,052]
- Used in: `SUM(l_extendedprice * (1.0 - l_discount))`

## Indexes

### orders_custkey_sorted (fk_sorted on o_custkey)
- File: `indexes/orders_custkey_sorted.bin`
- Layout (from build_indexes.cpp `build_fk_sorted`):
  ```
  uint64_t  num_pairs                          // 15,000,000
  { int32_t key; int32_t row_id; } [num_pairs] // sorted ascending by key (= o_custkey)
  ```
- Struct: `struct Pair { int32_t key; int32_t row_id; };`
- Usage: binary-search for `c_custkey` value, then iterate while `pairs[pos].key == c_custkey`:
  ```cpp
  // Binary search for first occurrence of custkey
  size_t lo = 0, hi = num_pairs;
  while (lo < hi) {
      size_t mid = (lo + hi) / 2;
      if (pairs[mid].key < custkey) lo = mid + 1;
      else hi = mid;
  }
  // Iterate matching rows
  for (size_t p = lo; p < num_pairs && pairs[p].key == custkey; p++) {
      int32_t ord_row = pairs[p].row_id;
      // check o_orderdate[ord_row] < 9204
  }
  ```
- 1:many: each customer maps to up to 46 orders (workload_analysis max_duplicates)

### lineitem_orderkey_sorted (fk_sorted on l_orderkey)
- File: `indexes/lineitem_orderkey_sorted.bin`
- Layout (from build_indexes.cpp `build_fk_sorted`):
  ```
  uint64_t  num_pairs                          // 59,986,052
  { int32_t key; int32_t row_id; } [num_pairs] // sorted ascending by key (= l_orderkey)
  ```
- Struct: `struct Pair { int32_t key; int32_t row_id; };`
- Usage: binary-search for `o_orderkey`, iterate matching lineitem rows:
  ```cpp
  // Binary search for first occurrence of orderkey
  size_t lo = 0, hi = num_pairs;
  while (lo < hi) {
      size_t mid = (lo + hi) / 2;
      if (pairs[mid].key < orderkey) lo = mid + 1;
      else hi = mid;
  }
  for (size_t p = lo; p < num_pairs && pairs[p].key == orderkey; p++) {
      int32_t li_row = pairs[p].row_id;
      // check l_shipdate[li_row] > 9204
      // accumulate revenue
  }
  ```
- 1:many: each order maps to up to 7 lineitem rows

### orders_orderdate_zonemap (zone_map on o_orderdate)
- File: `indexes/orders_orderdate_zonemap.bin`
- Layout:
  ```
  uint64_t  n_blocks               // ceil(15,000,000 / 100,000) = 150
  { int32_t min_val; int32_t max_val; } [n_blocks]
  ```
- Usage: optional optimization — if iterating orders table directly rather than via FK index, skip blocks where `min_val >= 9204` (all dates in block fail `< 9204`).

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout:
  ```
  uint64_t  n_blocks               // ceil(59,986,052 / 100,000) = 600
  { int32_t min_val; int32_t max_val; } [n_blocks]
  ```
- Usage: if scanning lineitem directly, skip blocks where `max_val <= 9204` (all dates fail `> 9204`).

## Aggregation Strategy
Hash map keyed by `(l_orderkey, o_orderdate, o_shippriority)` accumulating revenue:
```cpp
struct GroupKey { int32_t orderkey, orderdate, shippriority; };
struct Acc { double revenue; };
std::unordered_map<GroupKey, Acc> agg;
// After all joins:
// Take top 10 by revenue DESC, then o_orderdate ASC
```
---
