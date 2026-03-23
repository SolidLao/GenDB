---
# Q18 Guide — Large Volume Customer

## Query
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100;
```

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate | 100,000    |
| customer | 1,500,000  | dimension | (none)      | 100,000    |

## Query Analysis
- **Phase 1 (subquery)**: Full scan of lineitem grouped by `l_orderkey`; collect orderkeys where `SUM(l_quantity) > 300`. Expected ~0.2% selectivity → ~few thousand qualifying orderkeys.
- **Phase 2 (main query)**: For each qualifying orderkey, probe `orders_orderkey_hash` → get order row → probe `orders_custkey_sorted` or `customer_custkey_hash` for customer info → re-scan lineitem rows for that orderkey via `lineitem_orderkey_sorted`.
- **GROUP BY**: `(c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)` — each orderkey is unique in orders, so groups ≈ number of qualifying orderkeys.
- **Output**: top 100 by o_totalprice DESC, o_orderdate ASC.

## Column Reference

### l_orderkey (FK_join_key, int32_t, raw)
- File: `lineitem/l_orderkey.bin` — int32_t[59,986,052]
- Phase 1: GROUP BY key for subquery aggregation
- Phase 2: join key to orders

### l_quantity (measure, double, raw)
- File: `lineitem/l_quantity.bin` — double[59,986,052]
- Phase 1: SUM per l_orderkey, compared to 300.0
- Phase 2: SUM per group for output

### o_orderkey (PK_join_key, int32_t, raw)
- File: `orders/o_orderkey.bin` — int32_t[15,000,000]
- Probe target for `orders_orderkey_hash`; also GROUP BY / output key

### o_custkey (FK_join_key, int32_t, raw)
- File: `orders/o_custkey.bin` — int32_t[15,000,000]
- Join key to customer.c_custkey (probe `customer_custkey_hash`)

### o_orderdate (date_filter, int32_t, days_since_epoch_1970)
- File: `orders/o_orderdate.bin` — int32_t[15,000,000]
- GROUP BY / output / ORDER BY key
- Display: convert back from days-since-epoch to YYYY-MM-DD for output

### o_totalprice (measure_output, double, raw)
- File: `orders/o_totalprice.bin` — double[15,000,000]
- GROUP BY / ORDER BY / output column

### c_custkey (PK_join_key, int32_t, raw)
- File: `customer/c_custkey.bin` — int32_t[1,500,000]
- GROUP BY / output key; looked up via `customer_custkey_hash`

### c_name (output_payload, varlen, offset_data)
- Files: `customer/c_name.offsets` — int32_t[1,500,001]; `customer/c_name.data` — raw bytes
- offsets[i] = byte start of row i; length = offsets[i+1] - offsets[i]
- Output column; fetched only for ~few-thousand qualifying customer rows

## Indexes

### lineitem_orderkey_sorted (fk_sorted on l_orderkey)
- File: `indexes/lineitem_orderkey_sorted.bin`
- Layout (from build_indexes.cpp `build_fk_sorted`):
  ```
  uint64_t  num_pairs                          // 59,986,052
  { int32_t key; int32_t row_id; } [num_pairs] // sorted ascending by key (= l_orderkey)
  ```
- Struct: `struct Pair { int32_t key; int32_t row_id; };`
- **Phase 1 usage**: This index is NOT needed for phase 1 — do a direct sequential scan of `l_orderkey.bin` and `l_quantity.bin` building a hash map:
  ```cpp
  // Phase 1: scan lineitem once
  std::unordered_map<int32_t, double> qty_sum;  // orderkey -> SUM(quantity)
  for (size_t i = 0; i < N_LI; i++)
      qty_sum[l_orderkey[i]] += l_quantity[i];
  // Collect qualifying orderkeys
  std::unordered_set<int32_t> hot_keys;
  for (auto& [ok, sq] : qty_sum)
      if (sq > 300.0) hot_keys.insert(ok);
  ```
- **Phase 2 usage**: For each hot orderkey, binary-search this index to get all lineitem row_ids:
  ```cpp
  size_t lo = 0, hi = num_pairs;
  while (lo < hi) {
      size_t mid = (lo + hi) / 2;
      if (pairs[mid].key < orderkey) lo = mid + 1;
      else hi = mid;
  }
  for (size_t p = lo; p < num_pairs && pairs[p].key == orderkey; p++) {
      int32_t li_row = pairs[p].row_id;
      sum_qty += l_quantity[li_row];
  }
  ```

### orders_orderkey_hash (hash_pk on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout (from build_indexes.cpp `build_pk_hash`):
  ```
  uint64_t  num_entries            // 15,000,000
  uint64_t  bucket_count           // first odd number >= ceil(15000000/0.6)+2
  { int32_t key; int32_t row_id; } [bucket_count]
  ```
- Empty sentinel: `key == INT32_MIN` (-2147483648)
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t h = ((uint64_t)(uint32_t)key * 2654435761ULL) % bucket_count;
  // linear probe: if occupied, h = (h+1 < bucket_count) ? h+1 : 0
  ```
- Usage: for each hot orderkey, probe to get orders row_id → fetch o_custkey, o_orderdate, o_totalprice

### customer_custkey_hash (hash_pk on c_custkey)
- File: `indexes/customer_custkey_hash.bin`
- Layout (from build_indexes.cpp `build_pk_hash`):
  ```
  uint64_t  num_entries            // 1,500,000
  uint64_t  bucket_count           // first odd number >= ceil(1500000/0.6)+2
  { int32_t key; int32_t row_id; } [bucket_count]
  ```
- Empty sentinel: `key == INT32_MIN` (-2147483648)
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t h = ((uint64_t)(uint32_t)key * 2654435761ULL) % bucket_count;
  // linear probe: if occupied, h = (h+1 < bucket_count) ? h+1 : 0
  ```
- Usage: probe with o_custkey → customer row_id → fetch c_name (varlen) and c_custkey (for output)

## Execution Plan

### Phase 1 — Build hot orderkey set
```
Sequential scan: lineitem/l_orderkey.bin + lineitem/l_quantity.bin
→ hash map { orderkey → double sum_qty }
→ filter sum_qty > 300.0
→ result: unordered_set<int32_t> hot_keys  (~few thousand)
```

### Phase 2 — Main join for hot orderkeys
```
For each hot orderkey:
  1. Probe orders_orderkey_hash → orders row_id
  2. Fetch o_custkey, o_orderdate, o_totalprice, o_orderkey
  3. Probe customer_custkey_hash(o_custkey) → customer row_id
  4. Fetch c_custkey, c_name (varlen)
  5. Probe lineitem_orderkey_sorted(orderkey) → lineitem row_ids
  6. Accumulate SUM(l_quantity) over matching lineitem rows
  7. Emit group: (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_qty)
```

### Phase 3 — Sort and limit
```
Sort result rows by o_totalprice DESC, then o_orderdate ASC
Emit top 100
```
