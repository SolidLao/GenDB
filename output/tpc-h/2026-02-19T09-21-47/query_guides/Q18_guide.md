# Q18 Guide — Large Volume Customer

## Column Reference

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 rows)
- This query: join key `c_custkey = o_custkey`. Also in SELECT output and GROUP BY.

### c_name (STRING, varstring)
- Files: `customer/c_name_offsets.bin` (uint32_t[1,500,001]) + `customer/c_name_data.bin`
- Access row `i`: `start = offsets[i]`, `len = offsets[i+1] - offsets[i]`, string = `data + start`
- This query: in SELECT output and GROUP BY.

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows, sorted by o_orderdate)
- This query: join key `o_orderkey = l_orderkey` (outer query). Also in SELECT, GROUP BY.
- Subquery: `o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)`

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 rows)
- This query: join key `o_custkey = c_custkey`.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows, sorted ascending)
- This query: in SELECT output and GROUP BY.
- No filter on o_orderdate in Q18 — full scan required.
- For output: convert epoch days back to date string using YEAR_DAYS[] and MONTH_STARTS[].

### o_totalprice (DECIMAL, double)
- File: `orders/o_totalprice.bin` (15,000,000 rows)
- Stored as native double. No scaling needed.
- This query: in SELECT output, GROUP BY, and ORDER BY (DESC).

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: used in subquery `GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`.
  Also outer join: `o_orderkey = l_orderkey`.

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows)
- Stored as native double. No scaling.
- This query: subquery `SUM(l_quantity) > 300`, outer `SUM(l_quantity) AS sum_qty`.

---

## Table Stats
| Table    | Rows       | Role      | Sort Order    | Block Size |
|----------|------------|-----------|---------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate ↑  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate ↑ | 100,000    |
| customer | 1,500,000  | dimension | none          | 100,000    |

---

## Query Analysis
- **Subquery type**: `IN (SELECT ... HAVING ...)` — decorrelate to semi-join.
  - Phase 1: Scan all of lineitem (l_orderkey + l_quantity only). Group by l_orderkey,
    compute SUM(l_quantity) per orderkey. Collect qualifying orderkeys where sum > 300.
    Store as a hash set of qualifying int32_t orderkeys.
    Expected: very few qualifying orderkeys (large volumes are rare in TPC-H SF10).
  - Phase 2: Scan orders, filter `o_orderkey IN qualifying_set`.
    For each qualifying order: probe c_custkey hash to get c_name.
    Then scan lineitem again (filtered by o_orderkey in qualifying set) for SUM(l_quantity).
- **Join pattern**: `customer ⨝ orders ⨝ lineitem`
  - customer → orders: FK o_custkey = c_custkey
  - orders → lineitem: FK l_orderkey = o_orderkey
- **Recommended execution**:
  1. **Pass 1** over lineitem (l_orderkey, l_quantity columns only, ~600+480 = 1GB combined):
     Build `std::unordered_map<int32_t, double> orderkey_sum`. Size: ~15M entries.
     Collect `qualifying_set` = {orderkeys where sum > 300}. Expected: ~very small set.
  2. Scan orders (all rows, no date filter). For each order with `o_orderkey IN qualifying_set`:
     Add to result. Join with customer via c_custkey_hash to get c_name.
  3. **Pass 2** over lineitem (l_orderkey, l_quantity), compute per-group SUM(l_quantity)
     for qualifying orderkeys.
  4. Merge and aggregate by (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice).
  5. ORDER BY o_totalprice DESC, o_orderdate ASC. LIMIT 100. Use top-K sort.
- **Subquery selectivity**: SUM(l_quantity) > 300 per order. TPC-H SF10 typical: ~few hundred qualifying orderkeys. Very selective.
- **Output**: 100 rows (LIMIT 100), ordered by o_totalprice DESC then o_orderdate ASC.
- **No date filters** on any table in Q18.
- **Zone maps**: NOT useful (no range predicates on stored columns).

---

## Indexes

### c_custkey_hash (hash on c_custkey → row_pos in customer)
- File: `customer/indexes/c_custkey_hash.bin`
- Layout: `[uint32_t capacity=4194304][uint32_t num_entries=1500000][HashSlot × capacity]`
- `HashSlot = {int32_t key, uint32_t row_pos}` = 8 bytes. Empty: `key == INT32_MIN`.
- Hash: `h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64-22)) & (cap-1)`
- This query: given `o_custkey`, look up row_pos in customer → access `c_name` (varstring).
  Since c_custkey = c_name are in the output and GROUP BY, retrieve c_name for qualifying orders.

### o_orderkey_hash (hash on o_orderkey → row_pos in orders)
- File: `orders/indexes/o_orderkey_hash.bin`
- Layout: `[uint32_t capacity=33554432][uint32_t num_entries=15000000][HashSlot × capacity]`
- `HashSlot = {int32_t key, uint32_t row_pos}` = 8 bytes. Empty: `key == INT32_MIN`.
- Hash: `h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64-25)) & (cap-1)`
- This query: given qualifying l_orderkey from subquery, look up o_orderdate, o_totalprice,
  o_custkey in orders. More efficient than scanning all of orders.

### l_shipdate_zonemap, l_discount_zonemap, l_quantity_zonemap, o_orderdate_zonemap
- **Not useful for Q18** — no range filters on any of these columns.

---

## Subquery Execution Detail
```
Phase 1 — Build qualifying orderkey set:
  mmap lineitem/l_orderkey.bin and lineitem/l_quantity.bin
  For each of 59,986,052 rows:
      orderkey_qty[l_orderkey[i]] += l_quantity[i]
  Collect qualifying = { k : orderkey_qty[k] > 300.0 }
  Store as a hash set (open addressing, int32_t keys)

Phase 2 — Scan and join:
  For each qualifying orderkey k in qualifying:
      Look up row_pos in orders via o_orderkey_hash
      Get o_custkey, o_orderdate, o_totalprice from orders columns
      Look up c_name via c_custkey_hash on o_custkey
      Scan lineitem for rows where l_orderkey == k → sum l_quantity
      Emit: (c_name, c_custkey, k, o_orderdate, o_totalprice, sum_qty)
  Sort by (o_totalprice DESC, o_orderdate ASC), take top 100
```

## Date Reconstruction for Output
o_orderdate stored as int32_t epoch days. To display:
```cpp
// Reconstruct year, month, day from epoch days d
int yr = extract_year(d);  // binary search on YEAR_DAYS[]
int remaining = d - YEAR_DAYS[yr-1970];
bool leap = is_leap_year(yr);
int mo = 1;
while (mo < 12 && MONTH_STARTS[leap][mo] <= remaining) mo++;
int dy = remaining - MONTH_STARTS[leap][mo-1] + 1;
// Format as YYYY-MM-DD
```
