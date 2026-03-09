# Q18 Guide — Large Volume Customer

## Query
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
       SUM(l_quantity) AS sum_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
)
AND c_custkey = o_custkey
AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate ASC
LIMIT 100;
```

## Table Stats
| Table    | Rows       | Role        | Sort Order    | Block Size |
|----------|------------|-------------|---------------|------------|
| lineitem | 59,986,052 | fact (×2)   | l_shipdate ↑  | 100,000    |
| orders   | 15,000,000 | dimension   | o_orderdate ↑ | 100,000    |
| customer | 1,500,000  | dimension   | c_custkey ↑   | 100,000    |

## Column Reference

### l_orderkey (fk_orders_pk, int32_t — raw)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes)
- **Pass 1 (subquery):** GROUP BY l_orderkey, compute SUM(l_quantity) per group,
  emit qualifying orderkeys where SUM > 300. Build a hash set of qualifying orderkeys.
- **Pass 2 (main query):** join predicate `l_orderkey = o_orderkey`;
  filter to qualifying orderkeys from Pass 1.

### l_quantity (decimal_15_2, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes)
- **Pass 1:** accumulated per l_orderkey group to compute SUM(l_quantity) > 300.
- **Pass 2:** SUM(l_quantity) output aggregate

### o_orderkey (pk, int32_t — raw)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes, sorted by o_orderdate)
- This query: FK join target; also GROUP BY / output column

### o_custkey (fk_customer_pk, int32_t — raw)
- File: `orders/o_custkey.bin` (15,000,000 × 4 bytes)
- This query: join predicate `o_custkey = c_custkey` via `c_custkey_hash`

### o_orderdate (date, int32_t — days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes)
- This query: GROUP BY key and ORDER BY key (ascending tiebreaker)
- Decode for output: apply JDN-to-calendar conversion (see Q9 guide for formula)
  or store as int32_t and convert only for the 100 output rows.

### o_totalprice (decimal_15_2, double)
- File: `orders/o_totalprice.bin` (15,000,000 × 8 bytes)
- This query: GROUP BY key (included in group key per SQL) and primary ORDER BY (DESC)

### c_custkey (pk, int32_t — raw)
- File: `customer/c_custkey.bin` (1,500,000 × 4 bytes)
- This query: GROUP BY / output column; join target via `c_custkey_hash`

### c_name (varchar_25, char[26] — fixed-width padded, stride=26)
- File: `customer/c_name.bin` (1,500,000 × 26 bytes = 39MB)
- Encoding from `ingest.cpp::ingest_customer`:
  ```cpp
  const int CNAME_STRIDE = 26;
  char name_buf[CNAME_STRIDE] = {};
  strncpy(name_buf, tok[1], CNAME_STRIDE - 1);
  // stored as 26 contiguous bytes, zero-padded
  ```
- Access by customer row index:
  ```cpp
  const char* cname = c_name_buf + cust_row * 26;
  // null-terminated within 26-byte slot; safe to use as C string
  ```
- This query: GROUP BY key and output column (only for ~few-thousand qualifying rows)

## Indexes

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
  Total file size: 16 + 33554432×4 + 33554432×4 = 16 + 268435456 = ~256MB
- Hash function (verbatim from `build_indexes.cpp::hash32`):
  ```cpp
  static inline uint32_t hash32(uint32_t x) {
      x = ((x >> 16) ^ x) * 0x45d9f3bu;
      x = ((x >> 16) ^ x) * 0x45d9f3bu;
      x = (x >> 16) ^ x;
      return x;
  }
  ```
- Probe sequence: open-addressing, linear probing:
  ```cpp
  int64_t mask = 33554432LL - 1;  // = 33554431
  uint64_t h = (uint64_t)hash32((uint32_t)orderkey) & (uint64_t)mask;
  while (keys[h] != -1) {
      if (keys[h] == orderkey) { orders_row = values[h]; break; }
      h = (h + 1) & (uint64_t)mask;
  }
  ```
- Usage: `l_orderkey` → orders row → fetch o_orderdate, o_totalprice, o_custkey, o_shippriority

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
  Total file size: 16 + 4194304×4 + 4194304×4 = ~32MB
- Hash function: same `hash32` as above.
- Probe sequence: same linear probing pattern with `mask = 4194304 - 1`.
- Usage: `o_custkey` → customer row → fetch c_name, c_custkey

## Query Analysis

### Pass 1 — Subquery: Build Qualifying Orderkey Set
Scan `lineitem/l_orderkey.bin` and `lineitem/l_quantity.bin` in parallel
(single pass, two columns):

```cpp
// Build per-orderkey quantity sums
// Estimated 15M distinct orderkeys; use unordered_map<int32_t, double>
std::unordered_map<int32_t, double> qty_sum;
qty_sum.reserve(16000000);  // avoid rehashing

for (int64_t i = 0; i < nlineitem; i++) {
    qty_sum[l_orderkey[i]] += l_quantity[i];
}

// Collect qualifying orderkeys (SUM > 300)
// Expected: very few orders (TPC-H scale factor: < 100 orders qualify)
std::unordered_set<int32_t> qualifying;
for (auto& [k, v] : qty_sum) {
    if (v > 300.0) qualifying.insert(k);
}
```

Note: at TPC-H scale factor 1 (1M customers), very few orderkeys qualify.
At SF 10 (15M orders), still only a few hundred are expected to have total
quantity > 300. The output LIMIT 100 confirms the result set is tiny.

### Pass 2 — Main Query: Join and Aggregate
For each row i in lineitem (second full scan):
```cpp
if (qualifying.count(l_orderkey[i]) == 0) continue;  // fast hash set check

// Look up orders row
int32_t orders_row = probe(o_orderkey_hash, l_orderkey[i]);
int32_t o_cust = o_custkey[orders_row];
int32_t o_date = o_orderdate[orders_row];
double  o_price = o_totalprice[orders_row];

// Look up customer row
int32_t cust_row = probe(c_custkey_hash, o_cust);

// Accumulate into group key (o_orderkey is the unique group identifier here,
// since each order has exactly one c_name, c_custkey, o_orderdate, o_totalprice)
groups[l_orderkey[i]].sum_qty += l_quantity[i];
groups[l_orderkey[i]].orders_row = orders_row;
groups[l_orderkey[i]].cust_row   = cust_row;
```

Since qualifying orderkeys are very few (≪ 100,000), use a small hash map
keyed by l_orderkey. The inner loop still scans all 60M lineitem rows but
the map lookups are O(1) and the actual insertions are rare.

### Optimization: Single-Pass Strategy
Because qualifying orderkeys are so rare, consider a single combined pass:
1. First pass: build qty_sum map (60M rows, 2 columns: l_orderkey + l_quantity)
2. Second pass: full lineitem scan (60M rows, 3 columns: l_orderkey + l_quantity + join keys)
   - If `qualifying.count(l_orderkey[i])`: do the joins and accumulate

Alternative single-pass (if memory allows):
- Build qty_sum in Pass 1 as before (necessary since we need HAVING aggregation before main query).
- No way to avoid two lineitem scans without significant memory overhead.

### Output Phase
After Pass 2, collect ~few-thousand group results:
```cpp
// For each qualifying group:
struct Result {
    char    c_name[26];
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;    // decode to "YYYY-MM-DD" for output only
    double  o_totalprice;
    double  sum_qty;
};
```
Sort by (o_totalprice DESC, o_orderdate ASC), then emit top 100.

### Data Volume Summary
| Pass | Columns Read                        | Rows   | ~MB  |
|------|-------------------------------------|--------|------|
| 1    | l_orderkey (int32), l_quantity (dbl)| 60M    | 480  |
| 2    | l_orderkey (int32), l_quantity (dbl)| 60M    | 480  |
| 2    | o_orderkey_hash (index)             | lookup | 256  |
| 2    | c_custkey_hash (index)              | lookup |  32  |
| 2    | o_custkey, o_orderdate, o_totalprice| few    |  —   |
| 2    | c_name                              | few    |  —   |

Both lineitem passes are I/O-bound on HDD. Consider parallelizing each pass
with 64 threads (one per core), partitioning the lineitem row range.
