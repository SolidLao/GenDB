# Q18 Guide

## Column Reference
### c_name (payload_text, bytes, varlen_bytes)
- Files: `gendb/customer/c_name.offsets.bin`, `gendb/customer/c_name.data.bin` (1500000 rows)
- Exact ingest container:
```cpp
struct VarLenColumn {
    std::vector<uint64_t> offsets{0};
    std::vector<char> bytes;
};
```
- Exact write order: `c_name.data.bin`, then `c_name.offsets.bin`.
- This query: projected in the outer select after joining from qualifying orders to customer.

### c_custkey (pk, int32_t, plain_i32)
- File: `gendb/customer/c_custkey.bin` (1500000 rows)
- This query: `c_custkey = o_custkey` and `GROUP BY c_custkey`.

### o_custkey (fk, int32_t, plain_i32)
- File: `gendb/orders/o_custkey.bin` (15000000 rows)
- This query: `c_custkey = o_custkey`.

### o_orderkey (pk, int32_t, plain_i32)
- File: `gendb/orders/o_orderkey.bin` (15000000 rows)
- This query: `o_orderkey IN (subquery)`, `o_orderkey = l_orderkey`, and `GROUP BY o_orderkey`.

### o_orderdate (date, int32_t, days_since_epoch_1970)
- File: `gendb/orders/o_orderdate.bin` (15000000 rows)
- This query: projected, grouped, and used in final `ORDER BY o_totalprice DESC, o_orderdate`.

### o_totalprice (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/orders/o_totalprice.bin` (15000000 rows)
- This query: projected, grouped, and used in final ordering.

### l_orderkey (fk, int32_t, plain_i32)
- File: `gendb/lineitem/l_orderkey.bin` (59986052 rows)
- This query: `GROUP BY l_orderkey` in the subquery and `o_orderkey = l_orderkey` in the outer query.
- Because `lineitem` is sorted by `l_orderkey, l_linenumber`, the grouped-run index summarizes exactly this key.

### l_quantity (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_quantity.bin` (59986052 rows)
- This query: `HAVING SUM(l_quantity) > 300` in the subquery and `SUM(l_quantity)` in the outer query.
- Constant derivation under scale-2 ingest: `300.00 -> 30000`.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| customer | 1500000 | dimension | `c_custkey` | 100000 |
| orders | 15000000 | fact | `o_orderkey` | 100000 |
| lineitem | 59986052 | fact | `l_orderkey`, `l_linenumber` | 100000 |

## Query Analysis
- Filter selectivity from `workload_analysis.json`: the subquery predicate `HAVING SUM(l_quantity) > 300` on `l_orderkey` has selectivity `0.00008`.
- Aggregation estimates from `workload_analysis.json`: subquery groups `15000000` orders; outer query groups about `1200` rows before the final top-100 ordering.
- The generated `lineitem_orderkey_groups.sum_quantity.bin` exactly matches the subquery aggregation grain, so the qualifying order-key set can be produced without rescanning all lineitem rows for the HAVING computation.
- After qualifying keys are known, the outer query can use `orders_pk_dense` to fetch order rows and `customer_pk_dense` to fetch customer rows, then revisit the contiguous lineitem run for the final `SUM(l_quantity)`.

## Indexes
### lineitem_orderkey_groups (sorted grouped runs on `l_orderkey`)
- Files: `gendb/lineitem/indexes/lineitem_orderkey_groups.keys.bin`, `gendb/lineitem/indexes/lineitem_orderkey_groups.row_starts.bin`, `gendb/lineitem/indexes/lineitem_orderkey_groups.row_counts.bin`, `gendb/lineitem/indexes/lineitem_orderkey_groups.sum_quantity.bin`
- Authoritative build code:
```cpp
std::vector<int32_t> group_keys;
std::vector<uint32_t> row_starts;
std::vector<uint32_t> row_counts;
std::vector<int64_t> sum_quantity;
group_keys.reserve(15000000);
row_starts.reserve(15000000);
row_counts.reserve(15000000);
sum_quantity.reserve(15000000);
for (uint32_t row = 0; row < orderkeys.size();) {
    int32_t key = orderkeys[row];
    uint32_t start = row;
    int64_t sum = 0;
    while (row < orderkeys.size() && orderkeys[row] == key) {
        sum += quantity[row];
        ++row;
    }
    group_keys.push_back(key);
    row_starts.push_back(start);
    row_counts.push_back(row - start);
    sum_quantity.push_back(sum);
}
```
- Exact on-disk layout: no explicit struct; four parallel arrays written in this order:
  1. `std::vector<int32_t> group_keys`
  2. `std::vector<uint32_t> row_starts`
  3. `std::vector<uint32_t> row_counts`
  4. `std::vector<int64_t> sum_quantity`
- Empty-slot sentinel: none.
- Multi-value format: group `i` owns the half-open lineitem row range `[row_starts[i], row_starts[i] + row_counts[i])`.
- Actual group count: `15000000` entries in every file.
- Subquery usage:
  - Test `sum_quantity[i] > 30000` to implement `HAVING SUM(l_quantity) > 300` without recomputing group sums.
  - The qualifying order key is `group_keys[i]`.
- Outer-query usage:
  - Reuse the same `row_starts[i]` and `row_counts[i]` to rescan only the matching lineitems and compute the final projected `SUM(l_quantity)`.

### orders_pk_dense (dense PK on `o_orderkey`)
- File: `gendb/orders/indexes/orders_pk_dense.bin`
- Exact build logic from `write_dense_pk`:
```cpp
int32_t max_key = *std::max_element(keys.begin(), keys.end());
std::vector<uint32_t> dense(static_cast<size_t>(max_key) + 1, std::numeric_limits<uint32_t>::max());
for (uint32_t row = 0; row < keys.size(); ++row) {
    dense[static_cast<size_t>(keys[row])] = row;
}
```
- Exact on-disk layout: one `std::vector<uint32_t> dense`.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Actual length: `60000001` entries.
- Query usage: `orders_row = orders_pk_dense[group_keys[i]]`; then read `o_custkey`, `o_orderdate`, and `o_totalprice` from aligned `orders/*.bin` files.

### customer_pk_dense (dense PK on `c_custkey`)
- File: `gendb/customer/indexes/customer_pk_dense.bin`
- Build uses the same `write_dense_pk` implementation as `orders_pk_dense`.
- Exact on-disk layout: one `std::vector<uint32_t> dense`.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Actual length: `1500001` entries because `max(c_custkey) = 1500000`.
- Query usage: `customer_row = customer_pk_dense[o_custkey[orders_row]]`; then read `c_name` and `c_custkey`.

## Implementation Notes
- Q18 has no separate persistent hash table for the subquery because the sorted grouped-run index already materializes `SUM(l_quantity)` at `l_orderkey` grain.
- The guide keeps `c_name` as a payload-only varlen fetch: use `c_name.offsets.bin` + `c_name.data.bin` only after the top candidate orders are known.
