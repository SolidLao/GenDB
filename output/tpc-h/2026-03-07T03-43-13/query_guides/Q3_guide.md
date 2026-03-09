# Q3 Guide

## Column Reference
### c_mktsegment (filter_key, uint8_t, dict_u8)
- Files: `gendb/customer/c_mktsegment.codes.bin`, `gendb/customer/c_mktsegment.dict.offsets.bin`, `gendb/customer/c_mktsegment.dict.data.bin` (1500000 rows; dictionary entries = `6 - 1 = 5`)
- Exact ingest container in `ingest.cpp`:
```cpp
template <typename CodeT>
struct DictColumn {
    std::unordered_map<std::string, CodeT> ids;
    std::vector<std::string> values;
    std::vector<CodeT> codes;
};
```
- Exact write order in `DictColumn::write`: `codes`, then dictionary `data`, then dictionary `offsets`.
- This query: `c_mktsegment = 'BUILDING'`.
- Runtime loading pattern: load the dictionary bytes/offsets, scan entries until the string equals `"BUILDING"`, record that runtime code, then use that code against `c_mktsegment.codes.bin` or `customer_mktsegment_postings`; do not hardcode a numeric code.

### c_custkey (pk, int32_t, plain_i32)
- File: `gendb/customer/c_custkey.bin` (1500000 rows)
- This query: `c_custkey = o_custkey` → join key from filtered customer rows into `orders_custkey_postings`.

### o_custkey (fk, int32_t, plain_i32)
- File: `gendb/orders/o_custkey.bin` (15000000 rows)
- This query: `c_custkey = o_custkey` → lookup key for `orders_custkey_postings`.

### o_orderkey (pk, int32_t, plain_i32)
- File: `gendb/orders/o_orderkey.bin` (15000000 rows)
- This query: `l_orderkey = o_orderkey` and `GROUP BY l_orderkey` → unique order row id and aggregation key component.

### o_orderdate (date, int32_t, days_since_epoch_1970)
- File: `gendb/orders/o_orderdate.bin` (15000000 rows)
- This query: `o_orderdate < DATE '1995-03-15'`, `SELECT o_orderdate`, and `ORDER BY ... o_orderdate`.
- Constant derivation: `DATE '1995-03-15'` encodes to `9204` days since `1970-01-01`.
- C++ comparison: `o_orderdate[row] < 9204`.

### o_shippriority (group_key, int32_t, plain_i32)
- File: `gendb/orders/o_shippriority.bin` (15000000 rows)
- This query: projected and grouped as part of `(l_orderkey, o_orderdate, o_shippriority)`.

### l_orderkey (fk, int32_t, plain_i32)
- File: `gendb/lineitem/l_orderkey.bin` (59986052 rows)
- This query: `l_orderkey = o_orderkey` and `GROUP BY l_orderkey`.
- Because `lineitem` is sorted by `l_orderkey, l_linenumber`, matching rows for one order are contiguous and are summarized by `lineitem_orderkey_groups`.

### l_shipdate (date, int32_t, days_since_epoch_1970)
- File: `gendb/lineitem/l_shipdate.bin` (59986052 rows)
- This query: `l_shipdate > DATE '1995-03-15'`.
- Constant derivation: `DATE '1995-03-15'` encodes to `9204`.
- C++ comparison: `l_shipdate[row] > 9204`.

### l_extendedprice (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_extendedprice.bin` (59986052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → scale-2 arithmetic in `int64_t`.

### l_discount (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_discount.bin` (59986052 rows)
- This query: `l_extendedprice * (1 - l_discount)` → scale-2 arithmetic in `int64_t`.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| customer | 1500000 | dimension | `c_custkey` | 100000 |
| orders | 15000000 | fact | `o_orderkey` | 100000 |
| lineitem | 59986052 | fact | `l_orderkey`, `l_linenumber` | 100000 |

## Query Analysis
- Filter selectivities from `workload_analysis.json`: `c_mktsegment = 'BUILDING'` → `0.22`; `o_orderdate < 1995-03-15` → `0.489`; `l_shipdate > 1995-03-15` → `0.511`.
- Join fanout from `workload_analysis.json`: `customer.c_custkey -> orders.o_custkey` has `right_max_duplicates = 4`; `orders.o_orderkey -> lineitem.l_orderkey` has `right_max_duplicates = 7`.
- Aggregation estimate from `workload_analysis.json`: about `800000` groups on `(l_orderkey, o_orderdate, o_shippriority)`.
- The storage design strongly supports a plan of `customer_mktsegment_postings` → `orders_custkey_postings` + `orders_orderdate_zone_map` → `lineitem_orderkey_groups`, with optional `orders_pk_dense` direct lookups by `o_orderkey`.

## Indexes
### customer_mktsegment_postings (code-postings on `c_mktsegment`)
- Files: `gendb/customer/indexes/customer_mktsegment_postings.offsets.bin`, `gendb/customer/indexes/customer_mktsegment_postings.row_ids.bin`
- Authoritative build call:
```cpp
auto codes = read_vector<uint8_t>(table_dir / "c_mktsegment.codes.bin");
write_code_postings(index_dir, "customer_mktsegment_postings", codes);
```
- Exact on-disk layout: no explicit struct; two parallel arrays written by `write_code_postings`:
  1. `std::vector<uint64_t> offsets`
  2. `std::vector<uint32_t> row_ids`
- Exact multi-value construction:
```cpp
CodeT max_code = *std::max_element(codes.begin(), codes.end());
std::vector<uint64_t> offsets(static_cast<size_t>(max_code) + 2, 0);
for (CodeT code : codes) {
    offsets[static_cast<size_t>(code) + 1]++;
}
for (size_t i = 1; i < offsets.size(); ++i) {
    offsets[i] += offsets[i - 1];
}
std::vector<uint64_t> cursor = offsets;
std::vector<uint32_t> row_ids(codes.size());
for (uint32_t row = 0; row < codes.size(); ++row) {
    size_t slot = static_cast<size_t>(codes[row]);
    row_ids[cursor[slot]++] = row;
}
```
- Empty-slot sentinel: none; an empty posting list is represented by `offsets[code] == offsets[code + 1]`.
- Actual sizes: `offsets` has `6` entries, `row_ids` has `1500000` entries.
- Query usage: after resolving the runtime code for `"BUILDING"`, scan `row_ids[offsets[code] .. offsets[code + 1])` to get qualifying customer row ids.

### orders_custkey_postings (dense postings on `o_custkey`)
- Files: `gendb/orders/indexes/orders_custkey_postings.offsets.bin`, `gendb/orders/indexes/orders_custkey_postings.row_ids.bin`
- Authoritative build call:
```cpp
write_dense_postings(index_dir, "orders_custkey_postings", custkeys);
```
- Exact on-disk layout: no explicit struct; two parallel arrays:
  1. `std::vector<uint64_t> offsets`
  2. `std::vector<uint32_t> row_ids`
- Exact multi-value construction:
```cpp
int32_t max_key = *std::max_element(keys.begin(), keys.end());
std::vector<uint64_t> offsets(static_cast<size_t>(max_key) + 2, 0);
for (int32_t key : keys) {
    offsets[static_cast<size_t>(key) + 1]++;
}
for (size_t i = 1; i < offsets.size(); ++i) {
    offsets[i] += offsets[i - 1];
}
std::vector<uint64_t> cursor = offsets;
std::vector<uint32_t> row_ids(keys.size());
for (uint32_t row = 0; row < keys.size(); ++row) {
    size_t slot = static_cast<size_t>(keys[row]);
    row_ids[cursor[slot]++] = row;
}
```
- Empty-slot sentinel: none; a customer with no orders has `offsets[custkey] == offsets[custkey + 1]`.
- Actual sizes: `offsets` has `1500001` entries, `row_ids` has `15000000` entries.
- Query usage: for each qualifying `c_custkey`, read matching order row ids from `row_ids[offsets[c_custkey] .. offsets[c_custkey + 1])`, then apply the date predicate.

### orders_orderdate_zone_map (zone_map on `o_orderdate`)
- Files: `gendb/orders/indexes/orders_orderdate_zone_map.mins.bin`, `gendb/orders/indexes/orders_orderdate_zone_map.maxs.bin`
- Authoritative build call:
```cpp
write_zone_map(index_dir, "orders_orderdate_zone_map", orderdates, 100000);
```
- Exact on-disk layout: no struct; `std::vector<int32_t> mins` then `std::vector<int32_t> maxs`.
- Empty-slot sentinel: none.
- Actual cardinality: `ceil(15000000 / 100000) = 150` blocks.
- Query usage:
  - Skip block `b` when `mins[b] >= 9204`.
  - Accept block `b` without row-level date checks when `maxs[b] < 9204`.
  - Otherwise apply `o_orderdate[row] < 9204` inside that block.

### orders_pk_dense (dense PK on `o_orderkey`)
- File: `gendb/orders/indexes/orders_pk_dense.bin`
- Exact build logic:
```cpp
int32_t max_key = *std::max_element(keys.begin(), keys.end());
std::vector<uint32_t> dense(static_cast<size_t>(max_key) + 1, std::numeric_limits<uint32_t>::max());
for (uint32_t row = 0; row < keys.size(); ++row) {
    dense[static_cast<size_t>(keys[row])] = row;
}
```
- Exact on-disk layout: one `std::vector<uint32_t>` named `dense`.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Actual length: `60000001` entries because `max(o_orderkey) = 60000000`.
- Query usage: direct order-row lookup by `o_orderkey` when the execution plan drives from lineitem-side order keys.

### lineitem_shipdate_zone_map (zone_map on `l_shipdate`)
- Files: `gendb/lineitem/indexes/lineitem_shipdate_zone_map.mins.bin`, `gendb/lineitem/indexes/lineitem_shipdate_zone_map.maxs.bin`
- Build uses the same `write_zone_map` implementation as Q1 with `block_size = 100000`.
- Empty-slot sentinel: none.
- Actual cardinality: `600` blocks.
- Query usage:
  - Skip block `b` when `maxs[b] <= 9204`.
  - Accept block `b` without row-level date checks when `mins[b] > 9204`.
  - Otherwise scan rows in the block and test `l_shipdate[row] > 9204`.

### lineitem_orderkey_groups (sorted grouped runs on `l_orderkey`)
- Files: `gendb/lineitem/indexes/lineitem_orderkey_groups.keys.bin`, `gendb/lineitem/indexes/lineitem_orderkey_groups.row_starts.bin`, `gendb/lineitem/indexes/lineitem_orderkey_groups.row_counts.bin`, `gendb/lineitem/indexes/lineitem_orderkey_groups.sum_quantity.bin`
- Authoritative grouped-run builder:
```cpp
std::vector<int32_t> group_keys;
std::vector<uint32_t> row_starts;
std::vector<uint32_t> row_counts;
std::vector<int64_t> sum_quantity;
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
- Exact on-disk layout: no explicit struct; four parallel arrays written in this order: `group_keys`, `row_starts`, `row_counts`, `sum_quantity`.
- Empty-slot sentinel: none.
- Multi-value format: group `i` represents all rows in the half-open row-id range `[row_starts[i], row_starts[i] + row_counts[i])`.
- Actual group count: `15000000` entries in each array.
- Query usage: after candidate orders are identified, jump directly to the contiguous lineitem run for `o_orderkey` instead of re-hashing or re-scanning `l_orderkey`.

## Implementation Notes
- Q3 mixes one dictionary filter (`c_mktsegment`) with two date predicates and two PK-FK joins.
- No guide text hardcodes dictionary code numbers; the authoritative ingest implementation assigns codes in first-seen order via `DictColumn::append`.
