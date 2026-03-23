# Q18 Guide

## Column Reference
### l_orderkey (foreign_key, int32_t, plain)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- This query: inner aggregation `GROUP BY l_orderkey HAVING SUM(l_quantity) > 300` and outer join `o_orderkey = l_orderkey`

### l_quantity (decimal, double, plain)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- This query: inner `SUM(l_quantity)` and outer `SUM(l_quantity)`

### o_orderkey (primary_key, int32_t, plain)
- File: `orders/o_orderkey.bin` (15000000 rows)
- This query: semijoin target from qualifying order keys and outer grouping key

### o_custkey (foreign_key, int32_t, plain)
- File: `orders/o_custkey.bin` (15000000 rows)
- This query: `c_custkey = o_custkey`

### o_orderdate (date, int32_t, plain)
- File: `orders/o_orderdate.bin` (15000000 rows)
- This query: output, grouping, and secondary sort key

### o_totalprice (decimal, double, plain)
- File: `orders/o_totalprice.bin` (15000000 rows)
- This query: output, grouping, and primary descending sort key

### c_custkey (primary_key, int32_t, plain)
- File: `customer/c_custkey.bin` (1500000 rows)
- This query: `c_custkey = o_custkey`

### c_name (name, varlen_utf8, varlen)
- Files:
  - `customer/c_name.offsets.bin`
  - `customer/c_name.data.bin`
- This query: output and grouping

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 131072 |
| orders | 15000000 | fact | `o_orderkey` | 131072 |
| customer | 1500000 | dimension | `c_custkey` | 131072 |

## Query Analysis
- Workload selectivity for `HAVING SUM(l_quantity) > 300` on grouped lineitems is `0.0000416`
- Natural execution pattern:
  - first pass over `lineitem` aggregates `SUM(l_quantity)` by `l_orderkey`
  - qualifying order keys form a sparse set
  - each qualifying key is resolved in `orders_pk_dense`
  - `o_custkey` then resolves through `customer_pk_dense`
  - `lineitem_order_postings` provides the outer query’s lineitems for each surviving order without rescanning the full fact table
- Final ordering is `o_totalprice DESC, o_orderdate` with `LIMIT 100`

## Indexes
### lineitem_order_postings (sorted postings on `l_orderkey`)
- Files:
  - `lineitem/indexes/lineitem_order_postings.offsets.bin`
  - `lineitem/indexes/lineitem_order_postings.rowids.bin`
- Build code from `build_indexes.cpp`:
```cpp
std::vector<uint64_t> offsets(static_cast<size_t>(max_key) + 2, 0);
for (KeyT key : keys) {
    offsets[static_cast<size_t>(key) + 1]++;
}
for (size_t i = 1; i < offsets.size(); ++i) {
    offsets[i] += offsets[i - 1];
}
std::vector<uint64_t> cursors = offsets;
std::vector<uint64_t> rowids(keys.size(), 0);
for (uint64_t row = 0; row < keys.size(); ++row) {
    size_t slot = static_cast<size_t>(keys[row]);
    rowids[cursors[slot]++] = row;
}
```
- Exact multi-value format:
  - posting slice for order key `k` is `rowids[offsets[k] .. offsets[k + 1])`
  - each posting is a `lineitem` row id
  - the same slice supports both the semijoin after the HAVING phase and the outer `SUM(l_quantity)`
- Struct layout: none; two raw `uint64_t` arrays
- Empty-slot sentinel: none

### orders_pk_dense (dense PK array on `o_orderkey`)
- File: `orders/indexes/orders_pk_dense.bin`
- Build code from `build_indexes.cpp`:
```cpp
std::vector<uint64_t> rowids(static_cast<size_t>(max_key) + 1, std::numeric_limits<uint64_t>::max());
for (uint64_t row = 0; row < keys.size(); ++row) {
    rowids[static_cast<size_t>(keys[row])] = row;
}
```
- Exact format:
  - slot `k` stores the `orders` row id for order key `k`
  - there is no hash function; lookup is direct addressing
- Empty-slot sentinel: `std::numeric_limits<uint64_t>::max()`

### customer_pk_dense (dense PK array on `c_custkey`)
- File: `customer/indexes/customer_pk_dense.bin`
- Exact format is the same `build_dense_pk` layout shown above
- Lookup: slot `o_custkey` returns the customer row id
- Empty-slot sentinel: `std::numeric_limits<uint64_t>::max()`

## Varlen Handling
- `customer/c_name` is a `StringColumn`
- Exact on-disk layout from ingest:
  - `c_name.offsets.bin`: `uint64_t` offsets with the first value initialized to `0`
  - `c_name.data.bin`: concatenated character bytes
- Row `r` decodes from byte range `[offsets[r], offsets[r + 1])`

## Formula Notes
- The qualifying-order subquery result is not materialized in storage
- Query code must compute `SUM(l_quantity)` from `lineitem/l_quantity.bin` at runtime, then use the stored indexes only for the subsequent joins and outer aggregation
