# Q9 Guide

## Column Reference
### p_partkey (primary_key, int32_t, plain)
- File: `part/p_partkey.bin` (2000000 rows)
- This query: `p_partkey = l_partkey`

### p_name (name, varlen_utf8, varlen)
- Files:
  - `part/p_name.offsets.bin`
  - `part/p_name.data.bin`
- Storage: `StringColumn` with `uint64_t offsets` and byte payload
- This query: `p_name LIKE '%green%'`
- Runtime predicate:
  - decode row `r` as bytes `[offsets[r], offsets[r + 1])`
  - substring-search for `"green"`; there is no auxiliary text index

### s_suppkey (primary_key, int32_t, plain)
- File: `supplier/s_suppkey.bin` (100000 rows)
- This query: `s_suppkey = l_suppkey`

### s_nationkey (foreign_key, int32_t, plain)
- File: `supplier/s_nationkey.bin` (100000 rows)
- This query: `s_nationkey = n_nationkey`

### n_nationkey (primary_key, int32_t, plain)
- File: `nation/n_nationkey.bin` (25 rows)
- This query: `s_nationkey = n_nationkey`

### n_name (nation_name, uint32_t, dictionary)
- Files:
  - `nation/n_name.bin` (25 rows)
  - `nation/n_name.dict.offsets.bin`
  - `nation/n_name.dict.data.bin`
- This query: `SELECT n_name AS nation` and `GROUP BY nation`
- Runtime loading pattern:
  - aggregate by `n_nationkey` or by `n_name` code
  - decode names from the dictionary after aggregation or when materializing final output

### l_suppkey (foreign_key, int32_t, plain)
- File: `lineitem/l_suppkey.bin` (59986052 rows)
- This query: `s_suppkey = l_suppkey` and `ps_suppkey = l_suppkey`

### l_partkey (foreign_key, int32_t, plain)
- File: `lineitem/l_partkey.bin` (59986052 rows)
- This query: `p_partkey = l_partkey` and `ps_partkey = l_partkey`

### l_orderkey (foreign_key, int32_t, plain)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- This query: `o_orderkey = l_orderkey`

### l_extendedprice (decimal, double, plain)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- This query: revenue component in `amount`

### l_discount (decimal, double, plain)
- File: `lineitem/l_discount.bin` (59986052 rows)
- This query: revenue component in `amount`

### l_quantity (decimal, double, plain)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- This query: cost component in `amount`

### ps_partkey (foreign_key, int32_t, plain)
- File: `partsupp/ps_partkey.bin` (8000000 rows)
- This query: `ps_partkey = l_partkey`

### ps_suppkey (foreign_key, int32_t, plain)
- File: `partsupp/ps_suppkey.bin` (8000000 rows)
- This query: `ps_suppkey = l_suppkey`

### ps_supplycost (decimal, double, plain)
- File: `partsupp/ps_supplycost.bin` (8000000 rows)
- This query: cost component in `amount`

### o_orderkey (primary_key, int32_t, plain)
- File: `orders/o_orderkey.bin` (15000000 rows)
- This query: `o_orderkey = l_orderkey`

### o_orderdate (date, int32_t, plain)
- File: `orders/o_orderdate.bin` (15000000 rows)
- Encoding: `parse_date()` in ingest writes `days_since_epoch_1970`
- This query: `EXTRACT(YEAR FROM o_orderdate) AS o_year`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| part | 2000000 | dimension | `p_partkey` | 131072 |
| supplier | 100000 | dimension | `s_suppkey` | 131072 |
| nation | 25 | dimension | `n_nationkey` | 131072 |
| partsupp | 8000000 | fact | `ps_partkey, ps_suppkey` | 131072 |
| orders | 15000000 | fact | `o_orderkey` | 131072 |
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 131072 |

## Query Analysis
- Workload selectivity for `p_name LIKE '%green%'` is `0.054391`
- The expensive relation is `lineitem`; all other joins are lookups or small posting scans
- Practical execution pattern:
  - scan `part/p_name` and mark qualifying `p_partkey` values whose name contains `"green"`
  - scan `lineitem` and keep rows whose `l_partkey` is marked
  - resolve supplier via dense PK, then nation via dense PK
  - resolve orders via dense PK to compute `o_year`
  - resolve `(ps_partkey, ps_suppkey)` by scanning the posting slice for `ps_partkey = l_partkey` and matching `ps_suppkey == l_suppkey`
- Output groups are small: workload estimate is `175` groups on `(nation, o_year)`

## Indexes
### part_pk_dense (dense PK array on `p_partkey`)
- File: `part/indexes/part_pk_dense.bin`
- Build code from `build_indexes.cpp`:
```cpp
std::vector<uint64_t> rowids(static_cast<size_t>(max_key) + 1, std::numeric_limits<uint64_t>::max());
for (uint64_t row = 0; row < keys.size(); ++row) {
    rowids[static_cast<size_t>(keys[row])] = row;
}
```
- Exact format:
  - slot `k` stores the row id for part key `k`
  - direct addressing only; there is no hash computation
- Empty-slot sentinel: `std::numeric_limits<uint64_t>::max()`

### supplier_pk_dense (dense PK array on `s_suppkey`)
- File: `supplier/indexes/supplier_pk_dense.bin`
- Exact format is the same `build_dense_pk` layout shown above
- Lookup: slot `s_suppkey` returns the supplier row id
- Empty-slot sentinel: `std::numeric_limits<uint64_t>::max()`

### nation_pk_dense (dense PK array on `n_nationkey`)
- File: `nation/indexes/nation_pk_dense.bin`
- Exact format is the same `build_dense_pk` layout shown above
- Lookup: slot `n_nationkey` returns the nation row id
- Empty-slot sentinel: `std::numeric_limits<uint64_t>::max()`

### orders_pk_dense (dense PK array on `o_orderkey`)
- File: `orders/indexes/orders_pk_dense.bin`
- Exact format is the same `build_dense_pk` layout shown above
- Lookup: slot `o_orderkey` returns the order row id
- Empty-slot sentinel: `std::numeric_limits<uint64_t>::max()`

### partsupp_part_postings (sorted postings on `ps_partkey`)
- Files:
  - `partsupp/indexes/partsupp_part_postings.offsets.bin`
  - `partsupp/indexes/partsupp_part_postings.rowids.bin`
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
  - posting slice for part key `k` is `rowids[offsets[k] .. offsets[k + 1])`
  - each posting is a `partsupp` row id
  - query code must still read `ps_suppkey.bin[rowid]` and test `ps_suppkey == l_suppkey`
- Struct layout: none; two raw `uint64_t` arrays
- Empty-slot sentinel: none

## Dictionary Handling
- `nation/n_name.bin` contains opaque `uint32_t` codes; never hardcode values
- Load `nation/n_name.dict.offsets.bin` and `nation/n_name.dict.data.bin` to decode the nation string for final output

## Formula Notes
- `o_year` must be derived from the encoded `o_orderdate`; the stored column is days since 1970, not a materialized year column
- The `partsupp` join is not a dense composite-key hash index; it is a posting list by `ps_partkey` plus a row-level `ps_suppkey` check
