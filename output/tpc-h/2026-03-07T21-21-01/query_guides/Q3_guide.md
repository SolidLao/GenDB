# Q3 Guide

## Column Reference
### c_mktsegment (market_segment, uint8_t, dictionary)
- Files:
  - `customer/c_mktsegment.bin` (1500000 rows)
  - `customer/c_mktsegment.dict.offsets.bin`
  - `customer/c_mktsegment.dict.data.bin`
- Storage: ingest writes the codes from `DictColumn<uint8_t>`; dictionary strings are stored as a varlen string column
- This query: `c_mktsegment = 'BUILDING'`
- Runtime loading pattern:
  - load `c_mktsegment.dict.offsets.bin` and `c_mktsegment.dict.data.bin`
  - scan dictionary entries to find the code whose decoded string equals `"BUILDING"`
  - compare `customer/c_mktsegment.bin[row] == building_code`

### c_custkey (primary_key, int32_t, plain)
- File: `customer/c_custkey.bin` (1500000 rows)
- This query: `c_custkey = o_custkey`

### o_custkey (foreign_key, int32_t, plain)
- File: `orders/o_custkey.bin` (15000000 rows)
- This query: `c_custkey = o_custkey`

### o_orderkey (primary_key, int32_t, plain)
- File: `orders/o_orderkey.bin` (15000000 rows)
- This query: `l_orderkey = o_orderkey` and `GROUP BY l_orderkey`

### o_orderdate (date, int32_t, plain)
- File: `orders/o_orderdate.bin` (15000000 rows)
- Encoding: `parse_date()` in ingest writes `days_since_epoch_1970`
- This query: `o_orderdate < DATE '1995-03-15'` and output/grouping payload

### o_shippriority (priority_value, int32_t, plain)
- File: `orders/o_shippriority.bin` (15000000 rows)
- This query: output and `GROUP BY o_shippriority`

### l_orderkey (foreign_key, int32_t, plain)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- This query: `l_orderkey = o_orderkey` and `GROUP BY l_orderkey`

### l_shipdate (date, int32_t, plain)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- This query: `l_shipdate > DATE '1995-03-15'`

### l_extendedprice (decimal, double, plain)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))`

### l_discount (decimal, double, plain)
- File: `lineitem/l_discount.bin` (59986052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| customer | 1500000 | dimension | `c_custkey` | 131072 |
| orders | 15000000 | fact | `o_orderkey` | 131072 |
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 131072 |

## Query Analysis
- Workload selectivities:
  - `c_mktsegment = 'BUILDING'`: `0.200184`
  - `o_orderdate < DATE '1995-03-15'`: `0.485963`
  - `l_shipdate > DATE '1995-03-15'`: `0.547105`
- Natural access path:
  - resolve the market-segment code from the dictionary
  - fetch matching customer row ids via the segment postings index
  - expand to orders via `orders_cust_postings`
  - apply `o_orderdate` filter
  - expand each surviving order to lineitems via `lineitem_order_postings`
  - apply `l_shipdate` filter and aggregate revenue by `(l_orderkey, o_orderdate, o_shippriority)`
- Final ordering is `revenue DESC, o_orderdate` with `LIMIT 10`

## Indexes
### customer_segment_postings (sorted postings on `c_mktsegment`)
- Files:
  - `customer/indexes/customer_segment_postings.offsets.bin`
  - `customer/indexes/customer_segment_postings.rowids.bin`
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
  - `offsets` length is `max(c_mktsegment_code) + 2`
  - rows for code `k` live in `rowids[offsets[k] .. offsets[k + 1])`
  - `rowids` stores customer row ids, not customer keys
- Struct layout: none; this index is two raw `uint64_t` arrays
- Empty-slot sentinel: none; empty codes satisfy `offsets[k] == offsets[k + 1]`

### orders_cust_postings (sorted postings on `o_custkey`)
- Files:
  - `orders/indexes/orders_cust_postings.offsets.bin`
  - `orders/indexes/orders_cust_postings.rowids.bin`
- Exact format is the same `build_dense_postings` layout shown above
- Multi-value interpretation:
  - key `k` is a customer key, not a customer row id
  - order row ids for customer `k` live in `rowids[offsets[k] .. offsets[k + 1])`
- Empty-slot sentinel: none

### orders_orderdate_zonemap (zone_map on `o_orderdate`)
- File: `orders/indexes/orders_orderdate_zonemap.bin`
- Struct layout from `build_indexes.cpp`:
```cpp
struct ZoneMap1I32 {
    int32_t min_value;
    int32_t max_value;
};
```
- Exact format:
  - block size `131072`
  - `115` entries because `ceil(15000000 / 131072) = 115`
- Query use:
  - for `o_orderdate < cutoff`, skip block `b` when `zones[b].min_value >= cutoff`
  - blocks with `zones[b].max_value < cutoff` are fully qualifying
  - mixed blocks still require row-level checks
- Empty-slot sentinel: none

### lineitem_order_postings (sorted postings on `l_orderkey`)
- Files:
  - `lineitem/indexes/lineitem_order_postings.offsets.bin`
  - `lineitem/indexes/lineitem_order_postings.rowids.bin`
- Exact format is the same `build_dense_postings` layout shown above
- Multi-value interpretation:
  - lineitem row ids for order key `k` live in `rowids[offsets[k] .. offsets[k + 1])`
  - this matches the table sort order because `lineitem` was ingested in source order and the index is explicitly materialized rather than inferred from adjacency
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
  - one `uint64_t` per possible key in `[0, max(o_orderkey)]`
  - array slot `k` stores the order row id for key `k`
- Empty-slot sentinel: `std::numeric_limits<uint64_t>::max()`
- There is no hash function in this index; lookup is direct array addressing by key

## Dictionary Handling
- `customer/c_mktsegment.bin` contains opaque `uint8_t` codes
- Never hardcode a numeric code for `"BUILDING"`
- Load `customer/c_mktsegment.dict.offsets.bin` plus `customer/c_mktsegment.dict.data.bin`, decode each entry, and match the string at runtime

## Formula Notes
- Convert `DATE '1995-03-15'` with the same ingest-side `parse_date()` logic before comparing against `o_orderdate` or `l_shipdate`
