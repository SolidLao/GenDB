# Q3 Guide

## Column Reference
### c_mktsegment (filter_key, uint32_t, dictionary)
- File: `customer/c_mktsegment.bin` (1500000 rows), dictionary: `customer/c_mktsegment.dict`
- SQL: `c_mktsegment = 'BUILDING'`
- Runtime loading pattern (no hardcoded code): scan `c_mktsegment.dict`, map string->code, lookup code for `"BUILDING"`, then filter `c_mktsegment.bin` by that code.

### c_custkey (pk, int32_t, plain)
- File: `customer/c_custkey.bin` (1500000 rows)
- SQL: `c_custkey = o_custkey`

### o_custkey (fk, int32_t, plain)
- File: `orders/o_custkey.bin` (15000000 rows)
- SQL: `c_custkey = o_custkey`

### o_orderkey (pk, int32_t, plain)
- File: `orders/o_orderkey.bin` (15000000 rows)
- SQL: `l_orderkey = o_orderkey`, `GROUP BY l_orderkey`

### o_orderdate (date_filter, int32_t, plain, days_since_epoch_1970)
- File: `orders/o_orderdate.bin` (15000000 rows)
- SQL: `o_orderdate < DATE '1995-03-15'`
- C++ compare: `o_orderdate < 9204`

### o_shippriority (group_key, int32_t, plain)
- File: `orders/o_shippriority.bin` (15000000 rows)
- SQL: `GROUP BY o_shippriority`

### l_orderkey (fk_group_key, int32_t, plain)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- SQL: `l_orderkey = o_orderkey`, `GROUP BY l_orderkey`

### l_shipdate (date_filter, int32_t, plain, days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- SQL: `l_shipdate > DATE '1995-03-15'`
- C++ compare: `l_shipdate > 9204`

### l_extendedprice (measure, double, plain)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- SQL: `SUM(l_extendedprice * (1 - l_discount))`

### l_discount (filter_measure, double, plain)
- File: `lineitem/l_discount.bin` (59986052 rows)
- SQL: arithmetic term `(1 - l_discount)`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| customer | 1500000 | dimension | `c_custkey` | 100000 |
| orders | 15000000 | fact | `o_orderkey` | 100000 |
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 100000 |

## Query Analysis
- Filters:
  - customer segment selectivity: `0.22` -> `1500000 * 0.22 = 330000` customers.
  - orders date selectivity: `0.489` -> `15000000 * 0.489 = 7335000` orders.
  - lineitem shipdate selectivity: `0.535` -> `59986052 * 0.535 = 32092537.82` rows.
- Join chain: `customer -> orders -> lineitem` via `c_custkey=o_custkey`, `o_orderkey=l_orderkey`.
- Grouping key: `(l_orderkey, o_orderdate, o_shippriority)`; sort by `revenue DESC, o_orderdate ASC`; limit 10.

## Indexes
### customer_mktsegment_hash (hash on c_mktsegment)
- File: `customer/customer_mktsegment_hash.idx`
- Exact hash computation (verbatim):
```cpp
const uint64_t h = mix64(keys[row]) & (buckets - 1);
```
- `mix64` (verbatim):
```cpp
x ^= x >> 33;
x *= 0xff51afd7ed558ccdULL;
x ^= x >> 33;
x *= 0xc4ceb9fe1a85ec53ULL;
x ^= x >> 33;
```
- Struct layout (verbatim): `struct Entry { uint64_t key; uint32_t rowid; };`
- On-disk layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- Bucket sizing formula: `buckets = next_pow2(max<uint64_t>(1024, n / 2))`; here `n=1500000`, `buckets=1048576`.
- Multi-value format: bucket range `[offsets[h], offsets[h+1])` stores all `(key,rowid)` with same bucket; post-filter `entry.key == target_code`.
- Empty-slot sentinel: none.

### orders_custkey_hash (hash on o_custkey)
- File: `orders/orders_custkey_hash.idx`
- Exact hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout (verbatim): `struct Entry { int64_t key; uint32_t rowid; };`
- On-disk layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- `n=15000000`, `buckets=8388608`.
- Multi-value format (1:N): same `o_custkey` appears in multiple entries across bucket range, consume all matching rowids.
- Empty-slot sentinel: none.

### orders_orderdate_zonemap (zone_map on o_orderdate)
- File: `orders/orders_orderdate_zonemap.idx`
- On-disk layout: `uint32_t block_size`, `uint64_t n`, `uint64_t blocks`, `int32_t mins[blocks]`, `int32_t maxs[blocks]`.
- For this run: `n=15000000`, `block_size=100000`, `blocks=150`.
- Use for `o_orderdate < 9204`: skip block when `mins[b] >= 9204`.
- Empty-slot sentinel: none.

### lineitem_orderkey_hash (hash on l_orderkey)
- File: `lineitem/lineitem_orderkey_hash.idx`
- Exact hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout (verbatim): `struct Entry { int64_t key; uint32_t rowid; };`
- On-disk layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- `n=59986052`, `buckets=33554432`.
- Multi-value format (1:N): for one `o_orderkey`, matching lineitems are found by scanning bucket slice and checking `entry.key == o_orderkey`.
- Empty-slot sentinel: none.

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `lineitem/lineitem_shipdate_zonemap.idx`
- On-disk layout: `uint32_t block_size`, `uint64_t n`, `uint64_t blocks`, `int32_t mins[blocks]`, `int32_t maxs[blocks]`.
- For this run: `n=59986052`, `block_size=100000`, `blocks=600`.
- Use for `l_shipdate > 9204`: skip block when `maxs[b] <= 9204`.
- Empty-slot sentinel: none.
