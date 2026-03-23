# Q9 Guide

## Column Reference
### p_name (filter_text, string, varlen)
- Files: `part/p_name.off`, `part/p_name.dat` (2000000 rows)
- SQL: `p_name LIKE '%green%'`
- C++ comparison: load string via offset slice `dat[off[i]..off[i+1])`, then substring search for `"green"`.

### p_partkey (pk, int32_t, plain)
- File: `part/p_partkey.bin` (2000000 rows)
- SQL: `p_partkey = l_partkey`

### l_partkey (fk, int32_t, plain)
- File: `lineitem/l_partkey.bin` (59986052 rows)
- SQL: `p_partkey = l_partkey` and `ps_partkey = l_partkey`

### l_suppkey (fk, int32_t, plain)
- File: `lineitem/l_suppkey.bin` (59986052 rows)
- SQL: `s_suppkey = l_suppkey` and `ps_suppkey = l_suppkey`

### l_orderkey (fk_group_key, int32_t, plain)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- SQL: `o_orderkey = l_orderkey`

### l_extendedprice (measure, double, plain)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- SQL: `l_extendedprice * (1 - l_discount)`

### l_discount (filter_measure, double, plain)
- File: `lineitem/l_discount.bin` (59986052 rows)
- SQL: `(1 - l_discount)`

### l_quantity (filter_measure, double, plain)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- SQL: `ps_supplycost * l_quantity`

### ps_partkey (fk, int32_t, plain)
- File: `partsupp/ps_partkey.bin` (8000000 rows)
- SQL: `ps_partkey = l_partkey`

### ps_suppkey (fk, int32_t, plain)
- File: `partsupp/ps_suppkey.bin` (8000000 rows)
- SQL: `ps_suppkey = l_suppkey`

### ps_supplycost (measure, double, plain)
- File: `partsupp/ps_supplycost.bin` (8000000 rows)
- SQL: subtract term `ps_supplycost * l_quantity`

### s_suppkey (pk, int32_t, plain)
- File: `supplier/s_suppkey.bin` (100000 rows)
- SQL: `s_suppkey = l_suppkey`

### s_nationkey (fk, int32_t, plain)
- File: `supplier/s_nationkey.bin` (100000 rows)
- SQL: `s_nationkey = n_nationkey`

### n_nationkey (pk, int32_t, plain)
- File: `nation/n_nationkey.bin` (25 rows)
- SQL: `s_nationkey = n_nationkey`

### n_name (dimension_name, uint32_t, dictionary)
- File: `nation/n_name.bin` (25 rows), dictionary: `nation/n_name.dict`
- SQL: output/group key `nation`
- Runtime loading pattern: decode dictionary file at runtime into `code->string`; do not assume numeric code assignments.

### o_orderkey (pk, int32_t, plain)
- File: `orders/o_orderkey.bin` (15000000 rows)
- SQL: `o_orderkey = l_orderkey`

### o_orderdate (date_filter, int32_t, plain, days_since_epoch_1970)
- File: `orders/o_orderdate.bin` (15000000 rows)
- SQL: `EXTRACT(YEAR FROM o_orderdate)`
- C++ derivation pattern: convert encoded day back to civil date and extract year.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| part | 2000000 | dimension | `p_partkey` | 100000 |
| supplier | 100000 | dimension | `s_suppkey` | 100000 |
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 100000 |
| partsupp | 8000000 | fact | `ps_partkey, ps_suppkey` | 100000 |
| orders | 15000000 | fact | `o_orderkey` | 100000 |
| nation | 25 | dimension | `n_nationkey` | 100000 |

## Query Analysis
- Selective driver: `p_name LIKE '%green%'` with workload selectivity `0.048` (~`2000000 * 0.048 = 96000` parts).
- Join pattern includes composite equality `(ps_partkey, ps_suppkey) = (l_partkey, l_suppkey)` and PK/FK joins to supplier/orders/nation.
- Aggregation key: `(nation, o_year)`; workload estimated groups: `175`.
- Amount formula per joined row: `l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity`.

## Indexes
### part_pk_hash (hash on p_partkey)
- File: `part/part_pk_hash.idx`
- Hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout: `struct Entry { int64_t key; uint32_t rowid; };`
- Layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- Empty-slot sentinel: none.

### supplier_pk_hash (hash on s_suppkey)
- File: `supplier/supplier_pk_hash.idx`
- Hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout: `struct Entry { int64_t key; uint32_t rowid; };`
- Layout: same as other i32 hash indexes.
- Empty-slot sentinel: none.

### orders_pk_hash (hash on o_orderkey)
- File: `orders/orders_pk_hash.idx`
- Hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout: `struct Entry { int64_t key; uint32_t rowid; };`
- Layout: same as other i32 hash indexes.
- Empty-slot sentinel: none.

### nation_pk_hash (hash on n_nationkey)
- File: `nation/nation_pk_hash.idx`
- Hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout: `struct Entry { int64_t key; uint32_t rowid; };`
- Layout: same as other i32 hash indexes.
- Empty-slot sentinel: none.

### partsupp_pk_hash (hash on (ps_partkey, ps_suppkey))
- File: `partsupp/partsupp_pk_hash.idx`
- Key packing (verbatim):
```cpp
const uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(a[row])) << 32) |
                     static_cast<uint32_t>(b[row]);
const uint64_t h = mix64(key) & (buckets - 1);
```
- Struct layout: `struct Entry { uint64_t key; uint32_t rowid; };`
- Layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- Empty-slot sentinel: none.

### lineitem_partsupp_hash (hash on (l_partkey, l_suppkey), multi-value)
- File: `lineitem/lineitem_partsupp_hash.idx`
- Key packing (verbatim):
```cpp
const uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(a[row])) << 32) |
                     static_cast<uint32_t>(b[row]);
const uint64_t h = mix64(key) & (buckets - 1);
```
- Struct layout: `struct Entry { uint64_t key; uint32_t rowid; };`
- Layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- Multi-value format (1:N): for one composite key, scan `[offsets[h], offsets[h+1])` and keep all `entries[i].key == packed_key`; each match yields one `lineitem` rowid.
- Empty-slot sentinel: none.
