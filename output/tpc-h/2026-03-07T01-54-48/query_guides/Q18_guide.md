# Q18 Guide

## Column Reference
### l_orderkey (fk_group_key, int32_t, plain)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- SQL: subquery `GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`; outer join `o_orderkey = l_orderkey`

### l_quantity (filter_measure, double, plain)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- SQL: used in subquery sum and outer `SUM(l_quantity)`

### o_orderkey (pk, int32_t, plain)
- File: `orders/o_orderkey.bin` (15000000 rows)
- SQL: `o_orderkey IN (subquery)` and `o_orderkey = l_orderkey`

### o_custkey (fk, int32_t, plain)
- File: `orders/o_custkey.bin` (15000000 rows)
- SQL: `c_custkey = o_custkey`

### o_orderdate (date_filter, int32_t, plain, days_since_epoch_1970)
- File: `orders/o_orderdate.bin` (15000000 rows)
- SQL: group/output/order column

### o_totalprice (measure, double, plain)
- File: `orders/o_totalprice.bin` (15000000 rows)
- SQL: output/order column

### c_custkey (pk, int32_t, plain)
- File: `customer/c_custkey.bin` (1500000 rows)
- SQL: `c_custkey = o_custkey`

### c_name (group_payload, string, varlen)
- Files: `customer/c_name.off`, `customer/c_name.dat` (1500000 rows)
- SQL: output/group column

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 100000 |
| orders | 15000000 | fact | `o_orderkey` | 100000 |
| customer | 1500000 | dimension | `c_custkey` | 100000 |

## Query Analysis
- Phase A: aggregate `lineitem` by `l_orderkey`, keep keys with `SUM(l_quantity) > 300`.
- Workload marks this predicate as highly selective (`selectivity` field is `0.0`, effectively sparse).
- Phase B: join qualifying orderkeys with orders and customer, then join back to lineitem for final `SUM(l_quantity)` grouped by customer/order attributes.
- Final sort: `o_totalprice DESC, o_orderdate ASC`, limit 100.

## Indexes
### lineitem_orderkey_hash (hash on l_orderkey, multi-value)
- File: `lineitem/lineitem_orderkey_hash.idx`
- Hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- `mix64` (verbatim):
```cpp
x ^= x >> 33;
x *= 0xff51afd7ed558ccdULL;
x ^= x >> 33;
x *= 0xc4ceb9fe1a85ec53ULL;
x ^= x >> 33;
```
- Struct layout (verbatim): `struct Entry { int64_t key; uint32_t rowid; };`
- Layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- Sizing for this run: `n=59986052`, `buckets=33554432`.
- Multi-value format: many rows share same `l_orderkey`; iterate bucket range `[offsets[h], offsets[h+1])`, equality-check `entry.key`, emit all matching rowids.
- Empty-slot sentinel: none.

### orders_pk_hash (hash on o_orderkey)
- File: `orders/orders_pk_hash.idx`
- Hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout: `struct Entry { int64_t key; uint32_t rowid; };`
- Layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- `n=15000000`, `buckets=8388608`.
- Empty-slot sentinel: none.

### orders_custkey_hash (hash on o_custkey, multi-value)
- File: `orders/orders_custkey_hash.idx`
- Hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout: `struct Entry { int64_t key; uint32_t rowid; };`
- Layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- Multi-value format: one `c_custkey` may map to multiple orders rowids.
- Empty-slot sentinel: none.

### customer_pk_hash (hash on c_custkey)
- File: `customer/customer_pk_hash.idx`
- Hash computation (verbatim):
```cpp
const uint64_t h = mix64(static_cast<uint32_t>(keys[row])) & (buckets - 1);
```
- Struct layout: `struct Entry { int64_t key; uint32_t rowid; };`
- Layout: `uint64_t buckets`, `uint64_t n`, `uint64_t offsets[buckets+1]`, `Entry entries[n]`.
- `n=1500000`, `buckets=1048576`.
- Empty-slot sentinel: none.
