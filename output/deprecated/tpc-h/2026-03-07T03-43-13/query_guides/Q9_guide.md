# Q9 Guide

## Column Reference
### p_name (filter_text, bytes, varlen_bytes)
- Files: `gendb/part/p_name.offsets.bin`, `gendb/part/p_name.data.bin` (2000000 rows)
- Exact ingest container in `ingest.cpp`:
```cpp
struct VarLenColumn {
    std::vector<uint64_t> offsets{0};
    std::vector<char> bytes;
};
```
- Exact write order in `VarLenColumn::write`: `data.bin`, then `offsets.bin`.
- This query: `p_name LIKE '%green%'`.
- The runtime query path should prefer `part_name_green_bitset` over rescanning the varlen strings.

### p_partkey (pk, int32_t, plain_i32)
- File: `gendb/part/p_partkey.bin` (2000000 rows)
- This query: `p_partkey = l_partkey` → key into `part_name_green_bitset` and join key into lineitem.

### s_suppkey (pk, int32_t, plain_i32)
- File: `gendb/supplier/s_suppkey.bin` (100000 rows)
- This query: `s_suppkey = l_suppkey` → dense PK lookup.

### s_nationkey (fk, int32_t, plain_i32)
- File: `gendb/supplier/s_nationkey.bin` (100000 rows)
- This query: `s_nationkey = n_nationkey` → dense PK lookup into nation.

### n_nationkey (pk, int32_t, plain_i32)
- File: `gendb/nation/n_nationkey.bin` (25 rows)
- This query: `s_nationkey = n_nationkey` → dense PK lookup target.

### n_name (group_key, uint8_t, dict_u8)
- Files: `gendb/nation/n_name.codes.bin`, `gendb/nation/n_name.dict.offsets.bin`, `gendb/nation/n_name.dict.data.bin` (25 rows; dictionary entries = `26 - 1 = 25`)
- This query: `SELECT n_name AS nation` and `GROUP BY nation`.
- Runtime loading pattern: aggregate by the stored `uint8_t` code, then decode the output code with `n_name.dict.offsets.bin` + `n_name.dict.data.bin`; never hardcode code values.

### l_suppkey (fk, int32_t, plain_i32)
- File: `gendb/lineitem/l_suppkey.bin` (59986052 rows)
- This query: joins to `supplier.s_suppkey` and to `partsupp.ps_suppkey`.

### l_partkey (fk, int32_t, plain_i32)
- File: `gendb/lineitem/l_partkey.bin` (59986052 rows)
- This query: joins to `part.p_partkey` and to `partsupp.ps_partkey`.

### l_orderkey (fk, int32_t, plain_i32)
- File: `gendb/lineitem/l_orderkey.bin` (59986052 rows)
- This query: joins to `orders.o_orderkey`.

### l_quantity (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_quantity.bin` (59986052 rows)
- This query: `ps_supplycost * l_quantity` inside `amount`.

### l_extendedprice (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_extendedprice.bin` (59986052 rows)
- This query: `l_extendedprice * (1 - l_discount)` inside `amount`.

### l_discount (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_discount.bin` (59986052 rows)
- This query: `l_extendedprice * (1 - l_discount)` inside `amount`.

### ps_partkey (fk, int32_t, plain_i32)
- File: `gendb/partsupp/ps_partkey.bin` (8000000 rows)
- This query: `ps_partkey = l_partkey` as half of the composite partsupp key.

### ps_suppkey (fk, int32_t, plain_i32)
- File: `gendb/partsupp/ps_suppkey.bin` (8000000 rows)
- This query: `ps_suppkey = l_suppkey` as half of the composite partsupp key.

### ps_supplycost (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/partsupp/ps_supplycost.bin` (8000000 rows)
- This query: `ps_supplycost * l_quantity` inside `amount`.

### o_orderkey (pk, int32_t, plain_i32)
- File: `gendb/orders/o_orderkey.bin` (15000000 rows)
- This query: `o_orderkey = l_orderkey` → dense PK lookup.

### o_orderdate (date, int32_t, days_since_epoch_1970)
- File: `gendb/orders/o_orderdate.bin` (15000000 rows)
- This query: `EXTRACT(YEAR FROM o_orderdate)` → derive calendar year from the stored day count after lookup.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| part | 2000000 | dimension | `p_partkey` | 100000 |
| supplier | 100000 | dimension | `s_suppkey` | 100000 |
| lineitem | 59986052 | fact | `l_orderkey`, `l_linenumber` | 100000 |
| partsupp | 8000000 | fact | `ps_partkey`, `ps_suppkey` | 100000 |
| orders | 15000000 | fact | `o_orderkey` | 100000 |
| nation | 25 | dimension | `n_nationkey` | 100000 |

## Query Analysis
- Filter selectivity from `workload_analysis.json`: `p_name LIKE '%green%'` → `0.048`.
- Join fanout from `workload_analysis.json`: `supplier -> lineitem` PK-FK has `right_max_duplicates = 9`; `partsupp composite -> lineitem composite` has `right_max_duplicates = 3`; `part -> lineitem` PK-FK has `right_max_duplicates = 4`; `nation -> supplier` has `right_max_duplicates = 53`.
- Aggregation estimate from `workload_analysis.json`: about `175` groups on `(nation, o_year)`.
- The generated storage favors a plan that scans lineitem once and performs direct lookups: bitset test on `l_partkey`, dense lookup on `l_suppkey`, open-addressed hash lookup on `(l_partkey, l_suppkey)`, dense lookup on `l_orderkey`, then dense lookup on `s_nationkey`.

## Indexes
### part_name_green_bitset (bitset keyed by `p_partkey`)
- File: `gendb/part/indexes/part_name_green_bitset.bin`
- Authoritative build logic:
```cpp
auto offsets = read_vector<uint64_t>(table_dir / "p_name.offsets.bin");
auto bytes = read_bytes(table_dir / "p_name.data.bin");
int32_t max_key = *std::max_element(keys.begin(), keys.end());
std::vector<uint8_t> bitset(static_cast<size_t>(max_key) + 1, 0);
for (size_t row = 0; row < keys.size(); ++row) {
    uint64_t begin = offsets[row];
    uint64_t end = offsets[row + 1];
    if (contains_green(std::string_view(bytes.data() + begin, static_cast<size_t>(end - begin)))) {
        bitset[static_cast<size_t>(keys[row])] = 1;
    }
}
```
- Exact predicate helper from `build_indexes.cpp`:
```cpp
bool contains_green(std::string_view sv) {
    constexpr std::string_view needle = "green";
    if (sv.size() < needle.size()) {
        return false;
    }
    for (size_t i = 0; i + needle.size() <= sv.size(); ++i) {
        bool match = true;
        for (size_t j = 0; j < needle.size(); ++j) {
            char c = static_cast<char>(std::tolower(static_cast<unsigned char>(sv[i + j])));
            if (c != needle[j]) {
                match = false;
                break;
            }
        }
        if (match) {
            return true;
        }
    }
    return false;
}
```
- Exact on-disk layout: one `std::vector<uint8_t> bitset` written by `write_vector`.
- Empty-slot sentinel: the vector is initialized with `0`; absent/non-green keys remain `0`, matching green keys are set to `1`.
- Actual length: `2000001` entries because `max(p_partkey) = 2000000`.
- Query usage: test `part_name_green_bitset[l_partkey] != 0` before any expensive joins.

### supplier_pk_dense (dense PK on `s_suppkey`)
- File: `gendb/supplier/indexes/supplier_pk_dense.bin`
- Exact build logic uses `write_dense_pk`:
```cpp
int32_t max_key = *std::max_element(keys.begin(), keys.end());
std::vector<uint32_t> dense(static_cast<size_t>(max_key) + 1, std::numeric_limits<uint32_t>::max());
for (uint32_t row = 0; row < keys.size(); ++row) {
    dense[static_cast<size_t>(keys[row])] = row;
}
```
- Exact on-disk layout: one `std::vector<uint32_t> dense`.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Query usage: `supplier_row = supplier_pk_dense[l_suppkey]`; then read `s_nationkey` from that row.

### nation_pk_dense (dense PK on `n_nationkey`)
- File: `gendb/nation/indexes/nation_pk_dense.bin`
- Build uses the same `write_dense_pk` implementation as `supplier_pk_dense`.
- Exact on-disk layout: one `std::vector<uint32_t> dense`.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Query usage: `nation_row = nation_pk_dense[s_nationkey]`; then read `n_name.codes.bin[nation_row]` as the aggregation key and decode at output time.

### orders_pk_dense (dense PK on `o_orderkey`)
- File: `gendb/orders/indexes/orders_pk_dense.bin`
- Build uses the same `write_dense_pk` implementation.
- Exact on-disk layout: one `std::vector<uint32_t> dense`.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Actual length: `60000001` entries.
- Query usage: `order_row = orders_pk_dense[l_orderkey]`; then read `o_orderdate` to derive `o_year`.

### partsupp_pk_hash (open-addressed hash on `(ps_partkey, ps_suppkey)`)
- Files: `gendb/partsupp/indexes/partsupp_pk_hash.keys.bin`, `gendb/partsupp/indexes/partsupp_pk_hash.row_ids.bin`
- Authoritative build code from `build_indexes.cpp`:
```cpp
uint64_t slots = next_power_of_two(static_cast<uint64_t>(partkeys.size()) * 2);
std::vector<uint64_t> keys(slots, 0);
std::vector<uint32_t> row_ids(slots, std::numeric_limits<uint32_t>::max());
uint64_t mask = slots - 1;
for (uint32_t row = 0; row < partkeys.size(); ++row) {
    uint64_t packed = (static_cast<uint64_t>(static_cast<uint32_t>(partkeys[row])) << 32) |
                      static_cast<uint32_t>(suppkeys[row]);
    uint64_t slot = (packed * 11400714819323198485ull) & mask;
    while (keys[slot] != 0) {
        slot = (slot + 1) & mask;
    }
    keys[slot] = packed;
    row_ids[slot] = row;
}
```
- Exact on-disk layout: no explicit struct; two parallel arrays written in this order:
  1. `std::vector<uint64_t> keys`
  2. `std::vector<uint32_t> row_ids`
- Exact empty-slot sentinels:
  - `keys[slot] == 0`
  - `row_ids[slot] == std::numeric_limits<uint32_t>::max()`
- Exact hash computation: `uint64_t slot = (packed * 11400714819323198485ull) & mask;`
- Exact collision resolution: linear probing with `slot = (slot + 1) & mask;`.
- Actual slot count: `next_power_of_two(8000000 * 2) = 16777216`; therefore `mask = 16777215`.
- Query probe pattern: pack the lineitem join key as
```cpp
uint64_t packed = (static_cast<uint64_t>(static_cast<uint32_t>(l_partkey[row])) << 32) |
                  static_cast<uint32_t>(l_suppkey[row]);
```
  then probe with the same multiply-and-mask plus the same linear-probing loop until either `keys[slot] == packed` or `keys[slot] == 0`.

## Implementation Notes
- Q9 is the only requested query that needs the exact hash function from `build_indexes.cpp`; the guide copies it verbatim.
- `n_name` and `p_name` are the only variable-length or dictionary-backed columns referenced by Q9, and both are documented without hardcoding dictionary codes.
