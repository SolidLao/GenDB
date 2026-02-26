---
# Q9 Guide — Product Type Profit Measure

## Query
```sql
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
  SELECT n_name AS nation,
         EXTRACT(YEAR FROM o_orderdate) AS o_year,
         l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity AS amount
  FROM part, supplier, lineitem, partsupp, orders, nation
  WHERE s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation ASC, o_year DESC;
```

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| part     | 2,000,000  | dimension | (none)      | 100,000    |
| supplier | 100,000    | dimension | (none)      | 100,000    |
| lineitem | 59,986,052 | fact      | l_shipdate  | 100,000    |
| partsupp | 8,000,000  | dimension | (none)      | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate | 100,000    |
| nation   | 25         | dimension | (none)      | 100,000    |

## Query Analysis
- **Driving filter**: `p_name LIKE '%green%'` — ~5.5% selectivity on part → ~110,000 qualifying parts.
- **Join order (recommended)**:
  1. Scan part, collect qualifying `p_partkey` set (p_name contains 'green')
  2. For each qualifying p_partkey, find lineitem rows via `lineitem_orderkey_sorted` is not directly applicable; instead scan lineitem and probe `part_partkey_hash` for l_partkey
  3. Alternatively: build in-memory hash set of qualifying partkeys, scan lineitem checking l_partkey in set
  4. For each qualifying lineitem row: probe `partsupp_pk_hash` for (l_partkey, l_suppkey), probe `orders_orderkey_hash` for l_orderkey, probe `supplier_suppkey_hash` for l_suppkey, probe `nation_nationkey_hash` for s_nationkey
- **EXTRACT(YEAR FROM o_orderdate)**: `o_orderdate` is int32_t days-since-epoch; extract year using Howard Hinnant civil_from_days (reverse of date_to_days).
- **GROUP BY**: `(n_name_string, o_year)` → ~25 nations × ~7 years ≈ 175 groups.
- **Output**: sort 175 rows by nation ASC, o_year DESC — trivially cheap.

## Year Extraction from o_orderdate
```cpp
// Howard Hinnant civil_from_days (inverse of date_to_days)
int year_from_days(int32_t z) {
    z += 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int y = (int)yoe + era * 400;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp = (5*doy + 2) / 153;
    if (mp >= 10) y++;
    return y;
}
int o_year = year_from_days(o_orderdate[row]);
```

## Column Reference

### p_partkey (PK_join_key, int32_t, raw)
- File: `part/p_partkey.bin` — int32_t[2,000,000]
- Used as join key with l_partkey and ps_partkey

### p_name (filter_like, varlen, offset_data)
- Files: `part/p_name.offsets` — int32_t[2,000,001]; `part/p_name.data` — raw bytes
- offsets[i] = byte start of row i in .data; length = offsets[i+1] - offsets[i]
- Predicate: string for row i contains substring "green"
  ```cpp
  int32_t start = offsets[i], len = offsets[i+1] - start;
  // Use memmem or manual scan for "green" in data[start..start+len)
  bool passes = (memmem(data + start, len, "green", 5) != nullptr);
  ```

### l_partkey (FK_join_key, int32_t, raw)
- File: `lineitem/l_partkey.bin` — int32_t[59,986,052]
- Join to part.p_partkey (probe in-memory qualifying set)

### l_suppkey (FK_join_key, int32_t, raw)
- File: `lineitem/l_suppkey.bin` — int32_t[59,986,052]
- Join to supplier.s_suppkey (probe `supplier_suppkey_hash`); also part of composite key for partsupp

### l_orderkey (FK_join_key, int32_t, raw)
- File: `lineitem/l_orderkey.bin` — int32_t[59,986,052]
- Join to orders.o_orderkey (probe `orders_orderkey_hash`)

### l_quantity (measure, double, raw)
- File: `lineitem/l_quantity.bin` — double[59,986,052]
- Used in: `ps_supplycost * l_quantity`

### l_extendedprice (measure, double, raw)
- File: `lineitem/l_extendedprice.bin` — double[59,986,052]
- Used in: `l_extendedprice * (1 - l_discount)`

### l_discount (measure_filter, double, raw)
- File: `lineitem/l_discount.bin` — double[59,986,052]
- Used in: `l_extendedprice * (1 - l_discount)`

### ps_supplycost (measure, double, raw)
- File: `partsupp/ps_supplycost.bin` — double[8,000,000]
- Fetched after probing `partsupp_pk_hash` for (l_partkey, l_suppkey)

### o_orderdate (date_filter, int32_t, days_since_epoch_1970)
- File: `orders/o_orderdate.bin` — int32_t[15,000,000]
- Used for: EXTRACT(YEAR FROM o_orderdate) → GROUP BY key

### s_nationkey (FK_join_key, int32_t, raw)
- File: `supplier/s_nationkey.bin` — int32_t[100,000]
- Fetched after probing `supplier_suppkey_hash`; used to look up nation

### n_nationkey (PK_join_key, int32_t, raw)
- File: `nation/n_nationkey.bin` — int32_t[25]
- Join key matched against s_nationkey via `nation_nationkey_hash`

### n_name (group_by_output, varlen, offset_data)
- Files: `nation/n_name.offsets` — int32_t[26]; `nation/n_name.data` — raw bytes
- GROUP BY and output column; load all 25 nation names at startup into a string array indexed by n_nationkey (or by row_id from hash lookup)

## Indexes

### part_partkey_hash (hash_pk on p_partkey)
- File: `indexes/part_partkey_hash.bin`
- Layout (from build_indexes.cpp `build_pk_hash`):
  ```
  uint64_t  num_entries            // 2,000,000
  uint64_t  bucket_count           // first odd number >= ceil(2000000/0.6)+2
  { int32_t key; int32_t row_id; } [bucket_count]
  ```
- Empty sentinel: `key == INT32_MIN` (-2147483648)
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t h = ((uint64_t)(uint32_t)key * 2654435761ULL) % bucket_count;
  // linear probe: if occupied, h = (h+1 < bucket_count) ? h+1 : 0
  ```
- Usage: probe with l_partkey to get row_id in part table; then check qualifying set OR fetch p_name

### partsupp_pk_hash (hash_pk_composite on (ps_partkey, ps_suppkey))
- File: `indexes/partsupp_pk_hash.bin`
- Key encoding (from build_indexes.cpp):
  ```cpp
  int64_t key = ((int64_t)ps_partkey << 32) | (uint32_t)ps_suppkey;
  ```
- Layout:
  ```
  uint64_t  num_entries            // 8,000,000
  uint64_t  bucket_count           // first odd number >= ceil(8000000/0.6)+2
  { int64_t key; int32_t row_id; int32_t pad; } [bucket_count]
  ```
- Empty sentinel: `key == INT64_MIN`
- Hash function (verbatim from build_indexes.cpp, splitmix64):
  ```cpp
  static uint64_t splitmix64(uint64_t x) {
      x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
      x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
      return x ^ (x >> 31);
  }
  uint64_t h = splitmix64((uint64_t)key) % bucket_count;
  // linear probe: if occupied, h = (h+1 < bucket_count) ? h+1 : 0
  ```
- Usage: probe with `(int64_t(l_partkey) << 32) | uint32_t(l_suppkey)` → row_id in partsupp → fetch ps_supplycost

### orders_orderkey_hash (hash_pk on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout:
  ```
  uint64_t  num_entries            // 15,000,000
  uint64_t  bucket_count           // first odd number >= ceil(15000000/0.6)+2
  { int32_t key; int32_t row_id; } [bucket_count]
  ```
- Empty sentinel: `key == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t h = ((uint64_t)(uint32_t)key * 2654435761ULL) % bucket_count;
  // linear probe: if occupied, h = (h+1 < bucket_count) ? h+1 : 0
  ```
- Usage: probe with l_orderkey → orders row_id → fetch o_orderdate

### supplier_suppkey_hash (hash_pk on s_suppkey)
- File: `indexes/supplier_suppkey_hash.bin`
- Layout:
  ```
  uint64_t  num_entries            // 100,000
  uint64_t  bucket_count           // first odd number >= ceil(100000/0.6)+2
  { int32_t key; int32_t row_id; } [bucket_count]
  ```
- Empty sentinel: `key == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t h = ((uint64_t)(uint32_t)key * 2654435761ULL) % bucket_count;
  // linear probe: if occupied, h = (h+1 < bucket_count) ? h+1 : 0
  ```
- Usage: probe with l_suppkey → supplier row_id → fetch s_nationkey

### nation_nationkey_hash (hash_pk on n_nationkey)
- File: `indexes/nation_nationkey_hash.bin`
- Layout:
  ```
  uint64_t  num_entries            // 25
  uint64_t  bucket_count           // first odd number >= ceil(25/0.6)+2
  { int32_t key; int32_t row_id; } [bucket_count]
  ```
- Empty sentinel: `key == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t h = ((uint64_t)(uint32_t)key * 2654435761ULL) % bucket_count;
  // linear probe: if occupied, h = (h+1 < bucket_count) ? h+1 : 0
  ```
- Usage: probe with s_nationkey → nation row_id → fetch n_name varlen string
- Tip: with only 25 rows, load entire nation table into memory at startup (array indexed by n_nationkey directly).

## Aggregation Strategy
Hash map keyed by `(nation_string, o_year)` with ~175 expected groups:
```cpp
struct GroupKey { std::string nation; int o_year; };
struct Acc { double sum_profit; };
std::map<GroupKey, Acc> agg;  // or unordered_map with custom hash
// For each qualifying lineitem row i:
double amount = extprice[i]*(1.0-disc[i]) - supplycost*qty[i];
agg[{nation_str, o_year}].sum_profit += amount;
// Final: sort by nation ASC, o_year DESC
```
