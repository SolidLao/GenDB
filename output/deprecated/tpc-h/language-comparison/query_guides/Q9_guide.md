# Q9 Guide

## SQL
```sql
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
    SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
      AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC;
```

## Column Reference

### p_partkey (PK, int32_t, native_binary)
- File: `part/p_partkey.bin` (~2,000,000 rows, 4 bytes each)
- This query: join key `p_partkey = l_partkey`

### p_name (attribute, varlen_string, offsets_plus_data)
- Files: `part/p_name_offsets.bin` (int64_t[nrows+1]) + `part/p_name_data.bin` (raw bytes)
- Offsets: int64_t array of size (nrows+1). `name[i]` spans bytes `[offsets[i], offsets[i+1])` in data file.
- This query: `WHERE p_name LIKE '%green%'`
- C++ filter: `memmem` or `strstr` on substring of data buffer
  ```cpp
  // Loading pattern:
  // int64_t* offsets = mmap(p_name_offsets.bin)  — (nrows+1) int64_t values
  // char* data = mmap(p_name_data.bin)
  // For row i: string is data[offsets[i]] .. data[offsets[i+1]]
  // Match: memmem(data + offsets[i], offsets[i+1] - offsets[i], "green", 5) != nullptr
  ```
- Selectivity: ~5.48% of parts match → ~109,600 parts

### l_orderkey (FK, int32_t, native_binary)
- File: `lineitem/l_orderkey.bin` (~60M rows, 4 bytes each)
- This query: join key `o_orderkey = l_orderkey`

### l_partkey (FK, int32_t, native_binary)
- File: `lineitem/l_partkey.bin` (~60M rows, 4 bytes each)
- This query: join key `p_partkey = l_partkey`, `ps_partkey = l_partkey`

### l_suppkey (FK, int32_t, native_binary)
- File: `lineitem/l_suppkey.bin` (~60M rows, 4 bytes each)
- This query: join key `s_suppkey = l_suppkey`, `ps_suppkey = l_suppkey`

### l_extendedprice (measure, double, native_binary)
- File: `lineitem/l_extendedprice.bin` (~60M rows, 8 bytes each)
- This query: in `l_extendedprice * (1 - l_discount)` part of amount expression

### l_discount (measure, double, native_binary)
- File: `lineitem/l_discount.bin` (~60M rows, 8 bytes each)
- This query: in `(1 - l_discount)` part of amount expression

### l_quantity (measure, double, native_binary)
- File: `lineitem/l_quantity.bin` (~60M rows, 8 bytes each)
- This query: in `ps_supplycost * l_quantity` part of amount expression

### ps_partkey (PK_component, int32_t, native_binary)
- File: `partsupp/ps_partkey.bin` (~8,000,000 rows, 4 bytes each)
- This query: join key `ps_partkey = l_partkey`

### ps_suppkey (PK_component, int32_t, native_binary)
- File: `partsupp/ps_suppkey.bin` (~8,000,000 rows, 4 bytes each)
- This query: join key `ps_suppkey = l_suppkey`

### ps_supplycost (measure, double, native_binary)
- File: `partsupp/ps_supplycost.bin` (~8,000,000 rows, 8 bytes each)
- This query: in `ps_supplycost * l_quantity` part of amount expression

### s_suppkey (PK, int32_t, native_binary)
- File: `supplier/s_suppkey.bin` (~100,000 rows, 4 bytes each)
- This query: join key `s_suppkey = l_suppkey`

### s_nationkey (FK, int32_t, native_binary)
- File: `supplier/s_nationkey.bin` (~100,000 rows, 4 bytes each)
- This query: join key `s_nationkey = n_nationkey`

### o_orderkey (PK, int32_t, native_binary)
- File: `orders/o_orderkey.bin` (~15,000,000 rows, 4 bytes each)
- This query: join key `o_orderkey = l_orderkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `orders/o_orderdate.bin` (~15,000,000 rows, 4 bytes each)
- This query: `EXTRACT(YEAR FROM o_orderdate)` → derive year from days_since_epoch
- Year extraction from days_since_epoch:
  ```cpp
  // Reverse of days_from_civil to get year:
  static inline int year_from_days(int z) {
      z += 719468;
      int era = (z >= 0 ? z : z - 146096) / 146097;
      int doe = z - era * 146097;
      int yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
      int y = yoe + era * 400;
      int doy = doe - (365*yoe + yoe/4 - yoe/100);
      int mp = (5*doy + 2) / 153;
      int m = mp + (mp < 10 ? 3 : -9);
      return y + (m <= 2);
  }
  ```

### n_nationkey (PK, int32_t, native_binary)
- File: `nation/n_nationkey.bin` (25 rows, 4 bytes each)
- This query: join key `s_nationkey = n_nationkey`

### n_name (attribute, varlen_string, offsets_plus_data)
- Files: `nation/n_name_offsets.bin` (int64_t[26]) + `nation/n_name_data.bin`
- This query: output column `nation`
- Only 25 rows — load entire nation table into memory at startup.
  Build array: `std::string nation_names[25]` indexed by n_nationkey.

## Table Stats

| Table    | Rows        | Role      | Sort Order              | Block Size |
|----------|-------------|-----------|-------------------------|------------|
| part     | ~2,000,000  | dimension | p_partkey               | 100,000    |
| supplier | ~100,000    | dimension | s_suppkey               | 100,000    |
| lineitem | ~59,986,052 | fact      | l_orderkey              | 100,000    |
| partsupp | ~8,000,000  | bridge    | ps_partkey, ps_suppkey  | 100,000    |
| orders   | ~15,000,000 | fact      | o_orderkey              | 100,000    |
| nation   | 25          | dimension | n_nationkey             | 100,000    |

## Query Analysis

### Join Pattern (6-way join)
```
part ──(p_partkey = l_partkey)──→ lineitem
supplier ──(s_suppkey = l_suppkey)──→ lineitem
partsupp ──(ps_partkey = l_partkey AND ps_suppkey = l_suppkey)──→ lineitem
orders ──(o_orderkey = l_orderkey)──→ lineitem (via lineitem.l_orderkey)
nation ──(n_nationkey = s_nationkey)──→ supplier
```

### Recommended Execution Strategy

1. **Load dimension tables** (tiny, fit in cache):
   - Nation: 25 rows → `nation_name[nationkey]` array
   - Supplier: 100K rows → `supplier_nationkey[suppkey]` dense array (suppkey is PK, max ~100K)
   - Combine: `nation_for_supplier[suppkey] = nation_name[supplier_nationkey[suppkey]]`
     Or just store nationkey per supplier for later lookup.

2. **Filter parts**: Scan p_name, find partkeys matching `%green%`.
   ~5.48% selectivity → ~109,600 qualifying partkeys.
   Store as a bitset or hash set of qualifying partkeys.

3. **Scan lineitem** (the driving table, ~60M rows):
   For each lineitem row:
   - Check if `l_partkey` is in the qualifying parts set. ~5.48% pass → ~3.3M rows.
   - For qualifying rows, look up:
     - `supplier_suppkey_lookup[l_suppkey]` → supplier row → s_nationkey → nation name
     - `orders_orderkey_lookup[l_orderkey]` → orders row → o_orderdate → extract year
     - `partsupp_pk_index[l_partkey]` → scan partsupp entries to find matching (l_partkey, l_suppkey)
       → get ps_supplycost
   - Compute: `amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`
   - Aggregate: accumulate into `sum_profit[nation][o_year]`

4. **partsupp lookup detail**: Use `partsupp_pk_index` to find all partsupp rows for a given partkey.
   Then linear scan those (typically 4 entries) to find matching suppkey.
   ```cpp
   auto [ps_start, ps_count] = partsupp_index[l_partkey];
   double supplycost = 0.0;
   for (uint32_t j = ps_start; j < ps_start + ps_count; j++) {
       if (ps_suppkey[j] == l_suppkey) { supplycost = ps_supplycost[j]; break; }
   }
   ```

5. **Grouping**: Group by (nation, o_year). nation has 25 values, o_year ~7 values (1992-1998).
   Estimated ~175 groups. Use a flat 2D array: `double sum_profit[25][8]` or small hash map.

6. **Output**: ORDER BY nation ASC, o_year DESC. Sort ~175 entries — trivial.

## Indexes

### supplier_suppkey_lookup (dense_lookup on s_suppkey)
- File: `indexes/supplier_suppkey_lookup.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_suppkey
  Byte 4+:    int32_t[max_suppkey + 1] — lookup[suppkey] = row_index, or -1
  ```
- Sentinel: `-1` means no supplier with that suppkey
- Usage: `lookup[l_suppkey]` → row index → `s_nationkey[row_index]`
- Since supplier is sorted by s_suppkey (dense 1..100000), `lookup[k] ≈ k-1`.
  Can also directly index: `s_nationkey[l_suppkey - 1]` if suppkeys are contiguous.

### orders_orderkey_lookup (dense_lookup on o_orderkey)
- File: `indexes/orders_orderkey_lookup.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_orderkey
  Byte 4+:    int32_t[max_orderkey + 1] — lookup[orderkey] = row_index, or -1
  ```
- Sentinel: `-1`
- Usage: `lookup[l_orderkey]` → row index → `o_orderdate[row_index]` → extract year

### partsupp_pk_index (dense_range on ps_partkey)
- File: `indexes/partsupp_pk_index.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_partkey
  Byte 4+:    struct { uint32_t start; uint32_t count; }[max_partkey + 1]
  ```
  Each entry is 8 bytes (two uint32_t).
- Sentinel: `{0, 0}` means no partsupp rows for that partkey
- partsupp is sorted by (ps_partkey, ps_suppkey), so entries for same partkey are contiguous.
- Usage: For a lineitem row with (l_partkey, l_suppkey):
  ```cpp
  auto entry = partsupp_index[l_partkey]; // {start, count}
  // Scan ps_suppkey[entry.start .. entry.start+entry.count) for match with l_suppkey
  // Typically count=4 (4 suppliers per part in TPC-H SF10)
  ```

## Performance Notes
- Part filter (`%green%` on p_name) is the primary selectivity driver: 5.48%.
- ~3.3M qualifying lineitem rows after part filter, each requiring 3 index lookups.
- Total index probes: ~3.3M × 3 = ~10M lookups. All dense arrays → O(1) per lookup.
- Main memory reads: lineitem partkey/suppkey scan (60M × 8B = 457MB), plus random access
  to extendedprice/discount/quantity for qualifying rows (~3.3M × 24B = 79MB).
- With 64 cores: partition lineitem rows across threads. Each thread maintains local
  `sum_profit[25][8]` array, merge at end.
- Nation/supplier are tiny — fully cached after first access.
