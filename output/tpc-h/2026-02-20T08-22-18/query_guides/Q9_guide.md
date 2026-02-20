# Q9 Guide — Product Type Profit Measure

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
ORDER BY nation, o_year DESC
```

---

## Column Reference

### p_name (STRING, char[56], fixed-char)
- File: `part/p_name.bin` (2000000 rows × 56 bytes = 112 MB)
- Stored as null-terminated fixed-width char[56] (stride=56). NOT dictionary-encoded (high cardinality, ~2M distinct).
- This query: `p_name LIKE '%green%'` → C++: `memchr` / `strstr` substring search.
  ```cpp
  const char* names = (const char*)mmap("part/p_name.bin", ...);
  std::vector<int32_t> green_partkeys;
  const int32_t* pkeys = ...; // mmap part/p_partkey.bin
  for (int i = 0; i < 2000000; i++) {
      if (strstr(names + i*56, "green") != nullptr)
          green_partkeys.push_back(pkeys[i]);
  }
  ```
  Estimated selectivity: ~4.8% → ~96,000 qualifying parts.

### p_partkey (INTEGER, int32_t)
- File: `part/p_partkey.bin` (2000000 rows × 4 bytes = 8 MB)
- Stored as int32_t. Range: 1 to 2,000,000.
- This query: join key `p_partkey = l_partkey`. After LIKE filter, collect qualifying p_partkeys
  into a hash set for probing lineitem.l_partkey, or use lineitem_partkey_hash index.

### l_partkey (INTEGER, int32_t)
- File: `lineitem/l_partkey.bin` (59986052 rows × 4 bytes = 240 MB)
- Stored as int32_t. Foreign key into part.
- This query: join `p_partkey = l_partkey`. Probe qualifying part set.

### l_suppkey (INTEGER, int32_t)
- File: `lineitem/l_suppkey.bin` (59986052 rows × 4 bytes = 240 MB)
- Stored as int32_t. Join key for supplier and partsupp.
- This query: `s_suppkey = l_suppkey` and `ps_suppkey = l_suppkey`.

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59986052 rows × 4 bytes = 240 MB)
- Stored as int32_t. Join key for orders.
- This query: `o_orderkey = l_orderkey` → join lineitem with orders.

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double.
- This query: `ps_supplycost * l_quantity` in amount formula.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double.
- This query: `l_extendedprice*(1-l_discount)` in amount formula.

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double.
- This query: `l_extendedprice*(1-l_discount)`. C++: `ep * (1.0 - disc)`.

### ps_supplycost (DECIMAL, double)
- File: `partsupp/ps_supplycost.bin` (8000000 rows × 8 bytes = 64 MB)
- Stored as native double. Range: 1.00 to 99999.99.
- This query: `ps_supplycost * l_quantity` in amount formula.

### ps_partkey (INTEGER, int32_t)
- File: `partsupp/ps_partkey.bin` (8000000 rows × 4 bytes = 32 MB)
- Join key: `ps_partkey = l_partkey`. Build hash map on (ps_partkey, ps_suppkey) → row_index.

### ps_suppkey (INTEGER, int32_t)
- File: `partsupp/ps_suppkey.bin` (8000000 rows × 4 bytes = 32 MB)
- Join key: `ps_suppkey = l_suppkey`. Composite join with ps_partkey.

### s_suppkey (INTEGER, int32_t)
- File: `supplier/s_suppkey.bin` (100000 rows × 4 bytes = 400 KB)
- Join key: `s_suppkey = l_suppkey`. Small table — build hash map or direct array.

### s_nationkey (INTEGER, int32_t)
- File: `supplier/s_nationkey.bin` (100000 rows × 4 bytes = 400 KB)
- Join key: `s_nationkey = n_nationkey`. Stored as int32_t, range 0-24.
- This query: look up nation for each qualifying supplier row.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15000000 rows × 4 bytes = 60 MB)
- Stored as int32_t, sorted ascending.
- This query: `EXTRACT(YEAR FROM o_orderdate)` = o_year used as GROUP BY key.
  Year extraction from epoch days — use an O(1) formula (see technique file
  `techniques/date-operations.md`):
  ```cpp
  // O(1) year extraction from epoch days d
  int year_from_epoch(int32_t d) {
      // Approximate year, then adjust for leap years
      int y = 1970 + d / 365;
      // Compute start of year y in epoch days
      auto yr_start = [](int y) -> int32_t {
          int y1 = y - 1;
          return 365*(y-1970) + (y1/4 - y1/100 + y1/400) - 477;
      };
      while (yr_start(y+1) <= d) ++y;
      while (yr_start(y)   >  d) --y;
      return y;
  }
  ```
  In practice: build a lookup table for years 1992-1998 (the TPC-H range) → 7 entries.

### n_name (STRING, int16_t, dictionary-encoded)
- File: `nation/n_name.bin` (25 rows × 2 bytes = 50 bytes)
- Dictionary: `nation/n_name_dict.txt` (format: `code=value`, 25 nation names)
- This query: `n_name AS nation` in GROUP BY and output.
- **Loading and usage:**
  ```cpp
  // Load nation dictionary: code → name string
  std::vector<std::string> nation_names(25);
  std::ifstream df("nation/n_name_dict.txt");
  std::string line;
  while (std::getline(df, line)) {
      size_t eq = line.find('=');
      int code = std::stoi(line.substr(0, eq));
      nation_names[code] = line.substr(eq+1);
  }
  // Build n_nationkey → nation_name mapping (25 rows, all fit in L1 cache)
  const int32_t* nkeys = mmap("nation/n_nationkey.bin");
  const int16_t* nname = mmap("nation/n_name.bin");
  std::array<std::string,26> key_to_name; // indexed by nationkey value
  for (int i = 0; i < 25; i++)
      key_to_name[nkeys[i]] = nation_names[nname[i]];
  // Lookup: key_to_name[s_nationkey[supplier_row]]
  ```

### n_nationkey (INTEGER, int32_t)
- File: `nation/n_nationkey.bin` (25 rows × 4 bytes = 100 bytes)
- 25 rows. Tiny table: linear scan faster than any index.

---

## Table Stats

| Table    | Rows     | Role      | Sort Order   | Block Size |
|----------|----------|-----------|--------------|------------|
| part     | 2000000  | dimension | none         | 100000     |
| supplier | 100000   | dimension | none         | 100000     |
| lineitem | 59986052 | fact       | l_shipdate ↑ | 100000     |
| partsupp | 8000000  | bridge     | none         | 100000     |
| orders   | 15000000 | fact       | o_orderdate ↑ | 100000   |
| nation   | 25       | dimension | none         | 100000     |

---

## Query Analysis
- **Early filter:** `p_name LIKE '%green%'` on part (2M rows) → ~96K qualifying parts.
  Build hash set of qualifying p_partkey values. Fast: 2M × 56B scan ≈ 112MB sequential read.
- **Join order (optimal — smallest result first):**
  1. nation (25 rows): build n_nationkey→n_name lookup array (trivial).
  2. supplier (100K): build hash map `s_suppkey → s_nationkey` (fits in L2 cache).
     Look up nation name: `n_name = key_to_name[s_nationkey]`.
  3. part filter result (~96K parts): build hash set of qualifying p_partkeys.
  4. partsupp (8M): filter `ps_partkey IN green_parts`, build hash map
     `(ps_partkey, ps_suppkey) → ps_supplycost`. ~4 entries per part key → ~384K entries.
  5. lineitem (60M): for each row, check `l_partkey IN green_parts` AND composite key
     `(l_partkey, l_suppkey)` in partsupp map. Join l_suppkey with supplier map for nation.
     Look up `l_orderkey` → `o_orderdate` (requires orders hash map or sort-merge).
  6. orders (15M): build hash map `o_orderkey → o_orderdate` OR sort-merge with lineitem.
- **Aggregation:** 25 nations × 7 years = ~175 groups. Tiny hash table / array.
- **Output:** Sort 175 rows by nation ASC, o_year DESC. Trivial.
- **Dominant cost:** lineitem scan (60M × ~6 columns) + hash probes. ~3.7 GB read.

---

## Indexes

### lineitem_partkey_hash (multi-value hash on l_partkey)
- File: `indexes/lineitem_partkey_hash.bin`
- Layout:
  ```
  uint32_t num_unique   // = 2000000 unique l_partkey values
  uint32_t ht_capacity  // = 4194304 (next pow2 above 4M, ~47% load)
  uint32_t num_rows     // = 59986052
  Slot ht[4194304]      // each Slot: {int32_t key (INT32_MIN=empty), uint32_t offset, uint32_t count}
  uint32_t positions[59986052]  // row indices grouped by l_partkey value
  ```
  Average ~30 lineitem rows per part key (60M / 2M = 30).
- **Hash function:** `slot = (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> (64 - 22))`
  (capacity = 4194304 = 2^22, shift = 64-22 = 42)
- **Lookup:** probe ht to find (offset, count). Access `positions[offset..offset+count-1]` for row indices.
  `row_offset` in positions is a ROW index. Access column data as `col_ptr[row_idx]`.
- **This query:** After filtering ~96K qualifying parts, use this index to find all lineitem rows
  for those parts (~96K × 30 = ~2.88M lineitem rows) directly, avoiding full 60M row scan.
  Trades 240MB sequential scan for ~50MB hash table + ~11MB positions lookup (much smaller I/O).

### lineitem_orderkey_hash (multi-value hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout: same structure, `num_unique=15000000`, `ht_capacity=33554432` (2^25).
- **This query:** Optional. If orders are probed for each lineitem row (vs. building orders hash map),
  this can look up order dates. However, building an orders hash map at runtime is likely faster
  for this 6-way join pattern.
