# Q9 Guide — Product Type Profit Measure

## Query
```sql
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
    SELECT n_name AS nation,
           EXTRACT(YEAR FROM o_orderdate) AS o_year,
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey  = l_suppkey
      AND ps_suppkey = l_suppkey
      AND ps_partkey = l_partkey
      AND p_partkey  = l_partkey
      AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation ASC, o_year DESC;
```

## Table Stats
| Table    | Rows       | Role            | Sort Order              | Block Size |
|----------|------------|-----------------|-------------------------|------------|
| lineitem | 59,986,052 | central fact    | l_shipdate ↑            | 100,000    |
| part     | 2,000,000  | filter/dim      | p_partkey ↑             | 100,000    |
| partsupp | 8,000,000  | bridge          | ps_partkey, ps_suppkey ↑| 100,000    |
| supplier | 100,000    | dimension       | s_suppkey ↑             | 100,000    |
| orders   | 15,000,000 | dimension       | o_orderdate ↑           | 100,000    |
| nation   | 25         | tiny dimension  | n_nationkey ↑           | 25         |

## Column Reference

### p_partkey (pk, int32_t — raw)
- File: `part/p_partkey.bin` (2,000,000 × 4 bytes)
- This query: used to filter qualifying parts via p_name LIKE '%green%';
  then as join key `l_partkey = p_partkey`

### p_name (varchar_55, char[56] — fixed-width padded, stride=56)
- File: `part/p_name.bin` (2,000,000 × 56 bytes = 112MB)
- Encoding: each name stored as a null-padded 56-byte field (stride=56).
  From `ingest.cpp::ingest_part`:
  ```cpp
  const int PNAME_STRIDE = 56;
  char name_buf[PNAME_STRIDE] = {};
  strncpy(name_buf, tok[1], PNAME_STRIDE - 1);
  // stored as 56 contiguous bytes, zero-padded at end
  ```
- This query: filter `p_name LIKE '%green%'`
  ```cpp
  const char* name = p_name_buf + row * 56;  // points to 56-byte slot
  bool matches = (strstr(name, "green") != nullptr);
  ```
  Access the name as a C-string (null-terminated within the 56-byte slot).
- Selectivity: ~5.53% → ~110,600 qualifying parts

### l_orderkey (fk_orders_pk, int32_t — raw)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes)
- This query: join `l_orderkey = o_orderkey` via `o_orderkey_hash`

### l_partkey (fk_part_pk, int32_t — raw)
- File: `lineitem/l_partkey.bin` (59,986,052 × 4 bytes)
- This query: join key for `p_partkey` (qualifying parts) and for `ps_composite_hash`

### l_suppkey (fk_supplier_pk, int32_t — raw)
- File: `lineitem/l_suppkey.bin` (59,986,052 × 4 bytes)
- This query: join key for `s_suppkey_hash` and for `ps_composite_hash`

### l_quantity (decimal_15_2, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes)
- This query: `amount -= ps_supplycost * l_quantity`

### l_extendedprice (decimal_15_2, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes)
- This query: `amount = l_extendedprice * (1 - l_discount) - ...`

### l_discount (decimal_15_2, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes)
- This query: multiplied in `l_extendedprice * (1 - l_discount)`

### o_orderdate (date, int32_t — days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes)
- This query: `EXTRACT(YEAR FROM o_orderdate)` — decode year from stored integer:
  ```cpp
  // Convert days-since-epoch back to calendar year (Gregorian)
  // Use Julian Day Number: jdn = days + 2440588
  // Then apply JDN-to-calendar conversion, or use a simple approach:
  int32_t days = o_orderdate[orders_row];
  // Approximate year (sufficient for GROUP BY grouping):
  // Use standard epoch-to-date conversion or pre-build a lookup:
  //   year = (int)(days / 365.2425) + 1970  [±1 error at year boundaries]
  // For exact result, use a proper days-to-date function.
  // Simplest exact approach: reconstruct via JDN
  int32_t jdn = days + 2440588;
  int32_t a = jdn + 32044;
  int32_t b2 = (4*a+3)/146097;
  int32_t c = a - (146097*b2)/4;
  int32_t d2 = (4*c+3)/1461;
  int32_t e = c - (1461*d2)/4;
  int32_t m = (5*e+2)/153;
  int32_t year = 100*b2 + d2 + (m < 10 ? 0 : 1);  // Gregorian year
  ```
- This query: GROUP BY key (years 1992–1998 → 7 distinct values)

### o_orderkey (pk, int32_t — raw)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes)
- This query: lookup target via `o_orderkey_hash`

### ps_supplycost (decimal_15_2, double)
- File: `partsupp/ps_supplycost.bin` (8,000,000 × 8 bytes)
- This query: `amount -= ps_supplycost * l_quantity`

### s_nationkey (fk_nation_pk, int32_t — raw)
- File: `supplier/s_nationkey.bin` (100,000 × 4 bytes)
- This query: join `s_nationkey = n_nationkey` → look up n_name for GROUP BY

### n_nationkey (pk, int32_t — raw)
- File: `nation/n_nationkey.bin` (25 × 4 bytes)
- This query: direct array index [0..24] (nationkey IS the row index)

### n_name (char_25, char[26] — fixed-width padded, stride=26)
- File: `nation/n_name.bin` (25 × 26 bytes = 650 bytes, fits in one cache line)
- Encoding from `ingest.cpp::ingest_nation`:
  ```cpp
  const int NNAME_STRIDE = 26;
  char name_buf[NNAME_STRIDE] = {};
  strncpy(name_buf, tok[1], NNAME_STRIDE - 1);
  ```
- This query: GROUP BY key (25 distinct nations)
- Usage: since n_nationkey is a sequential PK [0..24], access by direct offset:
  ```cpp
  const char* nation_name = n_name_buf + s_nationkey * 26;
  // nation_name is a null-terminated C string of up to 25 chars
  ```
  Load entire n_name.bin into a 650-byte array at startup.

## Indexes

### s_suppkey_hash (hash int32→int32 on supplier.s_suppkey)
- File: `supplier/s_suppkey_hash.bin`
- Capacity: 262,144 (= 2¹⁸); load factor ≈ 100,000 / 262,144 ≈ 0.38
- Binary layout:
  ```
  Bytes  0..7    int64_t  capacity       // = 262144
  Bytes  8..15   int64_t  num_entries    // = 100000
  Bytes  16 ..   int32_t  keys[262144]   // sentinel = -1
  Bytes  16 + 262144*4 ..
                 int32_t  values[262144] // row index into supplier columns
  ```
- Hash function (verbatim from `build_indexes.cpp::hash32`):
  ```cpp
  static inline uint32_t hash32(uint32_t x) {
      x = ((x >> 16) ^ x) * 0x45d9f3bu;
      x = ((x >> 16) ^ x) * 0x45d9f3bu;
      x = (x >> 16) ^ x;
      return x;
  }
  ```
- Probe sequence: open-addressing, linear probing:
  ```cpp
  int64_t mask = 262144LL - 1;  // = 262143
  uint64_t h = (uint64_t)hash32((uint32_t)l_suppkey) & (uint64_t)mask;
  while (keys[h] != -1) {
      if (keys[h] == l_suppkey) { supp_row = values[h]; break; }
      h = (h + 1) & (uint64_t)mask;
  }
  ```
- Usage: look up `lineitem.l_suppkey` → supplier row → fetch `s_nationkey`
- Note: with only 100K entries and 262K slots, the table is tiny (~2MB) and fits
  entirely in L2 cache (32MB available). Expect near-zero collision overhead.

### ps_composite_hash (hash int64→int32 on (ps_partkey, ps_suppkey))
- File: `partsupp/ps_composite_hash.bin`
- Capacity: 16,777,216 (= 2²⁴); load factor ≈ 8,000,000 / 16,777,216 ≈ 0.48
- Key encoding (verbatim from `build_indexes.cpp`):
  ```cpp
  int64_t k = ((int64_t)ps_partkey << 32) | (uint32_t)ps_suppkey;
  ```
  Construct the same key at query time:
  ```cpp
  int64_t key = ((int64_t)l_partkey << 32) | (uint32_t)l_suppkey;
  ```
- Binary layout:
  ```
  Bytes  0..7    int64_t  capacity          // = 16777216
  Bytes  8..15   int64_t  num_entries       // = 8000000
  Bytes  16 ..   int64_t  keys[16777216]    // sentinel = -1LL (all 64 bits set)
  Bytes  16 + 16777216*8 ..
                 int32_t  values[16777216]  // row index into partsupp columns
  ```
  Total size: 16 + 16777216×8 + 16777216×4 = 16 + 134217728 + 67108864 ≈ 192MB
- Hash function (verbatim from `build_indexes.cpp::hash64`):
  ```cpp
  static inline uint64_t hash64(uint64_t x) {
      x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
      x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
      return x ^ (x >> 31);
  }
  ```
- Probe sequence: open-addressing, linear probing, **64-bit sentinel**:
  ```cpp
  int64_t mask = 16777216LL - 1;  // = 16777215
  uint64_t h = hash64((uint64_t)key) & (uint64_t)mask;
  while (htkeys[h] != -1LL) {       // -1LL = 0xFFFFFFFFFFFFFFFF
      if (htkeys[h] == key) { ps_row = htvals[h]; break; }
      h = (h + 1) & (uint64_t)mask;
  }
  ```
- Usage: look up `(l_partkey, l_suppkey)` → partsupp row → fetch `ps_supplycost`

### o_orderkey_hash (hash int32→int32 on orders.o_orderkey)
- File: `orders/o_orderkey_hash.bin`
- Capacity: 33,554,432 (= 2²⁵); see Q3 guide for full layout.
- Usage: look up `lineitem.l_orderkey` → orders row → fetch `o_orderdate` for year extraction

## Query Analysis

### Build Phase (pre-compute qualifying part set)
1. Scan `part/p_name.bin` (112MB, stride=56): find rows where `strstr(name,"green") != nullptr`.
   Collect qualifying p_partkey values into a hash set: ~110,600 entries (~5.5% of 2M parts).
   ```cpp
   // Build a bitset or hash set of qualifying partkeys
   // Bitset over [1..2000000] requires 250KB — fits in L2 cache
   std::vector<bool> part_green(max_partkey + 1, false);
   for (int row = 0; row < npart; row++) {
       if (strstr(p_name_buf + row*56, "green"))
           part_green[p_partkey[row]] = true;
   }
   ```

2. Load nation lookup: read `nation/n_name.bin` (650 bytes) and `nation/n_nationkey.bin`
   (100 bytes) into arrays indexed by n_nationkey [0..24].

3. Load `s_suppkey_hash`, `ps_composite_hash`, `o_orderkey_hash` index files.

### Scan Phase (lineitem — central loop)
For each row i in lineitem (59,986,052 rows):
```
if (!part_green[l_partkey[i]]) continue;       // ~94.5% skip
// look up supplier
supp_row = probe(s_suppkey_hash, l_suppkey[i]);
s_nk = s_nationkey[supp_row];
// look up partsupp
ps_key = ((int64_t)l_partkey[i] << 32) | (uint32_t)l_suppkey[i];
ps_row = probe(ps_composite_hash, ps_key);
ps_cost = ps_supplycost[ps_row];
// look up orders
ord_row = probe(o_orderkey_hash, l_orderkey[i]);
o_year = extract_year(o_orderdate[ord_row]);
// compute amount
double amount = l_extendedprice[i]*(1.0-l_discount[i]) - ps_cost*l_quantity[i];
// accumulate: key = (s_nk, o_year)
agg[s_nk][o_year - 1992] += amount;
```

### Aggregation
- Key space: 25 nations × 7 years = 175 groups
- Fixed 2D array: `double agg[25][7] = {}` (175 × 8 bytes = 1400 bytes, fits in L1 cache)
- Year index: `o_year - 1992` (years in TPC-H range 1992–1998)

### Output
Sort the 175 groups by (nation_name ASC, o_year DESC) and emit.
Nation name comparison uses the 26-byte fixed strings from n_name.bin.

### Performance Notes
- The p_name LIKE scan (112MB) is a one-time cost at startup.
- After the green-part bitset is built, the lineitem scan rejects ~94.5% of rows
  after a single bitset lookup — a very cheap branch.
- Only ~3.3M lineitem rows survive the part filter and require hash probes.
- The 192MB ps_composite_hash may not fully fit in L3 cache (44MB). Expect
  some LLC misses during the partsupp lookup. Consider a 2-pass strategy if
  needed: first build a qualifying (l_partkey, l_suppkey, row) list, sort it
  by composite key, then scan partsupp linearly.
