# Q9 Guide — Product Type Profit Measure

## Query
```sql
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
    SELECT n_name AS nation,
           EXTRACT(YEAR FROM o_orderdate) AS o_year,
           l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey   = l_suppkey
      AND ps_suppkey  = l_suppkey
      AND ps_partkey  = l_partkey
      AND p_partkey   = l_partkey
      AND o_orderkey  = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation ASC, o_year DESC;
```

## Column Reference

### p_name (STRING, char[56], fixed_char56 encoding)
- File: `part/p_name.bin` (2,000,000 × 56 bytes = 112 MB)
- Encoding: null-padded fixed-width 56-byte char array. Row i at byte offset `i × 56`.
- Access: `const char* name = p_name_col + row_idx * 56;`
- **This query**: `p_name LIKE '%green%'` — substring search
  - C++: `strstr(p_name_col + row*56, "green") != nullptr`
  - Selectivity: ~5% of 2M parts = ~100,000 qualifying parts
  - Apply this filter FIRST to minimize join work

### p_partkey (INTEGER, int32_t)
- File: `part/p_partkey.bin` (2,000,000 × 4 bytes)
- **This query**: Join key `p_partkey = l_partkey`. After LIKE filter, collect qualifying part keys.
- Index: `indexes/part_partkey_hash.bin` (PK, 2M unique, cap=4,194,304)

### l_partkey (INTEGER, int32_t)
- File: `lineitem/l_partkey.bin` (59,986,052 × 4 bytes)
- **This query**: Join key `l_partkey = p_partkey` AND `l_partkey = ps_partkey`
- Access: mmap `lineitem/l_partkey.bin`, read as int32_t array

### l_suppkey (INTEGER, int32_t)
- File: `lineitem/l_suppkey.bin` (59,986,052 × 4 bytes)
- **This query**: Join key `l_suppkey = s_suppkey` AND `l_suppkey = ps_suppkey`

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes)
- **This query**: Join key `o_orderkey = l_orderkey`
- Index: `indexes/lineitem_orderkey_hash.bin` (15M unique keys)

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes)
- **This query**: Used in `ps_supplycost * l_quantity` for amount calculation

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes)
- **This query**: Used in `l_extendedprice * (1 - l_discount)` for amount calculation

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes)
- **This query**: Used in `(1 - l_discount)` expression. Range 0.00–0.10.

### ps_supplycost (DECIMAL, double)
- File: `partsupp/ps_supplycost.bin` (8,000,000 × 8 bytes)
- Stored as native double. Verified non-zero: first=771.64, last=647.58
- **This query**: `ps_supplycost * l_quantity` for profit calculation

### ps_partkey (INTEGER, int32_t)
- File: `partsupp/ps_partkey.bin` (8,000,000 × 4 bytes)
- **This query**: Part of composite join key `(ps_partkey=l_partkey AND ps_suppkey=l_suppkey)`

### ps_suppkey (INTEGER, int32_t)
- File: `partsupp/ps_suppkey.bin` (8,000,000 × 4 bytes)
- **This query**: Part of composite join key `(ps_partkey=l_partkey AND ps_suppkey=l_suppkey)`

### s_suppkey (INTEGER, int32_t)
- File: `supplier/s_suppkey.bin` (100,000 × 4 bytes)
- **This query**: Join key `s_suppkey = l_suppkey`
- Index: `indexes/supplier_suppkey_hash.bin` (PK, 100K unique, cap=262,144)

### s_nationkey (INTEGER, int32_t)
- File: `supplier/s_nationkey.bin` (100,000 × 4 bytes)
- **This query**: Join key `s_nationkey = n_nationkey`. Range 0–24 (25 nations).

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes)
- **This query**: `EXTRACT(YEAR FROM o_orderdate)` for GROUP BY
- Year extraction from epoch days (no loop needed — use closed-form):
  ```cpp
  // From epoch days d, extract year:
  int z = d + 719468;
  int era = (z >= 0 ? z : z - 146096) / 146097;
  int doe = z - era * 146097;
  int yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
  int y   = yoe + era * 400;
  int doy = doe - (365*yoe + yoe/4 - yoe/100);
  if (doy >= (y%4==0&&(y%100!=0||y%400==0)?60:59)) y++;  // adjust if past Feb
  // Actually simpler: use Howard Hinnant's civil_from_days for year
  ```
  Alternatively: `int year = (int)(1970 + d / 365.2425)` as approximation, then adjust.
  Best: use the reference inverse formula from Howard Hinnant's date library.
- Values: orders span 1992–1998 → o_year ranges 1992–1998 (7 distinct values)

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes)
- **This query**: Join key `o_orderkey = l_orderkey`
- Index: `indexes/orders_orderkey_hash.bin` (PK, 15M unique, cap=33,554,432)

### n_nationkey (INTEGER, int32_t)
- File: `nation/n_nationkey.bin` (25 × 4 bytes = 100 bytes)
- **This query**: Join key `n_nationkey = s_nationkey`. 25 rows — linear scan always optimal.
- No index built (too small).

### n_name (STRING, char[26], fixed_char26 encoding)
- File: `nation/n_name.bin` (25 × 26 bytes = 650 bytes)
- Encoding: null-padded fixed-width 26-byte char array.
- Access: `const char* name = n_name_col + row_idx * 26;`
- **This query**: Output column `nation` in GROUP BY and SELECT
- 25 distinct nation names. Build lookup array: `nation_name[nationkey] = n_name + key*26`
- Since n_nationkey = row index (0-based), direct array access: `n_name_col + nationkey * 26`

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| lineitem | 59,986,052 | fact      | none       | 100,000    |
| orders   | 15,000,000 | fact      | none       | 100,000    |
| partsupp | 8,000,000  | bridge    | none       | 100,000    |
| part     | 2,000,000  | dimension | none       | 100,000    |
| supplier | 100,000    | dimension | none       | 100,000    |
| nation   | 25         | dimension | none       | 100,000    |

## Query Analysis
- **Join graph**: lineitem is central hub. Joins:
  - lineitem.l_partkey  = part.p_partkey (filter: p_name LIKE '%green%', 5% selectivity → ~100K parts)
  - lineitem.l_suppkey  = supplier.s_suppkey (100K suppliers)
  - lineitem.(l_partkey,l_suppkey) = partsupp.(ps_partkey,ps_suppkey) (8M rows, composite key)
  - lineitem.l_orderkey = orders.o_orderkey (15M orders)
  - supplier.s_nationkey = nation.n_nationkey (25 nations, tiny)
- **Recommended join order** (filter smallest → build hash → probe):
  1. Scan part (2M), LIKE filter '%green%' → ~100K qualifying partkeys → hash set `green_parts`
  2. Scan lineitem (60M), filter `l_partkey IN green_parts` → ~3M qualifying lineitem rows (5%)
  3. For each qualifying lineitem row, join:
     - Lookup partsupp via composite hash: key=(l_partkey,l_suppkey)
     - Lookup supplier via s_suppkey hash → get s_nationkey
     - Lookup orders via o_orderkey hash → get o_orderdate (extract year)
     - Lookup nation via n_nationkey array → get n_name
  4. Compute amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity
  5. Accumulate in hash map keyed by (n_name, year)
- **Filter selectivities**:
  - p_name LIKE '%green%': 5% of parts = 100K parts
  - After joining: ~5% × 60M = ~3M lineitem rows involved
  - All other joins are equi-joins (no additional filter)
- **Aggregation**: GROUP BY (nation, o_year) → 25 nations × 7 years = 175 max groups (workload: ~200)
  - Use flat 175-slot accumulator after knowing nation+year combinations
- **Output**: ORDER BY nation ASC, o_year DESC → trivial sort of ≤175 rows

## Indexes

### part_partkey_hash (hash on p_partkey, single-value PK)
- File: `indexes/part_partkey_hash.bin`
- Layout: `[uint32_t num_positions=2000000][uint32_t positions[2M]][uint32_t cap=4194304][HEntry32 × 4194304]`
- Empty slot: key == INT32_MIN
- **This query**: After LIKE filter, can build green_parts as a hash set from part rows. The part_partkey_hash index maps part key → part row (for reverse lookup). Less needed here since we scan part sequentially for the LIKE filter.

### supplier_suppkey_hash (hash on s_suppkey, single-value PK)
- File: `indexes/supplier_suppkey_hash.bin`
- Layout: `[uint32_t num_positions=100000][uint32_t positions[100K]][uint32_t cap=262144][HEntry32 × 262144]`
- **This query**: Look up supplier row from l_suppkey → get s_nationkey
  - Alternative: since supplier has only 100K rows, build in-memory array `suppkey_to_nationkey[suppkey]` for O(1) lookup

### partsupp_composite_hash (hash on (ps_partkey,ps_suppkey), composite PK)
- File: `indexes/partsupp_composite_hash.bin`
- Layout:
  ```
  [uint32_t num_positions = 8,000,000]
  [uint32_t positions[8000000]]
  [uint32_t capacity = 16,777,216]
  [struct HEntry64 {int64_t key; uint32_t offset; uint32_t count;} × 16777216]
  // Empty slot: key == INT64_MIN (-9223372036854775808)
  ```
- Key encoding: `int64_t k = ((int64_t)l_partkey << 32) | (uint32_t)l_suppkey`
- Hash function: 64-bit multiply-mix:
  ```cpp
  uint32_t h64(int64_t key) {
      uint64_t x = (uint64_t)key;
      x = (x^(x>>30))*0xbf58476d1ce4e5b9ULL;
      x = (x^(x>>27))*0x94d049bb133111ebULL;
      return (uint32_t)(x^(x>>31)) & (cap-1);
  }
  ```
- Lookup: for lineitem row with (l_partkey, l_suppkey):
  ```cpp
  int64_t k = ((int64_t)l_partkey << 32) | (uint32_t)l_suppkey;
  uint32_t h = h64(k) & (cap-1);
  while (ht[h].key != INT64_MIN && ht[h].key != k) h = (h+1) & (cap-1);
  if (ht[h].key == k) { uint32_t ps_row = positions[ht[h].offset]; /* count=1 */ }
  ```
- row_offset is ROW index. Byte offset in ps_supplycost.bin = ps_row × 8

### orders_orderkey_hash (hash on o_orderkey, single-value PK)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t num_positions=15M][positions[15M]][uint32_t cap=33554432][HEntry32 × 33554432]`
- **This query**: Look up orders row from l_orderkey → get o_orderdate (for year extraction)

### lineitem_shipdate_zonemap — NOT USED in Q9 (no date filter on lineitem)
### orders_orderdate_zonemap — NOT USED in Q9 (no date filter on orders)
