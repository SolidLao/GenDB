# Q9 Guide — Product Type Profit Measure

## Column Reference

### p_partkey (INTEGER, int32_t)
- File: `part/p_partkey.bin` (2000000 rows)
- This query: join key `p_partkey = l_partkey`; used after filtering by p_name LIKE '%green%'

### p_name (STRING, char[55], fixed-width)
- File: `part/p_name.bin` (2000000 × 55 = 110MB; each record is exactly 55 null-padded bytes)
- This query: `p_name LIKE '%green%'`
  - C++ filter: `memchr(p_name_data + row*55, 'g', 55) != nullptr && memmem(p_name_data + row*55, 55, "green", 5) != nullptr`
  - Or more simply: use `strncmp`-style substring search on each 55-byte record.
  - Estimated selectivity: ~5.5% → ~110K qualifying parts
- Access: `const char* name = part_p_name_data + (row_idx * 55);`

### s_suppkey (INTEGER, int32_t)
- File: `supplier/s_suppkey.bin` (100000 rows)
- This query: join key `s_suppkey = l_suppkey`

### s_nationkey (INTEGER, int32_t)
- File: `supplier/s_nationkey.bin` (100000 rows)
- This query: join key `s_nationkey = n_nationkey` to get nation name

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- This query: join key `l_orderkey = o_orderkey`

### l_partkey (INTEGER, int32_t)
- File: `lineitem/l_partkey.bin` (59986052 rows)
- This query: join condition `p_partkey = l_partkey` and `ps_partkey = l_partkey`

### l_suppkey (INTEGER, int32_t)
- File: `lineitem/l_suppkey.bin` (59986052 rows)
- This query: join conditions `s_suppkey = l_suppkey`, `ps_suppkey = l_suppkey`

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- Stored as native double.
- This query: used in `amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity`

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- Stored as native double.
- This query: `l_extendedprice * (1 - l_discount)` component of amount

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59986052 rows)
- Stored as native double.
- This query: used in amount formula: `1 - l_discount`

### ps_partkey (INTEGER, int32_t)
- File: `partsupp/ps_partkey.bin` (8000000 rows)
- This query: join condition `ps_partkey = l_partkey`

### ps_suppkey (INTEGER, int32_t)
- File: `partsupp/ps_suppkey.bin` (8000000 rows)
- This query: join condition `ps_suppkey = l_suppkey`

### ps_supplycost (DECIMAL, double)
- File: `partsupp/ps_supplycost.bin` (8000000 rows)
- Stored as native double. Values like 771.64.
- This query: `ps_supplycost * l_quantity` component of amount

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15000000 rows)
- This query: join key `o_orderkey = l_orderkey`

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15000000 rows)
- Sorted column (orders sorted by o_orderdate).
- This query: `EXTRACT(YEAR FROM o_orderdate)` for GROUP BY o_year
  - Conversion: given int32_t `d`, compute year by adding d to epoch 1970-01-01 and extracting year.
  - Efficient O(1) approach: precompute a lookup table mapping epoch day → year, or use binary search over year boundaries.
  - Year boundaries (epoch days): 1992=8035, 1993=8400, 1994=8766, 1995=9131, 1996=9496, 1997=9862, 1998=10227
  - C++ year extraction: `int year = 1970; int rem = d; for each year: subtract days_in_year...`
  - Or: `int year = (int)(d / 365.2425) + 1970; /* adjust ±1 */`

### n_nationkey (INTEGER, int32_t)
- File: `nation/n_nationkey.bin` (25 rows)
- This query: join key `n_nationkey = s_nationkey`

### n_name (STRING, uint8_t, byte_pack)
- File: `nation/n_name.bin` (25 rows, uint8_t)
- Lookup: `nation/n_name_lookup.txt` — 25 nation names, one per line, alphabetically sorted.
  - Code 0="ALGERIA", 1="ARGENTINA", ..., 24="VIETNAM" (load at startup)
- This query: `n_name AS nation` for GROUP BY nation (use string value for grouping key)
  - Load lookup at startup: `std::vector<std::string> nation_names` (25 entries)
  - For each row: decode `nation_code → nation_name = nation_names[n_name[nationkey_row]]`

## Table Stats
| Table    | Rows     | Role      | Sort Order  | Block Size |
|----------|----------|-----------|-------------|------------|
| lineitem | 59986052 | fact      | l_shipdate  | 100000     |
| orders   | 15000000 | fact      | o_orderdate | 100000     |
| part     | 2000000  | dimension | none        | 100000     |
| partsupp | 8000000  | bridge    | none        | 100000     |
| supplier | 100000   | dimension | none        | 100000     |
| nation   | 25       | dimension | none        | 100000     |

## Query Analysis
- **Join pattern**: 6-way join: part ⋈ lineitem ⋈ partsupp ⋈ supplier ⋈ orders ⋈ nation
- **Key filter**: `p_name LIKE '%green%'` → ~5.5% of 2M parts = ~110K qualifying parts
- **Recommended join order** (smallest → largest):
  1. Scan `part/p_name.bin` (2M × 55 bytes): filter for '%green%' → ~110K part keys. Collect in hash set.
  2. Scan `lineitem/l_partkey.bin` (60M rows): filter by part key hash set → ~5.5% = ~3.3M lineitem rows.
     Also load l_suppkey, l_orderkey, l_quantity, l_extendedprice, l_discount for qualifying rows.
  3. For each qualifying lineitem row: look up partsupp via composite hash index on (l_partkey, l_suppkey) → get ps_supplycost.
  4. For each qualifying lineitem row: look up supplier via s_suppkey hash index → get s_nationkey.
  5. Decode nation name from n_name lookup (25 rows, direct array access by n_nationkey value).
  6. Look up order via orders_orderkey_hash on l_orderkey → get o_orderdate → extract year.
  7. Compute `amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity`
  8. Accumulate into GROUP BY (nation, o_year) → ~175 groups.
- **Selectivities**:
  - `p_name LIKE '%green%'`: 5.5% of 2M = ~110K parts
  - lineitem filtered by qualifying parts: ~5.5% = ~3.3M rows
  - No further row-level filters; all 3.3M contribute to profit
- **Aggregation**: GROUP BY (nation_name, year) → ~175 groups (25 nations × ~7 years). Small hash table.
- **Output**: ~175 rows, ORDER BY nation ASC, o_year DESC. Small sort.
- **Nation table**: 25 rows — load entirely into memory as array at query start. No index needed.

## Indexes

### part_partkey_hash (single-value hash on p_partkey)
- File: `indexes/part_partkey_hash.bin`
- Layout:
  ```
  uint32_t num_rows  (= 2000000)
  uint32_t capacity  (= 4194304)
  SvEntry[4194304]: { int32_t key; uint32_t row_idx; }
  // empty slot: key == INT32_MIN (-2147483648)
  ```
- Probe: `slot = (key * 2654435761ULL >> 32) & (capacity-1)`, linear probe.
- row_idx is the ROW index into p_partkey.bin (and all other part columns).
- This query: not the primary use (scanning part directly is fine); used if reverse-lookup needed.

### partsupp_composite_hash (composite single-value hash on (ps_partkey, ps_suppkey))
- File: `indexes/partsupp_composite_hash.bin`
- Layout:
  ```
  uint32_t num_rows  (= 8000000)
  uint32_t capacity  (= 16777216)
  CsEntry[16777216]: { int64_t key; uint32_t row_idx; uint32_t _pad; }  (16 bytes each)
  // empty slot: key == INT64_MIN
  ```
- Key encoding: `int64_t k = ((int64_t)ps_partkey << 32) | (uint32_t)ps_suppkey`
- Hash: 64-bit mix hash on k, masked to capacity.
- row_idx is the ROW index into ps_supplycost.bin.
- This query: for each qualifying lineitem row, lookup `(l_partkey, l_suppkey)` → ps_supplycost.
  - `int64_t k = ((int64_t)l_partkey << 32) | (uint32_t)l_suppkey`
  - hash → scan entries → get row_idx → `ps_supplycost[row_idx]`

### supplier_suppkey_hash (single-value hash on s_suppkey)
- File: `indexes/supplier_suppkey_hash.bin`
- Layout:
  ```
  uint32_t num_rows  (= 100000)
  uint32_t capacity  (= 262144)
  SvEntry[262144]: { int32_t key; uint32_t row_idx; }
  // empty: key == INT32_MIN
  ```
- This query: for each qualifying lineitem row, lookup supplier by l_suppkey → get s_nationkey row.
  - Then `s_nationkey[row_idx]` is the integer 0–24 → direct index into nation arrays.

### orders_orderkey_hash (single-value hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout:
  ```
  uint32_t num_rows  (= 15000000)
  uint32_t capacity  (= 33554432)
  SvEntry[33554432]: { int32_t key; uint32_t row_idx; }
  // empty: key == INT32_MIN
  ```
- This query: for each qualifying lineitem row, lookup order by l_orderkey → get o_orderdate row_idx.
  - Then `o_orderdate[row_idx]` → extract year for GROUP BY.
