# Q9 Guide — Product Type Profit Measure

## Column Reference

### p_partkey (INTEGER, int32_t)
- File: part/p_partkey.bin (2,000,000 rows × 4 bytes)
- Used as PK for part table; join key to lineitem.l_partkey and partsupp.ps_partkey
- Hash index: indexes/part_partkey_hash.bin (PK, 1:1)

### p_name (STRING, char[56], fixed-width)
- File: part/p_name.bin (2,000,000 rows × 56 bytes)
- Each entry is a null-padded 56-byte char array (max declared VARCHAR(55) + null terminator)
- This query: `p_name LIKE '%green%'` → `strstr(p_name + i*56, "green") != nullptr`
- Selectivity: 0.048 → ~96,000 qualifying parts out of 2M
- No index on p_name; must scan sequentially; use `strstr` or SIMD substring search

### ps_partkey (INTEGER, int32_t)
- File: partsupp/ps_partkey.bin (8,000,000 rows × 4 bytes)
- Composite PK with ps_suppkey; join key to part.p_partkey and lineitem.l_partkey

### ps_suppkey (INTEGER, int32_t)
- File: partsupp/ps_suppkey.bin (8,000,000 rows × 4 bytes)
- Composite PK with ps_partkey; join key to supplier.s_suppkey and lineitem.l_suppkey

### ps_supplycost (DECIMAL, double)
- File: partsupp/ps_supplycost.bin (8,000,000 rows × 8 bytes)
- Stored as native double — values match SQL directly (e.g., 771.64, 993.49)
- This query: used in `amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity`

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59,986,052 rows × 4 bytes)
- Used as FK join key to orders.o_orderkey

### l_partkey (INTEGER, int32_t)
- File: lineitem/l_partkey.bin (59,986,052 rows × 4 bytes)
- FK to part.p_partkey (and composite with l_suppkey to partsupp)

### l_suppkey (INTEGER, int32_t)
- File: lineitem/l_suppkey.bin (59,986,052 rows × 4 bytes)
- FK to supplier.s_suppkey and part of composite FK to partsupp

### l_quantity (DECIMAL, double)
- File: lineitem/l_quantity.bin (59,986,052 rows × 8 bytes)
- Stored as native double; used in `ps_supplycost * l_quantity` in amount formula

### l_extendedprice (DECIMAL, double)
- File: lineitem/l_extendedprice.bin (59,986,052 rows × 8 bytes)
- Stored as native double; used in `l_extendedprice * (1 - l_discount)` in amount formula

### l_discount (DECIMAL, double)
- File: lineitem/l_discount.bin (59,986,052 rows × 8 bytes)
- Stored as native double; used in amount formula

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15,000,000 rows × 4 bytes)
- PK; join key from lineitem.l_orderkey

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: orders/o_orderdate.bin (15,000,000 rows × 4 bytes)
- **orders sorted by o_orderdate**
- This query: `EXTRACT(YEAR FROM o_orderdate)` → convert epoch days back to year
  - Helper: given epoch days `d`, compute year via iterative or formula approach
  - Approximate: `year ≈ 1970 + d / 365.25` then verify with days_to_year_start
  - Simple approach: since range is 1992–1998, `o_year` = 7 possible values
  - Use a precomputed lookup: `year_start[y]` for y in 1992..1998, binary search or direct check
  - `if d < 8035: year=1991; else if d < 8401: year=1992; ...` — or use the epoch formula
  - year_starts: 1992=8035, 1993=8401, 1994=8766, 1995=9131, 1996=9496, 1997=9862, 1998=10227, 1999=10592

### s_suppkey (INTEGER, int32_t)
- File: supplier/s_suppkey.bin (100,000 rows × 4 bytes)
- PK; join key from lineitem.l_suppkey

### s_nationkey (INTEGER, int32_t)
- File: supplier/s_nationkey.bin (100,000 rows × 4 bytes)
- FK to nation.n_nationkey; used to look up nation name

### n_nationkey (INTEGER, int32_t)
- File: nation/n_nationkey.bin (25 rows × 4 bytes)
- PK; join key from supplier.s_nationkey
- Too small for index; linear scan or direct array lookup

### n_name (STRING, char[26], fixed-width)
- File: nation/n_name.bin (25 rows × 26 bytes)
- Each entry is a null-padded 26-byte char array (CHAR(25) + null)
- Used as GROUP BY key "nation" in output; 25 distinct values
- This query: look up n_name using supplier.s_nationkey as direct array index (nationkey = row position)

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate | 100,000    |
| part     | 2,000,000  | dimension | none        | 100,000    |
| partsupp | 8,000,000  | bridge    | none        | 100,000    |
| supplier | 100,000    | dimension | none        | 100,000    |
| nation   | 25         | dimension | none        | 100,000    |

## Query Analysis
- **Join pattern**: 6-table join — part, supplier, lineitem, partsupp, orders, nation
- **Filters**: only `p_name LIKE '%green%'` (selectivity 0.048 → 96K parts); no filters on other tables
- **Recommended join order** (minimize intermediate results):
  1. Scan part (2M rows), filter p_name LIKE '%green%' → 96K qualifying partkeys → build hash set
  2. Scan partsupp (8M rows), filter ps_partkey ∈ part_set → ~96K/2M × 8M = ~384K rows
     → build hash map: composite(ps_partkey, ps_suppkey) → ps_supplycost
  3. Scan lineitem (60M rows), filter l_partkey ∈ part_set → ~2.88M rows
     → for each qualifying row, lookup ps_supplycost via composite hash map
     → also need l_orderkey (→ orders), l_suppkey (→ supplier)
  4. Supplier lookup: build array s_nationkey[s_suppkey] (100K entries, fits in L1) using s_suppkey as index
  5. Nation lookup: n_name[n_nationkey] (25 entries) — direct array access
  6. Orders lookup: for each qualifying l_orderkey, need o_orderdate; build orders hash map (o_orderkey → o_orderdate row pos)
  7. Compute `amount = extprice*(1-disc) - ps_supplycost*qty` per row
  8. Group by (n_name, o_year) → ~125 groups (25 nations × 5-7 years); use flat array
- **Combined selectivity**: ~2.88M lineitem rows processed (after part filter)
- **Aggregation**: 125 groups, open-addressing hash or flat 2D array [25 nations][7 years]
- **Output**: ORDER BY nation ASC, o_year DESC (no LIMIT); ~125 rows sorted

## Indexes

### part_partkey_hash (hash, PK 1:1, on p_partkey)
- File: indexes/part_partkey_hash.bin
- Layout: `[uint32_t magic=0x48494458][uint32_t num_positions=2000000][uint32_t num_unique=2000000][uint32_t capacity=4194304][uint32_t positions[2000000]][SlotI32 ht[4194304]]`
- SlotI32: `{int32_t key; uint32_t offset; uint32_t count=1;}` (12 bytes)
- Empty slot sentinel: `key == INT32_MIN`
- Lookup: `h = (key * 0x9E3779B97F4A7C15ULL) >> (64 - 22)` (capacity=4194304=2^22, shift=42)
  then `slot = h & (capacity-1)`, linear probe; `positions[slot.offset]` = row in part table
- row_offset is ROW index into part column arrays
- This query: after filtering qualifying partkeys, use to find their row positions for other columns (rarely needed since p_name and p_partkey are scanned together)

### supplier_suppkey_hash (hash, PK 1:1, on s_suppkey)
- File: indexes/supplier_suppkey_hash.bin
- Layout: `num_positions=100000`, `num_unique=100000`, `capacity=262144`
- SlotI32: 12 bytes; empty sentinel INT32_MIN
- Lookup shift: `64 - 18 = 46` (capacity=262144=2^18)
- row_offset is ROW index into supplier column arrays
- This query: look up supplier row by s_suppkey to retrieve s_nationkey; but faster to build an in-memory array `s_nationkey_arr[100001]` indexed by suppkey during query execution

### partsupp_partkey_suppkey_hash (hash, composite PK, on ps_partkey+ps_suppkey)
- File: indexes/partsupp_partkey_suppkey_hash.bin
- Layout: `[uint32_t magic][uint32_t num_positions=8000000][uint32_t num_unique=8000000][uint32_t capacity=16777216][uint32_t positions[8000000]][SlotU64 ht[16777216]]`
- SlotU64: `{uint64_t key; uint32_t offset; uint32_t count=1;}` (16 bytes)
- Composite key encoding: `key = ((uint64_t)(uint32_t)ps_partkey << 32) | (uint32_t)ps_suppkey`
- Empty slot sentinel: `key == UINT64_MAX`
- Lookup: `h = murmur_mix(composite_key) >> (64 - 24)` (capacity=16M=2^24, shift=40)
  then linear probe on SlotU64 array; `positions[slot.offset]` = row in partsupp table
- row_offset is ROW index into partsupp column arrays (use to access ps_supplycost)
- This query: look up (l_partkey, l_suppkey) in this index to retrieve ps_supplycost for each lineitem row

### lineitem_orderkey_hash (hash, multi-value, on l_orderkey)
- File: indexes/lineitem_orderkey_hash.bin
- Layout: `num_positions=59986052`, `num_unique=15000000`, `capacity=33554432`
- SlotI32: 12 bytes per slot; positions grouped by orderkey
- This query: for each qualifying l_orderkey, used to look up o_orderdate in orders table
  - More efficient: build in-memory orders hash map (o_orderkey → o_orderdate) during query, then probe per lineitem row

### orders_orderkey_hash (hash, PK 1:1, on o_orderkey)
- File: indexes/orders_orderkey_hash.bin
- Layout: `num_positions=15000000`, `num_unique=15000000`, `capacity=33554432`
- SlotI32: 12 bytes; shift = `64 - 25 = 39`
- This query: given l_orderkey, look up the row in orders to get o_orderdate
  - Build in-memory hash map from orders: `unordered_map<int32_t, int32_t>` (orderkey → epoch_orderdate) for 15M entries — or use this persistent index

## Year Extraction from Epoch Days
```
// year_starts for 1992..1999 (inclusive):
static const int32_t year_starts[] = {
  8035,  // 1992 (1992 is leap: 366 days)
  8401,  // 1993
  8766,  // 1994
  9131,  // 1995
  9496,  // 1996 (leap: 366 days)
  9862,  // 1997
  10227, // 1998
  10592  // 1999 (sentinel)
};
// Extract year from epoch days d:
// binary search or linear scan over year_starts
int o_year = 1992;
for (int y = 0; y < 7; y++) {
  if (d < year_starts[y+1]) { o_year = 1992 + y; break; }
}
```
