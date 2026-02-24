# Q9 Guide — Product Type Profit Measure

## Column Reference

### p_partkey (INTEGER, int32_t)
- File: part/p_partkey.bin (2000000 rows)
- This query: join key `p_partkey = l_partkey`. After LIKE filter, collect qualifying partkeys.

### p_name (STRING, char[56], fixed-width 56 bytes per row)
- File: part/p_name.bin (2000000 rows × 56 bytes = 112 MB)
- Each entry is a null-padded 56-byte field (VARCHAR(55) + null terminator).
- Read row i: `const char* name = p_name_ptr + i * 56;`
- This query: `p_name LIKE '%green%'` → scan all 2M entries, check `strstr(name, "green") != nullptr`.
  Estimated selectivity: 5.5% → ~110,000 qualifying part keys.
- After filtering, build hash set of qualifying p_partkey values.

### l_partkey (INTEGER, int32_t)
- File: lineitem/l_partkey.bin (59986052 rows)
- This query: join with part — `l_partkey = p_partkey`. Use qualifying partkey hash set as bloom/hash filter.

### l_suppkey (INTEGER, int32_t)
- File: lineitem/l_suppkey.bin (59986052 rows)
- This query: join with supplier (`s_suppkey = l_suppkey`) and partsupp (`ps_suppkey = l_suppkey`).

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59986052 rows)
- This query: join key `o_orderkey = l_orderkey` to retrieve o_orderdate for EXTRACT(YEAR).

### l_extendedprice (DECIMAL, double)
- File: lineitem/l_extendedprice.bin (59986052 rows)
- Stored as native double. Values match SQL directly.
- This query: contributes to `amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`

### l_discount (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_discount.bin (59986052 rows)
- Compression: byte_pack. code = round(value × 100). Lookup: lineitem/l_discount_lookup.bin (256 doubles).
- Actual value: `disc_lut[discount_code]`
- This query: `(1 - l_discount)` → `1.0 - disc_lut[disc_code]`
  amount term: `extprice * (1.0 - disc_lut[disc_code])`

### l_quantity (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_quantity.bin (59986052 rows)
- Compression: byte_pack. code = round(value). Lookup: lineitem/l_quantity_lookup.bin (256 doubles).
- Actual value: `qty_lut[quantity_code]`
- This query: `ps_supplycost * l_quantity` → `ps_supplycost * qty_lut[qty_code]`

### s_suppkey (INTEGER, int32_t)
- File: supplier/s_suppkey.bin (100000 rows)
- This query: join key `s_suppkey = l_suppkey`. Hash index available for lookup.

### s_nationkey (INTEGER, int32_t)
- File: supplier/s_nationkey.bin (100000 rows)
- This query: join key `s_nationkey = n_nationkey` to get nation name.

### ps_partkey (INTEGER, int32_t)
- File: partsupp/ps_partkey.bin (8000000 rows)
- This query: composite join key — `ps_partkey = l_partkey AND ps_suppkey = l_suppkey`.

### ps_suppkey (INTEGER, int32_t)
- File: partsupp/ps_suppkey.bin (8000000 rows)
- This query: composite join key (see ps_partkey above).

### ps_supplycost (DECIMAL, double)
- File: partsupp/ps_supplycost.bin (8000000 rows)
- Stored as native double. Values match SQL directly.
- This query: `amount` term = `... - ps_supplycost * qty_lut[qty_code]`
  For a qualifying lineitem row with (l_partkey, l_suppkey), look up partsupp composite hash
  to get row_idx, then read `ps_supplycost[row_idx]`.

### n_nationkey (INTEGER, int32_t)
- File: nation/n_nationkey.bin (25 rows)
- This query: join key `n_nationkey = s_nationkey`. Only 25 rows — linear scan.

### n_name (STRING, char[26], fixed-width 26 bytes per row)
- File: nation/n_name.bin (25 rows × 26 bytes = 650 bytes)
- Each entry is a null-padded 26-byte field.
- Read row i: `const char* name = n_name_ptr + i * 26;`
- This query: GROUP BY n_name (as "nation" in subquery alias). Output nation name string.
  Build a small array of 25 nation names keyed by n_nationkey (keys 0–24).

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15000000 rows)
- This query: join key `o_orderkey = l_orderkey`. Hash index available for lookup.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: orders/o_orderdate.bin (15000000 rows)
- Column is sorted ascending.
- This query: `EXTRACT(YEAR FROM o_orderdate)` → compute year from epoch days.
  Year extraction: use inverse of epoch formula, or iterate: `year = 1970 + (raw * 400) / 146097`
  (approximate); then verify by recomputing. Simpler: use a helper function that applies
  the civil calendar inverse.
  GROUP BY o_year → accumulate per (nation_name, year) pair.

## Table Stats

| Table    | Rows     | Role      | Sort Order  | Block Size |
|----------|----------|-----------|-------------|------------|
| part     | 2000000  | dimension | none        | 100000     |
| supplier | 100000   | dimension | none        | 100000     |
| lineitem | 59986052 | fact      | l_shipdate  | 100000     |
| partsupp | 8000000  | bridge    | none        | 100000     |
| orders   | 15000000 | fact      | o_orderdate | 100000     |
| nation   | 25       | dimension | none        | 100000     |

## Query Analysis

- **Join pattern**: 6-table star join (most complex query in workload)
  - part ⋈ lineitem on p_partkey = l_partkey (filter: p_name LIKE '%green%')
  - supplier ⋈ lineitem on s_suppkey = l_suppkey
  - partsupp ⋈ lineitem on (ps_partkey,ps_suppkey) = (l_partkey,l_suppkey)
  - orders ⋈ lineitem on o_orderkey = l_orderkey
  - nation ⋈ supplier on n_nationkey = s_nationkey
- **Filters**: `p_name LIKE '%green%'` on part — 5.5% selectivity → ~110K qualifying parts.
  This is the primary filter; all other joins are PK-FK (no additional row reduction).
- **Recommended execution plan**:
  1. Scan part (2M × 56 bytes), filter `strstr(p_name, "green")`, collect ~110K partkeys → hash set P
  2. Build supplier lookup: array[s_suppkey] = {s_nationkey} (100K rows, trivial)
  3. Build nation lookup: array[n_nationkey] = {n_name} (25 rows, trivial)
  4. For each lineitem row: check l_partkey ∈ P (hash set probe) → if yes:
     a. Look up s_nationkey via supplier array[l_suppkey]
     b. Get nation_name via nation array[s_nationkey]
     c. Look up partsupp composite hash with key = l_partkey × 100003 + l_suppkey → get ps_supplycost
     d. Look up orders hash with l_orderkey → get o_orderdate → extract year
     e. Compute amount = extprice*(1-disc_lut[dc]) - supplycost*qty_lut[qc]
     f. Accumulate into aggregation map: (nation_name, year) → sum_profit
  5. Sort result by (nation ASC, o_year DESC)
- **Combined selectivity**: ~5.5% of lineitem rows qualify (those with green parts) → ~3.3M rows processed after filter.
- **Aggregation**: ~175 groups (25 nations × ~7 years). Very low cardinality — use fixed array or small hash map.
- **Output**: ~175 rows, ORDER BY nation ASC, o_year DESC. No LIMIT.
- **Year extraction from epoch days**: `int year = 1970; int d = raw; /* iterate or use formula */`
  Reliable method: since o_orderdate ranges 1992–1998 (7 years), a simple loop works:
  precompute year boundaries as epoch days (1992-01-01=8035, 1993-01-01=8400, ..., 1999-01-01=10592),
  binary search for year.

## Indexes

### partkey_hash (hash on p_partkey) — part
- File: part/indexes/partkey_hash.bin
- Layout: `[uint32_t capacity=4194304]` then `[int32_t key, uint32_t row_idx]` × capacity
- Empty slot: key == INT32_MIN. Hash function: `((uint32_t)key * 2654435761u) & (capacity-1)`.
- This query: optional — after scanning part for LIKE filter, you have the qualifying partkeys
  directly. The hash is more useful if joining from lineitem side to retrieve part row data
  (not needed since we only need partkey itself from part table).

### suppkey_hash (hash on s_suppkey) — supplier
- File: supplier/indexes/suppkey_hash.bin
- Layout: `[uint32_t capacity=262144]` then `[int32_t key, uint32_t row_idx]` × capacity
- Empty slot: key == INT32_MIN. Hash function: `((uint32_t)key * 2654435761u) & (capacity-1)`.
- This query: look up s_nationkey for a given l_suppkey.
  `h = hash(l_suppkey, mask); while(table[h].key != l_suppkey ...) h=(h+1)&mask; row=table[h].row_idx; nk=s_nationkey[row];`
- 100K rows → alternative: build array[s_suppkey] = row_idx in memory (100K ints = 400KB, fits L2).

### composite_hash (hash on (ps_partkey, ps_suppkey)) — partsupp
- File: partsupp/indexes/composite_hash.bin
- Layout: `[uint32_t capacity=16777216]` then `[int64_t key, uint32_t row_idx, uint32_t _pad]` × capacity
- Empty slot: key == INT64_MIN.
- Key encoding: `composite_key = (int64_t)ps_partkey * 100003LL + ps_suppkey`
- Hash function: `h = (uint32_t)((uint64_t)composite_key * 0x9E3779B97F4A7C15ULL >> 32) & (capacity-1)`
- Usage: given l_partkey and l_suppkey: `key = (int64_t)l_partkey * 100003LL + l_suppkey;`
  `h = hash64(key, mask); while(table[h].key != key && table[h].key != INT64_MIN) h=(h+1)&mask;`
  Then read `ps_supplycost[table[h].row_idx]`.
- This query: primary path to retrieve ps_supplycost for each qualifying lineitem row.

### orderkey_hash (hash on o_orderkey) — orders
- File: orders/indexes/orderkey_hash.bin
- Layout: `[uint32_t capacity=33554432]` then `[int32_t key, uint32_t row_idx]` × capacity
- Empty slot: key == INT32_MIN. Hash function: `((uint32_t)key * 2654435761u) & (capacity-1)`.
- This query: for each qualifying lineitem row, look up l_orderkey to get o_orderdate (for year extraction).
  `row = lookup(l_orderkey); year = extract_year(o_orderdate[row]);`
