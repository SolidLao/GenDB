# Q20 Guide — Potential Part Promotion

## Column Reference

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: `s_suppkey IN (subquery)` → filtered output

### s_name (text, varlen string)
- Files: `storage/supplier/s_name_offsets.bin`, `storage/supplier/s_name_data.bin`
- This query: output column, ORDER BY s_name

### s_address (text, varlen string)
- Files: `storage/supplier/s_address_offsets.bin`, `storage/supplier/s_address_data.bin`
- This query: output column

### s_nationkey (fk, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows)
- This query: `s_nationkey = n_nationkey` → filter to CANADA

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin`, `storage/nation/n_name_data.bin`
- This query: `n_name = 'CANADA'`

### p_partkey (pk, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows)
- This query: `p_name LIKE 'forest%'` → collect qualifying partkeys

### p_name (text, varlen string)
- Files: `storage/part/p_name_offsets.bin`, `storage/part/p_name_data.bin`
- This query: `p_name LIKE 'forest%'` → check if name starts with "forest"
- Selectivity: ~1.1% → ~22K parts

### ps_partkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows)
- This query: `ps_partkey IN (subquery of qualifying partkeys)`

### ps_suppkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows)
- This query: join key for correlated subquery

### ps_availqty (measure, int32_t, raw)
- File: `storage/partsupp/ps_availqty.bin` (8,000,000 rows)
- This query: `ps_availqty > 0.5 * SUM(l_quantity)` threshold

### l_partkey (fk, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows)
- This query: `l_partkey = ps_partkey` correlated subquery

### l_suppkey (fk, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows)
- This query: `l_suppkey = ps_suppkey` correlated subquery

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows)
- This query: `l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'`
  → C++ `l_shipdate[i] >= 8766 && l_shipdate[i] < 9131`

### l_quantity (measure, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows)
- This query: `0.5 * SUM(l_quantity)` in correlated subquery

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| supplier | 100,000 | Filtered output | s_suppkey | 65536 |
| nation | 25 | Dimension filter | n_nationkey | 65536 |
| part | 2,000,000 | Filtered dimension | p_partkey | 65536 |
| partsupp | 8,000,000 | Bridge | ps_partkey, ps_suppkey | 65536 |
| lineitem | 59,986,052 | Fact (subquery) | l_orderkey | 65536 |

## Query Analysis
- **Supplier filter**: CANADA nation → ~4% → ~4,000 suppliers.
- **Part filter**: p_name LIKE 'forest%' → ~1.1% → ~22K parts.
- **Correlated subquery**: For each (ps_partkey, ps_suppkey) where ps_partkey is qualifying, compute SUM(l_quantity) for lineitems matching (l_partkey, l_suppkey, date range). Check if ps_availqty > 0.5 * that sum.
- **Output**: s_name, s_address for qualifying suppliers, ORDER BY s_name.

## Indexes

### partsupp_ps_partkey_grouped (sorted_grouped on ps_partkey)
- File: `storage/indexes/partsupp_ps_partkey_grouped.bin`
- Layout: Header: `uint64_t num_entries`. Body: `uint32_t[num_entries*2]` interleaved `[start, count, ...]`.
- Usage: For each qualifying partkey, find its partsupp rows.

### partsupp_pk_hash (hash on (ps_partkey, ps_suppkey))
- File: `storage/indexes/partsupp_pk_hash.bin`
- Layout:
  - Header: `uint64_t capacity`
  - Body: `struct { int32_t partkey; int32_t suppkey; int32_t row_id; int32_t padding; } [capacity]`
  - Empty slot sentinel: `partkey == -1`
  - Hash function: `((uint64_t)(uint32_t)partkey * 2654435761ULL) ^ ((uint64_t)(uint32_t)suppkey * 40499ULL)`
  - Probe: `slot = hash & (capacity - 1)`, linear probing `slot = (slot + 1) & (capacity - 1)`
- Usage: Given (partkey, suppkey), lookup partsupp row_id → get ps_availqty.

### lineitem_l_shipdate_zonemap (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout: Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536). Body: `int32_t[num_blocks*2]` (min, max pairs).
- Usage: Skip blocks outside [8766, 9131) for lineitem scan.

### supplier_s_suppkey_lookup (dense_pk_array on s_suppkey)
- File: `storage/indexes/supplier_s_suppkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[suppkey] = row_id`, `-1` if missing.

## Recommended Approach
1. Load nation. Find CANADA nationkey. Scan supplier: collect CANADA suppkeys into set/bitset.
2. Scan part: collect partkeys where p_name starts with "forest". Build bitset.
3. **Precompute SUM(l_quantity)**: Scan lineitem (use shipdate zonemap for [8766,9131)). For qualifying rows where l_partkey is in forest-parts set:
   - Accumulate into hash_map[(l_partkey, l_suppkey)] → sum_qty.
4. Scan partsupp via grouped index for qualifying partkeys:
   - For each partsupp row with a CANADA suppkey:
     - Lookup sum_qty for (ps_partkey, ps_suppkey). If `ps_availqty > 0.5 * sum_qty` → suppkey qualifies.
5. Collect qualifying CANADA suppkeys. Lookup supplier rows for s_name, s_address.
6. Sort by s_name, output.
