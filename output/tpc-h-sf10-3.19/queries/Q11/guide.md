# Q11 Guide — Important Stock Identification

## Column Reference

### ps_partkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows)
- This query: GROUP BY ps_partkey → output

### ps_suppkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows)
- This query: `ps_suppkey = s_suppkey` → join key

### ps_supplycost (measure, double, raw)
- File: `storage/partsupp/ps_supplycost.bin` (8,000,000 rows)
- This query: `SUM(ps_supplycost * ps_availqty)` → value

### ps_availqty (measure, int32_t, raw)
- File: `storage/partsupp/ps_availqty.bin` (8,000,000 rows)
- This query: `SUM(ps_supplycost * ps_availqty)` → value

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: `ps_suppkey = s_suppkey` → join key

### s_nationkey (fk, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows)
- This query: `s_nationkey = n_nationkey` → filter to GERMANY

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)
- This query: `s_nationkey = n_nationkey` → join key

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin`, `storage/nation/n_name_data.bin`
- This query: `n_name = 'GERMANY'` → find GERMANY nationkey

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| partsupp | 8,000,000 | Fact | ps_partkey, ps_suppkey | 65536 |
| supplier | 100,000 | Dimension | s_suppkey | 65536 |
| nation | 25 | Dimension | n_nationkey | 65536 |

## Query Analysis
- **Nation filter**: n_name = 'GERMANY' → 1/25 nations → ~4% of suppliers (~4,000 suppliers).
- **Join**: partsupp → supplier → nation. Each supplier has ~80 partsupp rows.
- **Matching partsupp rows**: ~4,000 suppliers × 80 = ~320K rows.
- **Aggregation**: GROUP BY ps_partkey → ~300K groups (parts supplied by GERMANY suppliers).
- **HAVING**: SUM(ps_supplycost * ps_availqty) > total_sum * 0.0001 (threshold computed from same data).
- **Output**: ORDER BY value DESC.

## Indexes

### supplier_s_suppkey_lookup (dense_pk_array on s_suppkey)
- File: `storage/indexes/supplier_s_suppkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[suppkey] = row_id`, `-1` if missing.
- Usage: Not strictly needed — can build a set/bitset of GERMANY supplier suppkeys directly.

## Recommended Approach
1. Load nation (25 rows). Find GERMANY nationkey.
2. Scan supplier: collect all suppkeys where `s_nationkey == germany_nationkey`. Build bitset or bool array indexed by suppkey.
3. Scan partsupp: for each row, check if `ps_suppkey` is a GERMANY supplier.
   - If yes: compute `ps_supplycost * (double)ps_availqty`. Accumulate into hash_map[ps_partkey].
   - Also accumulate grand total for HAVING threshold.
4. Compute threshold = grand_total * 0.0001.
5. Filter hash_map entries where value > threshold.
6. Sort remaining entries by value DESC. Output (ps_partkey, value).
