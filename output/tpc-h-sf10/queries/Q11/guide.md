# Q11 Guide — Important Stock Identification

## Column Reference

### ps_partkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows of int32_t)
- This query: GROUP BY column, output

### ps_suppkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows of int32_t)
- This query: join key `ps_suppkey = s_suppkey`

### ps_supplycost (decimal, double, raw)
- File: `storage/partsupp/ps_supplycost.bin` (8,000,000 rows of double)
- This query: `SUM(ps_supplycost * ps_availqty)`

### ps_availqty (integer, int32_t, raw)
- File: `storage/partsupp/ps_availqty.bin` (8,000,000 rows of int32_t)
- This query: `ps_supplycost * ps_availqty` — cast to double for multiplication

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: join key `ps_suppkey = s_suppkey`

### s_nationkey (FK, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows of int32_t)
- This query: `s_nationkey = n_nationkey`

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: `n_name = 'GERMANY'` → load dict, find GERMANY code, get nationkey

## Table Stats
| Table    | Rows      | Role      | Sort Order              | Block Size |
|----------|-----------|-----------|-------------------------|------------|
| partsupp | 8,000,000 | fact      | (ps_partkey, ps_suppkey) | 65536      |
| supplier | 100,000   | dimension | s_suppkey               | 65536      |
| nation   | 25        | dimension | n_nationkey             | 65536      |

## Query Analysis
- **Pattern**: 3-way join, nation filter, group by partkey, HAVING with correlated subquery (same computation as main query)
- **Strategy**:
  1. Find GERMANY nationkey from nation dict
  2. Build set of German supplier suppkeys: scan supplier.s_nationkey, collect suppkeys where `s_nationkey == germany_nationkey`. ~100K/25 ≈ 4,000 suppliers.
  3. Scan partsupp: check `ps_suppkey` is in German suppliers set. For qualifying rows, compute `ps_supplycost * (double)ps_availqty`, accumulate SUM per ps_partkey. Also accumulate global total.
  4. HAVING threshold = `global_total * 0.0001`
  5. Filter groups where value > threshold, ORDER BY value DESC
- **Optimization**: German suppliers set is small (~4K). Use a bitset of size max_suppkey (100K bits = 12.5KB) for O(1) lookup.
- **Single pass**: Compute both per-partkey sums and global sum in one scan of partsupp.

## Indexes

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1
