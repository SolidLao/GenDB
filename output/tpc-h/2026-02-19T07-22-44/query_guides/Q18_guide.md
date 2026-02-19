# Q18 Guide — Large Volume Customer

## Query
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS sum_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100;
```

## Column Reference

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes)
- Subquery: GROUP BY l_orderkey, HAVING SUM(l_quantity) > 300. Full scan required.
- Main query: join key `l_orderkey = o_orderkey`.
- Multi-value hash index available: `lineitem_orderkey_hash.bin`

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. No scaling needed.
- Subquery: `SUM(l_quantity) > 300` per l_orderkey. Range 1–50; need sum across all lineitems per order to exceed 300. Extremely selective — very few orders qualify (estimated ~10,000 orders out of 15M).
- Main query: `SUM(l_quantity)` aggregation for output.

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- Join key: `o_orderkey = l_orderkey`; output column.
- Used to probe the qualifying order set from the subquery.

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 rows × 4 bytes)
- Join key: `o_custkey = c_custkey`

### o_totalprice (DECIMAL, double)
- File: `orders/o_totalprice.bin` (15,000,000 rows × 8 bytes)
- Stored as native double. No scaling needed. Output column and ORDER BY key.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- **orders is physically sorted by o_orderdate ascending**
- Output column and ORDER BY key (secondary). No filter predicate — all dates are candidates.
- For output: convert back to date string using reverse epoch calculation, OR pass as-is and format in output layer.

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 rows × 4 bytes)
- Join key: `c_custkey = o_custkey`; output column.

### c_name (STRING, char[26], fixed-width)
- File: `customer/c_name.bin` (1,500,000 rows × 26 bytes = 39 MB)
- Each row: exactly 26 bytes, null-padded. String starts at `&c_name[row * 26]`, null-terminated within 26 bytes.
- Output column only — not used for filtering or joining.
- Access: once qualifying c_custkey row index is found, read `&c_name[row * 26]`.

## Table Stats
| Table    | Rows       | Role      | Sort Order   | Block Size |
|----------|------------|-----------|--------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate↑  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate↑ | 100,000    |
| customer | 1,500,000  | dimension | none         | 100,000    |

## Query Analysis
- **Subquery type**: IN (SELECT ... GROUP BY ... HAVING) — decorrelate as a materialization step
- **Join pattern**: lineitem (subquery probe) → orders (semi-join filter) → customer (lookup)
- **Filter selectivities** (from workload analysis):
  - Subquery: `SUM(l_quantity) > 300` per l_orderkey → ~10,000 qualifying orders out of 15M (0.067%). This is the dominant selectivity reducer.
  - Main query inherits this filter via `o_orderkey IN (subquery_set)`.
  - `c_custkey = o_custkey`: lookup join, no extra filtering.
  - Combined: ~10,000 orders qualify → 100 output rows after LIMIT.
- **Execution plan** (recommended):
  1. **Subquery phase** (full lineitem scan): Scan `l_orderkey.bin` and `l_quantity.bin` together. Build `unordered_map<int32_t, double>` accumulating SUM(l_quantity) per l_orderkey. After scan (~60M rows), collect all keys where sum > 300 into an `unordered_set<int32_t>` (qualifying_orderkeys, ~10K entries, tiny).
  2. **Orders scan**: Scan `o_orderkey.bin`, filter with `qualifying_orderkeys` set. For qualifying orders (~10K rows), collect (o_orderkey, o_custkey, o_totalprice, o_orderdate). Build `unordered_map<int32_t, CustKey>` (o_custkey for lookup).
  3. **Customer lookup**: For each qualifying order's o_custkey, do random access into `c_custkey.bin` (if custkeys are 1-based sequential, direct array index). Fetch `c_name` from `c_name.bin`.
  4. **Second lineitem scan**: For each qualifying o_orderkey (~10K), look up lineitem rows using `lineitem_orderkey_hash.bin` to get SUM(l_quantity). Alternatively: during phase 1 subquery scan, also accumulate the qualifying rows for the final SUM.
  5. **Top-100**: apply heap sort on o_totalprice DESC, o_orderdate ASC.
- **Optimization note**: The subquery is the bottleneck (full 60M-row scan). No zone map helps here since there is no date filter. Parallelize the subquery scan across 32 threads with local partial accumulators.
- **Aggregation**: GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice) → effectively one row per qualifying order (~10K groups). Use a hash map or sort on the ~10K qualifying result set. LIMIT 100 is trivial top-K on 10K rows.

## Indexes

### lineitem_orderkey_hash (multi-value hash on l_orderkey) — USEFUL FOR Q18 SECOND LINEITEM PASS
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout:
  - `[uint32_t capacity=33554432]` (4 bytes)
  - `[uint32_t num_positions=59986052]` (4 bytes)
  - `[capacity × {int32_t key, uint32_t offset, uint32_t count}]` (capacity × 12 bytes)
  - `[59986052 × uint32_t positions]` (59986052 × 4 bytes)
- Positions array byte offset: `4 + 4 + 33554432 × 12 = 402653192` bytes from file start
- Empty bucket marker: `key == INT32_MIN`
- Lookup: `bucket = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (capacity-1)`; linear probe
- Returns `(offset, count)`: lineitem row indices are `positions[offset..offset+count-1]`
- `row_offset` values in the positions array are ROW indices into lineitem column files.
- This query: after subquery finds qualifying o_orderkeys (~10K), use this index to look up all lineitem rows per order for the main-query SUM(l_quantity). Each lookup retrieves ~4 rows on average.

### orders_orderkey_hash (single-value hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t capacity=33554432]` then `[capacity × {int32_t key, uint32_t pos}]` (8 bytes/bucket)
- Empty bucket marker: `key == INT32_MIN`
- Lookup: same multiply-shift hash as above
- Returns `pos` = row index into orders column files
- This query: look up order details (o_totalprice, o_orderdate, o_custkey) for qualifying o_orderkeys from the subquery set.

### No zone maps are needed for Q18 (no date predicates on scan paths).
