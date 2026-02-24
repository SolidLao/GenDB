# Q18 Guide — Large Volume Customer

## Column Reference

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59986052 rows)
- This query (subquery): GROUP BY l_orderkey, compute SUM(l_quantity) per orderkey,
  keep only orderkeys where sum > 300. Result: a small set of "heavy" orderkeys.
- This query (outer): join `o_orderkey = l_orderkey` (second scan of lineitem for SUM(l_quantity) per output row).
- Subquery selectivity: ~0.02% of orderkeys qualify → ~3,000 qualifying orderkeys.

### l_quantity (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_quantity.bin (59986052 rows)
- Compression: byte_pack. code = round(double_value). code IS the integer quantity (1–50).
- Load lookup: lineitem/l_quantity_lookup.bin (256 doubles); `qty_lut[code]` = actual double value.
- This query (subquery): accumulate SUM(l_quantity) per l_orderkey.
  sum_qty per orderkey += qty_lut[qty_code]  (or just += qty_code since values are integers)
- This query (output): SUM(l_quantity) per output group — same accumulation on second pass.
- For the HAVING SUM > 300 check: since l_quantity is always an integer 1–50, accumulate as integer:
  `sum_int += quantity_code;` then check `sum_int > 300`.

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15000000 rows)
- This query: `o_orderkey IN (subquery result)` → probe the heavy-orderkey hash set.
  Only ~3,000 orderkeys qualify → build hash set of these keys.

### o_custkey (INTEGER, int32_t)
- File: orders/o_custkey.bin (15000000 rows)
- This query: join key `c_custkey = o_custkey`. For qualifying orders, look up customer name.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: orders/o_orderdate.bin (15000000 rows)
- Column is sorted ascending.
- This query: included in GROUP BY and output. Retrieve for qualifying orders.
- Output format: convert epoch days back to "YYYY-MM-DD" string at display time.
  Inverse formula: use civil calendar inverse of the ingestion formula.

### o_totalprice (DECIMAL, double)
- File: orders/o_totalprice.bin (15000000 rows)
- Stored as native double. Values match SQL directly.
- This query: included in GROUP BY and ORDER BY (sort by o_totalprice DESC). Output value.

### c_custkey (INTEGER, int32_t)
- File: customer/c_custkey.bin (1500000 rows)
- This query: join key `c_custkey = o_custkey`. Look up customer name for output.

### c_name (STRING, char[26], fixed-width 26 bytes per row)
- File: customer/c_name.bin (1500000 rows × 26 bytes = 39 MB)
- Each entry is a null-padded 26-byte field (VARCHAR(25) + null terminator).
- Read row i: `const char* name = c_name_ptr + i * 26;`
- This query: output column — retrieve c_name for each qualifying (customer, order) pair.
  Look up via custkey_hash index to get row_idx, then read c_name[row_idx * 26].

## Table Stats

| Table    | Rows     | Role      | Sort Order  | Block Size |
|----------|----------|-----------|-------------|------------|
| customer | 1500000  | dimension | none        | 100000     |
| orders   | 15000000 | fact      | o_orderdate | 100000     |
| lineitem | 59986052 | fact      | l_shipdate  | 100000     |

## Query Analysis

- **Join pattern**: customer ⋈ orders ⋈ lineitem with IN-subquery semi-join
  - Subquery: lineitem → group by l_orderkey → filter SUM(l_quantity) > 300 → heavy orderkey set
  - Outer: orders ⋈ lineitem on o_orderkey = l_orderkey, orders ⋈ customer on o_custkey = c_custkey
  - Semi-join: o_orderkey must be in heavy orderkey set
- **Filters**:
  1. Subquery (lineitem full scan): compute per-orderkey quantity sum → ~3,000 qualifying orderkeys (0.02%)
  2. `o_orderkey IN (heavy set)`: filters 15M orders to ~3,000 matching orders
  3. Join customer for c_name output
- **Recommended execution plan (decorrelated)**:
  1. Full scan lineitem: hash aggregate l_orderkey → SUM(qty_code) [integer accumulation]
     Keep only orderkeys where sum_qty > 300 → build hash set `heavy_keys` (~3K keys)
  2. Scan lineitem second pass (OR reuse aggregated data): for each `l_orderkey ∈ heavy_keys`,
     collect (l_orderkey → SUM(l_quantity)) — already computed in step 1.
  3. Scan orders: probe `heavy_keys` for each o_orderkey → ~3,000 matches.
     For each match: probe custkey_hash with o_custkey → get c_name, c_custkey.
     Build result tuples: (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_l_quantity).
  4. Sort by o_totalprice DESC, o_orderdate ASC → take top 100.
- **Subquery optimization**: decorrelate the IN-subquery into a hash semi-join.
  Step 1 (full lineitem scan for aggregation) is the main cost — 60M rows × (l_orderkey + l_quantity).
  Byte-packed l_quantity (uint8_t) reduces memory bandwidth for this step.
- **Output**: 100 rows max (LIMIT 100), ORDER BY o_totalprice DESC, o_orderdate ASC.
- **Aggregation**: ~3,000 groups in outer query (small), 1 group per qualifying order.
- **Optimization hint**: Since the subquery result is tiny (~3K keys), the semi-join filter on orders
  is extremely selective. After computing the heavy key set, orders and customer processing is cheap.
  Use a top-K heap (K=100) to avoid full sort of the ~3K result rows.

## Indexes

### custkey_hash (hash on c_custkey) — customer
- File: customer/indexes/custkey_hash.bin
- Layout: `[uint32_t capacity=4194304]` then `[int32_t key, uint32_t row_idx]` × capacity
- Empty slot: key == INT32_MIN. Hash function: `((uint32_t)key * 2654435761u) & (capacity-1)`.
- Usage: given o_custkey from a qualifying order, find customer row:
  `h = hash(o_custkey, mask); while(table[h].key != o_custkey && table[h].key != INT32_MIN) h=(h+1)&mask;`
  Then read `c_name[table[h].row_idx * 26]` and `c_custkey[table[h].row_idx]`.
- This query: ~3,000 lookups (one per qualifying order) — negligible cost.

### orderkey_hash (hash on o_orderkey) — orders
- File: orders/indexes/orderkey_hash.bin
- Layout: `[uint32_t capacity=33554432]` then `[int32_t key, uint32_t row_idx]` × capacity
- Empty slot: key == INT32_MIN. Hash function: `((uint32_t)key * 2654435761u) & (capacity-1)`.
- This query: optional — you can scan orders sequentially and check each o_orderkey against the
  heavy_keys hash set (~3K keys), which is very fast. The persistent index is useful if doing
  random-access lookups from the lineitem side.
- row_offset is ROW index, not byte offset.

### orderdate_zonemap (zone_map on o_orderdate) — orders
- File: orders/indexes/orderdate_zonemap.bin
- Layout: `[uint32_t num_blocks=150]` then `[int32_t min, int32_t max, uint32_t start_row]` × 150
- This query: no date filter on orders in Q18 — zone map not needed for filtering.
  Useful only if doing range-based partial scan (not applicable here).
