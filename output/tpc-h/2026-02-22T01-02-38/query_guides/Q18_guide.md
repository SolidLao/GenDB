# Q18 Guide — Large Volume Customer

## SQL
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS sum_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate ASC
LIMIT 100;
```

## Column Reference

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 rows × 4 bytes)
- This query: join key `c_custkey = o_custkey`; also output column

### c_name (STRING, char[26], fixed-width, null-padded)
- File: `customer/c_name.bin` (1,500,000 rows × 26 bytes)
- Layout: flat array of 26-byte null-padded records; row i at byte offset `i * 26`
- Accessing row i: `const char* name = c_name_data + (size_t)i * 26;`
- This query: output column; decode to std::string via `std::string(name, strnlen(name, 26))`

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- This query: subquery filter `o_orderkey IN (qualifying_set)`; output column; join key with lineitem
- Hash index: `indexes/orders_orderkey_hash.bin`

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- orders is sorted by o_orderdate
- This query: output column; ORDER BY o_orderdate ASC (secondary sort key)
- Decode for output: `o_year = ...; o_month = ...; o_day = ...` (reverse epoch formula)
  - Or store raw int32_t and format at output time

### o_totalprice (DECIMAL, double)
- File: `orders/o_totalprice.bin` (15,000,000 rows × 8 bytes)
- Stored as native double. Values match SQL directly.
- This query: output column; primary ORDER BY key (DESC)

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes)
- This query: subquery `GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`; outer join key
- Hash index: `indexes/lineitem_orderkey_hash.bin` (multi-value: ~4 rows per key)

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values 1.0..50.0 match SQL directly.
- This query: subquery `SUM(l_quantity) > 300` per orderkey; outer `SUM(l_quantity)` as output

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate | 100,000    |
| customer | 1,500,000  | dimension | none        | 100,000    |

## Query Analysis
- **Subquery**: Decorrelate `IN (SELECT ... HAVING SUM > 300)` to a hash semi-join.
- **Step 1 — Subquery materialization** (single lineitem scan):
  - Scan `lineitem/l_orderkey.bin` and `lineitem/l_quantity.bin` in parallel across 64 threads
  - Build per-orderkey sum: `unordered_map<int32_t, double> qty_sum` (15M distinct orderkeys)
  - Qualifying orderkeys: `SUM(l_quantity) > 300` → estimated ~2,400 orderkeys (selectivity 0.016%)
  - Materialize as hash set `qualifying_orderkeys` (tiny: ~2,400 entries)
- **Step 2 — Filter orders** (15M rows):
  - Scan orders; for each row, check `o_orderkey` in qualifying hash set and `c_custkey` availability
  - ~2,400 qualifying orderkeys → ~2,400 qualifying orders (unique orderkeys in TPC-H)
  - Collect: (o_orderkey, o_custkey, o_orderdate, o_totalprice) for ~2,400 rows
- **Step 3 — Lookup customers** (2,400 probes):
  - For each qualifying order, look up `c_custkey → row_idx` via `customer_custkey_hash.bin`
  - Retrieve c_name from customer/c_name.bin at the found row_idx
- **Step 4 — Scan lineitem for qualifying orderkeys** (~2,400 orderkeys → ~9,600 lineitem rows):
  - Use `lineitem_orderkey_hash.bin` to find lineitem rows for each qualifying orderkey
  - Sum l_quantity per qualifying (orderkey) → output SUM(l_quantity)
- **Aggregation**: GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
  - Since each qualifying o_orderkey is unique, this is effectively identity (1 row per group)
  - ~2,400 output rows; sort by (o_totalprice DESC, o_orderdate ASC); emit top 100
- **Output**: LIMIT 100 — use partial sort (heap of 100) on ~2,400 rows

## Indexes

### lineitem_orderkey_hash (hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout: `[uint32_t ht_size=33554432][uint32_t num_positions=59986052]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- Hash function: `(uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (ht_size-1)`
- Slot struct: `struct HtSlot { int32_t key; uint32_t offset; uint32_t count; };` (12 bytes)
- Row positions: at byte offset `8 + ht_size*12`, array of `uint32_t row_idx[num_positions]`
- Lookup for key K: `h = hash32(K, mask); while (ht[h].key != K && ht[h].key != INT32_MIN) h = (h+1)&mask; if (ht[h].key == K) { positions from positions[ht[h].offset .. offset+count) }`
- This query: Step 1 uses scanning (not this index). Step 4: probe with ~2,400 qualifying orderkeys to get the ~9,600 lineitem row_idxs → access l_quantity.bin[row_idx].

### orders_orderkey_hash (hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t ht_size=33554432][uint32_t num_positions=15000000]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- This query: Step 2 alternative — probe ~2,400 qualifying orderkeys to find order row_idxs → access o_custkey, o_orderdate, o_totalprice. More efficient than scanning all 15M orders.

### customer_custkey_hash (hash on c_custkey)
- File: `indexes/customer_custkey_hash.bin`
- Layout: `[uint32_t ht_size=4194304][uint32_t num_positions=1500000]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- This query: Step 3 — probe with ~2,400 o_custkey values → retrieve customer row_idx → access c_name.bin[row_idx * 26 .. +26].

### orders_custkey_hash (hash on o_custkey)
- File: `indexes/orders_custkey_hash.bin`
- This query: NOT used (we access orders by orderkey, not custkey).
