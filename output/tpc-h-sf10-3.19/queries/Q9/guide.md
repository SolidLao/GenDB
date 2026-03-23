# Q9 Guide — Product Type Profit Measure

## Column Reference

### p_partkey (pk, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2000000 rows × 4 bytes)
- This query: JOIN key `p_partkey = l_partkey`, filter driver

### p_name (text, varlen string)
- Files: `storage/part/p_name_offsets.bin` (2000001 × 4 bytes), `storage/part/p_name_data.bin`
- Varlen format: offsets are `uint32_t[N+1]`, data is raw chars. String `i` spans `[offsets[i], offsets[i+1])`.
- This query: `p_name LIKE '%green%'` → C++ substring search: `strstr()` or `memmem()` for "green"
- Selectivity: ~0.048 → ~96K qualifying parts

### l_partkey (fk, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59986052 rows × 4 bytes)
- This query: JOIN key `p_partkey = l_partkey`

### l_suppkey (fk, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59986052 rows × 4 bytes)
- This query: JOIN keys `s_suppkey = l_suppkey` and `ps_suppkey = l_suppkey`

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59986052 rows × 4 bytes)
- This query: JOIN key `o_orderkey = l_orderkey`

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59986052 rows × 8 bytes)
- This query: `l_extendedprice * (1 - l_discount)` in profit amount

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59986052 rows × 8 bytes)
- This query: `l_extendedprice * (1 - l_discount)` in profit amount

### l_quantity (measure, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59986052 rows × 8 bytes)
- This query: `ps_supplycost * l_quantity` in profit amount

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100000 rows × 4 bytes)
- This query: JOIN key `s_suppkey = l_suppkey`

### s_nationkey (fk, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100000 rows × 4 bytes)
- This query: JOIN key `s_nationkey = n_nationkey` — used to get nation name

### ps_partkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8000000 rows × 4 bytes)
- This query: JOIN key `ps_partkey = l_partkey`

### ps_suppkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8000000 rows × 4 bytes)
- This query: JOIN key `ps_suppkey = l_suppkey`

### ps_supplycost (measure, double, raw)
- File: `storage/partsupp/ps_supplycost.bin` (8000000 rows × 8 bytes)
- This query: `ps_supplycost * l_quantity` in profit amount

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15000000 rows × 4 bytes)
- This query: JOIN key `o_orderkey = l_orderkey`

### o_orderdate (date, int32_t, days_since_epoch_1970)
- File: `storage/orders/o_orderdate.bin` (15000000 rows × 4 bytes)
- This query: `EXTRACT(YEAR FROM o_orderdate)` — extract year from days-since-epoch
- Year extraction from days_since_epoch: reverse the Hinnant algorithm, or precompute year lookup

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows × 4 bytes)
- This query: JOIN key `s_nationkey = n_nationkey`

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin` (26 × 4 bytes), `storage/nation/n_name_data.bin`
- This query: GROUP BY key (as `nation`), ORDER BY key
- Only 25 values — load entirely into a `std::string[25]` array indexed by n_nationkey

## Table Stats
| Table    | Rows       | Role      | Sort Order           | Block Size |
|----------|------------|-----------|----------------------|------------|
| part     | 2,000,000  | dimension | p_partkey            | 65536      |
| supplier | 100,000    | dimension | s_suppkey            | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey           | 65536      |
| partsupp | 8,000,000  | bridge    | ps_partkey,ps_suppkey| 65536      |
| orders   | 15,000,000 | fact      | o_orderkey           | 65536      |
| nation   | 25         | dimension | n_nationkey          | 65536      |

## Query Analysis

### Join Pattern (6-way join)
1. part → lineitem: `p_partkey = l_partkey` (filter on part drives)
2. supplier → lineitem: `s_suppkey = l_suppkey`
3. partsupp → lineitem: `ps_partkey = l_partkey AND ps_suppkey = l_suppkey`
4. orders → lineitem: `o_orderkey = l_orderkey`
5. nation → supplier: `n_nationkey = s_nationkey`

### Filter
- `p_name LIKE '%green%'`: sel ~0.048 → ~96K qualifying parts

### Aggregation
- GROUP BY (nation, o_year): ~175 groups (25 nations × 7 years)
- SUM(amount) where `amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`

### Output
- ORDER BY nation ASC, o_year DESC
- ~175 result rows

## Indexes

### partsupp_pk_hash (hash on ps_partkey, ps_suppkey)
- File: `storage/indexes/partsupp_pk_hash.bin`
- Layout:
  - Header: `uint64_t capacity`
  - Body: `struct PSHashEntry { int32_t partkey; int32_t suppkey; int32_t row_id; int32_t padding; } [capacity]`
  - Entry size: 16 bytes. Total: 16 header + capacity × 16 body.
  - Empty slot sentinel: `partkey == -1`
  - Capacity: next power of 2 above `8000000 * 2` = `16777216` (2^24)
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t h = ((uint64_t)(uint32_t)partkey * 2654435761ULL) ^ ((uint64_t)(uint32_t)suppkey * 40499ULL);
  uint64_t slot = h & mask;  // mask = capacity - 1
  ```
- Lookup pattern:
  ```cpp
  uint64_t h = ((uint64_t)(uint32_t)pk * 2654435761ULL) ^ ((uint64_t)(uint32_t)sk * 40499ULL);
  uint64_t slot = h & mask;
  while (table[slot].partkey != -1) {
      if (table[slot].partkey == pk && table[slot].suppkey == sk)
          return table[slot].row_id;  // found
      slot = (slot + 1) & mask;  // linear probing
  }
  return -1;  // not found
  ```

### orders_o_orderkey_lookup (dense_pk_array on o_orderkey)
- File: `storage/indexes/orders_o_orderkey_lookup.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_orderkey + 1)
  - Body: `int32_t[num_entries]` where `arr[orderkey] = row_id`, `-1` if missing
- Usage: Given `l_orderkey`, get orders row: `int32_t orow = order_lookup[l_orderkey]`

## Recommended Execution Strategy
1. **Load small tables**: nation (25 rows) into array indexed by nationkey. Supplier (100K) — build `s_nationkey` array indexed by s_suppkey (dense, suppkey is 1-based up to 100K).
2. **Filter parts**: Scan p_name varlen, find parts where name contains "green". Build a bitset/set of qualifying p_partkeys (~96K).
3. **Scan lineitem**: For each lineitem row:
   - Check if `l_partkey` is in the qualifying part set. If not, skip.
   - Look up supplier's nation: `nation_name = n_name[s_nationkey[l_suppkey]]` (supplier is dense by suppkey)
   - Look up partsupp: use `partsupp_pk_hash` with (l_partkey, l_suppkey) → get ps_supplycost
   - Look up order year: use `orders_o_orderkey_lookup[l_orderkey]` → get o_orderdate, extract year
   - Compute: `amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`
   - Aggregate into (nation_idx, year) → sum_profit
4. **Output**: Sort 175 groups by nation ASC, o_year DESC
