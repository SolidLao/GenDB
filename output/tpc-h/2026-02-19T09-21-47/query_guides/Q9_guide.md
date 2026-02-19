# Q9 Guide — Product Type Profit Measure

## Column Reference

### p_partkey (INTEGER, int32_t)
- File: `part/p_partkey.bin` (2,000,000 rows)
- This query: join key `p_partkey = l_partkey`. After LIKE filter, qualifying partkeys form the probe set for lineitem/partsupp.

### p_name (STRING, varstring)
- Files: `part/p_name_offsets.bin` (uint32_t[2,000,001]) + `part/p_name_data.bin` (raw bytes)
- Access row `i`: `start = offsets[i]`, `len = offsets[i+1] - offsets[i]`, string = `data + start`
- This query: `p_name LIKE '%green%'`
  - C++ filter: use `strstr(str_ptr, "green") != nullptr` (substring search, no anchors)
  - Selectivity: ~5% of parts qualify → ~100K qualifying p_partkey values

### s_suppkey (INTEGER, int32_t)
- File: `supplier/s_suppkey.bin` (100,000 rows)
- This query: join key `l_suppkey = s_suppkey` and `ps_suppkey = s_suppkey`.

### s_nationkey (INTEGER, int32_t)
- File: `supplier/s_nationkey.bin` (100,000 rows)
- This query: join key `s_nationkey = n_nationkey` to get nation name.

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: join key `l_orderkey = o_orderkey` to get o_orderdate for year extraction.

### l_partkey (INTEGER, int32_t)
- File: `lineitem/l_partkey.bin` (59,986,052 rows)
- This query: join key `l_partkey = p_partkey` — filter to only "green" parts.

### l_suppkey (INTEGER, int32_t)
- File: `lineitem/l_suppkey.bin` (59,986,052 rows)
- This query: join key `l_suppkey = s_suppkey` and `ps_suppkey = l_suppkey`.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows)
- Stored as native double. No scaling.
- This query: `l_extendedprice * (1 - l_discount)` in amount calculation.

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows)
- Stored as native double.
- This query: `l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows)
- Stored as native double.
- This query: `ps_supplycost * l_quantity` in amount calculation.

### ps_partkey (INTEGER, int32_t)
- File: `partsupp/ps_partkey.bin` (8,000,000 rows)
- This query: join key `ps_partkey = l_partkey`.

### ps_suppkey (INTEGER, int32_t)
- File: `partsupp/ps_suppkey.bin` (8,000,000 rows)
- This query: join key `ps_suppkey = l_suppkey`.

### ps_supplycost (DECIMAL, double)
- File: `partsupp/ps_supplycost.bin` (8,000,000 rows)
- Stored as native double. No scaling.
- This query: `ps_supplycost * l_quantity` in amount formula.

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows)
- This query: join key `o_orderkey = l_orderkey`.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows, sorted ascending)
- This query: `EXTRACT(YEAR FROM o_orderdate)` → compute year from epoch days.
  - Algorithm: binary search on precomputed `YEAR_DAYS[]` array.
  - Or: linear scan since years span 1992-1998 (only 7 values to check).
  ```cpp
  int extract_year(int32_t epoch_days) {
      // YEAR_DAYS[y-1970] = days from epoch to Jan 1 of year y
      for (int y = 1998; y >= 1992; y--)
          if (epoch_days >= YEAR_DAYS[y-1970]) return y;
      return 1992;
  }
  ```

### n_nationkey (INTEGER, int32_t)
- File: `nation/n_nationkey.bin` (25 rows)
- This query: join key `n_nationkey = s_nationkey`.

### n_name (STRING, int8_t, dictionary-encoded)
- File: `nation/n_name.bin` (25 rows, 1 byte/row)
- Dictionary: `nation/n_name_dict.txt` — 25 nation names sorted alphabetically (0=ALGERIA, ..., 24=VIETNAM)
- This query: `n_name AS nation` — used in GROUP BY and output. Decode code → string via dict.
  - Lookup: `dict[n_name_col[row]]`

---

## Table Stats
| Table    | Rows       | Role      | Sort Order    | Block Size |
|----------|------------|-----------|---------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate ↑  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate ↑ | 100,000    |
| part     | 2,000,000  | dimension | none          | 100,000    |
| supplier | 100,000    | dimension | none          | 100,000    |
| partsupp | 8,000,000  | bridge    | none          | 100,000    |
| nation   | 25         | dimension | none          | 100,000    |

---

## Query Analysis
- **Join pattern**: 6-way join: `part ⨝ lineitem ⨝ partsupp ⨝ supplier ⨝ orders ⨝ nation`
- **Critical filter**: `p_name LIKE '%green%'` — **5% selectivity** → only ~100K qualifying parts.
  Apply this first. Build a hash set of qualifying `p_partkey` values.
- **Recommended join order** (minimize intermediate results):
  1. Scan `part` (2M rows), filter p_name LIKE '%green%' → ~100K qualifying partkeys → build hash set
  2. Scan `lineitem` (60M rows), filter `l_partkey IN qualifying_partkeys` → ~3M rows (5% of 60M)
     - For each qualifying lineitem: look up `ps_supplycost` via partsupp hash index
     - Look up `s_nationkey` via supplier hash index
     - Look up `o_orderdate` via orders hash index
     - Get year from epoch days
     - Get nation name (25 rows — fit in array, direct lookup by n_nationkey)
  3. Compute `amount = l_extendedprice * (1-l_discount) - ps_supplycost * l_quantity`
  4. Aggregate: GROUP BY (nation_name, year) — ~150 groups (25 nations × 7 years)
- **Subquery**: None. Flat join with WHERE clauses.
- **Aggregation**: `SUM(amount)` per (nation, year) group. Very few groups (≤175).
- **Output**: ORDER BY nation ASC, o_year DESC — sort 150 groups.
- **No date range filter** on orders (all years contribute).
- **Nation table** (25 rows): do NOT use hash index (< 10K rows). Load into array indexed by n_nationkey.

---

## Indexes

### p_partkey_hash (hash on p_partkey → row_pos in part)
- File: `part/indexes/p_partkey_hash.bin`
- Layout: `[uint32_t capacity=4194304][uint32_t num_entries=2000000][HashSlot × capacity]`
- `HashSlot = {int32_t key, uint32_t row_pos}` = 8 bytes. Empty: `key == INT32_MIN`.
- Hash: `h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64-22)) & (cap-1)`
- This query: NOT needed for the primary access path (we build a runtime hash set of qualifying partkeys).

### s_suppkey_hash (hash on s_suppkey → row_pos in supplier)
- File: `supplier/indexes/s_suppkey_hash.bin`
- Layout: `[uint32_t capacity=262144][uint32_t num_entries=100000][HashSlot × capacity]`
- `HashSlot = {int32_t key, uint32_t row_pos}` = 8 bytes. Empty: `key == INT32_MIN`.
- Hash: `h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> (64-18)) & (cap-1)`
- This query: given `l_suppkey`, look up s_nationkey. Probe this index for each qualifying lineitem.

### ps_key_hash (hash on (ps_partkey, ps_suppkey) → row_pos in partsupp)
- File: `partsupp/indexes/ps_key_hash.bin`
- Layout: `[uint32_t capacity=16777216][uint32_t num_entries=8000000][HashSlot64 × capacity]`
- `HashSlot64 = {int64_t key, uint32_t row_pos, uint32_t _pad}` = 16 bytes. Empty: `key == INT64_MIN`.
- Composite key: `int64_t k = ((int64_t)ps_partkey << 32) | (uint32_t)ps_suppkey`
- Hash: `h = (uint32_t)(((uint64_t)k * 0x9E3779B97F4A7C15ULL) >> (64-24)) & (cap-1)`
- This query: given `(l_partkey, l_suppkey)` from lineitem, form composite key and look up ps_supplycost.

### o_orderkey_hash (hash on o_orderkey → row_pos in orders)
- File: `orders/indexes/o_orderkey_hash.bin`
- Layout: `[uint32_t capacity=33554432][uint32_t num_entries=15000000][HashSlot × capacity]`
- `HashSlot = {int32_t key, uint32_t row_pos}` = 8 bytes.
- This query: given `l_orderkey`, look up o_orderdate (for year extraction).

### l_shipdate_zonemap, o_orderdate_zonemap
- Not useful for Q9 (no date range filter).

---

## Year Extraction from Epoch Days
```cpp
// For o_orderdate (int32_t epoch days), extract year:
static const int32_t YEAR_DAYS[] = {
    0,365,730,1096,1461,1826,2191,2557,2922,3287,3652,4018,4383,4748,5113,
    5479,5844,6209,6574,6940,7305,7670,8035,8401,8766,9131,9496,9862,10227,10592,10957
};
int extract_year(int32_t d) {
    // TPC-H orders span 1992-1998. YEAR_DAYS[22]=8035 (1992), YEAR_DAYS[28]=10227 (1998)
    for (int y = 28; y >= 22; y--)
        if (d >= YEAR_DAYS[y]) return 1970 + y;
    return 1992;
}
```
