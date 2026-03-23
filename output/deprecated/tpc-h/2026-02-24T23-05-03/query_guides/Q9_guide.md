# Q9 Guide — Product Type Profit Measure

## Query
```sql
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
  SELECT n_name AS nation,
         EXTRACT(YEAR FROM o_orderdate) AS o_year,
         l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity AS amount
  FROM part, supplier, lineitem, partsupp, orders, nation
  WHERE s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey  = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation ASC, o_year DESC;
```

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| part     | 2,000,000  | dimension | (none)     | 65536      |
| supplier | 100,000    | dimension | (none)     | 65536      |
| lineitem | 59,986,052 | fact      | (none)     | 65536      |
| partsupp | 8,000,000  | fact      | (none)     | 65536      |
| orders   | 15,000,000 | fact      | (none)     | 65536      |
| nation   | 25         | dimension | (none)     | 65536      |

## Column Reference

### p_partkey (key, int32_t — direct)
- File: `part/p_partkey.bin` (2,000,000 × 4 bytes = 8,000,000 bytes)
- This query: filter by p_name LIKE '%green%', collect qualifying p_partkey into bitmap

### p_name (string, int32_t — dict32-encoded)
- File: `part/p_name.bin` (2,000,000 × 4 bytes = 8,000,000 bytes)
- Dict file: `part/p_name_dict.txt` (~2,000,000 distinct values; int32_t codes, NOT int16_t)
- This query: `p_name LIKE '%green%'` → ~5.4% of parts qualify (~108,000 parts)
- **C2**: Load dict at runtime, scan for 'green' substring. NEVER hardcode codes.
- **P35**: Use mmap + memmem() for efficient substring scan over large dict:
  ```cpp
  // 1. mmap part/p_name_dict.txt as raw bytes
  size_t dict_sz;
  const char* dict_raw = (const char*)mmap_ro(dict_file, dict_sz);
  // 2. Scan dict lines, find lines containing "green"
  // 3. Build set of qualifying int32_t codes (line number = code)
  // 4. Build bitmap of qualifying p_partkey values:
  //    scan p_name.bin — if code is qualifying, set bit for that part's p_partkey
  std::vector<bool> part_bitmap(2000001, false);
  for (uint32_t row = 0; row < 2000000; row++) {
      if (code_qualifies[p_name_col[row]])
          part_bitmap[p_partkey_col[row]] = true;
  }
  ```
- p_partkey range ∈ [1, 2,000,000] → 2M-bit bitmap = 250KB (fits L2 cache, P32)

### s_suppkey (key, int32_t — direct)
- File: `supplier/s_suppkey.bin` (100,000 × 4 bytes = 400,000 bytes)
- This query: join key s_suppkey = l_suppkey; also join s_nationkey = n_nationkey

### s_nationkey (foreign key, int32_t — direct)
- File: `supplier/s_nationkey.bin` (100,000 × 4 bytes = 400,000 bytes)
- This query: join s_nationkey = n_nationkey → look up nation name for output

### l_orderkey (key, int32_t — direct)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes = 239,944,208 bytes)
- This query: join key o_orderkey = l_orderkey → probe orders_pk_hash

### l_partkey (foreign key, int32_t — direct)
- File: `lineitem/l_partkey.bin` (59,986,052 × 4 bytes = 239,944,208 bytes)
- This query: semi-join filter p_partkey bitmap; also join key for partsupp composite

### l_suppkey (foreign key, int32_t — direct)
- File: `lineitem/l_suppkey.bin` (59,986,052 × 4 bytes = 239,944,208 bytes)
- This query: join key s_suppkey = l_suppkey (probe supplier_pk_hash); also partsupp composite

### l_quantity (measure, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; values ∈ [1, 50]
- This query: used in amount = ep*(1-disc) - ps_supplycost*l_quantity

### l_extendedprice (measure, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; max individual value ~104,949
- This query: used in amount = ep*(1-disc) - ps_supplycost*qty (derived multi-column expression)
- **C35**: Use `long double` for per-row amount and accumulation:
  ```cpp
  long double amount = (long double)ep*(1.0L - disc) - (long double)ps_sc*qty;
  ```

### l_discount (measure, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; values ∈ [0.00, 0.10]
- This query: used in amount = ep*(1-disc) - ...

### ps_partkey (key, int32_t — direct)
- File: `partsupp/ps_partkey.bin` (8,000,000 × 4 bytes = 32,000,000 bytes)
- This query: composite join key (ps_partkey, ps_suppkey) = (l_partkey, l_suppkey)

### ps_suppkey (key, int32_t — direct)
- File: `partsupp/ps_suppkey.bin` (8,000,000 × 4 bytes = 32,000,000 bytes)
- This query: composite join key (ps_partkey, ps_suppkey) = (l_partkey, l_suppkey)

### ps_supplycost (measure, double)
- File: `partsupp/ps_supplycost.bin` (8,000,000 × 8 bytes = 64,000,000 bytes)
- Encoding: IEEE 754 double
- This query: used in amount = ep*(1-disc) - ps_supplycost*qty

### o_orderkey (key, int32_t — direct)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- This query: join key o_orderkey = l_orderkey (via orders_pk_hash)

### o_orderdate (date, int32_t — days_since_epoch_1970)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- Encoding: Howard Hinnant algorithm; values confirmed > 3000
- This query: `EXTRACT(YEAR FROM o_orderdate)` → GROUP BY o_year
- **C11/C1**: Use `date_utils.h`:
  ```cpp
  gendb::init_date_tables();  // C11: call once at top of main()
  int32_t o_year = gendb::extract_year(o_orderdate[row]);  // NOT manual arithmetic
  ```
- Distinct years in data: ~7 (1992–1998) → very low cardinality per nation

### n_nationkey (key, int32_t — direct)
- File: `nation/n_nationkey.bin` (25 × 4 bytes = 100 bytes)
- This query: join s_nationkey = n_nationkey → look up n_name

### n_name (string, int16_t — dict-encoded)
- File: `nation/n_name.bin` (25 × 2 bytes = 50 bytes)
- Dict file: `nation/n_name_dict.txt` (25 distinct nation names)
- This query: GROUP BY nation (as string); output column
- **C2**: Load dict at runtime:
  ```cpp
  std::vector<std::string> nation_dict;
  // read nation/n_name_dict.txt line by line into nation_dict
  // n_name[row] gives int16_t code → nation_dict[code] is the name string
  ```
- With only 25 rows, load entire nation table into arrays at startup

## Query Analysis

### Join Graph
```
nation(25) --[n_nationkey=s_nationkey]-->
  supplier(100K) --[s_suppkey=l_suppkey]-->
    lineitem(60M) <--[l_partkey=p_partkey]-- part(2M, filter p_name LIKE '%green%')
    lineitem(60M) <--[l_partkey,l_suppkey=ps_partkey,ps_suppkey]-- partsupp(8M)
    lineitem(60M) --[l_orderkey=o_orderkey]--> orders(15M)
```

### Recommended Execution Plan
1. **Nation table** (25 rows): load n_nationkey → n_name mapping into array
2. **Supplier table** (100K rows): load s_suppkey → s_nationkey mapping; probe nation for n_name
   → build `suppkey_to_nation[s_suppkey]` array (dense, 100K entries)
3. **Part filter** (2M rows): scan p_name dict for 'green' substring → build part_bitmap[p_partkey]
   → ~108K qualifying parts; bitmap = 250KB (fits L2)
4. **Scan lineitem** (60M rows — inner loop):
   - Filter: `part_bitmap[l_partkey[row]]` (bitmap semi-join, P32)
   - Probe `supplier_pk_hash` on `l_suppkey` → get nation_name (or code)
   - Probe `partsupp_pk_hash` on `(l_partkey, l_suppkey)` → get ps_supplycost
   - Probe `orders_pk_hash` on `l_orderkey` → get o_orderdate → extract year
   - Compute `amount = ep*(1-disc) - ps_sc*qty`; accumulate into (nation, year) group
5. **Aggregate**: 25 nations × 7 years = 175 groups max → tiny, direct array

### Selectivities
| Filter                | Selectivity | Qualifying Rows     |
|-----------------------|-------------|---------------------|
| p_name LIKE '%green%' | 5.4%        | ~108,000 parts      |
| lineitem after part bitmap | ~5.4%  | ~3,239,247 lineitem |

### Aggregation
- GROUP BY (nation, o_year) — at most 25 × 7 = 175 groups
- **C15**: key must include BOTH nation (string or code) AND year
- Use direct 2D array `agg[nation_idx][year_offset]` — no hash table needed
- year_offset = o_year - min_year (e.g., 1992 = offset 0)

## Indexes

### supplier_pk_hash (hash on s_suppkey)
- File: `supplier/indexes/supplier_pk_hash.bin`
- File size: 4 + 262,144 × 8 = 2,097,156 bytes (~2MB — fits L2 cache entirely)
- Layout: `[uint32_t cap][PKSlot[cap]]`
  ```cpp
  struct PKSlot { int32_t key; uint32_t row_idx; };
  ```
- Cap: 262,144 (2^18 = next_power_of_2(100,000 × 2))
- Mask: 262,143 (cap - 1)
- Empty sentinel: `key == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  static uint32_t pk_hash(int32_t key, uint32_t mask) {
      return ((uint32_t)key * 2654435761u) & mask;
  }
  ```
- Probe pattern (C24 bounded):
  ```cpp
  uint32_t h = ((uint32_t)l_suppkey * 2654435761u) & mask;
  for (uint32_t pr = 0; pr < cap; pr++) {
      uint32_t idx = (h + pr) & mask;
      if (ht[idx].key == INT32_MIN) break;        // empty — not found
      if (ht[idx].key == l_suppkey) {
          uint32_t srow = ht[idx].row_idx;
          int32_t nationkey = s_nationkey[srow];
          // look up nation string
          break;
      }
  }
  ```
- **Tip**: At 2MB, this index fits in L2/L3 — madvise WILLNEED at startup; cache-hot throughout scan

### partsupp_pk_hash (composite hash on ps_partkey, ps_suppkey)
- File: `partsupp/indexes/partsupp_pk_hash.bin`
- File size: 4 + 16,777,216 × 12 = 201,326,596 bytes (~192MB)
- Layout: `[uint32_t cap][PSSlot[cap]]`
  ```cpp
  struct PSSlot { int32_t partkey; int32_t suppkey; uint32_t row_idx; };
  ```
- Cap: 16,777,216 (2^24 = next_power_of_2(8,000,000 × 2))
- Mask: 16,777,215 (cap - 1)
- Empty sentinel: `partkey == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  static uint32_t ps_hash(int32_t pk, int32_t sk, uint32_t mask) {
      uint64_t k = ((uint64_t)(uint32_t)pk << 32) | (uint32_t)sk;
      return (uint32_t)((k * 11400714819323198485ull) >> 32) & mask;
  }
  ```
- Probe pattern (C24 bounded):
  ```cpp
  uint64_t k = ((uint64_t)(uint32_t)l_partkey << 32) | (uint32_t)l_suppkey;
  uint32_t h = (uint32_t)((k * 11400714819323198485ull) >> 32) & mask;
  for (uint32_t pr = 0; pr < cap; pr++) {
      uint32_t idx = (h + pr) & mask;
      if (ht[idx].partkey == INT32_MIN) break;    // empty — not found
      if (ht[idx].partkey == l_partkey && ht[idx].suppkey == l_suppkey) {
          uint32_t psrow = ht[idx].row_idx;
          double ps_sc = ps_supplycost[psrow];
          break;
      }
  }
  ```

### orders_pk_hash (hash on o_orderkey)
- File: `orders/indexes/orders_pk_hash.bin`
- File size: 4 + 33,554,432 × 8 = 268,435,460 bytes (~256MB)
- Layout: `[uint32_t cap][PKSlot[cap]]`
  ```cpp
  struct PKSlot { int32_t key; uint32_t row_idx; };
  ```
- Cap: 33,554,432 (2^25 = next_power_of_2(15,000,000 × 2))
- Mask: 33,554,431 (cap - 1)
- Empty sentinel: `key == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  return ((uint32_t)key * 2654435761u) & mask;
  ```
- Probe pattern (C24 bounded):
  ```cpp
  uint32_t h = ((uint32_t)l_orderkey * 2654435761u) & mask;
  for (uint32_t pr = 0; pr < cap; pr++) {
      uint32_t idx = (h + pr) & mask;
      if (ht[idx].key == INT32_MIN) break;
      if (ht[idx].key == l_orderkey) {
          uint32_t orow = ht[idx].row_idx;
          int32_t o_year = gendb::extract_year(o_orderdate[orow]); // C11/C7
          break;
      }
  }
  ```

### o_orderdate_zone_map — not directly used in Q9
- No range filter on o_orderdate in Q9 (EXTRACT YEAR is computed after join, not filtered)
- Do NOT use zone map for Q9 — it does not apply to EXTRACT operations

## Aggregation Layout (recommended)
```cpp
// 25 nations × ~7 years → 175 max groups
// nation_idx = n_nationkey (0–24); year_idx = o_year - 1992 (0–6 approx)
struct ProfitSlot {
    long double sum_profit;
    bool        used;
};
ProfitSlot agg[25][10];  // 10 year slots for safety (1992–2001)
// key: agg[nation_idx][o_year - BASE_YEAR]
// C15: GROUP BY (nation, o_year) — both dimensions required
```

## Date/Extract Summary
| SQL Expression                  | C++ Pattern                                          |
|---------------------------------|------------------------------------------------------|
| EXTRACT(YEAR FROM o_orderdate)  | `gendb::extract_year(o_orderdate[orow])`             |
| (no date range filter in Q9)    | —                                                    |
