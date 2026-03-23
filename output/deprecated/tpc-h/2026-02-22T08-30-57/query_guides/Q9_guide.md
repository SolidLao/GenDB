# Q9 Guide — Product Type Profit Measure

## SQL
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

## Column Reference

### p_partkey (INTEGER, int32_t)
- File: `part/p_partkey.bin` (2,000,000 rows × 4 bytes)
- This query: join key; filter on `p_name LIKE '%green%'` first, collect qualifying partkeys

### p_name (STRING, char[56], fixed-width, null-padded)
- File: `part/p_name.bin` (2,000,000 rows × 56 bytes)
- Layout: flat array of 56-byte null-padded records; row i at byte offset `i * 56`
- Accessing row i: `const char* name = p_name_data + i * 56;`
- This query: `p_name LIKE '%green%'` → `strstr(name, "green") != nullptr` (or memchr-based scan)
- Selectivity: ~5.3% of 2M parts → ~106,000 qualifying parts (keys collected into hash set)

### l_partkey (INTEGER, int32_t)
- File: `lineitem/l_partkey.bin` (59,986,052 rows × 4 bytes)
- This query: join key with part and partsupp

### l_suppkey (INTEGER, int32_t)
- File: `lineitem/l_suppkey.bin` (59,986,052 rows × 4 bytes)
- This query: join key with supplier and partsupp

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes)
- This query: join key with orders

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values match SQL directly.
- This query: `l_extendedprice * (1 - l_discount)` — part of amount formula

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values 0.00..0.10 match SQL directly.
- This query: `(1 - l_discount)` multiplied with l_extendedprice

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values 1.0..50.0 match SQL directly.
- This query: `ps_supplycost * l_quantity` — part of amount formula

### ps_partkey (INTEGER, int32_t)
- File: `partsupp/ps_partkey.bin` (8,000,000 rows × 4 bytes)
- This query: join key (with l_partkey)

### ps_suppkey (INTEGER, int32_t)
- File: `partsupp/ps_suppkey.bin` (8,000,000 rows × 4 bytes)
- This query: join key (with l_suppkey)

### ps_supplycost (DECIMAL, double)
- File: `partsupp/ps_supplycost.bin` (8,000,000 rows × 8 bytes)
- Stored as native double. Values match SQL directly.
- This query: `ps_supplycost * l_quantity` — part of amount formula

### s_suppkey (INTEGER, int32_t)
- File: `supplier/s_suppkey.bin` (100,000 rows × 4 bytes)
- This query: join key; all 100K suppliers participate (no filter on supplier)

### s_nationkey (INTEGER, int32_t)
- File: `supplier/s_nationkey.bin` (100,000 rows × 4 bytes)
- This query: join key with nation; used to look up n_name for group key

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key with lineitem

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- orders is sorted by o_orderdate
- This query: `EXTRACT(YEAR FROM o_orderdate)` as group-by key
- Year extraction: `year = 1970 + approx_division`, but use proper formula:
  - Since data spans 1992-1998, precompute year boundaries as epoch days:
    - 1992: 8035, 1993: 8400, 1994: 8766, 1995: 9131, 1996: 9496, 1997: 9862, 1998: 10227, 1999: 10593
  - Given raw epoch day d: scan boundaries to find year, or use `date_epoch_to_year(d)`
  - Simpler: `date_to_epoch(y+1, 1, 1) - date_to_epoch(y, 1, 1)` to bracket

### n_nationkey (INTEGER, int32_t)
- File: `nation/n_nationkey.bin` (25 rows × 4 bytes)
- This query: join key with s_nationkey

### n_name (STRING, char[26], fixed-width, null-padded)
- File: `nation/n_name.bin` (25 rows × 26 bytes)
- Layout: flat array of 26-byte null-padded records; row i at byte offset `i * 26`
- This query: output as `nation` group key. Since only 25 nations, load all into array at startup:
  `nation_name[n_nationkey[i]] = std::string(n_name + i*26, strnlen(n_name+i*26, 26))`
  Then look up by `s_nationkey` → O(1) direct array access

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate | 100,000    |
| customer | —          | —         | not used    | —          |
| part     | 2,000,000  | dimension | none        | 100,000    |
| partsupp | 8,000,000  | bridge    | none        | 100,000    |
| supplier | 100,000    | dimension | none        | 100,000    |
| nation   | 25         | dimension | none        | —          |

## Query Analysis
- **Filter**: `p_name LIKE '%green%'` → 5.3% of 2M parts → 106K qualifying partkeys (hash set)
- **Join order (recommended)**:
  1. Scan part (2M) → filter on p_name contains 'green' → hash set of 106K partkeys
  2. Build supplier array: load all 100K rows into suppkey→nationkey array (direct lookup)
  3. Build nation array: load all 25 rows into nationkey→n_name array (direct lookup)
  4. Build partsupp hash: scan 8M partsupp rows, keep only where `ps_partkey` in part-hashset → ~424K qualifying rows. Build in-memory hash on composite key `(ps_partkey << 32) | ps_suppkey`.
     - OR: use `indexes/partsupp_keys_hash.bin` to probe by (partkey, suppkey)
  5. Scan lineitem (60M rows): for each row, check `l_partkey` in part hash set (~5.3% pass → 3.18M), then look up (l_partkey, l_suppkey) in partsupp → get ps_supplycost, then look up o_orderkey in orders → get o_orderdate → extract year, look up s_nationkey via supplier array → get n_name. Compute amount, aggregate into 175-group hash table.
- **Aggregation**: 25 nations × 7 years = 175 groups. Hash aggregate or array of 175 slots.
- **Output**: 175 rows ordered by (nation ASC, o_year DESC). Full sort is trivial.
- **Key bottleneck**: lineitem full scan (60M rows), with ~5.3% passing the partkey filter (3.18M qualifying). After that, multi-level hash lookups per row.

## Indexes

### part_partkey_hash (hash on p_partkey)
- File: `indexes/part_partkey_hash.bin`
- Layout: `[uint32_t ht_size=4194304][uint32_t num_positions=2000000]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- Hash function: `(uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (ht_size-1)`
- This query: after filtering part by p_name LIKE '%green%', NOT needed for part itself (we do a scan). Instead build an in-memory bitset/hash set of the 106K qualifying partkeys for lineitem probe.

### partsupp_keys_hash (composite hash on ps_partkey, ps_suppkey)
- File: `indexes/partsupp_keys_hash.bin`
- Layout: `[uint32_t ht_size=16777216][uint32_t num_positions=8000000]` then `ht_size × {int64_t key, uint32_t offset, uint32_t count}` (16 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT64_MIN`
- Composite key encoding: `int64_t key = ((int64_t)ps_partkey << 32) | (uint32_t)ps_suppkey`
- Hash function: `uint64_t h = key * 0x9E3779B97F4A7C15ULL; h ^= h >> 33; return h & mask`
- This query: for qualifying lineitem rows (partkey in green-set), probe with `(l_partkey << 32) | (uint32_t)l_suppkey` to get the ps_supplycost row index. Then access `partsupp/ps_supplycost.bin[row_idx]`.

### supplier_suppkey_hash (hash on s_suppkey)
- File: `indexes/supplier_suppkey_hash.bin`
- Layout: `[uint32_t ht_size=262144][uint32_t num_positions=100000]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- This query: supplier is small (100K rows). Better to load all into a direct array `suppkey_to_nationkey[s_suppkey]` than use the hash index (TPC-H suppkeys are 1..100000).

### orders_orderkey_hash (hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t ht_size=33554432][uint32_t num_positions=15000000]` then `ht_size × {int32_t key, uint32_t offset, uint32_t count}` (12 bytes/slot) then `num_positions × uint32_t row_idx`
- Empty slot sentinel: `key == INT32_MIN`
- This query: probe with l_orderkey to get o_orderdate row index → extract year. Alternative: build in-memory array `orderkey_to_orderdate[o_orderkey]` if memory allows (15M × 4 = 60MB).

### lineitem_orderkey_hash (hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- This query: NOT used (we scan lineitem rather than looking up by orderkey).
