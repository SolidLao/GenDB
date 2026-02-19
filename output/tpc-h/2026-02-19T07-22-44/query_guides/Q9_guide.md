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
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderkey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC;
```

## Column Reference

### p_name (STRING, char[56], fixed-width)
- File: `part/p_name.bin` (2,000,000 rows × 56 bytes = 112 MB)
- Each row: exactly 56 bytes, null-padded. String starts at `&p_name[row * 56]`, null-terminated within 56 bytes.
- This query: `p_name LIKE '%green%'` → C++ `strstr(&p_name[row*56], "green") != nullptr`
- No index available for LIKE '%green%'. Must linearly scan all 2M part rows.
- Selectivity: 4.8% → ~96,000 qualifying parts

### p_partkey (INTEGER, int32_t)
- File: `part/p_partkey.bin` (2,000,000 rows × 4 bytes)
- Join key: `p_partkey = l_partkey` and `p_partkey = ps_partkey`
- After filtering on p_name LIKE '%green%', build a hash set of qualifying p_partkey values.

### l_partkey (INTEGER, int32_t)
- File: `lineitem/l_partkey.bin` (59,986,052 rows × 4 bytes)
- Join key: `l_partkey = p_partkey`

### l_suppkey (INTEGER, int32_t)
- File: `lineitem/l_suppkey.bin` (59,986,052 rows × 4 bytes)
- Join key: `l_suppkey = s_suppkey` and `l_suppkey = ps_suppkey`

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes)
- Join key: `l_orderkey = o_orderkey`

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Used in: `ps_supplycost * l_quantity`

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Used in: `l_extendedprice * (1 - l_discount)`

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Used in: `l_extendedprice * (1 - l_discount)`

### ps_partkey (INTEGER, int32_t)
- File: `partsupp/ps_partkey.bin` (8,000,000 rows × 4 bytes)
- Join key: `ps_partkey = l_partkey` (composite PK join)

### ps_suppkey (INTEGER, int32_t)
- File: `partsupp/ps_suppkey.bin` (8,000,000 rows × 4 bytes)
- Join key: `ps_suppkey = l_suppkey` (composite PK join)

### ps_supplycost (DECIMAL, double)
- File: `partsupp/ps_supplycost.bin` (8,000,000 rows × 8 bytes)
- Stored as native double. Used in: `ps_supplycost * l_quantity`

### s_suppkey (INTEGER, int32_t)
- File: `supplier/s_suppkey.bin` (100,000 rows × 4 bytes)
- Join key: `s_suppkey = l_suppkey`

### s_nationkey (INTEGER, int32_t)
- File: `supplier/s_nationkey.bin` (100,000 rows × 4 bytes)
- Join key: `s_nationkey = n_nationkey`

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- Join key: `o_orderkey = l_orderkey`

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- **orders is sorted by o_orderdate ascending**
- This query: `EXTRACT(YEAR FROM o_orderdate)` — extract calendar year from epoch days
  - Reconstruct year: find largest y such that `YEAR_START[y-1970] <= raw`; use binary search or table lookup
  - YEAR_START table (index = year-1970): `{0,365,730,1096,1461,1826,2191,2557,2922,3287,3652,4018,4383,4748,5113,5479,5844,6209,6574,6940,7305,7670,8035,8401,8766,9131,9496,9862,10227,10592,...}`
  - TPC-H orders span 1992–1998; years index 22–28 in YEAR_START
  - Fast extraction: `int year = 1992; while (YEAR_START[year+1-1970] <= raw) year++; return year;` (max 7 iterations)

### n_nationkey (INTEGER, int32_t)
- File: `nation/n_nationkey.bin` (25 rows × 4 bytes)
- Join key: `n_nationkey = s_nationkey`

### n_name (STRING, int8_t, dictionary-encoded)
- File: `nation/n_name.bin` (25 rows × 1 byte)
- Dictionary: `nation/n_name_dict.txt` — load as `std::vector<std::string>`
- Dict contents (code → nation name, in insertion order from ingestion):
  `0="ALGERIA"`, `1="ARGENTINA"`, `2="BRAZIL"`, `3="CANADA"`, `4="EGYPT"`,
  `5="ETHIOPIA"`, `6="FRANCE"`, `7="GERMANY"`, `8="INDIA"`, `9="INDONESIA"`,
  `10="IRAN"`, `11="IRAQ"`, `12="JAPAN"`, `13="JORDAN"`, `14="KENYA"`,
  `15="MOROCCO"`, `16="MOZAMBIQUE"`, `17="PERU"`, `18="CHINA"`, `19="ROMANIA"`,
  `20="SAUDI ARABIA"`, `21="VIETNAM"`, `22="RUSSIA"`, `23="UNITED KINGDOM"`, `24="UNITED STATES"`
- This query: GROUP BY nation (output as string via `n_name_dict[code]`)
- Since nation has only 25 rows, load everything into a small array at startup; no index needed.

## Table Stats
| Table    | Rows       | Role      | Sort Order   | Block Size |
|----------|------------|-----------|--------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate↑  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate↑ | 100,000    |
| partsupp | 8,000,000  | bridge    | none         | 100,000    |
| part     | 2,000,000  | dimension | none         | 100,000    |
| supplier | 100,000    | dimension | none         | 100,000    |
| nation   | 25         | dimension | none         | 100,000    |

## Query Analysis
- **Join pattern**: 6-way join `part ⋈ lineitem ⋈ partsupp ⋈ supplier ⋈ orders ⋈ nation`
- **Recommended join order** (minimize intermediates):
  1. Filter part on `p_name LIKE '%green%'` → 96,000 qualifying parts (4.8%). Build `unordered_set<int32_t>` of qualifying p_partkey.
  2. Probe lineitem on l_partkey ∈ qualifying_parts (~4.8% × 60M = ~2.88M lineitem rows). Also filter by l_suppkey ∈ qualifying_suppkeys (see step 3). Collect tuples (l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount, l_quantity).
  3. Load supplier (100K rows, tiny). Build `unordered_map<int32_t, int32_t>` mapping s_suppkey → s_nationkey. 100K entries fit in L2 cache.
  4. Load nation (25 rows). Build array `nation_name[n_nationkey]` → string.
  5. Load partsupp (8M rows). Build `unordered_map<uint64_t, double>` mapping `(ps_partkey<<32|ps_suppkey)` → ps_supplycost. ~8M entries, ~200MB.
  6. Load orders (15M rows). Build `unordered_map<int32_t, int32_t>` mapping o_orderkey → o_orderdate (→ o_year). ~15M entries.
  7. For each qualifying lineitem row: probe partsupp map, probe supplier map, probe orders map. Compute amount. Accumulate SUM(amount) per (nation, year) using `unordered_map<pair<int8_t,int16_t>, double>`.
- **Filter**: p_name LIKE '%green%' is the key selectivity reducer (4.8%). No index for LIKE — scan part sequentially.
- **Aggregation**: ~200 distinct (nation, year) groups. Small result, trivial memory.
- **Output**: 200 rows, ORDER BY nation ASC, o_year DESC — sort the result array.
- **Parallelism**: divide lineitem scan into 32 chunks; thread-local partial aggregates; merge. Use zone map is not useful here (no date filter on lineitem).

## Indexes

### orders_orderkey_hash (single-value hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t capacity=33554432]` then `[capacity × {int32_t key, uint32_t pos}]` (8 bytes/bucket)
- Empty bucket marker: `key == INT32_MIN`
- Lookup: `bucket = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (capacity-1)`; linear probe
- Returns `pos` = row index into orders column files
- This query: look up o_orderdate (→ o_year) for a given o_orderkey from lineitem

### lineitem_orderkey_hash (multi-value hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Not the primary access path for Q9 (scanning lineitem filtered by l_partkey is better). May be skipped.

### No useful zone maps for Q9 (no date range filters on lineitem or orders in this query).
