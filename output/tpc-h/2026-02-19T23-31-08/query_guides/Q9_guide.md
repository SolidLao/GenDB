# Q9 Guide — Product Type Profit Measure

## Column Reference

### p_partkey (INTEGER, int32_t)
- File: `part/p_partkey.bin` (2,000,000 rows × 4 bytes)
- This query: join key `p_partkey = l_partkey`; build hash table of qualifying partkeys

### p_name (STRING, offsets_data encoding)
- Files:
  - `part/p_name_offsets.bin`: uint32_t[2,000,001] — offsets[i] = byte start of name i in data blob
  - `part/p_name_data.bin`: concatenated raw chars (no null terminators)
- Access string i: `char* s = data + offsets[i]; size_t len = offsets[i+1] - offsets[i];`
- This query: `p_name LIKE '%green%'` → `std::string_view(s, len).find("green") != npos`
  - Estimated selectivity: ~4.8% → ~96,000 qualifying parts out of 2,000,000
  - Scan all 2M p_name entries; for qualifying rows, record p_partkey

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes, sorted by l_shipdate)
- This query: join key `l_orderkey = o_orderkey`

### l_partkey (INTEGER, int32_t)
- File: `lineitem/l_partkey.bin` (59,986,052 rows × 4 bytes)
- This query: join filter — only rows where l_partkey is in the qualifying part set

### l_suppkey (INTEGER, int32_t)
- File: `lineitem/l_suppkey.bin` (59,986,052 rows × 4 bytes)
- This query: join key `l_suppkey = s_suppkey` AND `l_suppkey = ps_suppkey`

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- This query: used in `amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity`

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- This query: used in `amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity`

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- This query: `(1 - l_discount)` factor in amount calculation

### ps_partkey (INTEGER, int32_t)
- File: `partsupp/ps_partkey.bin` (8,000,000 rows × 4 bytes)
- This query: composite join key `(ps_partkey, ps_suppkey) = (l_partkey, l_suppkey)`

### ps_suppkey (INTEGER, int32_t)
- File: `partsupp/ps_suppkey.bin` (8,000,000 rows × 4 bytes)
- This query: composite join key with ps_partkey

### ps_supplycost (DECIMAL, double)
- File: `partsupp/ps_supplycost.bin` (8,000,000 rows × 8 bytes)
- Stored as native double; range [1.00, 100.00]
- This query: `amount = ... - ps_supplycost * l_quantity`

### s_suppkey (INTEGER, int32_t)
- File: `supplier/s_suppkey.bin` (100,000 rows × 4 bytes)
- This query: join key `s_suppkey = l_suppkey`

### s_nationkey (INTEGER, int32_t)
- File: `supplier/s_nationkey.bin` (100,000 rows × 4 bytes)
- This query: join key `s_nationkey = n_nationkey`

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key `o_orderkey = l_orderkey`

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- This query: `EXTRACT(YEAR FROM o_orderdate)` → used as GROUP BY and output key (`o_year`)
- Year extraction from epoch days (O(1)):
  ```
  approx_year = 1970 + days / 365
  // Adjust: find exact year by checking cumulative days
  // Use a fast O(1) formula or small lookup. See date-operations.md knowledge base.
  ```
  - TPC-H orders range: epoch ~8036 (1992) to ~10471 (1998), years 1992–1998 → 7 distinct years

### n_nationkey (INTEGER, int32_t)
- File: `nation/n_nationkey.bin` (25 rows × 4 bytes)
- This query: join key `n_nationkey = s_nationkey`

### n_name (STRING, uint8_t, dictionary-encoded)
- File: `nation/n_name.bin` (25 rows × 1 byte)
- Dictionary: `nation/n_name_dict.txt`
  - Format: `0=ALGERIA\n1=ARGENTINA\n...\n24=VIETNAM\n` (25 entries)
  - Load: `std::vector<std::string> dict(25); parse code=value lines`
- This query: GROUP BY nation (output as string) → decode `dict[n_name_code]`
- With 25 rows, scan linearly — no hash index needed (fits in L1 cache)

## Table Stats
| Table    | Rows       | Role      | Sort Order   | Block Size |
|----------|------------|-----------|--------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate ↑ | 100,000    |
| orders   | 15,000,000 | fact      | none         | 100,000    |
| partsupp | 8,000,000  | bridge    | none         | 100,000    |
| part     | 2,000,000  | dimension | none         | 100,000    |
| supplier | 100,000    | dimension | none         | 100,000    |
| nation   | 25         | dimension | none         | 100,000    |

## Query Analysis
- **Join pattern**: 6-way join (flattened from subquery): part ⋈ lineitem ⋈ partsupp ⋈ supplier ⋈ orders ⋈ nation
- **Filter**: `p_name LIKE '%green%'` — crucial early filter, reduces part from 2M to ~96K rows
- **Join ordering** (by size, smallest first for build):
  1. nation (25 rows): array lookup by n_nationkey → `nation_name[n_nationkey]`, `nation_suppkey_map` via s_nationkey
  2. supplier (100K): hash table `suppkey → nationkey`, apply nation join → `suppkey → nation_name`
  3. part (2M, filtered to 96K): hash set of qualifying p_partkey
  4. partsupp (8M, filtered by qualifying partkeys): hash map `(ps_partkey, ps_suppkey) → ps_supplycost`
  5. orders (15M): hash map `o_orderkey → year(o_orderdate)`
  6. lineitem (60M, filtered by qualifying partkeys from part): probe all 5 hash tables
- **Aggregation**: GROUP BY (nation, o_year) → ~25 nations × 7 years = 175 groups
  - Use hash aggregation with pre-allocated 175-entry array or open-addressing hash table
- **Output**: 175 rows ordered by nation ASC, o_year DESC

## Indexes
No persistent indexes are used by Q9. All join hash tables are built in-memory at query time:
- part filtering (LIKE '%green%'): full scan of p_name_offsets + p_name_data
- supplier lookup: small enough to scan or build in-memory hash
- partsupp lookup: build in-memory hash on (ps_partkey, ps_suppkey) — 8M entries, pre-filter by qualifying partkeys to reduce to ~384K entries
- orders lookup: build in-memory hash on o_orderkey → year — 15M entries

**Note on composite join key (ps_partkey, ps_suppkey):**
Combine as `uint64_t key = ((uint64_t)(uint32_t)ps_partkey << 32) | (uint32_t)ps_suppkey`
Use the same multiply-shift hash on this 64-bit key.

## Execution Strategy
1. Scan part (2M): load p_name_offsets + p_name_data, LIKE '%green%' filter → hash set of ~96K qualifying p_partkey values
2. Build supplier→nation map: scan supplier (100K), build `suppkey → nation_name_code` (array indexed by s_suppkey values 1..100K)
3. Scan partsupp (8M): filter by qualifying p_partkey → build map `(ps_partkey, ps_suppkey) → ps_supplycost` (~384K entries after filter)
4. Build orders map: scan orders (15M) → `o_orderkey → year(o_orderdate)` (15M entry hash table)
5. Scan lineitem (60M): filter by l_partkey in qualifying part set (probe hash set), then:
   - Look up ps_supplycost via (l_partkey, l_suppkey)
   - Look up year via l_orderkey → orders_map
   - Look up nation_name via l_suppkey → supplier_map
   - Compute amount = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity
   - Aggregate into `profit[nation_code][year_offset]` (e.g., year_offset = 1998-o_year for DESC)
6. Emit 175 rows ordered by nation ASC, o_year DESC
