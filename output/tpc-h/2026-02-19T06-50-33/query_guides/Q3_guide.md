# Q3 Guide — Shipping Priority

## Column Reference

### c_custkey (INTEGER, int32_t)
- File: customer/c_custkey.bin (1,500,000 rows × 4 bytes)
- No filter on this column; used as JOIN key and GROUP BY output
- Hash index: indexes/customer_custkey_hash.bin (PK, 1:1 mapping)

### c_mktsegment (STRING, int8_t, dictionary-encoded)
- File: customer/c_mktsegment.bin (1,500,000 rows × 1 byte)
- Dictionary: customer/c_mktsegment_dict.txt → `["AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"]`
  (0=AUTOMOBILE, 1=BUILDING, 2=FURNITURE, 3=HOUSEHOLD, 4=MACHINERY)
- This query: `c_mktsegment = 'BUILDING'` → C++ `c_mktsegment[i] == 1`
- Selectivity: 0.20 → ~300,000 qualifying customers

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15,000,000 rows × 4 bytes)
- Used as join key to lineitem and GROUP BY output
- Hash index: indexes/orders_orderkey_hash.bin (PK → row position)

### o_custkey (INTEGER, int32_t)
- File: orders/o_custkey.bin (15,000,000 rows × 4 bytes)
- Used as FK join key to customer.c_custkey; no filter on this column

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: orders/o_orderdate.bin (15,000,000 rows × 4 bytes)
- **orders is sorted by o_orderdate** → zone map highly effective
- This query: `o_orderdate < DATE '1995-03-15'` → `o_orderdate[i] < 9204`
  - `parse_date("1995-03-15")`: days_to_year_start(1995) = 9125+6 = 9131; Jan(31)+Feb(28)=59; day-1=14 → **9204**
- Zone map: indexes/orders_orderdate_zonemap.bin (150 blocks)
  - Skip blocks where `block_min >= 9204`
  - Selectivity 0.489 → ~48.9% qualify; sorted → rightmost ~77 blocks skipped
- GROUP BY: output as epoch int32_t, then format as "YYYY-MM-DD" for display

### o_shippriority (INTEGER, int32_t)
- File: orders/o_shippriority.bin (15,000,000 rows × 4 bytes)
- No filter; used in GROUP BY and output. In TPC-H SF10, all values = 0
- GROUP BY key alongside o_orderdate and l_orderkey

### o_totalprice (DECIMAL, double)
- File: orders/o_totalprice.bin (15,000,000 rows × 4 bytes... wait: 8 bytes)
- File: orders/o_totalprice.bin (15,000,000 rows × 8 bytes, double)
- No filter; not in output for Q3 (only in Q18)

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59,986,052 rows × 4 bytes)
- Used as FK join key to orders.o_orderkey and GROUP BY key
- Hash index: indexes/lineitem_orderkey_hash.bin (multi-value: 59.9M positions, 15M unique orderkeys)

### l_extendedprice (DECIMAL, double)
- File: lineitem/l_extendedprice.bin (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly
- This query: used in `SUM(l_extendedprice * (1 - l_discount))` per group (revenue)

### l_discount (DECIMAL, double)
- File: lineitem/l_discount.bin (59,986,052 rows × 8 bytes)
- Stored as native double — values in [0.00, 0.10]
- This query: used in revenue expression `l_extendedprice * (1.0 - l_discount)`

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: lineitem/l_shipdate.bin (59,986,052 rows × 4 bytes)
- **lineitem is sorted by l_shipdate** → zone map on shipdate effective for Q3 filter
- This query: `l_shipdate > DATE '1995-03-15'` → `l_shipdate[i] > 9204`
  - Epoch of 1995-03-15 = **9204** (same date as orderdate threshold)
- Selectivity: 0.511 → ~30.6M lineitem rows qualify
- Zone map: indexes/lineitem_shipdate_zonemap.bin
  - Skip blocks where `block_max <= 9204`
  - Since sorted by shipdate, first ~294 blocks (roughly 50%) can be skipped

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate | 100,000    |
| customer | 1,500,000  | dimension | none        | 100,000    |

## Query Analysis
- **Join pattern**: customer → orders → lineitem (3-table join, PK-FK chains)
  - Build: customer hash set on c_custkey (filtered by mktsegment)
  - Probe: orders.o_custkey → filter + build second hash map on o_orderkey
  - Probe: lineitem.l_orderkey → filter + aggregate
- **Filters**:
  - `c_mktsegment = 'BUILDING'` (selectivity 0.20) → 300K qualifying customers
  - `o_orderdate < 9204` (selectivity 0.489) → 7.3M qualifying orders before customer filter
  - `l_shipdate > 9204` (selectivity 0.511) → 30.6M qualifying lineitem rows before order filter
- **Combined selectivity across join**: ~3K distinct l_orderkey groups qualify
- **Aggregation**: GROUP BY (l_orderkey, o_orderdate, o_shippriority)
  - ~3K groups per workload_analysis; use hash aggregation (open addressing)
  - Per group: accumulate SUM(revenue), capture o_orderdate and o_shippriority (constant per group)
- **Output**: ORDER BY revenue DESC, o_orderdate ASC; LIMIT 10
  - Use partial sort / heap of top-10 instead of full sort

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: indexes/lineitem_shipdate_zonemap.bin
- Layout: `[uint32_t num_blocks=600]` then per block: `[int32_t min, int32_t max, uint32_t block_nrows]`
- Usage: mmap file; for each block, `skip if block_max[b] <= 9204`
- row_offset is ROW index, not byte offset. Access `col[row_offset .. row_offset + nrows)`
- This query: lineitem sorted by shipdate → first ~294 blocks (half) skipped, saving ~50% lineitem I/O

### orders_orderdate_zonemap (zone_map on o_orderdate)
- File: indexes/orders_orderdate_zonemap.bin
- Layout: `[uint32_t num_blocks=150]` then per block: `[int32_t min, int32_t max, uint32_t block_nrows]`
- Usage: mmap file; for each block, `skip if block_min[b] >= 9204`
- row_offset is ROW index. Access `col[row_offset .. row_offset + nrows)`
- This query: orders sorted by orderdate → rightmost ~77 blocks (with min >= 9204) skipped

### lineitem_orderkey_hash (hash, multi-value, on l_orderkey)
- File: indexes/lineitem_orderkey_hash.bin
- Layout: `[uint32_t magic=0x48494458][uint32_t num_positions=59986052][uint32_t num_unique=15000000][uint32_t capacity=33554432][uint32_t positions[59986052]][SlotI32 ht[33554432]]`
- SlotI32: `{int32_t key; uint32_t offset; uint32_t count;}` (12 bytes each)
- Empty slot sentinel: `key == INT32_MIN`
- Lookup: `hash_i32(key, shift=39) & (capacity-1)`, linear probe until key found or sentinel
- After finding slot: `pos_array[slot.offset .. slot.offset + slot.count]` gives row indices into lineitem columns
- row_offset is ROW index into the positions array, then use it as index into column arrays
- This query: NOT used for primary access pattern. Instead, scan lineitem with zone-map pruning; the hash can also be used to probe which lineitem rows belong to qualifying orders.

### orders_orderkey_hash (hash, PK 1:1, on o_orderkey)
- File: indexes/orders_orderkey_hash.bin
- Layout: same format as above; `num_positions=15000000`, `num_unique=15000000`, `capacity=33554432`
- SlotI32 count=1 for every entry (PK)
- This query: during lineitem scan, look up qualifying l_orderkey in this index to retrieve o_orderdate, o_shippriority; build this as in-memory hash during query execution instead

### customer_custkey_hash (hash, PK 1:1, on c_custkey)
- File: indexes/customer_custkey_hash.bin
- Layout: `num_positions=1500000`, `num_unique=1500000`, `capacity=4194304`
- This query: build in-memory hash of BUILDING customers during query (faster than mmap lookup for this pattern)
