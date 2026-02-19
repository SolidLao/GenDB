# Q18 Guide — Large Volume Customer

## Column Reference

### c_custkey (INTEGER, int32_t)
- File: customer/c_custkey.bin (1,500,000 rows × 4 bytes)
- Used as PK join key to orders.o_custkey and GROUP BY output
- Hash index: indexes/customer_custkey_hash.bin (PK, 1:1)

### c_name (STRING, char[26], fixed-width)
- File: customer/c_name.bin (1,500,000 rows × 26 bytes)
- Each entry is a null-padded 26-byte char array (VARCHAR(25) + null)
- No filter on this column; used as GROUP BY key and output
- Access: `c_name + row_idx * 26` gives a null-terminated string of up to 25 chars

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15,000,000 rows × 4 bytes)
- **orders sorted by o_orderdate** (not orderkey) — no benefit for orderkey lookup
- Used as PK, GROUP BY key, and subquery join key
- Hash index: indexes/orders_orderkey_hash.bin (PK, 1:1)

### o_custkey (INTEGER, int32_t)
- File: orders/o_custkey.bin (15,000,000 rows × 4 bytes)
- FK to customer.c_custkey; used as equi-join predicate

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: orders/o_orderdate.bin (15,000,000 rows × 4 bytes)
- **orders sorted by o_orderdate**
- No filter in Q18; used as GROUP BY key and output
- Output: display as "YYYY-MM-DD"; convert epoch int to date string at output time
- This query: no date predicate → no zone-map pruning for Q18

### o_totalprice (DECIMAL, double)
- File: orders/o_totalprice.bin (15,000,000 rows × 8 bytes)
- Stored as native double — values match SQL directly
- No filter; used as GROUP BY key and ORDER BY key (`ORDER BY o_totalprice DESC`)
- Values represent the total order price in dollars

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59,986,052 rows × 4 bytes)
- FK to orders.o_orderkey; used in subquery aggregation and outer join
- Hash index: indexes/lineitem_orderkey_hash.bin (multi-value: 59.9M positions, 15M unique keys)
  - positions grouped by orderkey; average ~4 lineitem rows per orderkey

### l_quantity (DECIMAL, double)
- File: lineitem/l_quantity.bin (59,986,052 rows × 8 bytes)
- Stored as native double; values in {1.0, 2.0, ..., 50.0}
- This query:
  - **Subquery**: `GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`
    - Use hash aggregation over all lineitem rows: accumulate sum_qty per orderkey
    - Keep orderkeys where sum_qty > 300.0
    - Typical result: very few orderkeys (~few hundred in SF10)
  - **Outer query**: `SUM(l_quantity)` in GROUP BY result
    - After joining with qualifying orderkeys, re-sum l_quantity per group

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate  | 100,000    |
| orders   | 15,000,000 | fact      | o_orderdate | 100,000    |
| customer | 1,500,000  | dimension | none        | 100,000    |

## Query Analysis
- **Subquery type**: `IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)`
  - Decorrelate: pre-compute the subquery as a hash set of qualifying orderkeys
  - Scan all 60M lineitem rows, aggregate sum(l_quantity) per l_orderkey (hash aggregation over ~15M groups)
  - Filter groups where sum > 300.0 → very few qualifying orderkeys (TPC-H estimate: ~57 in SF1, ~575 in SF10)
- **Filters**:
  - Subquery: l_quantity summed per orderkey > 300 → very few orders (estimate: ~300-600 in SF10)
  - Outer: `c_custkey = o_custkey AND o_orderkey = l_orderkey` (joins, not filters)
- **Join pattern** (outer query):
  - qualifying_orderkeys is very small (~300-600 keys) → nearly all cost is the subquery
  - Build hash set of qualifying_orderkeys
  - Scan orders (15M rows), filter o_orderkey ∈ qualifying_set → ~300-600 matching orders
  - For each matching order, look up customer (1.5M, via custkey) → trivial
  - Re-scan lineitem for qualifying orderkeys (use lineitem_orderkey_hash to get positions efficiently)
  - Aggregate sum(l_quantity) per (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice) group
- **Aggregation**: outer GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
  - ~300-600 groups (one per qualifying order); use hash aggregation or just collect tuples
- **Output**: ORDER BY o_totalprice DESC, o_orderdate ASC; LIMIT 100

## Indexes

### lineitem_orderkey_hash (hash, multi-value, on l_orderkey) — KEY FOR OUTER QUERY
- File: indexes/lineitem_orderkey_hash.bin
- Layout: `[uint32_t magic=0x48494458][uint32_t num_positions=59986052][uint32_t num_unique=15000000][uint32_t capacity=33554432][uint32_t positions[59986052]][SlotI32 ht[33554432]]`
- SlotI32: `{int32_t key; uint32_t offset; uint32_t count;}` (12 bytes each)
- Empty slot sentinel: `key == INT32_MIN`
- Lookup shift: `64 - 25 = 39` (capacity=33554432=2^25)
- Lookup: `h = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 39`, probe ht
- After match: `positions[slot.offset .. slot.offset + slot.count]` = row indices into lineitem columns
- row_offset is ROW index into lineitem column arrays (not byte offset)
- **Subquery phase**: do NOT use this index — instead scan all lineitem rows sequentially and hash-aggregate by l_orderkey. Sequential scan of l_quantity.bin is faster than random index lookups.
- **Outer join phase**: use this index to efficiently retrieve lineitem rows for the ~300-600 qualifying orderkeys. For each qualifying orderkey, use the hash index to find all its lineitem positions, then sum l_quantity.

### orders_orderkey_hash (hash, PK 1:1, on o_orderkey)
- File: indexes/orders_orderkey_hash.bin
- Layout: `num_positions=15000000`, `num_unique=15000000`, `capacity=33554432`
- SlotI32: 12 bytes; shift=39; positions[slot.offset] = row index in orders table
- row_offset is ROW index into orders column arrays
- This query: after identifying qualifying orderkeys from subquery, look up each one in orders to get o_orderdate, o_totalprice, o_custkey. For ~300-600 lookups, this is very fast.

### customer_custkey_hash (hash, PK 1:1, on c_custkey)
- File: indexes/customer_custkey_hash.bin
- Layout: `num_positions=1500000`, `num_unique=1500000`, `capacity=4194304`
- SlotI32: 12 bytes; shift = `64 - 22 = 42`; positions[slot.offset] = row in customer table
- row_offset is ROW index into customer column arrays
- This query: look up each order's o_custkey to retrieve c_name for output. Only ~300-600 lookups.

## Implementation Strategy
```
Phase 1 — Subquery (scan all 60M lineitem rows):
  HashMap<int32_t, double> sum_qty_per_order;  // ~15M entries
  for each lineitem row i:
    sum_qty_per_order[l_orderkey[i]] += l_quantity[i];
  HashSet<int32_t> qualifying_orders;
  for each (key, sum) in sum_qty_per_order:
    if sum > 300.0: qualifying_orders.insert(key);

Phase 2 — Outer query (use lineitem_orderkey_hash for qualifying orders):
  for each orderkey in qualifying_orders:
    1. Look up orders_orderkey_hash → orders row → o_orderdate, o_totalprice, o_custkey
    2. Look up customer_custkey_hash → customer row → c_name
    3. Look up lineitem_orderkey_hash → list of lineitem rows → sum l_quantity
    4. Build result tuple (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_qty)

Phase 3 — Sort & output:
  Sort result by (o_totalprice DESC, o_orderdate ASC); take first 100
```
- Parallelize Phase 1: divide 60M lineitem rows into 64 chunks; thread-local partial sum maps; merge
- Phase 2 and 3 are trivially fast (~300-600 qualifying orders)
