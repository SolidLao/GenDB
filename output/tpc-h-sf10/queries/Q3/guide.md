# Q3 Guide — Shipping Priority

## Column Reference

### c_custkey (PK, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### c_mktsegment (string, uint8_t, dictionary)
- File: `storage/customer/c_mktsegment.bin` (1,500,000 rows of uint8_t)
- Dict: `storage/customer/c_mktsegment_dict.bin` — format: `uint32_t count`, then `{uint16_t len, char[len]}` per entry
- This query: `c_mktsegment = 'BUILDING'` → Load dict, find code for "BUILDING", then `c_mktsegment[i] == building_code`
- Selectivity: ~0.21

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key `l_orderkey = o_orderkey`, GROUP BY, output

### o_custkey (FK, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows of int32_t)
- This query: `o_orderdate < DATE '1995-03-15'` → C++: `o_orderdate[i] < days_from_civil(1995, 3, 15)`
- Selectivity: ~0.487; GROUP BY column, output

### o_shippriority (integer, int32_t, raw)
- File: `storage/orders/o_shippriority.bin` (15,000,000 rows of int32_t)
- This query: GROUP BY column, output

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: join key `l_orderkey = o_orderkey`, GROUP BY

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows of int32_t)
- This query: `l_shipdate > DATE '1995-03-15'` → C++: `l_shipdate[i] > days_from_civil(1995, 3, 15)`
- Selectivity: ~0.513

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice * (1 - l_discount))`

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: revenue calculation

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| customer | 1,500,000  | dimension | c_custkey  | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |

## Query Analysis
- **Pattern**: 3-way join (customer → orders → lineitem), filter + aggregate + top-10
- **Strategy**:
  1. Build BUILDING customer set: scan customer, collect custkeys where `c_mktsegment == building_code` into a bitset (max_custkey=1,500,000)
  2. Scan orders: filter `o_orderdate < threshold` AND `custkey in building_set` → collect qualifying (orderkey, orderdate, shippriority) tuples. ~0.487 * 0.21 * 15M ≈ 1.53M qualifying orders
  3. Build hash map: orderkey → (orderdate, shippriority, revenue_accumulator)
  4. Scan lineitem: filter `l_shipdate > threshold`, look up orderkey in hash map, accumulate revenue = `l_extendedprice * (1 - l_discount)`
  5. Top-10 by revenue DESC, o_orderdate ASC
- **Alternative**: Use orders_pk_idx for lineitem→orders join (lineitem is sorted by l_orderkey, orders has dense PK index)

## Indexes

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1
- Usage: Given l_orderkey from lineitem, `orders_pk_index[l_orderkey]` → orders row_id for direct attribute access

### customer_pk_idx (dense_pk on c_custkey)
- File: `storage/indexes/customer_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by custkey → row_id, sentinel -1
- Usage: Given o_custkey, look up customer row for mktsegment check

### orders_orderdate_zm (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Meta: `storage/indexes/orders_o_orderdate_zonemap_meta.txt`
- Layout: `struct { int32_t min_val; int32_t max_val; }` per block, 65536 rows/block
- Usage: Skip blocks where `zm[b].min_val >= date_threshold`

### lineitem_shipdate_zm (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Meta: `storage/indexes/lineitem_l_shipdate_zonemap_meta.txt`
- Layout: `struct { int32_t min_val; int32_t max_val; }` per block, 65536 rows/block
- Usage: Skip blocks where `zm[b].max_val <= date_threshold`

### lineitem_orderkey_idx (dense_range on l_orderkey)
- File: `storage/indexes/lineitem_orderkey_idx.bin`
- Meta: `storage/indexes/lineitem_orderkey_idx_meta.txt`
- Layout: `struct { uint32_t start; uint32_t count; }` array, indexed by orderkey
- Usage: For a given orderkey, find all lineitem rows: `[entry.start, entry.start + entry.count)`
