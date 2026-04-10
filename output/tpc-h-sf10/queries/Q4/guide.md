# Q4 Guide — Order Priority Checking

## Column Reference

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key for EXISTS subquery `l_orderkey = o_orderkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows of int32_t)
- This query: `o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01'`
  → C++: `o_orderdate[i] >= days_from_civil(1993, 7, 1) && o_orderdate[i] < days_from_civil(1993, 10, 1)`
- Selectivity: ~0.039 (3-month window)

### o_orderpriority (string, uint8_t, dictionary)
- File: `storage/orders/o_orderpriority.bin` (15,000,000 rows of uint8_t)
- Dict: `storage/orders/o_orderpriority_dict.bin` — format: `uint32_t count`, then `{uint16_t len, char[len]}` per entry
- This query: GROUP BY column → group by dict code, decode to string for output
- ~5 distinct values

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: EXISTS subquery join key

### l_commitdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_commitdate.bin` (59,986,052 rows of int32_t)
- This query: `l_commitdate < l_receiptdate`

### l_receiptdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_receiptdate.bin` (59,986,052 rows of int32_t)
- This query: `l_commitdate < l_receiptdate`

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| orders   | 15,000,000 | fact | o_orderkey | 65536      |
| lineitem | 59,986,052 | fact | l_orderkey | 65536      |

## Query Analysis
- **Pattern**: Orders scan with date range filter + EXISTS semi-join to lineitem
- **Strategy**:
  1. Build a bitset of orderkeys that have at least one lineitem where `l_commitdate < l_receiptdate`. Scan lineitem's l_commitdate and l_receiptdate columns; for each row where condition holds, set bit for `l_orderkey[i]`. Use a bitset sized to max_orderkey (~60M bits = ~7.5MB).
  2. Scan orders: filter by date range (~0.039 → ~585K orders), check bitset for EXISTS, accumulate COUNT(*) per o_orderpriority dict code.
  3. Decode dict codes to strings for output, sort by orderpriority ASC.
- **Alternative**: Use lineitem_orderkey_idx to look up lineitem rows per qualifying order (but building bitset is more efficient for semi-join).

## Indexes

### orders_orderdate_zm (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Layout: `struct { int32_t min_val; int32_t max_val; }` per block, 65536 rows/block
- Usage: Skip blocks where `zm[b].min_val >= upper_bound` OR `zm[b].max_val < lower_bound`

### lineitem_orderkey_idx (dense_range on l_orderkey)
- File: `storage/indexes/lineitem_orderkey_idx.bin`
- Layout: `struct { uint32_t start; uint32_t count; }` indexed by orderkey
- Usage: Alternative to bitset approach — for each date-qualifying order, look up its lineitem rows and check condition. Trade-off: avoids full lineitem scan but has random access pattern.
