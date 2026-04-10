# Q12 Guide — Shipping Modes and Order Priority

## Column Reference

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: join key `o_orderkey = l_orderkey`

### l_shipmode (string, uint8_t, dictionary)
- File: `storage/lineitem/l_shipmode.bin` (59,986,052 rows of uint8_t)
- Dict: `storage/lineitem/l_shipmode_dict.bin`
- This query: `l_shipmode IN ('MAIL', 'SHIP')` → load dict, find codes for MAIL and SHIP, then `l_shipmode[i] == mail_code || l_shipmode[i] == ship_code`
- Selectivity: ~0.286 (2 of 7 modes); GROUP BY column

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows of int32_t)
- This query: `l_shipdate < l_commitdate`

### l_commitdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_commitdate.bin` (59,986,052 rows of int32_t)
- This query: `l_commitdate < l_receiptdate` AND `l_shipdate < l_commitdate`

### l_receiptdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_receiptdate.bin` (59,986,052 rows of int32_t)
- This query: `l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01'`
  → C++: `>= days_from_civil(1994, 1, 1) && < days_from_civil(1995, 1, 1)`
- Also: `l_commitdate < l_receiptdate`

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key `o_orderkey = l_orderkey`

### o_orderpriority (string, uint8_t, dictionary)
- File: `storage/orders/o_orderpriority.bin` (15,000,000 rows of uint8_t)
- Dict: `storage/orders/o_orderpriority_dict.bin`
- This query: CASE WHEN `o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'` → load dict, find codes for '1-URGENT' and '2-HIGH'

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | l_orderkey | 65536      |
| orders   | 15,000,000 | fact | o_orderkey | 65536      |

## Query Analysis
- **Pattern**: 2-way join (lineitem → orders), multiple date comparisons on lineitem, shipmode filter, aggregate by shipmode
- **Strategy**:
  1. Scan lineitem with filters:
     - `l_shipmode IN ('MAIL', 'SHIP')` (0.286)
     - `l_receiptdate` in 1994 year range
     - `l_commitdate < l_receiptdate`
     - `l_shipdate < l_commitdate`
  2. For qualifying lineitem rows, look up o_orderpriority via orders_pk_idx:
     `orders_pk_index[l_orderkey]` → row_id → `o_orderpriority[row_id]`
  3. Check orderpriority against '1-URGENT' and '2-HIGH' dict codes, increment high_line_count or low_line_count per shipmode.
  4. Output: 2 rows (MAIL, SHIP), ORDER BY l_shipmode ASC
- **Only 2 groups**: Can use fixed arrays indexed by shipmode dict code

## Indexes

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1
- Usage: For each qualifying lineitem, `orders_pk_index[l_orderkey]` → orders row_id → read o_orderpriority[row_id]

### lineitem_shipdate_zm (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Note: Zone map is on l_shipdate, but this query filters on l_receiptdate. Zone map is NOT directly useful here unless you also check l_shipdate bounds.
