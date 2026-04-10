# Q13 Guide — Customer Distribution

## Column Reference

### c_custkey (PK, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows of int32_t)
- This query: LEFT OUTER JOIN key, GROUP BY (inner)

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: `COUNT(o_orderkey)` — counts non-NULL (matching) orders per customer

### o_custkey (FK, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### o_comment (string, varlen)
- File: `storage/orders/o_comment.bin` (offsets, uint32_t[15000001]), `storage/orders/o_comment_data.bin`
- This query: `o_comment NOT LIKE '%special%requests%'` — exclude orders whose comment matches this pattern
- Pattern matching: find "special" then "requests" anywhere after it in the comment string

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| customer | 1,500,000  | dimension | c_custkey  | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |

## Query Analysis
- **Pattern**: LEFT OUTER JOIN customer→orders with comment exclusion, double aggregation (count per customer, then distribution)
- **Strategy**:
  1. Scan orders: for each order, check `o_comment NOT LIKE '%special%requests%'` (varlen string search). If passes, increment count for `o_custkey`.
  2. Use dense array `counts[max_custkey+1]` initialized to 0 (6MB for 1.5M entries). For each qualifying order: `counts[o_custkey]++`
  3. Scan all custkeys 1..max_custkey to build the distribution:
     - For each custkey, `c_count = counts[custkey]`
     - Build histogram: `dist[c_count]++`
     - Customers with 0 orders (LEFT JOIN semantics) are those where `counts[custkey] == 0` AND custkey exists. Use customer_pk_idx to verify existence.
  4. Output: GROUP BY c_count, ORDER BY custdist DESC, c_count DESC
- **Comment filter**: For each order row `i`, extract comment string from varlen:
  ```cpp
  uint32_t off_start = o_comment_offsets[i];
  uint32_t off_end = o_comment_offsets[i + 1];
  // search for "special" then "requests" in o_comment_data[off_start..off_end)
  ```

## Indexes

### custkey_to_orders (grouped_fk on o_custkey)
- File: `storage/indexes/custkey_to_orders_offsets.bin` — `uint32_t[max_custkey+2]`
- File: `storage/indexes/custkey_to_orders_rows.bin` — `uint32_t[15000000]` (order row_ids)
- Meta: `storage/indexes/custkey_to_orders_meta.txt`
- Layout: For custkey `k`, order row_ids are at `rows[offsets[k] .. offsets[k+1])`.
  Count of orders for custkey `k` = `offsets[k+1] - offsets[k]`.
- Usage: Alternative approach — iterate custkeys, use index to get each customer's orders, check comment filter, count qualifying orders. This provides per-customer counts directly.
- Note: Requires checking o_comment filter for each order, so must read the varlen comment for each row_id.

### customer_pk_idx (dense_pk on c_custkey)
- File: `storage/indexes/customer_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by custkey → row_id, sentinel -1
- Usage: Verify which custkeys exist (for LEFT JOIN — include custkeys with 0 qualifying orders)
