# Q22 Guide — Global Sales Opportunity

## Column Reference

### c_custkey (PK, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows of int32_t)
- This query: NOT EXISTS subquery check against orders

### c_phone (string, varlen)
- File: `storage/customer/c_phone.bin` (offsets, uint32_t[1500001]), `storage/customer/c_phone_data.bin`
- This query: `SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')`
  → Extract first 2 chars from varlen string. Phone format is fixed-length, first 2 chars = country code.
- Selectivity: ~0.28 (7 of 25 country codes)

### c_acctbal (decimal, double, raw)
- File: `storage/customer/c_acctbal.bin` (1,500,000 rows of double)
- This query: `c_acctbal > AVG(c_acctbal)` (where acctbal > 0 and matching country codes), also `SUM(c_acctbal)` in output

### o_custkey (FK, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows of int32_t)
- This query: `NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)` — check if customer has any orders

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| customer | 1,500,000  | dimension | c_custkey  | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |

## Query Analysis
- **Pattern**: Customer scan with phone prefix filter, acctbal threshold from subquery, NOT EXISTS against orders, group by country code
- **Strategy**:
  1. Build "has orders" bitset: scan o_custkey, set bit for each custkey that appears (max 1.5M bits = 188KB). This inverts the NOT EXISTS.
  2. First pass over customer: compute AVG(c_acctbal) for customers where `c_acctbal > 0.0` AND phone prefix matches one of the 7 codes. Extract phone prefix from varlen: read first 2 bytes of phone string.
  3. Second pass over customer: for each customer where:
     - Phone prefix in target set
     - `c_acctbal > avg_acctbal` (from step 2)
     - NOT in "has orders" bitset
     → Group by cntrycode (2-char string → can use integer key), accumulate COUNT and SUM(c_acctbal)
  4. Output: 7 rows, ORDER BY cntrycode ASC
- **Phone prefix extraction**:
  ```cpp
  uint32_t off = c_phone_offsets[i];
  char cc[3] = { c_phone_data[off], c_phone_data[off+1], '\0' };
  ```
  Or convert to int: `int cntry = (c_phone_data[off] - '0') * 10 + (c_phone_data[off+1] - '0');`

## Indexes

### custkey_to_orders (grouped_fk on o_custkey)
- File: `storage/indexes/custkey_to_orders_offsets.bin` — `uint32_t[max_custkey+2]`
- File: `storage/indexes/custkey_to_orders_rows.bin` — `uint32_t[15000000]`
- Meta: `storage/indexes/custkey_to_orders_meta.txt`
- Layout: For custkey `k`, orders exist if `offsets[k+1] > offsets[k]`
- Usage: Instead of building a bitset from scratch, check `offsets[k+1] > offsets[k]` to determine if customer has orders. This is O(1) per customer, no need for full scan of o_custkey.
