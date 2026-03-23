# Q22 Guide — Global Sales Opportunity

## Column Reference

### c_custkey (pk, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows)
- This query: NOT EXISTS check against orders

### c_phone (text, varlen string)
- Files: `storage/customer/c_phone_offsets.bin` (uint32_t[1500001]), `storage/customer/c_phone_data.bin`
- This query: `SUBSTRING(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')` → extract first 2 chars of phone string
- Phone strings are fixed-length (15 chars: "CC-CCC-CCC-CCCC"), first 2 chars = country code

### c_acctbal (measure, double, raw)
- File: `storage/customer/c_acctbal.bin` (1,500,000 rows)
- This query: `c_acctbal > avg_acctbal` (where avg is over positive-balance customers in target country codes); also SUM(c_acctbal) in output

### o_custkey (fk, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows)
- This query: `NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| customer | 1,500,000 | Filtered dimension | c_custkey | 65536 |
| orders | 15,000,000 | Anti-join | o_orderkey | 65536 |

## Query Analysis
- **Country code filter**: First 2 chars of phone in {'13','31','23','29','30','18','17'} → 7/25 countries → ~28%.
- **Acctbal filter**: c_acctbal > AVG(c_acctbal) among positive-balance customers in those 7 codes. This avg is typically ~4-5K.
- **Anti-join**: Customer must have NO orders at all. ~1/3 of customers have no orders in TPC-H.
- **Aggregation**: GROUP BY cntrycode (= first 2 chars of phone) → 7 groups. COUNT(*) and SUM(c_acctbal).
- **Output**: ORDER BY cntrycode ASC.

## Indexes

### customer_c_custkey_lookup (dense_pk_array on c_custkey)
- File: `storage/indexes/customer_c_custkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[custkey] = row_id`, `-1` if missing.
- Usage: Not directly needed — scanning customer fully.

## Recommended Approach
1. **Build "has orders" bitset**: Scan o_custkey array (15M rows). For each o_custkey, set bit in bitset. This identifies all custkeys that have at least one order.
2. **Compute average**: Scan customer. For each customer:
   - Extract first 2 bytes of c_phone (from varlen: `data[offsets[i]]` and `data[offsets[i]+1]`).
   - Check if country code is in target set.
   - If yes AND c_acctbal > 0: accumulate sum and count for AVG computation.
3. Compute `avg_acctbal = sum / count`.
4. **Main scan**: Scan customer again. For each customer:
   - Extract country code from c_phone.
   - Check: country code in target set AND c_acctbal > avg_acctbal AND NOT has_orders[c_custkey].
   - If all pass: accumulate into (cntrycode) bucket: count++, sum_acctbal += c_acctbal.
5. Output 7 rows sorted by cntrycode.

### Country code extraction from varlen
```cpp
// c_phone is varlen: offsets[i] gives start position in data
// Phone format: "CC-CCC-CCC-CCCC", first 2 chars are country code
uint32_t off = phone_offsets[i];
char c1 = phone_data[off];
char c2 = phone_data[off + 1];
// Country code as string: {c1, c2}
// Or as int: (c1 - '0') * 10 + (c2 - '0')
```
Target codes as integers: {13, 17, 18, 23, 29, 30, 31}.
