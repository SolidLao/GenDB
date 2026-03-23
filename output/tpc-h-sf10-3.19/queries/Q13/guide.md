# Q13 Guide — Customer Distribution

## Column Reference

### c_custkey (pk, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows)
- This query: LEFT OUTER JOIN key, GROUP BY in inner query

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows)
- This query: `COUNT(o_orderkey)` — count non-NULL (i.e., matching) order keys per customer

### o_custkey (fk, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows)
- This query: `c_custkey = o_custkey` → join key

### o_comment (text, varlen string)
- Files: `storage/orders/o_comment_offsets.bin` (uint32_t[15000001]), `storage/orders/o_comment_data.bin`
- This query: `o_comment NOT LIKE '%special%requests%'` → exclude matching orders from count
- Selectivity: ~98.9% pass (only ~1.1% contain 'special...requests')

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| customer | 1,500,000 | Dimension (all rows) | c_custkey | 65536 |
| orders | 15,000,000 | Fact (LEFT JOIN) | o_orderkey | 65536 |

## Query Analysis
- **LEFT OUTER JOIN**: Every customer appears even if they have zero qualifying orders.
- **Comment filter**: `o_comment NOT LIKE '%special%requests%'` — must search for substring pattern in varlen string. ~1.1% excluded.
- **Inner aggregation**: For each customer, count qualifying orders → c_count.
- **Outer aggregation**: GROUP BY c_count → COUNT(*) as custdist. ~40 distinct c_count values.
- **Output**: ORDER BY custdist DESC, c_count DESC.

## Indexes

### customer_c_custkey_lookup (dense_pk_array on c_custkey)
- File: `storage/indexes/customer_c_custkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[custkey] = row_id`, `-1` if missing.
- Usage: Not directly needed — customer is scanned fully.

## Recommended Approach
1. Allocate array `c_count[max_custkey+1]` initialized to 0.
2. Scan orders: for each order row, check `o_comment NOT LIKE '%special%requests%'`:
   - Load o_comment string from varlen: `data[offsets[i]..offsets[i+1]]`.
   - Use substring search (e.g., strstr or custom) for pattern `special.*requests` (NOT regex — look for "special" then any chars then "requests" after it).
   - If comment passes filter: `c_count[o_custkey[i]]++`.
3. Customers with no qualifying orders remain at c_count=0 (LEFT JOIN semantics).
4. Scan all custkeys (1 to max_custkey): build histogram `custdist[c_count]++`.
   - Note: must count ALL customers, including those with c_count=0.
   - Total customers = 1,500,000 (from meta.bin).
5. Sort histogram entries by (custdist DESC, c_count DESC). Output.

### Pattern matching for `NOT LIKE '%special%requests%'`
The SQL `LIKE '%special%requests%'` means: the string contains "special" somewhere, followed (possibly with gap) by "requests".
```cpp
bool matches_special_requests(const char* s, size_t len) {
    // Find "special" first
    const char* p = (const char*)memmem(s, len, "special", 7);
    if (!p) return false;
    // Then find "requests" after "special"
    size_t remaining = len - (p - s) - 7;
    return memmem(p + 7, remaining, "requests", 8) != nullptr;
}
```
Exclude (do NOT count) orders where this returns true.
