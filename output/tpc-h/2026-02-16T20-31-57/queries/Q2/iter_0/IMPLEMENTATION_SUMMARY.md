# Q2 Query Implementation - Iteration 0

## Status
✅ **VALIDATION PASSED** - 100/100 rows match expected output

## Performance
- **Wall-clock time**: ~470-570 ms
- **Profiled time breakdown** (with -DGENDB_PROFILE):
  - Region filter: 0.16 ms
  - Nation filter: 0.01 ms
  - Supplier filter: 2.48 ms
  - Part filter: 28.44 ms
  - Subquery + Index building: 373.82 ms
  - Load dictionaries: 60.81 ms
  - Build results: 15.83 ms
  - Sort (100 rows): 2.53 ms
  - CSV output: 2.62 ms
  - **Total computation**: 486.84 ms

## Implementation Strategy

### Query Analysis
The Q2 query is a complex join with a correlated subquery:
```sql
SELECT ... FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region WHERE ...)
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
```

### Logical Query Plan
1. **Filter region** (5 rows): Scan for r_name = 'EUROPE' → 1 region
2. **Filter nation** (25 rows): Scan for matching n_regionkey → ~5 nations (European nations)
3. **Filter supplier** (100K rows): Scan for s_nationkey IN (European nations) → ~4K-5K suppliers
4. **Filter part** (2M rows): Scan for p_size = 15 AND p_type LIKE '%BRASS' → ~2K parts
5. **Subquery pre-computation**: Single pass through partsupp (8M rows)
   - Filter by: suppkey IN (qualified_suppliers) AND partkey IN (qualified_parts)
   - Compute: MIN(ps_supplycost) grouped by partkey
   - Result: hash map<partkey, min_cost>
6. **Main query execution**: For each qualified part, find matching suppliers where cost = min
7. **Join with dimensions**: Fetch supplier and nation names from dictionaries
8. **Sort**: By (s_acctbal DESC, n_name, s_name, p_partkey)
9. **Limit**: Keep first 100 rows

### Physical Query Plan
- **Direct array lookup** for region (5 entries) and nation (25 entries) - O(1) access
- **Hash tables** for supplier filtering and partsupp indexing
- **Single-pass scan** through 8M partsupp rows to compute both:
  1. Minimum supplycost per partkey (for subquery)
  2. Index of partsupp entries by partkey (for main query)
- **Dictionary caching**: Load all necessary dictionaries in memory
- **String matching**: Dictionary-based filtering for p_type LIKE '%BRASS'
  - Found 30 dictionary codes matching the pattern
  - Used unordered_set for O(1) membership testing during part filtering
- **CSV output**: Proper escaping and quoting, Windows line endings (\r\n)

### Key Optimizations
1. **Single-pass partsupp scan**: Combined subquery computation and index building into one scan, avoiding duplicate work
2. **Early predicate rejection**: Check qualified_partkey_set before processing each partsupp row
3. **Dictionary code caching**: Precompute all BRASS type codes instead of searching repeatedly
4. **Efficient CSV writing**: Only load and decode dictionaries for columns in the output, defer loading until needed
5. **Proper sorting**: Use stable sort with multiple-key comparator for consistent ordering

### Critical Implementation Details

#### Dictionary Handling
- Dictionary codes are 0-indexed in the binary files
- Loaded from `<column>_dict.txt` files at runtime
- For LIKE patterns: precompute all matching codes into a set, then use O(1) lookups during filtering

#### Data Type Mappings
- **DATE columns** (epoch days): int32_t, compared as integers
- **DECIMAL columns** (scale 2): int64_t, converted to float only for output (s_acctbal / 100.0)
- **STRING columns**: Dictionary-encoded as int32_t codes
- **INTEGER columns**: Direct int32_t values

#### Subquery Optimization
Rather than executing the correlated subquery for each row (O(n×m) complexity), we:
1. Pre-compute the minimum supplycost for each part in a single pass
2. Use the precomputed hash map to filter results in O(1) time

#### Sort Order
The query specifies: ORDER BY s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC
Implemented using std::sort with a custom comparator that properly handles:
- Descending order for s_acctbal (by negating the comparison)
- Ascending order for string fields (lexicographic)
- Ascending order for p_partkey (numeric)

### CSV Output Format
- Uses standard comma delimiter
- Properly quotes fields containing commas or special characters
- Uses Windows line endings (\r\n) to match expected output format
- Monetary values (s_acctbal) formatted with 2 decimal places
- All other string/numeric fields output as-is

## Compilation
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o q2 q2.cpp
```

## Execution
```bash
./q2 <gendb_dir> [results_dir]
```

Output file: `results_dir/Q2.csv`

## Validation Results
- Expected rows: 100
- Actual rows: 100
- Match: ✅ PASS
- All rows verified correct in validation tool
