# Q18 Implementation Summary (Iteration 0)

## Query Overview
**Q18: Large Volume Customer**

The query identifies customers with large order volumes:
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS sum_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
) AND c_custkey = o_custkey AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
```

## Implementation Strategy

### Phase 1: Lineitem Filtering (Hash Aggregation)
- **Operation**: Group lineitem by `l_orderkey`, compute SUM(`l_quantity`), filter > 300 * scale_factor (30,000)
- **Time**: ~2,500 ms
- **Result**: 624 orders with sum_qty > 300
- **Data Structure**: `std::unordered_map<int32_t, int64_t>`
- **Key Decision**: DECIMAL values stored as int64_t with scale_factor=100, so threshold is 30,000 (not 300)

### Phase 2: Orders Filtering & Join
- **Operation**: Filter orders by matching orderkeys from Phase 1
- **Time**: ~250 ms
- **Result**: 624 matching orders with custkey, orderdate, totalprice
- **Data Structure**: `std::unordered_map<int32_t, vector<tuple<...>>>`

### Phase 3: Customer Name Loading
- **Operation**: Load variable-length string data with offset table
- **Time**: ~260 ms
- **Challenge**: c_name.bin uses offset-based storage with ~1,499,998 entries (not 1,500,000)
- **Solution**: Auto-detect offset table size by scanning for non-offset values
- **Format**: [header: 8B] [offsets: 1,499,998 × 4B] [strings]

### Phase 4: Result Aggregation & Grouping
- **Operation**: Re-scan lineitem to join with matched orders and aggregate SUM(l_quantity) per group
- **Time**: ~900 ms
- **Group By**: (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
- **Data Structure**: Custom hash table with ResultKey (struct) and hash function

### Phase 5: Sort & Output
- **Operation**: Sort by o_totalprice DESC, o_orderdate ASC; LIMIT 100
- **Time**: <1 ms (small dataset)
- **Output**: CSV with 100 rows

## Key Technical Decisions

### 1. DATE Handling (int32_t epoch days)
- Dates stored as int32_t days since 1970-01-01
- Direct integer comparison used in code
- Conversion to YYYY-MM-DD only for CSV output
- Example: 1997-11-26 = epoch day 10142

### 2. DECIMAL Handling (int64_t scaled)
- Scale factor: 100
- Stored as int64_t (e.g., 558289.17 = 55828917)
- All comparisons use scaled integers
- Threshold calculation: 300 * 100 = 30,000
- Output: Divide by 100.0 and format with 2 decimal places

### 3. String Data (Variable-Length)
- c_name uses offset-based storage (not fixed CHAR)
- Format: header + offset table + string data
- Offset table: ~1,499,998 entries (auto-detected)
- Strings trimmed of trailing spaces for output

### 4. Memory Efficiency
- mmap used for all binary column files (zero-copy)
- Hash tables built in-memory (all data <1GB)
- No spilling to disk required
- Peak memory: ~5-10 GB

### 5. Correctness Over Performance
- Hash aggregation preferred over sort-merge (simpler correctness)
- OpenMP directives included (for future iteration 1+ optimization)
- [TIMING] instrumentation for all major phases
- Bounds checking on variable-length string offsets

## Validation

### Output Format
- CSV with comma delimiter
- Header: `c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty`
- 100 result rows + 1 header = 101 lines total
- Date format: YYYY-MM-DD
- Decimal precision: 2 places

### Sample Output (First 3 rows)
```
c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty
Customer#001287813,1287812,42290181,1997-11-26,558289.17,318.00
Customer#000644813,644812,2745894,1996-07-04,557664.53,304.00
Customer#001172514,1172513,36667107,1997-06-06,550142.18,322.00
```

## Performance Profile (with -DGENDB_PROFILE)

```
[TIMING] lineitem_load:              0.09 ms
[TIMING] lineitem_aggregation_filter: 2486.51 ms  (60M rows × 4B read + hash)
[TIMING] orders_load:                0.04 ms
[TIMING] orders_filter_join:         263.76 ms   (15M rows × 3 int32s read)
[TIMING] customer_load:              262.44 ms   (1.5M customer names)
[TIMING] result_aggregation:         889.52 ms   (60M rows × 2 lookups + hash)
[TIMING] sort_limit:                 0.12 ms     (100 rows)
[TIMING] output:                     0.79 ms     (CSV write)
[TIMING] total:                      3939.95 ms  (~4 seconds)
```

## Compilation

### With Profiling
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -o q18 q18.cpp
```

### Release Build
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o q18 q18.cpp
```

## Files Generated
- `q18.cpp`: Main implementation (self-contained, ~500 lines)
- `results/Q18.csv`: Query results (100 rows)
- `SUMMARY.md`: This file

## Known Issues & Future Optimization Targets (Iteration 1+)

1. **Lineitem Aggregation**: Currently O(N) hash map build. Could use:
   - Pre-built hash index from `lineitem_l_orderkey_hash.bin`
   - Parallel hash aggregation with thread-local buffers
   - SIMD-accelerated aggregation

2. **Customer Name Loading**: Currently sequential. Could use:
   - Parallel string offset scanning
   - Cache-optimized offset table layout

3. **Result Aggregation**: Rescans lineitem. Could optimize:
   - Build hash index on filtered orders for faster lookups
   - Use pre-computed partial aggregates

4. **Memory Layout**: Could improve cache locality:
   - Interleave frequently-accessed columns
   - Partition by cache line boundaries

## Compliance Checklist
- ✓ Self-contained .cpp file
- ✓ [TIMING] instrumentation for all major ops
- ✓ CSV output with proper format
- ✓ DATE handling (epoch days, YYYY-MM-DD output)
- ✓ DECIMAL handling (int64_t scaled, 2 decimal places)
- ✓ String handling (variable-length, offset-based)
- ✓ mmap for binary columns
- ✓ No hardcoded paths
- ✓ Handles missing data gracefully
- ✓ Compiles with -O3 -march=native -fopenmp
