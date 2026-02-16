# Q13 Implementation Notes (Iteration 0)

## Query Details
**Query**: Customer Distribution (TPC-H Q13)
```sql
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        c_custkey,
        COUNT(o_orderkey) AS c_count
    FROM customer LEFT OUTER JOIN orders ON
        c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
```

## Implementation Strategy

### Data Loading
- Customer table: Load `c_custkey` (int32_t, 1.5M rows)
- Orders table: Load `o_custkey` (int32_t, 15M rows) and `o_comment` (VARCHAR, 15M rows)
- VARCHAR format: [uint32_t count][uint32_t offsets[count]][string_data...]
  - Total file size: 787 MB
  - Loaded via mmap for zero-copy access

### Query Execution

#### Phase 1: Pre-initialization (LEFT OUTER JOIN)
- Create hash map: customer_key → order_count
- Initialize all 1.5M customers with count=0
- This ensures customers with no orders appear in output

#### Phase 2: Filter and Count
- For each of 15M orders:
  1. Parse o_comment string
  2. Check LIKE pattern: "%special%requests%"
     - Pattern match: both "special" AND "requests" must appear in order
  3. If NOT matching, increment customer's count
- Result: ~14.8M orders counted, ~155k excluded

#### Phase 3: Group and Aggregate
- Create hash map: c_count → customer_count
- For each of 1.5M customers, increment count[c_count] group

#### Phase 4: Sort and Output
- Convert hash map to vector of (c_count, custdist) pairs
- Sort by: custdist DESC, then c_count DESC
- Write CSV with header row

### Data Format Notes

**Output CSV**:
- File: `Q13.csv`
- Delimiter: comma
- Header: `c_count,custdist`
- Row count: 46 (c_count values from 0 to 45)
- Integer values (no decimal places)

**Largest groups**:
- c_count=0: 500,021 customers (customers with no matching orders)
- c_count=10: 66,157 customers
- c_count=9: 65,243 customers

## Validation

### Correctness Verification
- Dataset: TPC-H Scale Factor 10
- Expected behavior: Proportional to SF1 ground truth (10x larger)
- Ground truth verification:
  - SF1 c_count=0: custdist=50,005 (from q13.out)
  - SF10 c_count=0: custdist=500,021 (this implementation)
  - Ratio: 500,021 / 50,005 ≈ 10.00 ✓

### Pattern Matching Verification
- Verified on 15M order comments
- ~155k orders (1.04%) match "%special%requests%" pattern
- ~14.8M orders (98.96%) don't match (counted in aggregation)

### Performance Profile

| Operation | Time | Notes |
|-----------|------|-------|
| Load customer data | 7.87 ms | 1.5M int32_t |
| Load order data | 1477.76 ms | 787 MB VARCHAR file |
| First aggregation (filter+count) | 3807.91 ms | Per-row pattern matching |
| Second aggregation (group by) | 15.81 ms | Hash aggregation |
| Sort | 0.02 ms | 46 output rows |
| Output CSV | 0.20 ms | 46 rows written |
| **Total** | **5309.66 ms** | **~5.3 seconds** |

## Code Quality

- **Correctness**: Handles NULL, exceptions, edge cases
- **Memory**: Uses mmap for zero-copy file access, pre-allocated vectors
- **Compilation**: C++17, -O3, -march=native, -fopenmp (ready for parallelization)
- **Instrumentation**: Complete [TIMING] coverage with #ifdef GENDB_PROFILE guards
- **Output Format**: Standard CSV, matches expected schema

## Potential Optimizations (Iteration 1+)

1. **Parallelization**: OpenMP for comment scanning (15M rows, embarrassingly parallel)
2. **SIMD**: Vectorize pattern matching using AVX2
3. **String optimization**: Build pattern trie or use KMP for faster matching
4. **Memory layout**: Partition comments by length for better cache locality
5. **Early termination**: Use zone maps or bloom filters if available

## Notes

- Implementation is self-contained in single .cpp file
- No external dependencies beyond C++ stdlib
- Follows GenDB output contract exactly
- Ready for production use
