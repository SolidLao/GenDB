# Q3 Iteration 0 - TPC-H Shipping Priority Query

## Status
✓ **COMPLETE** - Iteration 0 query implementation validated and ready for optimization

## Overview

This directory contains a self-contained C++ implementation of TPC-H Query 3 (Shipping Priority), generated for GenDB iteration 0.

The query joins three tables (customer, orders, lineitem), applies predicates on market segment and dates, performs a multi-key aggregation, and outputs the top 10 results by revenue.

## Files

### Primary Deliverables
- **q3.cpp** (474 lines) - Self-contained C++ implementation
- **results/Q3.csv** - Query output (10 rows + header)

### Documentation
- **VALIDATION_REPORT.txt** - Detailed execution metrics and validation results
- **IMPLEMENTATION_SUMMARY.txt** - Output contract compliance checklist
- **README.md** - This file

## Quick Start

### Compile
```bash
# With profiling (shows timing information)
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -o q3 q3.cpp

# Production build (silent execution)
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o q3 q3.cpp
```

### Run
```bash
./q3 /path/to/gendb/tpch_sf10.gendb /path/to/results
```

This will generate `Q3.csv` in the results directory.

### View Output
```bash
cat Q3.csv
```

## Query Specification

```sql
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM customer, orders, lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate ASC
LIMIT 10;
```

## Key Implementation Details

### Data Types & Encoding

- **c_mktsegment**: Dictionary-encoded (uint8_t), runtime lookup for "BUILDING" code
- **o_orderdate, l_shipdate**: int32_t days since epoch (1970-01-01)
  - 1995-03-15 = 9204 days
- **l_extendedprice, l_discount**: int64_t scaled by 100
  - Revenue = l_extendedprice × (100 - l_discount), divided by 10,000 for output
  - No intermediate floating-point arithmetic (preserves precision)

### Query Execution Plan

1. **Filter Customer** (1.5M → 300K)
   - Load c_custkey, c_mktsegment
   - Keep rows where c_mktsegment == 0 (BUILDING)

2. **Filter Orders** (15M → 1.46M)
   - Load o_orderkey, o_custkey, o_orderdate, o_shippriority
   - Keep rows where c_custkey in filtered_customers and o_orderdate < 9204

3. **Filter & Aggregate Lineitem** (60M rows)
   - Load l_orderkey, l_shipdate, l_extendedprice, l_discount
   - Filter l_shipdate > 9204 and l_orderkey in filtered_orders
   - Compute revenue for each matching lineitem
   - Hash aggregate by (l_orderkey, o_orderdate, o_shippriority)

4. **Sort & Output** (114K → 10 rows)
   - Sort aggregation results by revenue DESC, o_orderdate ASC
   - Output top 10 rows as CSV

## Performance

**Total Execution Time**: ~4.8 seconds (with GENDB_PROFILE)

### Timing Breakdown
- Data loading (mmap): ~14 ms
- Filtering & join preparation: ~1.6 seconds
- Aggregation: ~3.1 seconds
- Sorting: ~8 ms
- Output: ~0.2 ms

### Optimization Opportunities (for Iteration 1+)

- **Parallelization**: Filter operations parallelizable across cores
- **SIMD**: Integer filtering and arithmetic
- **Index Usage**: Zone maps for date filters, hash indexes for joins
- **Memory**: Cache-aware aggregation layout, arena allocation

## Validation

✓ **Correctness**: Byte-for-byte match with ground truth
✓ **Compilation**: No errors, optimized binary (57 KB)
✓ **Execution**: ~4.8 seconds total time
✓ **Output Format**: CSV with CRLF line endings, 4 decimal places

### Sample Output
```
l_orderkey,revenue,o_orderdate,o_shippriority
4791171,440715.2185,1995-02-23,0
46678469,439855.3250,1995-01-27,0
23906758,432728.5737,1995-03-14,0
23861382,428739.1368,1995-03-09,0
59393639,426036.0662,1995-02-12,0
3355202,425100.6657,1995-03-04,0
9806272,425088.0568,1995-03-13,0
22810436,423231.9690,1995-01-02,0
16384100,421478.7294,1995-03-02,0
52974151,415367.1195,1995-02-05,0
```

## Implementation Highlights

### Correctness Guarantees
✓ Dictionary codes loaded at runtime (not hardcoded)
✓ Date thresholds computed from epoch values
✓ Decimal precision preserved through aggregation
✓ Standalone hash struct (no std namespace specialization)
✓ Proper mmap usage with error handling

### Code Quality
- Single self-contained .cpp file (no dependencies)
- Comprehensive [TIMING] instrumentation
- Clear comments on critical sections
- Follows GenDB output contract exactly

## Next Steps

This implementation is ready for:

1. **Query Optimizer (Iteration 1+)**: Apply advanced optimizations while maintaining correctness
2. **Performance Benchmarking**: Use timing points to track improvements
3. **Production Deployment**: Compile without GENDB_PROFILE for silent execution
4. **Regression Testing**: Output file serves as baseline for validation

## Contact & Support

For questions about this implementation, see:
- VALIDATION_REPORT.txt - Detailed execution analysis
- IMPLEMENTATION_SUMMARY.txt - Contract compliance details
- q3.cpp - Source code with inline comments

---

Generated: 2026-02-15
GenDB Version: Iteration 0 (Code Generator)
Hardware: 64-core CPU, 376 GB RAM, HDD storage
