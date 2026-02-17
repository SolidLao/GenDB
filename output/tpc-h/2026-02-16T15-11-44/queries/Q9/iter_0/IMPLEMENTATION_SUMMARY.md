# Q9 Implementation Summary (Iteration 0)

## Query
Product Type Profit Measure - TPC-H Query Q9

## Status: ✓ COMPLETE AND VALIDATED

### Files Generated
- **q9.cpp** - Complete C++ implementation (800+ lines)
- **results/Q9.csv** - Query results (175 groups × 25 nations × 7 years)

## Execution Plan

### Logical Plan
1. **Filter Stage**: Part table filtered by p_name LIKE '%green%' (~108K rows, 6% selectivity)
2. **Dimension Joins**:
   - Nation (25 rows) → hash table on n_nationkey
   - Supplier (100K rows) → join with nation
3. **Fact Joins**:
   - PartSupp (8M rows) → join with filtered part
   - Lineitem (60M rows) → join with partsupp & supplier
   - Orders (15M rows) → join with lineitem on orderkey
4. **Computation**: Amount = l_extendedprice × (1 - l_discount) - ps_supplycost × l_quantity
5. **Aggregation**: GROUP BY (nation, year) → 175 result groups

### Physical Plan
- **Hash Tables**: 6 hash tables built (nation, part, supplier, partsupp, orders, lineitem results)
- **Join Strategy**: Hash joins with build side on smaller relations
- **Aggregation**: Hash aggregation (std::unordered_map) with 175 groups
- **Output**: Sorted by nation ASC, then year DESC
- **Parallelism**: Single-threaded for iteration 0 (no OpenMP)

## Performance Metrics

### Execution Time (Release Build)
- **Total**: ~16.7 seconds (wall clock)
- **CPU**: ~9.4 seconds
- **System**: ~1.9 seconds

### Profiling (with -DGENDB_PROFILE)
```
[TIMING] load: 858.53 ms           (mmap 6 tables, 7 columns)
[TIMING] hash_build: 152.17 ms     (build 6 hash tables)
[TIMING] part_filtered_rows: 108782 (6% selectivity)
[TIMING] partsupp_process: 432.01 ms (filter & hash)
[TIMING] ps_hash_entries: 435128
[TIMING] orders_hash: 2013.94 ms   (build orders hash table)
[TIMING] lineitem_join: 12329.90 ms (60M rows × 6 lookups)
[TIMING] lineitem_matched: 3261613 (5% of 60M)
[TIMING] agg_groups: 175
[TIMING] output: 0.49 ms
[TIMING] total: 15787.12 ms (computation only)
```

## Key Implementation Details

### Decimal Arithmetic
- **Scale Factor**: All decimals are scaled by 100 (e.g., 12.34 = 1234)
- **Computation Precision**: Keep intermediate results at scale 10000 to avoid division losses
- **Formula**: amount = (l_extendedprice × (100 - l_discount) - ps_supplycost × l_quantity) / 100
- **Output**: Divide by 10000 only when writing CSV (4 decimal places)

### Dictionary Encoding
- **p_name dictionary**: Loaded from `part/p_name_dict.txt` at runtime
- **n_name dictionary**: Loaded from `nation/n_name_dict.txt` at runtime
- **LIKE Pattern**: Substring matching for '%green%'

### Date Handling
- **Encoding**: Epoch days since 1970-01-01 (int32_t)
- **Year Extraction**: Custom algorithm computing complete years + months + days
- **Output**: YYYY-MM-DD format (only in CSV, not used in computation)

### Memory Layout
- All data accessed via mmap for zero-copy performance
- Hash tables pre-sized based on cardinality estimates
- No external dependencies beyond C++17 standard library

## Validation

### Test Result: ✓ EXACT MATCH
```
Expected: /home/jl4492/GenDB/benchmarks/tpc-h/query_results/Q9.csv
Actual:   /home/jl4492/GenDB/output/tpc-h/2026-02-16T15-11-44/queries/Q9/iter_0/results/Q9.csv

Binary comparison: IDENTICAL (176 lines, all rows and values match)
```

### Sample Results (First 3 groups)
```
nation,o_year,sum_profit
ALGERIA,1998,271504046.5508
ALGERIA,1997,457035986.9555
ALGERIA,1996,457125199.0414
```

## Compilation

### Standard Build (Release)
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o q9 q9.cpp
```

### Profile Build
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -o q9 q9.cpp
```

## Correctness Guarantees

1. **Single-table Predicates**: All applied before joins (filter part by name)
2. **Join Ordering**: Smallest filtered result first (nation → part → supplier → partsupp → lineitem → orders)
3. **Aggregation Correctness**: Full-precision arithmetic with delayed division
4. **Dictionary Resolution**: Runtime lookup ensures correctness across database versions
5. **Date Arithmetic**: Proper epoch conversion with leap year handling

## Optimization Opportunities (Future Iterations)

1. **Parallelization**: OpenMP on lineitem scan (60M rows, 64 cores available)
   - Expected speedup: 40-50x on lineitem join phase
   
2. **Pre-built Indexes**: Leverage hash_multi_value indexes for partition joins
   - Orders index (442 MB) available but requires binary format parsing
   
3. **Vectorization**: SIMD on hash probes and arithmetic
   - Expected speedup: 2-4x on lookup-heavy operations
   
4. **Partitioned Hash Join**: For lineitem (60M), partition by hash to fit in L3 cache
   - Expected improvement: Better cache locality, fewer TLB misses

## Notes

- This is iteration 0 focusing on **correctness** over performance
- Single-threaded execution wastes 63/64 cores (63x potential improvement)
- The lineitem_join phase (60M rows) is the bottleneck consuming 78% of total time
- Hash table construction on dimension tables is very fast (<2s total)
- Output formatting uses Windows line endings (\r\n) to match expected format

