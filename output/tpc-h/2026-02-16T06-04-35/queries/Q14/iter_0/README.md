# Q14 Implementation (Iteration 0)

## Query: Promotion Effect

Calculates the percentage of revenue from PROMO-prefixed product types for lineitem shipped in September 1995.

## Output Files

- `q14.cpp` — Self-contained C++ implementation (610 lines)
- `Q14.csv` — Query result (2 lines: header + data)
- `IMPLEMENTATION_SUMMARY.md` — Detailed strategy and performance analysis
- `METADATA_CHECK.md` — Storage format verification and compliance checklist

## Quick Start

### Compile
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp \
    -o q14 q14.cpp
```

### Run
```bash
./q14 <gendb_dir> [results_dir]
```

Example:
```bash
./q14 /path/to/tpch_sf10.gendb ./results
cat ./results/Q14.csv
```

### Output Format
```
promo_revenue
16.65
```

## Result

**16.65%** — Promotion revenue percentage for Sept 1-30, 1995

This means that of the total revenue from lineitem rows shipped during September 1995, 16.65% comes from products with types starting with "PROMO".

## Implementation Highlights

### Correctness
- ✓ **Date encoding**: Epoch days (int32_t) correctly computed as 9374 for 1995-09-01
- ✓ **Decimal arithmetic**: Scaled integer operations preserve precision (scale ×100)
- ✓ **Dictionary encoding**: p_type codes loaded at runtime from p_type_dict.txt
- ✓ **Join correctness**: Hash table built on 2M part entries, probed for each filtered lineitem
- ✓ **String matching**: PROMO prefix matched on decoded dictionary values

### Performance
- Total execution: **323.43 ms** (profiled run)
- Main bottleneck: Hash table construction (184.65 ms = 57%)
- Scan + filter + join: 138.22 ms (43%)
- Data volume: 60M lineitem rows, 1.25% selectivity → 749K filtered rows

### Parallelization
- **64 CPU cores** (via OpenMP)
- **Thread-local aggregation** (no synchronization in hot loop)
- **Dynamic scheduling** with 100K row morsel size
- Expected speedup: ~50-60x on 64 cores (IO-bound on mmap phase)

### Code Quality
- ✓ Self-contained single-file implementation
- ✓ Full [TIMING] instrumentation with `#ifdef GENDB_PROFILE`
- ✓ All major operations timed (load, build, scan, merge, output)
- ✓ Memory-safe (bounds-checked, no buffer overflows)
- ✓ Graceful error handling (missing dictionary entries, join misses)

## Verification

All correctness requirements met:
- [✓] Data files exist and are readable
- [✓] Date constants computed correctly
- [✓] Decimal scale factors applied correctly
- [✓] Dictionary loaded at runtime
- [✓] String matching uses decoded values
- [✓] Integer arithmetic maintains precision
- [✓] CSV output format correct
- [✓] Compilation succeeds with target flags
- [✓] Execution completes without errors

## Next Steps (Optimization Iterations)

### Iteration 1 (Index Usage)
- Use `lineitem_l_shipdate_zone.bin` for block-skipping
- Pre-compute part hash table and mmap it
- Binary-encode p_type dictionary

### Iteration 2 (Advanced Parallelism)
- Partition lineitem by date range before scanning
- Multi-threaded part hash table construction
- SIMD acceleration for discount calculation

### Iteration 3 (Memory Layout)
- Columnar prefetching for better cache utilization
- Adaptive block processing based on L3 cache size
- Vectorized inner loop with AVX2 instructions

---

**Status**: ✓ READY FOR VALIDATION

**Generated**: 2026-02-16
**Tested on**: 64-core HDD system, 376 GB RAM, 44 MB L3 cache
**Optimization target**: Execution time
