# Memory Layout Optimization

## What It Is
Organizing data structures to maximize CPU cache utilization and memory bandwidth. Modern CPUs have cache hierarchies (L1: 32KB, L2: 256KB, L3: 8MB+) with 64-byte cache lines. Poor layout causes cache misses (100+ cycle penalty).

## When To Use
- **Always**: Cache-conscious design is fundamental to performance
- **Hot Paths**: Critical query execution loops (scans, joins, aggregations)
- **Large Data**: When working set exceeds L1/L2 cache (most analytical queries)
- **NUMA Systems**: Multi-socket servers require NUMA-aware allocation
- **Vectorized Execution**: SIMD operations benefit from aligned, contiguous data

## Key Implementation Ideas
- **Cache line alignment (64 bytes)**: Align column chunks and contended data to 64-byte boundaries to avoid false sharing between threads
- **Struct-of-Arrays (SoA) vs Array-of-Structs (AoS)**: SoA layout touches only needed columns per query, achieving 3-5x better cache utilization than AoS
- **Column chunking for cache**: Break columns into chunks that fit in L1 cache (e.g., 4096 values) so processing stays cache-resident
- **Software prefetching**: Issue prefetch instructions ahead of pointer-chasing access patterns (e.g., hash table probes) to hide memory latency
- **NUMA-aware allocation**: Allocate data on the same NUMA node as the thread accessing it; remote access is 2-3x slower
- **Loop tiling / cache blocking**: Process data in tile-sized blocks that fit in L1/L2 cache to maximize reuse before eviction
- **Hot-cold data separation**: Keep frequently accessed fields in a compact struct; store rarely accessed metadata separately
- **SIMD-friendly alignment**: Align data to 32 bytes (AVX2) or 64 bytes (AVX-512) for efficient aligned SIMD loads
- **Padding for false sharing prevention**: Pad thread-local counters or accumulators to separate cache lines

## Performance Characteristics
- **SoA vs AoS**: 3-5x better cache hit rate for column-oriented queries
- **Prefetching**: 2x speedup for hash joins by hiding 100-cycle memory latency
- **Cache miss costs**: L1=4 cycles, L2=12 cycles, L3=40 cycles, DRAM=100+ cycles

## Pitfalls
- **Over-alignment**: Aligning small structs to 64 bytes wastes memory; use only for contended data
- **Prefetch distance tuning**: Too early (>100 instructions) causes eviction; too late (<10 instructions) gives no benefit
- **Ignoring NUMA**: Remote NUMA access is 2-3x slower; measure with `numastat`
