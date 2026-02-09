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

### Cache Line Alignment (64 bytes)
```cpp
// BAD: False sharing between threads
struct Counter {
    std::atomic<int64_t> count; // 8 bytes
};
Counter counters[4]; // All on same cache line!

// GOOD: Each counter on separate cache line
struct alignas(64) Counter {
    std::atomic<int64_t> count;
    char padding[56];
};

// For column chunks
void* alloc_aligned(size_t size) {
    void* ptr;
    posix_memalign(&ptr, 64, size); // 64-byte aligned
    return ptr;
}
```

### Struct-of-Arrays (SoA) vs Array-of-Structs (AoS)
```cpp
// AoS: Bad for columnar operations
struct Row {
    int32_t id;
    int32_t age;
    double salary;
};
Row rows[1000000];

// SoA: Good for columnar operations
struct Table {
    int32_t* ids;
    int32_t* ages;
    double* salaries;
};

// Query: SELECT avg(age) WHERE age > 30
// AoS: Touch 16 bytes per row (id, age, salary)
// SoA: Touch 4 bytes per row (only age)
// 4x better cache utilization
```

### Column Chunking for Cache
```cpp
// Break columns into cache-friendly chunks
constexpr size_t CHUNK_SIZE = 4096; // Fits in L1 cache (32KB)

struct ColumnChunk {
    int32_t data[CHUNK_SIZE];
};

std::vector<ColumnChunk> column_chunks;

// Process one chunk at a time (stays in L1)
for (auto& chunk : column_chunks) {
    for (size_t i = 0; i < CHUNK_SIZE; i++) {
        result += chunk.data[i]; // Hot in L1
    }
}
```

### Prefetching
```cpp
// Software prefetch for pointer-chasing
void hash_probe_with_prefetch(HashTable& ht, int32_t* keys, size_t count) {
    constexpr size_t PREFETCH_DISTANCE = 16;

    for (size_t i = 0; i < count; i++) {
        // Prefetch future keys
        if (i + PREFETCH_DISTANCE < count) {
            size_t hash = hash_fn(keys[i + PREFETCH_DISTANCE]);
            __builtin_prefetch(&ht.buckets[hash], 0, 3); // 0=read, 3=high temporal locality
        }

        // Process current key (prefetched 16 iterations ago)
        auto* entry = ht.lookup(keys[i]);
    }
}
```

### NUMA-Aware Allocation
```cpp
#include <numa.h>

void* alloc_numa_local(size_t size) {
    int node = numa_node_of_cpu(sched_getcpu());
    return numa_alloc_onnode(size, node);
}

// Partition data across NUMA nodes
void parallel_scan(Table& table, size_t num_threads) {
    for (size_t tid = 0; tid < num_threads; tid++) {
        size_t start = tid * (table.rows / num_threads);
        size_t end = (tid + 1) * (table.rows / num_threads);

        // Allocate thread-local buffers on same NUMA node
        int node = tid / (num_threads / numa_num_nodes());
        void* buffer = numa_alloc_onnode(BUFFER_SIZE, node);
    }
}
```

### Loop Tiling for Cache Blocking
```cpp
// Matrix operations: tile to fit in L1/L2
void tiled_matrix_multiply(double* A, double* B, double* C, size_t N) {
    constexpr size_t TILE = 64; // 64×64 doubles = 32KB (fits L1)

    for (size_t i = 0; i < N; i += TILE) {
        for (size_t j = 0; j < N; j += TILE) {
            for (size_t k = 0; k < N; k += TILE) {
                // Process TILE×TILE block (stays in cache)
                for (size_t ii = i; ii < i + TILE; ii++) {
                    for (size_t jj = j; jj < j + TILE; jj++) {
                        for (size_t kk = k; kk < k + TILE; kk++) {
                            C[ii*N + jj] += A[ii*N + kk] * B[kk*N + jj];
                        }
                    }
                }
            }
        }
    }
}
```

### Hot-Cold Data Separation
```cpp
// Keep hot fields together, cold fields separate
struct TupleHot {
    int32_t key;
    int32_t value;
} __attribute__((packed));

struct TupleCold {
    char metadata[256]; // Rarely accessed
};

std::vector<TupleHot> hot_data;   // Dense, cache-friendly
std::vector<TupleCold> cold_data; // Sparse access

// Access patterns
for (auto& tuple : hot_data) {
    if (tuple.key == target) {
        // Only fetch cold data if needed
        auto& cold = cold_data[index];
    }
}
```

### SIMD-Friendly Layout
```cpp
// Ensure 32-byte alignment for AVX2 (256-bit)
alignas(32) int32_t column[1024];

void simd_sum(int32_t* data, size_t count) {
    __m256i sum_vec = _mm256_setzero_si256();

    for (size_t i = 0; i < count; i += 8) {
        __m256i vals = _mm256_load_si256((__m256i*)&data[i]); // Aligned load
        sum_vec = _mm256_add_epi32(sum_vec, vals);
    }
}
```

## Performance Characteristics
- **Cache Line Alignment**: Avoids false sharing (10x speedup for multi-threaded counters)
- **SoA vs AoS**: 3-5x better cache hit rate for column-oriented queries
- **Prefetching**: 2x speedup for hash joins (hide 100-cycle memory latency)
- **NUMA**: 2-3x speedup on multi-socket systems (avoid remote memory access)
- **Loop Tiling**: 5-10x for matrix operations (L1 hit rate: 50% → 95%)
- **Cache Misses**: L1 miss = 4 cycles, L2 miss = 12 cycles, L3 miss = 40 cycles, DRAM = 100+ cycles

## Real-World Examples

### DuckDB
- Vector size = 2048 (fits in L1 cache: ~16KB per column)
- Cache-conscious hash tables (linear probing, prefetching)
- SIMD-aligned column chunks

### ClickHouse
- Block size = 65K rows (sweet spot for L3 cache)
- NUMA-aware memory allocation for distributed queries
- SoA layout for all tables

### HyPer/Umbra
- Cache-conscious query compilation
- Generated code optimized for L1/L2 cache
- Prefetching in generated tight loops

### Google BigQuery
- Capacitor storage format: columnar, cache-aligned
- 64KB chunks per column stripe
- Separate hot/cold tiers

## Pitfalls
- **Over-Alignment**: Aligning small structs to 64 bytes wastes memory (use only for contended data)
- **Excessive Chunking**: Too many small chunks hurt sequential scan (prefetcher thrashing)
- **Ignoring NUMA**: Remote NUMA access is 2-3x slower; measure with `numastat`
- **Prefetch Too Early**: 100+ instructions ahead → evicted before use
- **Prefetch Too Late**: <10 instructions ahead → no benefit
- **AoS for Analytics**: Row layout kills cache for column-oriented queries
- **Unaligned SIMD**: `_mm256_loadu_si256()` is slower than `_mm256_load_si256()` on aligned data
