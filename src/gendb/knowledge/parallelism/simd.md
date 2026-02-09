# SIMD (Single Instruction Multiple Data)

## What It Is
SIMD instructions process multiple data elements in parallel using specialized CPU registers (128-bit SSE, 256-bit AVX2, 512-bit AVX-512). One instruction operates on 4-8 integers or floats simultaneously.

## When To Use
- Filtering large arrays (e.g., WHERE price > 100 on millions of rows)
- Aggregations (sum, min, max) over columnar data
- String comparisons and pattern matching
- Hash computation for multiple keys
- Bitmap operations (NULL checking, selection vectors)

## Key Implementation Ideas

### AVX2 Integer Comparison (8 x int32 at once)
```cpp
#include <immintrin.h>

// Filter: select rows where values[i] > threshold
void simd_filter(const int32_t* values, size_t count, int32_t threshold,
                 uint8_t* selection_vector) {
    __m256i thresh_vec = _mm256_set1_epi32(threshold);

    for (size_t i = 0; i < count; i += 8) {
        __m256i data = _mm256_loadu_si256((__m256i*)&values[i]);
        __m256i cmp = _mm256_cmpgt_epi32(data, thresh_vec);
        int mask = _mm256_movemask_epi8(cmp);
        selection_vector[i/8] = (mask != 0);
    }
}
```

### SIMD-Friendly Data Layout
- Store columns contiguously (columnar layout)
- Align to 32-byte boundaries: `alignas(32) int32_t data[N]`
- Pad arrays to multiples of vector width to avoid edge cases
- Use `std::vector<T, aligned_allocator<T, 32>>`

### Horizontal Aggregation
```cpp
// Sum 8 int32 values in a single AVX2 register
int32_t horizontal_sum(__m256i vec) {
    __m128i low = _mm256_castsi256_si128(vec);
    __m128i high = _mm256_extracti128_si256(vec, 1);
    __m128i sum128 = _mm_add_epi32(low, high);
    __m128i shuf = _mm_shuffle_epi32(sum128, _MM_SHUFFLE(1, 0, 3, 2));
    sum128 = _mm_add_epi32(sum128, shuf);
    shuf = _mm_shuffle_epi32(sum128, _MM_SHUFFLE(2, 3, 0, 1));
    sum128 = _mm_add_epi32(sum128, shuf);
    return _mm_cvtsi128_si32(sum128);
}
```

### Compiler Auto-Vectorization
```cpp
// Help compiler vectorize with restrict, alignment hints
void scalar_sum(const int32_t* __restrict__ data, size_t count, int64_t& sum) {
    #pragma omp simd reduction(+:sum)
    for (size_t i = 0; i < count; ++i) {
        sum += data[i];
    }
}
```

## Performance Characteristics
- **Speedup**: 4-8x for integer operations, 2-4x for floating-point (depends on memory bandwidth)
- **Cache**: Works best when data is already in L1/L2 cache
- **Memory Bandwidth**: Often bottlenecked by DRAM speed, not compute
- **Overhead**: Unaligned loads (_mm256_loadu) ~2x slower than aligned (_mm256_load)

## Real-World Examples
- **DuckDB**: SIMD filters, aggregations, and hash table probing
- **ClickHouse**: SIMD string comparisons, bitset operations
- **Vectorscan (Hyperscan)**: Regex matching with SIMD
- **Arrow**: SIMD-optimized kernels for filtering and transformations

## Pitfalls
- **Branching**: Avoid conditionals inside SIMD loops (breaks vectorization)
- **Unaligned Access**: 32-byte alignment critical for performance
- **Portability**: AVX2 not available on all CPUs (check with __builtin_cpu_supports("avx2"))
- **Horizontal Operations**: Shuffles and reductions are slow; minimize them
- **Scalar Tail**: Handle remaining elements (<8) separately in scalar code
- **Premature Optimization**: Profile first; SIMD helps only when memory-bound tasks fit in cache
