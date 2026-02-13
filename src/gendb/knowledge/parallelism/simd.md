# SIMD (Single Instruction Multiple Data)

## What It Is

SIMD instructions process multiple data elements in parallel using specialized CPU registers (128-bit SSE, 256-bit AVX2, 512-bit AVX-512). One instruction operates on 4-8 integers or floats simultaneously.

## Key Implementation Ideas

- **Vectorized comparison**: use AVX2 to compare 8 int32 values against a threshold in a single instruction, producing a bitmask of matching rows

- **SIMD-friendly data layout**: store columns contiguously (columnar format) and align to 32-byte boundaries for optimal load performance

- **Array padding**: pad arrays to multiples of vector width to eliminate scalar tail-handling logic

- **Horizontal aggregation**: accumulate partial sums in SIMD registers across the loop, then reduce to a scalar result once at the end

- **Compiler auto-vectorization**: use `__restrict__` pointers, `#pragma omp simd`, and simple loop structures to help the compiler vectorize automatically

- **Selection vector output**: SIMD filters produce compact bitmasks or selection vectors that downstream operators consume without branching

- **Aligned vs unaligned loads**: prefer aligned loads (_mm256_load) over unaligned (_mm256_loadu) for ~2x better throughput

- **Branchless SIMD loops**: replace conditional logic with mask-based selection to keep the SIMD pipeline full
