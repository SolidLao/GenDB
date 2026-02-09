# Vectorized Execution

## What It Is
Vectorized execution processes data in batches (vectors) of 1024-2048 tuples at a time, rather than tuple-at-a-time, enabling better CPU cache utilization, SIMD vectorization, and reduced function call overhead.

## When To Use
- Analytical workloads with large scans and aggregations
- When CPU efficiency matters more than minimal latency
- Columnar storage layouts (aligned with columnar access patterns)
- Tight loops that benefit from compiler auto-vectorization
- When avoiding virtual function calls per tuple is critical

## Key Implementation Ideas

**Core Data Structure:**
```cpp
struct DataChunk {
    std::vector<Vector> columns;
    size_t size;  // Actual number of tuples (≤ VECTOR_SIZE)
    static constexpr size_t VECTOR_SIZE = 2048;
};

struct Vector {
    void* data;           // Array of primitives (int64_t[], double[], etc.)
    bool* nullmask;       // Bitmap for NULL values
    VectorType type;      // Physical type
};
```

**Selection Vectors:**
```cpp
struct SelectionVector {
    uint16_t* sel_vector;  // Indices of selected tuples
    size_t count;          // Number of selected tuples
};

// Filter operation produces selection vector
void filter_greater_than(Vector& input, int64_t threshold, SelectionVector& result) {
    int64_t* data = (int64_t*)input.data;
    result.count = 0;
    for (size_t i = 0; i < input.size; i++) {
        if (data[i] > threshold) {
            result.sel_vector[result.count++] = i;
        }
    }
}
```

**Operator Interface:**
```cpp
class VectorizedOperator {
public:
    virtual void Execute(DataChunk& input, DataChunk& output) = 0;
    virtual void GetChunk(DataChunk& output) = 0;  // Pull-based execution
};
```

**Avoiding Per-Row Overhead:**
```cpp
// BAD: Virtual function call per tuple
for (size_t i = 0; i < chunk.size; i++) {
    output[i] = func->evaluate(input[i]);
}

// GOOD: Batch processing with templated inner loop
template<typename T>
void add_constant_vectorized(Vector& input, Vector& output, T constant) {
    T* in_data = (T*)input.data;
    T* out_data = (T*)output.data;
    for (size_t i = 0; i < input.size; i++) {
        out_data[i] = in_data[i] + constant;  // Auto-vectorizes
    }
}
```

## Performance Characteristics
- **Speedup:** 3-10x vs tuple-at-a-time for scan-heavy workloads
- **Cache behavior:** Better spatial locality, prefetching works efficiently
- **Memory overhead:** Fixed-size buffers (8-32 KB per vector), predictable allocation
- **Branch prediction:** Selection vectors help avoid unpredictable branches
- **SIMD utilization:** Compiler can auto-vectorize tight loops (SSE, AVX2, AVX-512)

## Real-World Examples

**DuckDB:** Uses 2048-tuple vectors with columnar layout, selection vectors for filters
```cpp
// DuckDB's DataChunk
class DataChunk {
    vector<Vector> data;
    idx_t size;
    SelectionVector sel_vector;
};
```

**ClickHouse:** Block-based processing (8192-65536 rows per block)
```cpp
// ClickHouse Block
struct Block {
    ColumnsWithTypeAndName columns;
    size_t rows;
};
```

**PostgreSQL (with JIT):** Still tuple-at-a-time, but JIT compiles tight loops to reduce overhead

## Pitfalls
- **Small result sets:** Overhead of vectorization not worth it for <100 tuples
- **Complex expressions:** Deep expression trees still pay function call costs; need expression compilation
- **Variable-length data:** Strings/BLOBs require indirection, reducing vectorization benefits
- **Selection vector explosion:** Chaining many filters creates nested selection vectors; need to "materialize" occasionally
- **Over-large vectors:** >4KB vectors thrash L1 cache; sweet spot is 2048 tuples for 64-bit types
- **Non-contiguous access:** Random lookups (hash probes) benefit less from vectorization
