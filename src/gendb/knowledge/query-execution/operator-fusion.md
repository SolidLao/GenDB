# Operator Fusion

## What It Is
Operator fusion combines multiple query operators (scan, filter, projection, aggregation) into a single tight loop, eliminating intermediate materialization and reducing memory traffic.

## When To Use
- Sequential operations that can share a tight inner loop (scan → filter → project)
- When intermediate results would be large and short-lived
- Streaming operators that don't require materialization
- When memory bandwidth is the bottleneck
- CPU-bound operations where cache locality matters

## Key Implementation Ideas

**Basic Fusion Pattern:**
```cpp
// BEFORE: Separate operators with materialization
vector<Tuple> scan_results = scan_table();
vector<Tuple> filter_results = apply_filter(scan_results);
vector<Tuple> project_results = apply_projection(filter_results);

// AFTER: Fused into single loop
for (auto& tuple : scan_table()) {
    if (filter_predicate(tuple)) {
        emit(project_columns(tuple));
    }
}
```

**Push-Based Fused Pipeline:**
```cpp
class FusedScanFilterProject {
    Filter filter_;
    Projection projection_;

    void Execute(DataChunk& output) {
        DataChunk scan_chunk;
        scan_->GetChunk(scan_chunk);

        // Fused: filter creates selection vector, projection uses it
        SelectionVector sel;
        filter_.Apply(scan_chunk, sel);
        projection_.Apply(scan_chunk, sel, output);
    }
};
```

**Expression Compilation for Fusion:**
```cpp
// Generate specialized code for entire pipeline
std::string generate_fused_code() {
    return R"(
        void fused_pipeline(Table& table, OutputBuffer& output) {
            for (size_t i = 0; i < table.row_count; i++) {
                int64_t col1 = table.col1[i];
                int64_t col2 = table.col2[i];

                // Fused filter + projection
                if (col1 > 100 && col2 < 50) {
                    output.emit(col1 + col2);
                }
            }
        }
    )";
}
```

**Adaptive Fusion:**
```cpp
class AdaptivePipeline {
    bool should_fuse(Operator& op1, Operator& op2) {
        // Don't fuse if intermediate result is tiny
        if (op1.estimated_selectivity() < 0.01) return false;

        // Don't fuse blocking operators
        if (op1.is_pipeline_breaker()) return false;

        // Don't fuse if result reused multiple times
        if (op1.num_consumers() > 1) return false;

        return true;
    }
};
```

**Vectorized Fusion with Selection Vectors:**
```cpp
void fused_scan_filter_project(Table& table, DataChunk& output) {
    DataChunk scan_chunk;
    table.Scan(scan_chunk);

    // Filter produces selection vector (no materialization)
    SelectionVector sel;
    sel.count = 0;
    for (size_t i = 0; i < scan_chunk.size; i++) {
        if (evaluate_filter(scan_chunk, i)) {
            sel.sel_vector[sel.count++] = i;
        }
    }

    // Project only selected tuples
    for (size_t i = 0; i < sel.count; i++) {
        size_t idx = sel.sel_vector[i];
        output.data[i] = project(scan_chunk, idx);
    }
    output.size = sel.count;
}
```

## Performance Characteristics
- **Speedup:** 2-5x for scan+filter+project pipelines vs materialized
- **Memory bandwidth:** Reduces by 50-90% (no intermediate buffers)
- **Cache behavior:** Input data stays hot in L1/L2 cache through entire pipeline
- **Code bloat:** Fused operators generate more specialized code
- **Compilation time:** JIT/template instantiation overhead increases

## Real-World Examples

**DuckDB:** Aggressive operator fusion with push-based execution
```cpp
// DuckDB fuses scan → filter → project into single operator
class PhysicalFilter : public PhysicalOperator {
    void GetChunk(DataChunk& output) {
        input->GetChunk(input_chunk);
        // Apply filter directly to input, use selection vectors
        filter.Select(input_chunk, sel_vector);
        // Project using selection vector (fused)
        projection.Execute(input_chunk, sel_vector, output);
    }
};
```

**HyPer/Umbra:** Full query compilation with LLVM, entire pipeline in one function

**ClickHouse:** Limited fusion; prefers vectorized operators with explicit materialization points

**Spark/Catalyst:** WholeStageCodegen fuses operators into single JVM bytecode function

## Pitfalls
- **Complex predicates:** Deeply nested expressions reduce fusion benefits; compiler can't optimize
- **Mixed data types:** Fusion with type conversions can hurt SIMD vectorization
- **Multiple consumers:** Can't fuse if intermediate result is used by multiple operators
- **Debugging difficulty:** Fused code is harder to profile and debug
- **Register pressure:** Too much fusion exhausts CPU registers, causing spills
- **Pipeline breakers:** Hash joins, sorts, window functions require materialization
- **Over-fusion:** Fusing too many operators creates code bloat without performance gain
- **String operations:** Variable-length data and copies reduce fusion benefits
