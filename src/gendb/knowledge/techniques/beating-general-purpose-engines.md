# Beating General-Purpose Engines

## What It Is
Generated code is specialized to one specific query — it should exploit this to outperform general-purpose engines like DuckDB, ClickHouse, or Umbra. If your generated code is slower than these engines, the plan-level design is fundamentally wrong.

## When to Use
Always. This is the philosophical foundation for all generated code.

## Specialization Advantages

General engines pay overhead that generated code eliminates entirely:

1. **No operator dispatch**: Everything is inlined into tight loops. No virtual function calls, no operator tree traversal, no tuple-at-a-time interpretation.

2. **No buffer management**: Direct mmap access to columnar files. No buffer pool, no page replacement, no pin/unpin.

3. **Full compiler optimization**: `-O3 -march=native -flto` lets the compiler optimize the entire query as a single compilation unit — auto-vectorization, constant propagation, dead code elimination.

4. **Known cardinalities**: Pre-size all data structures (hash tables, arrays, buffers) to exact or estimated sizes. No dynamic resizing.

5. **Known value domains**: When a column has a small known domain (e.g., small-domain dimension keys, status flags), use direct array lookup instead of hash tables. `value[key]` is faster than any hash probe.

6. **Fused pipelines**: Combine scan + filter + join + aggregate into tight loops. No materialization of intermediate results between operators.

## Key Implementation Ideas

### Pipeline Fusion
```cpp
// BAD: Separate operators with intermediate materialization
auto filtered = scan_and_filter(table);      // materializes vector
auto joined = hash_join(filtered, other);     // materializes another vector
auto result = aggregate(joined);              // materializes final

// GOOD: Fused pipeline — single pass, no intermediate materialization
for (int64_t i = 0; i < num_rows; i++) {
    if (col_a[i] >= threshold) {              // filter
        auto it = ht.find(col_key[i]);        // join probe
        if (it != ht.end()) {
            agg[it->group] += col_val[i];     // aggregate
        }
    }
}
```

### Pre-sized Data Structures
```cpp
// BAD: Default-sized, grows dynamically
std::unordered_map<int32_t, int64_t> agg;

// GOOD: Pre-sized from known cardinality
CompactHashTable<int32_t, int64_t> agg(estimated_groups * 2);
```

### Known-Domain Direct Lookup
```cpp
// BAD: Hash lookup for 25 nation keys
std::unordered_map<int32_t, std::string> nation_names;

// GOOD: Direct array — O(1), zero hash overhead
std::string nation_names[25];  // nationkey 0-24
```

## The Hand-Written Code Advantage

Hand-written C++ has fundamental advantages over ANY general-purpose engine:
1. **Query-specific data structures**: flat array for 25 keys, compact hash for 10K, pre-sized for 1M
2. **Compile-time constants**: the compiler eliminates all type dispatch and branch misprediction
3. **Loop fusion**: scan + filter + join + aggregate in ONE pass, zero intermediate materialization
4. **Hardware-specific compilation**: `-march=native` enables AVX2 auto-vectorization, `-flto` enables cross-function inlining
5. **Zero runtime overhead**: no parser, no catalog, no buffer pool, no lock manager

If your generated code is slower than a general-purpose engine, the PLAN is wrong.

## When Generated Code Loses to General Engines
If your code is slower, check these root causes (in order of impact):
1. **Wrong join order**: Building hash table on large side instead of small filtered side
2. **Missing predicate pushdown**: Scanning full table before filtering
3. **Wrong data structure**: `std::unordered_map` instead of flat array or open-addressing table
4. **Multiple passes**: Scanning same large table multiple times when one pass suffices
5. **Loop-based computation**: O(n) per-row operations that should be O(1) lookups
