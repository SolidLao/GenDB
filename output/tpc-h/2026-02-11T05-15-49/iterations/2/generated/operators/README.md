# GenDB Operator Library

This directory contains reusable physical database operators for query execution.

## Operators

### 1. **scan.h** - Parallel Table Scan
Implements morsel-driven parallel table scans with predicate pushdown.

**Key Features:**
- Thread-local processing with hardware_concurrency() threads
- Predicate filtering during scan (avoids materialization)
- Returns vector of matching row indices

**Usage Pattern:**
```cpp
// Example: Filter rows where l_shipdate <= cutoff
auto predicate = [&](size_t i) {
    return l_shipdate.data[i] <= cutoff_date;
};
auto matching_rows = operators::parallel_scan(num_rows, predicate);
```

**Used in:** Q1, Q3, Q6 (implicitly - filter during aggregation/join)

---

### 2. **hash_agg.h** - Parallel Hash Aggregation
Implements thread-local hash aggregation with lock-free execution.

**Key Features:**
- Thread-local hash tables (no locking during aggregation)
- Generic key/aggregate state types with custom hash functions
- Single merge phase after parallel execution
- Supports += operator for aggregate merge

**Usage Pattern:**
```cpp
// Define aggregate function
auto agg_fn = [&](size_t i, Q1GroupKey& key, Q1AggResult& delta) {
    key = {l_returnflag.data[i], l_linestatus.data[i]};
    delta.sum_qty = l_quantity.data[i];
    delta.count = 1;
    // ... set other fields
};

// Execute parallel aggregation
ParallelHashAgg<Q1GroupKey, Q1AggResult, Q1GroupKeyHash> agg_op;
auto results = agg_op.execute(num_rows, agg_fn);
```

**Used in:** Q1 (GROUP BY l_returnflag, l_linestatus), Q3 (GROUP BY orderkey, orderdate, shippriority)

**Performance:** Near-linear scaling with thread count (tested 64 threads on 60M rows)

---

### 3. **hash_join.h** - Hash Join
Implements build-probe hash join with parallel probe phase.

**Key Features:**
- Build phase: constructs hash table from smaller relation
- Probe phase: parallel lookup and join with larger relation
- Supports predicate pushdown on both sides
- Generic key type and payload type

**Usage Pattern:**
```cpp
// Define payload type
struct OrderPayload {
    int32_t orderdate;
    int32_t shippriority;
};

// Build hash table on orders (filtered)
HashJoin<int32_t, OrderPayload> join_op;
join_op.build_filtered(
    o_orderkey.size,
    [&](size_t i) { return o_orderkey.data[i]; },  // extract key
    [&](size_t i) { return OrderPayload{o_orderdate.data[i], o_shippriority.data[i]}; },  // extract payload
    [&](size_t i) { return o_orderdate.data[i] < cutoff; }  // predicate
);

// Probe with lineitem (parallel)
auto join_results = join_op.probe_parallel_filtered(
    l_orderkey.size,
    [&](size_t i) { return l_orderkey.data[i]; },  // extract key
    [&](size_t i) { return l_shipdate.data[i] > cutoff; }  // predicate
);

// join_results contains tuples: (probe_row_idx, build_row_idx, payload)
```

**Used in:** Q3 (customer-orders join, orders-lineitem join)

**Performance:** Build: O(n), Probe: O(m) with near-perfect parallel scaling

---

## Design Principles

1. **Reusability First:** Operators are templated and work across multiple queries
2. **Composability:** Clean interfaces allow pipelining operators
3. **Correctness over Performance:** Simple, correct baseline implementations
4. **Separation of Concerns:**
   - Operators handle algorithms (hash join, hash aggregation)
   - Execution Optimizer handles parallelism/SIMD tuning
   - I/O Optimizer handles storage access patterns
   - Join Order Optimizer handles multi-way join ordering

## Query-Operator Mapping

| Query | Operators Used |
|-------|----------------|
| Q1 | Parallel scan + Hash aggregation |
| Q3 | Hash join (2x) + Hash aggregation |
| Q6 | Parallel scan + Simple aggregation |

## Operator Selection Rules

- **Hash Aggregation:** Medium-high cardinality (100-10M groups), unordered input
- **Hash Join:** Unordered data, equi-joins, build side < memory
- **Parallel Scan:** Large tables (>100K rows), any predicate

## Future Extensions

For Phase 2 (optimization), potential algorithm-level changes:
- **Sorted Aggregation:** For pre-sorted input or very low cardinality (<10 groups)
- **Sort-Merge Join:** For pre-sorted inputs or band joins
- **Adaptive Operators:** Runtime switching based on observed cardinality

All operators support the standard execution model: columnar storage → mmap → parallel execution → merge results.
