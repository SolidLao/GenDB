# Hash Aggregation

**When to use**: GROUP BY with medium-high cardinality (100–10M groups) where flat array indexing is not feasible.
**Impact**: 2-5x faster than `std::unordered_map` with open-addressing; near-optimal with correct sizing.

## Principle
- Pre-size open-addressing hash table at 75% load factor with power-of-2 sizing
- Store hash values alongside entries to speed up comparison during probing
- Pack aggregate state compactly — separate keys from values for cache locality
- For <256 groups, skip hash table entirely: use flat array indexed by group key
- For parallel aggregation, use thread-local tables + final merge (or atomic ops for small group counts)

## Pattern
```cpp
// Low cardinality (<256 groups): flat array
int64_t sum_by_group[MAX_GROUPS] = {};
int64_t count_by_group[MAX_GROUPS] = {};
for (int64_t i = 0; i < num_rows; i++) {
    int32_t gk = group_key[i];
    sum_by_group[gk] += value[i];
    count_by_group[gk]++;
}

// Medium-high cardinality: open-addressing hash table
CompactHashTable<int64_t, AggState> agg(estimated_groups);
for (int64_t i = 0; i < num_rows; i++) {
    auto* slot = agg.find_or_insert(group_key[i]);
    slot->sum += value[i];
    slot->count++;
}

// Parallel aggregation with thread-local merge
std::vector<CompactHashTable<int64_t, AggState>> local_tables(num_threads, estimated_groups);
#pragma omp parallel for
for (int64_t i = 0; i < num_rows; i++) {
    int tid = omp_get_thread_num();
    auto* slot = local_tables[tid].find_or_insert(group_key[i]);
    slot->sum += value[i];
    slot->count++;
}
// Merge local tables into global result
```

## Pitfalls
- Using `std::unordered_map` for >10K groups causes 2-5x slowdown from pointer chasing
- Forgetting to pre-size with `reserve()` causes expensive rehashing mid-aggregation
- Sequential merge of thread-local tables can bottleneck for many groups (>10K) — use partitioned approach
- Kahan summation required for floating-point aggregates to avoid precision loss over millions of rows
- Thread-local aggregation tables: each thread may see up to the full distinct key count (not total/nthreads), especially with dynamic/work-stealing scheduling. Size each thread's map for worst case.
- Use bounded probing (see compact-hash-tables.md) — never unbounded `while` loops in open-addressing tables.
