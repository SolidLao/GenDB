# Query Planning: Logical → Physical → Code Pipeline

Do NOT jump from SQL to C++ code. Plan first, code second. Wrong operation ordering causes 10-100x slowdowns even with perfect operator implementations.

## Part 1: Logical Plan Rules

### Rule 1: Filter Before Everything
Apply all single-table predicates BEFORE any joins. This reduces cardinalities dramatically.
- A table with 60M rows and a selective filter (e.g., matching 1,600 rows) should be filtered FIRST
- Then use the filtered result as the build side of a hash join
- Never scan a large table fully when a filter can reduce it 10-100x

### Rule 2: Predicate Pushdown
Push predicates as close to the data scan as possible:
- Single-table predicates → apply during or immediately after scan
- Join predicates → apply during the join
- Post-aggregation predicates (HAVING) → apply after aggregation only

### Rule 3: Decorrelate Subqueries
Convert correlated subqueries into pre-computation + lookup:
- `WHERE x IN (SELECT ...)` → pre-compute the subquery result into a hash set, then probe
- `WHERE x > (SELECT AVG(...) FROM t WHERE t.key = outer.key)` → pre-compute all averages into a hash map keyed by `key`, then look up per outer row
- Never re-scan a table per outer row when you can pre-compute once

### Rule 4: Join Ordering
Process tables from smallest (after filtering) to largest:
- Estimate cardinality after applying all single-table predicates
- Build hash table on the smallest filtered result
- Probe with the next-smallest, producing an intermediate result
- Continue building on intermediates and probing with next table
- Goal: minimize intermediate result sizes at each step

### Rule 5: Semi-Join Reduction for Subqueries
Convert subquery patterns to hash semi-join or anti-join:
- `EXISTS (SELECT ...)` → hash semi-join: pre-compute inner query keys into hash set, probe outer table, emit on match
- `NOT EXISTS (SELECT ...)` → hash anti-join: same set, emit when NO match
- `col IN (SELECT ...)` → hash semi-join: pre-compute subquery result set, probe outer
- Apply when subquery evaluates a large inner table or is correlated
- See `techniques/semi-join-patterns.md` for implementation patterns

### Rule 6: Late Materialization
Defer loading of columns not needed in the current pipeline step:
- Load integer/numeric columns first, apply all predicates and joins
- Load string/varchar columns only for qualifying rows
- Apply when query outputs string columns but filters on integer columns
- Apply when selectivity is high (few qualifying rows relative to table size)
- See `techniques/late-materialization.md` for implementation patterns

## Part 2: Physical Plan Rules

### Join Implementation
- **Hash join** (default): Build hash table on smaller side, probe with larger side
- **Sort-merge join**: Only when both inputs are already sorted on the join key
- **Pre-built index lookup**: When a hash index or B+ tree index exists (check the Storage & Index Guide)
- Build side should fit in memory; if not, consider partitioned hash join

### Aggregation Data Structure Selection
Choose based on the number of distinct groups:
- **<256 groups**: Flat array indexed by group key (e.g., `result[group_key] += value`)
- **256–100K groups**: Open-addressing hash table (robin hood or linear probing). Pre-size with `reserve()`
- **>100K groups**: Partitioned aggregation with thread-local buffers + final merge
- **NEVER use `std::unordered_map`** for aggregation or joins with >10K entries — it is 2-5x slower than open-addressing due to pointer chasing and poor cache locality

### Scan Strategy
- **Full scan**: Default for non-selective queries or when no index exists
- **Zone map pruning**: For range predicates — skip blocks whose min/max fall outside the predicate range
- **B+ tree index**: For selective range queries (<10% selectivity)
- **Hash index**: For selective equi-joins or point lookups

### Parallelism Strategy
- **Scans**: OpenMP `parallel for` with morsel-driven chunking
- **Hash join probe**: OpenMP `parallel for` on the probe side (build side is sequential)
- **Aggregation**: Thread-local aggregation buffers, merge after parallel phase
- **Sort**: Parallel merge sort or parallel partitioned sort

## Part 3: Planning Template

Before writing any C++ code, produce a plan following these steps:

```
Step 1 (Logical): For each table, list predicates and estimate filtered cardinality
  Table X: predicate "col = 'VALUE'" → estimated N rows (from workload_analysis.json)
  Table Y: no single-table predicates → full table (M rows)

Step 2 (Logical): Determine join graph and ordering
  Smallest filtered result first → build hash table
  Probe with next table → intermediate result
  Continue until all joins complete

Step 3 (Logical): Identify subqueries to decorrelate
  Subquery S: correlated on key K → pre-compute into hash map<K, result>

Step 4 (Physical): For each join → hash join / sort-merge / index lookup
Step 5 (Physical): For each aggregation → flat array / open-addressing / partitioned
Step 6 (Physical): For each scan → full scan / zone map / index
Step 7 (Physical): Parallelism strategy per operation
```

Write this plan as a comment block at the top of the .cpp file, then implement the code to follow the plan exactly.

## Part 4: Advanced OLAP Optimization Principles

### Principle 1: Star Schema Optimization
Filter dimension tables first (small) → build hash on filtered dimensions → probe fact table (largest).
Never build hash table on the fact table. The fact table is always the probe side.

### Principle 2: Semi-Join Reduction
Before expensive multi-table joins, use bloom filters from small filtered dimensions to
pre-filter the fact table. Can eliminate 90%+ of fact table rows before the real join.

### Principle 3: Subquery Decorrelation Priority
Every correlated subquery MUST be decorrelated into pre-computation + lookup:
- Correlated: O(outer × inner) per-row re-evaluation
- Decorrelated: O(inner + outer) with one-time pre-computation
For self-referencing subqueries (inner references same table as outer): single-pass pre-computation.
Combined EXISTS + NOT EXISTS on same table: ONE pass, TWO output structures.

### Principle 4: Hash Table Build Cost Awareness
Building hash table on N rows costs ~N × (hash + insert + cache miss).
For N = 60M: 500-2000ms build time alone.
Alternatives: pre-built index via mmap (0ms build), filter first (reduce N), array for small domains.

### Principle 5: Parallel Aggregation Without Merge Bottleneck
Thread-local hash tables + sequential merge is slow for many groups.
- <256 groups: shared flat array with atomic add (no merge needed)
- 256–10K groups: pre-partition by key hash, each thread owns a partition
- >10K groups: concurrent hash table with CAS, or partition-based approach
