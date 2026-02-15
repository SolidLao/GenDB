# Join Ordering

## What It Is
Join ordering determines the sequence of binary joins to evaluate a multi-way join query. Optimal ordering minimizes intermediate result sizes and total work. This is a combinatorial optimization problem with exponential search space.

## Key Implementation Ideas
- **Greedy heuristic**: Iteratively join the pair of relations with the smallest estimated intermediate size; O(n^3) time, within 2-5x of optimal
- **Dynamic programming (exact)**: Enumerate all subset bipartitions and memoize optimal sub-plans; O(3^n) time, practical for n <= 12 relations
- **Left-deep trees**: Fully pipelined join chains with no intermediate materialization; preferred for OLTP and sequential execution
- **Bushy trees**: Allow independent subtrees to be joined in parallel; useful when join graph has disconnected components
- **Filter pushdown before joins**: Push selective single-table predicates down to base scans to reduce input cardinalities before any join
- **Selectivity estimation**: Estimate join output size using column distinct counts: selectivity = 1 / max(distinct_left, distinct_right)
- **Foreign key join detection**: When a foreign key relationship exists, result size equals the referencing table's cardinality—use this for tighter estimates
- **Cardinality feedback / adaptive re-optimization**: Monitor actual vs estimated cardinalities at runtime and re-optimize if estimates are far off
- **Genetic algorithms (GEQO)**: For large join counts (>12 tables), use randomized search heuristics instead of exact DP
- **Star schema optimization**: Join dimension tables first (small, filtered) then probe the fact table to minimize intermediate sizes
- **Join graph analysis**: Build a graph of join predicates to identify connected components, cliques, and chains for structural optimizations
- **Cross-product detection**: Identify and warn about missing join predicates that produce Cartesian products
- **Cost model integration**: Combine CPU cost, I/O cost, and memory cost into a unified cost metric for plan comparison
- **Interesting orderings**: Track sort orders produced by joins that benefit downstream operators (GROUP BY, ORDER BY, subsequent merge joins)

## Data-Driven Join Ordering via Sampling

When per-operation timing shows that **join is the dominant operation** (>50% of total time), use data sampling to empirically determine the best join order instead of relying on heuristic estimates.

### When to Use
- Multi-table joins (3+ tables)
- Join operation dominates query execution time
- Heuristic estimates may be inaccurate (e.g., correlated columns, skewed data)

### Technique

Generate a small C++ sampling program that:
1. Reads the first N rows (e.g., 100K) from each table involved in the join
2. Applies single-table predicates to filter each table's sample
3. Tries different join orders and counts intermediate result sizes
4. Reports which order produces the smallest total intermediate results

```cpp
// sampling_join_order.cpp - Join order sampling
// For each candidate join order, count intermediate result sizes

// Example for TPC-H Q3 (customer ⋈ orders ⋈ lineitem):
// Order A: customer → orders(filtered) → lineitem(filtered)
// Order B: orders(date filter) → customer → lineitem(shipdate filter)
// Order C: lineitem(shipdate filter) → orders(date filter) → customer

// For each order:
//   1. Apply single-table filters to get filtered sizes
//   2. Join first two tables, count result size
//   3. Join with third table, count result size
//   4. Total intermediate = sum of intermediate sizes

// Choose order with smallest total intermediate result size
```

### Example: TPC-H Q3

```sql
SELECT ... FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
```

Candidate orders to sample:
- **A**: customer(segment='BUILDING') → orders(date<cutoff) → lineitem(shipdate>cutoff)
- **B**: orders(date<cutoff) → customer(segment='BUILDING') → lineitem(shipdate>cutoff)
- **C**: lineitem(shipdate>cutoff) → orders(date<cutoff) → customer(segment='BUILDING')

Sampling reveals: Order B is typically best because the date filter on orders is very selective (~48% of rows), and joining the smaller filtered orders with customer (via FK) produces a small intermediate that probes efficiently into lineitem.

### Implementation Steps
1. Compile and run the sampling program (takes <1 second on 100K row samples)
2. Parse output to get intermediate result counts for each order
3. Implement the query using the empirically-best join order
4. Verify correctness after reordering

## Sampling Program Template

For automated join order determination, see `joins/sampling-program-template.md` which provides:
- Complete C++ sampling program template
- Handling of encoded columns (dictionary, delta, date, decimal)
- Greedy heuristic for 4+ table joins
- Expected runtime benchmarks

Use the sampling approach when:
- Per-operation timing shows join > 40% of total time
- Query has 2+ JOINs
- Current join order hasn't been empirically validated
