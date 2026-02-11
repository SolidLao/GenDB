# Join Ordering

## What It Is
Join ordering determines the sequence of binary joins to evaluate a multi-way join query. Optimal ordering minimizes intermediate result sizes and total work. This is a combinatorial optimization problem with exponential search space.

## When To Use
- Queries with 3+ tables being joined
- Star schemas (fact table + dimension tables): dimension filters reduce fact table early
- Snowflake schemas with long join chains
- Complex queries where naive left-to-right ordering produces huge intermediates
- OLAP workloads with selective filters on dimension tables

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

## Performance Characteristics
- Optimal ordering can reduce query time by 10-1000x for complex queries
- DP is O(3^n), practical for n <= 12; greedy is O(n^3), within 2-5x of optimal
- Star schema with pushed filters: 100x+ speedup by filtering dimensions first

## Pitfalls
- **Stale statistics**: Outdated cardinality estimates lead to catastrophic join ordering
- **Ignoring correlation**: Assuming independence between predicates can underestimate selectivity by 10-100x
- **Over-optimization**: Spending seconds optimizing a millisecond query; limit optimizer time budget
