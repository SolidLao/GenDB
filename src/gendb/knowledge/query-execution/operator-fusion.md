# Operator Fusion

## What It Is
Operator fusion combines multiple query operators (scan, filter, projection, aggregation) into a single tight loop, eliminating intermediate materialization and reducing memory traffic.

## Key Implementation Ideas
- **Loop fusion:** Merge scan, filter, and projection into a single loop so data is read once and processed in-place without intermediate buffers
- **Push-based fused pipelines:** Parent operators push tuples/chunks through a chain of fused operators via callbacks, avoiding per-tuple virtual dispatch
- **Selection vector fusion:** Filter produces a selection vector; downstream projection and aggregation operate only on selected indices without copying
- **Expression compilation for fusion:** Generate specialized code (JIT or templates) for the entire fused pipeline as a single function
- **Adaptive fusion decisions:** Fuse only when selectivity is reasonable, operators are streaming, and intermediate results have a single consumer
- **Vectorized fusion:** Combine batch-based processing with fusion by passing selection vectors between stages within the same DataChunk
- **Predicate reordering within fused loops:** Place the most selective or cheapest predicate first to short-circuit evaluation early
- **Fused aggregation:** Combine filter and group-by aggregation so qualifying tuples are directly inserted into the hash table without materialization
