# Operator Fusion

## What It Is
Operator fusion combines multiple query operators (scan, filter, projection, aggregation) into a single tight loop, eliminating intermediate materialization and reducing memory traffic.

## When To Use
- Sequential operations that can share a tight inner loop (scan -> filter -> project)
- When intermediate results would be large and short-lived
- Streaming operators that don't require materialization
- When memory bandwidth is the bottleneck
- CPU-bound operations where cache locality matters

## Key Implementation Ideas
- **Loop fusion:** Merge scan, filter, and projection into a single loop so data is read once and processed in-place without intermediate buffers
- **Push-based fused pipelines:** Parent operators push tuples/chunks through a chain of fused operators via callbacks, avoiding per-tuple virtual dispatch
- **Selection vector fusion:** Filter produces a selection vector; downstream projection and aggregation operate only on selected indices without copying
- **Expression compilation for fusion:** Generate specialized code (JIT or templates) for the entire fused pipeline as a single function
- **Adaptive fusion decisions:** Fuse only when selectivity is reasonable, operators are streaming, and intermediate results have a single consumer
- **Vectorized fusion:** Combine batch-based processing with fusion by passing selection vectors between stages within the same DataChunk
- **Predicate reordering within fused loops:** Place the most selective or cheapest predicate first to short-circuit evaluation early
- **Fused aggregation:** Combine filter and group-by aggregation so qualifying tuples are directly inserted into the hash table without materialization

## Performance Characteristics
- **Speedup:** 2-5x for scan+filter+project pipelines vs materialized execution
- **Memory bandwidth reduction:** 50-90% (no intermediate buffers)
- **Cache behavior:** Input data stays hot in L1/L2 through the entire pipeline

## Pitfalls
- Cannot fuse if intermediate result is consumed by multiple operators
- Register pressure: too much fusion exhausts CPU registers, causing spills
- Pipeline breakers (hash joins, sorts) force materialization and break fusion chains
