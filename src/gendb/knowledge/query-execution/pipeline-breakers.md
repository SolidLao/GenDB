# Pipeline Breakers

## What It Is
Pipeline breakers are operators that require full materialization of input data before producing output (e.g., hash joins, sorts, window functions), breaking the streaming data flow and requiring intermediate storage.

## Key Implementation Ideas
- **Classifying operators:** Each operator declares whether it is a pipeline breaker (hash join build, sort, distinct) or streaming (filter, projection, hash join probe)
- **Pipeline segmentation:** Split a physical plan into pipelines at every breaker; each pipeline is a source, a chain of streaming operators, and a sink (materialization point)
- **Hash join build/probe split:** Build phase materializes the inner table into a hash table (breaker); probe phase streams the outer table through lookups (non-breaker)
- **Sort with sink/finalize/source pattern:** Accumulate all input in a buffer (sink), sort after seeing everything (finalize), then stream sorted output to the next pipeline (source)
- **Memory-bounded materialization with spilling:** Track memory usage at each breaker; when exceeding a threshold, spill partitions to disk and process them in passes
- **Top-K optimization:** Replace full sort + limit with a bounded heap so only K tuples are materialized, converting a breaker into a quasi-streaming operator
- **Pipeline scheduling:** Execute pipelines in dependency order; build pipelines must complete before their corresponding probe pipelines start
- **Two-phase aggregation:** Pre-aggregate locally (streaming) before a global hash aggregation to reduce materialization size at the breaker
- **Partitioned spilling (grace hash join):** When hash tables exceed memory, partition both sides to disk and rejoin partition-by-partition
