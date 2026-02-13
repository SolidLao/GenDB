# Sort-Merge Join

## What It Is
Sort-merge join sorts both input relations on join keys, then merges them in a single linear scan. It exploits sorted order to match tuples without building hash tables, and is particularly efficient when inputs are pre-sorted or sorting cost is amortized.

## Key Implementation Ideas
- **Two-phase structure**: Phase 1 sorts both sides on join keys; Phase 2 performs a single linear merge pass
- **Skip redundant sorting**: Detect if input is already sorted (from index scan, prior ORDER BY) and skip the sort phase entirely
- **Index scan for sorted stream**: Use B-tree index scans to produce pre-sorted input, eliminating sort cost
- **Merge with duplicate handling**: When duplicate keys exist, mark the start of each duplicate run and emit all cross-product combinations
- **Band/range join optimization**: For inequality or range predicates, sort each side on the relevant bound and advance pointers to skip non-overlapping ranges
- **Cache-conscious chunked merge**: Process the merge in cache-sized chunks (~64KB) and use binary search to find the right-side starting position per chunk
- **External merge sort**: When data exceeds memory, sort in runs that fit in memory, write to disk, and merge runs using a priority queue
- **Parallel sorting**: Split each relation into partitions, sort partitions independently in parallel, then merge
- **Sort key prefix optimization**: Compare fixed-length prefixes first to short-circuit full key comparisons on mismatches
- **Streaming output**: Merge phase can emit results in a pipelined fashion without materializing intermediate results
- **Amortized sort cost**: If downstream operators also need sorted output (ORDER BY, GROUP BY), the sort is "free" for the join
