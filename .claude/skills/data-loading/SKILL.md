---
name: data-loading
description: Data loading and I/O optimization patterns. Load when implementing or optimizing mmap-based column access, cold/hot start tradeoffs, zone-map-guided prefetching, and column load ordering.
user-invocable: false
---

# Skill: Data Loading & I/O Optimization

## When to Load
All queries — I/O is often the dominant cost on cold start.

## Key Principles
- mmap with MAP_PRIVATE|MAP_POPULATE for zero-copy column access
- posix_fadvise(SEQUENTIAL) for columns scanned linearly
- Zone-map-guided selective madvise: only prefetch qualifying block byte ranges
- Column load order: zone maps → filter columns → join keys → payload columns
- Separate data_loading phase (GENDB_PHASE) from computation for diagnostics

## NUMA-Aware Large Allocation

For allocations >100MB, use mmap(MAP_ANONYMOUS) + parallel first-touch to distribute pages across NUMA nodes:
```
// Single-threaded std::fill causes all pages to fault on one NUMA node
// Instead:
auto* buf = (uint8_t*)mmap(nullptr, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
#pragma omp parallel for schedule(static)
for (size_t i = 0; i < size; i += 4096) {
    buf[i] = 0;  // first-touch distributes pages across NUMA nodes
}
```
This eliminates sequential page fault stall (measured: 49s for 512MB with single-threaded std::fill).

## Prefetch Granularity
- Align madvise calls to 4KB page boundaries
- For selective prefetch via zone maps, batch one madvise per qualifying block range (see indexing skill: Zone Maps)
- Avoid calling madvise per-row or per-small-range — syscall overhead dominates

## Cold vs Hot Tradeoffs
- Cold start (OS cache empty): I/O dominates. Minimize bytes read from disk.
  - Zone maps reduce I/O by skipping non-qualifying blocks
  - Byte-packing columns with <256 distinct values (uint8_t vs int32_t = 4x I/O reduction)
  - Parallel madvise across columns: `#pragma omp parallel for`
- Hot start (OS cache warm): Compute dominates. mmap faults are near-zero.
  - Focus on data structures, parallelism, algorithmic efficiency
- Avoid regressing hot performance to improve cold. Both should improve or stay neutral. If a tradeoff is necessary, prioritize the scenario that matches the deployment target (hot-run benchmarks vs cold-start production).

## GenDB-Specific mmap Pattern

MAP_POPULATE usage depends on optimization context:
- **Hot optimization target** (repeated benchmark runs, pages in OS cache): use `MAP_PRIVATE` only (no MAP_POPULATE). Eager page population wastes time when pages are already cached. Add `madvise(ptr, sz, MADV_SEQUENTIAL)` for sequential columns.
- **Cold optimization target** with full sequential scan: use `MAP_PRIVATE|MAP_POPULATE` for eager population to overlap I/O with computation.
- **Selective access** (<50% of file via zone maps): never MAP_POPULATE; use zone-map-guided madvise on qualifying ranges only.
- Always add `madvise(ptr, sz, MADV_SEQUENTIAL)` for sequentially-scanned columns.

```
int fd = open(path, O_RDONLY);
struct stat st; fstat(fd, &st);
// Hot path: MAP_PRIVATE only; Cold path: MAP_PRIVATE|MAP_POPULATE
auto* col = reinterpret_cast<const T*>(mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
madvise((void*)col, st.st_size, MADV_SEQUENTIAL);
posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
size_t n = st.st_size / sizeof(T);
```
Do NOT copy mmap'd data into std::vector. Do NOT use read()/fread() into malloc'd buffers.

## Selective Loading with Zone Maps
When zone maps exist and filter selectivity < 50%:
1. Load zone map file (small: ~num_blocks * 12 bytes)
2. Scan blocks, identify qualifying ranges
3. Issue madvise(MADV_WILLNEED) only on qualifying byte ranges
4. Skip madvise on non-qualifying blocks — saves disk I/O on cold start

## Technique Keywords
mmap, madvise, posix_fadvise, zone-map-guided loading, byte-packing,
parallel prefetch, column-ordered loading, late materialization, NUMA first-touch

## Reference Papers
- Harizopoulos et al. 2008 — "OLTP Through the Looking Glass"
- Abadi et al. 2013 — "The Design and Implementation of Modern Column-Oriented Database Systems"

## Common Pitfalls
→ See experience skill: P2 (copying mmap'd data), P4 (multiple passes), P13 (full WILLNEED), P14 (missing data_loading phase), P16 (unoptimized I/O), P22 (large allocation page faults)
