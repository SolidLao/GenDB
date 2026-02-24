---
name: indexing
description: Index usage patterns for GenDB queries. Load when designing and implementing storage and indexes, planning or implementing queries that can leverage pre-built indexes (zone maps, hash indexes, sorted indexes, bloom filters). Covers zone map skip logic, hash index probing, index construction, bloom filter as storage-time index.
user-invocable: false
---

# Skill: Indexing

## When to Load
Queries that can leverage pre-built indexes (zone maps, hash indexes, sorted indexes, bloom filters).

## Index Type Selection

| Query Pattern | Column Characteristics | Best Index |
|--------------|----------------------|------------|
| Equality point lookup | Any | Hash index |
| Range filter (BETWEEN, <, >) | Naturally clustered / sorted | Zone map |
| Range filter | Unsorted column | Sorted index (or no index if small) |
| Existence check (semi/anti-join) | Table where hash set would exceed LLC | Bloom filter (see data-structures skill) |
| Multi-column equality | Composite key | Hash index on composite key |

## Key Principles
- Zone maps: min/max per block, cheapest index. Skip blocks outside predicate range.
- Hash indexes: O(1) equi-join lookup. Multi-value design for 1:N foreign key joins.
- Sorted indexes: range queries, ordered iteration, merge joins.
- Always check Query Guide for available indexes before planning runtime data structures.

## Zone Maps

- Format: [uint32_t num_blocks][per block: min(T), max(T), uint32_t block_size]
- Skip logic: `col <= X` → skip if `block_min > X`; `col >= X` → skip if `block_max < X`
- row_offset is ROW index, not byte offset. Access as col[row_idx].
- Only effective on columns with natural clustering or sort order.
- For selective filters (<50% blocks qualifying): guide madvise to only prefetch qualifying blocks.

### Effectiveness Criterion
Zone maps useful when column has "natural clustering" — block-level min/max ranges are significantly narrower than global range. When NOT useful: columns with uniform random distribution (every block spans full range).

### Block Size
Default 65536 rows (matches typical morsel size and OS page alignment).

## Hash Indexes (Pre-Built)

- GenDB builds hash indexes during Phase 1 ingestion
- Format varies per run — always read Query Guide for exact binary layout
- Typical: [uint32_t num_entries][entries...][uint32_t num_buckets][bucket_offset...]
- mmap the index file, probe directly — zero build cost at query time
- For joins where runtime hash table build is a significant cost: prefer pre-built index (zero build cost)

## Bloom Filter as Storage-Time Index

### Construction During Ingestion
- Parallel build using atomic OR operations, ~12 bits/item (1% FPR)
- Storage: `indexes/<column>_bloom.bin` or `indexes/<composite>_bloom.bin`
- When to build: when query analysis shows semi-join or anti-join where equivalent hash set would exceed LLC
- See data-structures skill: Bloom Filter for sizing and implementation details

### Probe at Query Time
- mmap bloom file + bit-test (zero build cost at query time)
- Eliminates bloom construction phase entirely

## Join Cardinality and Index Safety

Before building a hash index for a join key, check join cardinality from the workload analysis:
- **1:1 join** (`left_unique: true` AND `right_unique: true`): single-value hash index is safe
- **1:N join** (either side `unique: false`): MUST use multi-value hash index (see hash-tables skill: Multi-Value Hash Table)
- **NEVER** build a single-value hash index on a non-unique key — this silently drops duplicate rows, producing incorrect query results

When `right_max_duplicates` is available, use it to choose between:
- Chain-based multi-value: good for variable-count duplicates
- Offset-based multi-value: better cache locality for iteration (preferred for most joins)

## Construction Guidelines (for Storage Designer)

- Sort-based grouping for hash index construction: sort positions by key, scan for boundaries
- NEVER use std::unordered_map<K, std::vector<uint32_t>> for construction
- Use multiply-shift hash, not std::hash
- Skip hash indexes on very small tables where linear scan is cache-resident and equally fast
- Build indexes in parallel (OpenMP) when independent

## Technique Keywords
zone maps, hash indexes, sorted indexes, B+ trees, bloom filters, zone-map-guided scan

## Reference Papers
- Graefe 2011 — "Modern B-Tree Techniques"
- Sidirourgos & Kersten 2013 — "Column Imprints: A Secondary Index Structure"

## Common Pitfalls
→ See experience skill: C19 (zone-map on unsorted column), P10 (ignoring zone maps), P11 (not using pre-built indexes)
