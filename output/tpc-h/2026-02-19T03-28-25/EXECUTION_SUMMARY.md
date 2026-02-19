# GenDB Storage Design & Ingestion Summary

**Execution Date:** 2026-02-18  
**Dataset:** TPC-H Scale Factor 10  
**Hardware:** 64 cores, 376GB RAM, HDD, AVX-512 SIMD

## 1. Storage Design

### Design Decisions
- **Format:** Binary columnar storage with mmap-based zero-copy access
- **Block Size:** 100K rows (optimized for 64 cores, ~1.5MB per block at 15-20 bytes/row)
- **Encodings:**
  - **DECIMAL columns:** int64_t with scale_factor=100 (never double)
  - **DATE columns:** int32_t (days since epoch 1970-01-01)
  - **STRING (low-cardinality):** Dictionary-encoded as int16_t codes
  - **STRING (high-cardinality):** Raw std::string with length-prefix encoding
- **Sort Order:**
  - lineitem: sorted by l_shipdate (most filtered column in Q1, Q3, Q6)
  - orders: sorted by o_orderdate (filtered in Q3)
  - Other tables: no explicit sorting (dimension tables or not heavily filtered)

### Dictionary Encoding
6 low-cardinality STRING columns encoded:
- nation.n_name (25 nations)
- region.r_name (5 regions)
- customer.c_mktsegment (5 market segments: AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY)
- orders.o_orderstatus (3 statuses: F, O, P)
- lineitem.l_returnflag (3 flags: A, N, R)
- lineitem.l_linestatus (2 statuses: F, O)

### Indexes Built
- **Zone Maps:** Coarse block-level (min/max) for range filters
  - lineitem.l_shipdate (600 blocks)
  - orders.o_orderdate (150 blocks)
- **Hash Indexes (Multi-Value):** For FK join columns
  - supplier.s_nationkey (25 keys)
  - part.p_partkey (2M keys)
  - partsupp.ps_partkey (2M keys)
  - partsupp.ps_suppkey (100K keys)
  - customer.c_custkey (1.5M keys)
  - orders.o_orderkey (15M keys)
  - orders.o_custkey (999K keys)
  - lineitem.l_orderkey (15M keys)

## 2. Ingestion Results

### Ingestion Performance
- **Total Rows Ingested:** 86.6M (across 8 tables)
- **Time:** 1m 56s (all tables in parallel)
- **Tables:**
  - nation: 25 rows
  - region: 5 rows
  - supplier: 100,000 rows
  - part: 2,000,000 rows
  - partsupp: 8,000,000 rows
  - customer: 1,500,000 rows
  - orders: 15,000,000 rows
  - lineitem: 59,986,052 rows
- **Output Size:** 15GB binary columnar storage

### Ingestion Approach
- **Parallelism:** 8 threads (one per table) for concurrent ingestion
- **Column-wise parallelism:** Columns written in sequence per row (single-threaded per table due to buffer management)
- **I/O:** 1MB buffered writes + mmap for input reading
- **Dictionary building:** Concurrent for each table, serialized per column

### Data Verification
✓ Date values: Correctly encoded (e.g., 1996-03-13 → 9568, 1998-09-02 → 10471)
✓ Decimal values: Properly scaled (e.g., 33078.90 → stored 3307890)
✓ Dictionary codes: 3 return flags, 2 line statuses, 5 regions/segments
✓ File sizes: Consistent with row counts (e.g., 59M rows × 4 bytes = 236MB for int32_t column)

## 3. Index Building Results

### Index Building Performance
- **Time:** 9.4s
- **Indexes Built:** 10 total (2 zone maps + 8 hash indexes)
- **Hash Index Stats:**
  - Largest: lineitem.l_orderkey (15M keys, capacity 33M, load factor 0.45)
  - Smallest: supplier.s_nationkey (25 keys, capacity 64)

### Hash Index Construction Method
- Sort-based grouping (not std::unordered_map per key)
- Open addressing with multiply-shift hash function
- Multi-value design: positions array + hash table mapping key → (offset, count)
- Load factor: ~0.5 for optimal space/performance

### Zone Map Precision
- lineitem: 600 blocks × 100K rows = 60M coverage
- orders: 150 blocks × 100K rows = 15M coverage
- Expected skip rate: 30-60% of blocks on typical range queries

## 4. Query Guides Generated

Per-query storage guides created in `/query_guides/`:
- **Q1_guide.md** (4.4 KB): Full lineitem scan + group aggregation on 2 columns (6 groups)
- **Q3_guide.md** (6.4 KB): 3-way join (customer-orders-lineitem) with complex filters
- **Q6_guide.md** (3.7 KB): Highly selective compound filter (2.5% final selectivity)
- **Q9_guide.md** (6.9 KB): Complex 6-way join with string filter (p_name LIKE)
- **Q18_guide.md** (5.1 KB): Materialized subquery + semi-join execution pattern

Each guide includes:
- Column reference (type, encoding, scale factors, file paths)
- Table statistics
- Query analysis (filters, joins, aggregations, output cardinality)
- Index usage patterns
- Performance optimization hints

## 5. Correctness Verification

### Date Encoding
- Self-test: 1970-01-01 → 0 ✓
- Sample: 1996-03-13 → 9568 ✓
- Boundary: 1998-12-01 → 10561 ✓

### Decimal Encoding
- Scale factor: 100 (maintain .2 precision)
- Sample: 33078.90 → 3307890 ✓
- No IEEE 754 doubles used ✓

### Dictionary Encoding
- n_name: 25 nations
- l_returnflag: 3 values (A, N, R)
- l_linestatus: 2 values (F, O)
- c_mktsegment: 5 values (AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY)

### Index Correctness
- Zone maps: min/max per block for range pruning
- Hash indexes: Multi-value design with position arrays for FK joins
- All indexes successfully constructed and written to disk

## 6. Files Generated

### Binary Data
- Location: `/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/`
- 8 subdirectories (one per table)
- 61 binary column files (.bin)
- 6 dictionary files (_dict.txt)
- 10 index files (.idx)
- **Total:** 77 files, 15GB

### Design & Code
- **storage_design.json** (2.8 KB): Schema, encodings, indexes, hardware config
- **generated_ingest/ingest.cpp** (16 KB): Data ingestion executable
- **generated_ingest/build_indexes.cpp** (7.8 KB): Index building executable
- **generated_ingest/Makefile**: Compilation rules
- **query_guides/*.md** (5 files, 26 KB): Per-query storage reference

### Compilation
- `ingest`: 72 KB executable (O2, C++17)
- `build_indexes`: 37 KB executable (O3 -march=native -fopenmp, C++17)

## 7. Recommendations for Query Optimization

### Q1 (Full Scan Aggregation)
- Vectorized SIMD filtering on l_shipdate (process 8 dates in parallel)
- Parallel scan with morsel-driven execution (100K rows per thread × 64 cores)
- Low-cardinality aggregation (6 groups) → array-based aggregation, not hash

### Q3 (Multi-Table Join)
- Early filter pushdown: customer (20%) → orders (8%) → lineitem (85%)
- Hash join build on filtered customer, probe with orders
- Zone maps eliminate 50%+ blocks from orders.o_orderdate scan
- Top-K: heap for 10 largest revenue, avoid full external sort

### Q6 (Compound Filter)
- Branch-free multi-predicate evaluation: date AND discount AND quantity
- SIMD batch processing: process 16 rows with 3 predicates in parallel
- ~60% blocks eliminated by zone map on l_shipdate
- Single SUM aggregation: thread-local accumulator + reduction

### Q9 (Complex Multi-Join)
- Semi-join on part (p_name LIKE filter) before partsupp/supplier joins
- Join ordering: smallest first (nation 25 → supplier 100K → part 100K filtered)
- Parallel scan of lineitem split across cores; hash aggregation for 200 groups

### Q18 (Subquery + Semi-Join)
- Two-phase: (1) Materialize subquery (50K qualifying orders); (2) Filter + join
- Hash set for subquery result: O(1) membership test vs O(N) scan
- Late materialization: load customer names only for 50K qualifying rows
- Partial sort for top-100: heap or k-way merge

## 8. Next Steps (Phase 2)

Query planners and code generators should:
1. Read storage_design.json for table schema, encodings, indexes
2. Consult per-query guides (Q1_guide.md, etc.) for column details
3. Use zone maps for block-level filtering (saves 20-60% I/O)
4. Leverage hash indexes for FK joins (multi-value design)
5. Apply early filter pushdown before large scans
6. Parallelize across 64 cores with morsel-driven execution

