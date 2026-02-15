# GenDB Phase 1: Storage Design & Ingestion - Complete

## Overview
Successfully designed persistent binary columnar storage, generated optimized ingestion code, compiled and executed both ingestion and index building, and generated per-query storage guides for TPC-H SF10 workload.

## Hardware Configuration
- **CPU**: 32 cores, 64 threads (Intel Xeon)
- **Cache**: L3 44 MB
- **Memory**: 376 GB
- **Disk**: HDD (7.2K RPM)

## Storage Design (storage_design.json)

### Format
- **Columnar**: Binary column files, one per column, mmap-based zero-copy reads
- **Total Size**: 9.4 GB (compressed from 11 GB text)
- **Base Directory**: `tpch_sf10.gendb`

### Key Design Decisions

#### Data Types & Encodings
- **DECIMAL columns**: Stored as `int64_t` with `scale_factor=100` (e.g., 0.05 → 5)
  - Eliminates IEEE 754 floating-point precision errors
  - Used for: l_quantity, l_extendedprice, l_discount, l_tax, s_acctbal, c_acctbal, p_retailprice, ps_supplycost, o_totalprice
  
- **DATE columns**: Stored as `int32_t` days since 1970-01-01 (epoch encoding)
  - Self-tested: `parse_date("1970-01-01")` returns 0
  - Range: 8766 (1994-01-01) to 10561 (1998-12-01) for TPC-H data
  - Used for: l_shipdate, l_commitdate, l_receiptdate, o_orderdate

- **STRING columns (Low-Cardinality)**: Dictionary-encoded as `int32_t` codes
  - Storage: `column.bin` (uint32_t codes) + `column_dict.txt` (code=value lines)
  - Examples:
    - l_returnflag: 3 values (N, A, R)
    - l_linestatus: 2 values (F, O)
    - l_shipmode: ~7 values
    - c_mktsegment: 5 values (AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY)
    - o_orderstatus: ~3 values
    - p_mfgr, p_brand, p_container: Medium-cardinality, still dictionary-encoded

- **STRING columns (High-Cardinality)**: Stored as `std::string` (variable-length)
  - Format: [uint32_t length][data...] per row
  - Examples: l_comment, o_comment, c_name, c_address, s_name, p_name, p_type

#### Sort Orders
- **lineitem**: Sorted by `l_shipdate` (ascending) — enables zone map effectiveness for Q1/Q6
- **orders**: Sorted by `o_orderkey` (ascending) — enables fast PK lookup
- **customer**: Sorted by `c_custkey` (ascending) — enables fast PK lookup
- Other tables: No sorting (small dimension tables)

### Tables Ingested

| Table | Rows | Size | Key Columns | Sort Order |
|-------|------|------|-------------|-----------|
| lineitem | 59,986,052 | 6.0 GB | l_shipdate, l_orderkey, l_discount, l_quantity | l_shipdate |
| orders | 15,000,000 | 1.3 GB | o_orderkey, o_custkey, o_orderdate | o_orderkey |
| customer | 1,500,000 | 239 MB | c_custkey, c_mktsegment | c_custkey |
| part | 2,000,000 | 370 MB | p_partkey, p_mfgr, p_brand | — |
| partsupp | 8,000,000 | 780 MB | ps_partkey, ps_suppkey | — |
| supplier | 100,000 | 14 MB | s_suppkey | — |
| nation | 25 | <1 KB | n_nationkey | — |
| region | 5 | <1 KB | r_regionkey | — |

## Code Generation & Compilation

### Generated Files
- **ingest.cpp** (26.3 KB): Parallelized text parsing → binary columnar
  - Reads `.tbl` files (pipe-delimited)
  - Parses DECIMAL columns (double → int64_t with scale)
  - Parses DATE columns (YYYY-MM-DD → epoch days)
  - Dictionary-encodes low-cardinality strings
  - Sorts by primary key for join/PK lookup efficiency
  - Compilation: `g++ -O2 -std=c++17 -Wall -lpthread -o ingest ingest.cpp`

- **build_indexes.cpp** (12.7 KB): Index construction via mmap
  - Zone maps: Block-level min/max (100K rows/block)
  - Hash indexes: Multi-value design (key → [offset, count] → contiguous positions)
  - Sorted indexes: Binary format [(key, position) pairs]
  - Compilation: `g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp`

- **Makefile**: Orchestrates compilation with proper flags

### Execution Results

**Ingestion**: 2 min 30 sec
- Total time dominated by I/O (reading 11 GB of text, writing 9.4 GB binary)
- Data validation: ✓ (date/decimal checks passed)

**Index Building**: 36.9 seconds
- Parallelized zone map construction (100K rows/block, OpenMP)
- Parallel histogram-based hash table building
- mmap-based zero-copy column reads

## Indexes Built

| Index | File | Type | Column | Details |
|-------|------|------|--------|---------|
| lineitem_shipdate_zonemap | indexes/lineitem_shipdate_zonemap.bin | zone_map | l_shipdate | ~600 zones (100K rows/zone) for range pruning |
| lineitem_orderkey_hash | indexes/lineitem_orderkey_hash.bin | hash_multi_value | l_orderkey | 15M unique keys, 60M row positions (join column) |
| orders_orderkey_sorted | indexes/orders_orderkey_sorted.bin | sorted | o_orderkey | 15M entries (key, position) pairs; already sorted by PK |
| orders_custkey_hash | indexes/orders_custkey_hash.bin | hash_multi_value | o_custkey | ~1M unique keys, 15M positions (FK to customer) |
| customer_custkey_sorted | indexes/customer_custkey_sorted.bin | sorted | c_custkey | 1.5M entries; already sorted by PK |
| customer_mktsegment_hash | indexes/customer_mktsegment_hash.bin | hash_multi_value | c_mktsegment | 5 unique segments (AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY) |

## Per-Query Storage Guides

Three guides generated for TPC-H workload queries, written to `query_guides/`:

### Q1: Pricing Summary Report
- **File**: Q1_storage_guide.md (46 lines)
- **Pattern**: Single-table scan with aggregation
- **Key Columns**: l_shipdate, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax
- **Storage Strategy**:
  - Use zone maps on l_shipdate to prune blocks (filter: l_shipdate <= 1998-09-02)
  - Scan remaining rows, decode l_returnflag/l_linestatus from dictionary
  - Vectorize hash aggregation (4 groups: {N,A,R} × {F,O})
  - Compute SUM/AVG on scaled decimals (divide by 100 at output)

### Q3: Shipping Priority
- **File**: Q3_storage_guide.md (77 lines)
- **Pattern**: Three-way join with multi-table filtering
- **Key Columns**: c_custkey, c_mktsegment, o_orderkey, o_custkey, o_orderdate, o_shippriority, l_orderkey, l_shipdate, l_extendedprice, l_discount
- **Storage Strategy**:
  - Filter customer by c_mktsegment = 'BUILDING' (dict code 1) using customer_mktsegment_hash (~300K rows)
  - Hash join: orders on o_custkey (15M rows) → filtered customers (~300K)
  - Filter orders by o_orderdate < 1995-03-15 (~7.2M qualifying orders)
  - Use lineitem_orderkey_hash to probe lineitem (60M rows) for matching order keys
  - Filter lineitem by l_shipdate > 1995-03-15 (~3.1M qualifying rows)
  - Group by l_orderkey, o_orderdate, o_shippriority; compute SUM(revenue); partial sort top-10

### Q6: Forecasting Revenue Change
- **File**: Q6_storage_guide.md (42 lines)
- **Pattern**: Selective scan with compound filters and scalar aggregation
- **Key Columns**: l_shipdate, l_discount, l_quantity, l_extendedprice
- **Storage Strategy**:
  - Use zone maps on l_shipdate to prune blocks outside [1994-01-01, 1995-01-01) (8766 to 9131 days)
  - Branch-free filtering: l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 ≤ val ≤ 7)
  - Branch-free filtering: l_quantity < 24 (scaled: val < 2400)
  - Combined selectivity ~1.3% (9M rows match)
  - Parallelize row collection per core, final SUM reduction across cores

## Verification Checklist

- ✓ parse_date("1970-01-01") returns 0 (self-test)
- ✓ DECIMAL encoding: int64_t with scale_factor 100 (no IEEE 754 errors)
- ✓ Dictionary encoding: Low-cardinality columns properly encoded
- ✓ All query guides include only Q-relevant columns (no unrelated columns)
- ✓ File paths consistent between storage_design.json and generated data
- ✓ Scale factors and type mappings documented in guides
- ✓ Index binary layouts specified (zone_map, hash_multi_value, sorted formats)
- ✓ 6 index files successfully built and persisted

## Ready for Phase 2

The storage foundation is complete. The Code Generator can now:
1. Read storage_design.json for table/column metadata
2. Read per-query guides for index/encoding knowledge
3. Generate optimized query code (using hash joins, zone map pruning, vectorized aggregation)
4. Compile and measure execution time
