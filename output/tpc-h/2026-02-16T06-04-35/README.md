# GenDB Phase 1: Storage Design & Ingestion
## TPC-H SF10 Benchmark (2026-02-16)

### Summary

Completed Phase 1 of the GenDB pipeline: persistent binary columnar storage design, parallelized ingestion code generation, index building, and per-query storage guides.

**Key Results:**
- **Ingestion**: 86.6M rows in 2m 35s (60M lineitem, 15M orders, 11.6M other)
- **Index Building**: 12 indexes (2 zone maps, 10 hash multi-value) in 1m 2s
- **Binary Output**: 9.2 GB (5.3 GB lineitem, 1.3 GB indexes, rest dimension tables)
- **Per-Query Guides**: 22 markdown files with file paths, encodings, and index layouts
- **Code Generated**: 804 lines ingest.cpp, 305 lines build_indexes.cpp

### File Organization

```
/home/jl4492/GenDB/output/tpc-h/2026-02-16T06-04-35/
├── storage_design.json              # Storage schema, encodings, indexes (127 lines)
├── EXECUTION_SUMMARY.txt            # Detailed execution report
├── README.md                        # This file
├── generated_ingest/
│   ├── ingest.cpp                   # Data ingestion (804 lines)
│   ├── build_indexes.cpp            # Index building (305 lines)
│   └── Makefile                     # Build targets
└── query_guides/
    ├── Q1_storage_guide.md          # Per-query file paths, types, encodings
    ├── Q2_storage_guide.md
    ├── ... (Q3-Q21)
    └── Q22_storage_guide.md

Binary data: /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/
├── lineitem/            # 5.3 GB (16 columns + 4 dictionaries)
├── orders/              # 1.2 GB (9 columns + 3 dictionaries)
├── customer/            # 234 MB (8 columns + 1 dictionary)
├── part/                # 176 MB (9 columns + 3 dictionaries)
├── partsupp/            # 1.1 GB (5 columns)
├── supplier/            # 15 MB (7 columns)
├── nation/              # <10 KB (4 columns)
├── region/              # <10 KB (3 columns)
└── indexes/             # 1.3 GB (zone maps + hash multi-value indexes)
```

### Key Design Decisions

#### 1. Binary Columnar Storage
- **Why**: Text parsing is ~10 minutes for 60M rows; binary columnar is <3 minutes.
- **Format**: Each column written as typed array (int32_t, int64_t, string with offsets)
- **I/O**: mmap for zero-copy reads; MADV_SEQUENTIAL for streaming access

#### 2. Date Encoding (int32_t epoch days)
- **Formula**: Cumulative days from 1970-01-01 using arithmetic (no mktime)
- **Self-Test**: `parse_date("1970-01-01")` must return 0 (verified ✓)
- **Example**: 1995-03-15 = 9204 days (not 19950315)

#### 3. Decimal Encoding (int64_t with scale_factor=100)
- **Why**: Avoids IEEE 754 rounding errors; keeps precision to 2 decimal places
- **Parse**: string → double → multiply by 100 → round → int64_t
- **Pitfall Avoided**: Using `std::from_chars<int32_t>` on "0.04" (reads only "0")

#### 4. Dictionary Encoding (int8_t/int16_t)
- **Columns**: returnflag (3), linestatus (2), mktsegment (5), shipmode (7), etc.
- **Space Savings**: ~80% vs inline strings (e.g., "BUILDING" → 1 byte)
- **Files**: `lineitem/l_returnflag_dict.txt` (code=value mapping)

#### 5. Zone Maps (block-level min/max)
- **Block Size**: 200K rows (lineitem), 150K rows (orders)
- **Purpose**: Skip entire blocks for range predicates (e.g., `l_shipdate <= '1998-09-01'`)
- **Lineitem**: 300 zones, compact binary format ~3.6 KB

#### 6. Hash Multi-Value Indexes (join columns)
- **Design**: Two-array approach (positions array + hash table)
- **Why**: Join columns have duplicates (~4 rows per order); one hash entry per unique key
- **Load Factor**: 0.5–0.6 for fast probing
- **Examples**:
  - `lineitem_l_orderkey`: 15M unique keys, 573 MB index
  - `lineitem_l_partkey`: 2M unique keys, 275 MB index
  - `lineitem_l_suppkey`: 100K unique keys, 232 MB index

### Ingestion Details

**Command:**
```bash
cd /home/jl4492/GenDB/output/tpc-h/2026-02-16T06-04-35/generated_ingest
make all
./ingest /home/jl4492/GenDB/benchmarks/tpc-h/data/sf10 /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb
```

**Performance:**
- lineitem: 155s (60M rows, includes dictionary building)
- orders: 28s (15M rows)
- customer, part, partsupp, supplier, nation, region: <10s total

**Post-Ingestion Verification (all passed ✓):**
- Date encoding: `parse_date("1970-01-01") = 0` ✓
- Decimal encoding: `parse_decimal("0.04", 100) = 4` ✓
- Spot-check dates: epoch range 8000–10000 (1992–1998) ✓
- Spot-check decimals: scaled by 100 ✓

### Index Building Details

**Command:**
```bash
cd /home/jl4492/GenDB/output/tpc-h/2026-02-16T06-04-35/generated_ingest
./build_indexes /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb
```

**Indexes Built (62s total):**
- **Zone Maps**: 2 indexes (l_shipdate, o_orderdate) — ~16ms
- **Hash Multi-Value**: 10 indexes
  - lineitem: l_orderkey (12.6s), l_partkey (30.5s), l_suppkey (4.2s)
  - orders: o_custkey (8.8s)
  - customer, partsupp, supplier, nation: <1s each

**Binary Layout (Hash Multi-Value):**
```
[uint32_t num_unique_keys]
[uint32_t hash_table_size]
[HashMultiValueEntry...] per slot (12 bytes: key + offset + count)
[uint32_t positions_array_size]
[uint32_t positions...] (all row positions grouped by key)
```

**Binary Layout (Zone Map):**
```
[uint32_t num_zones]
[ZoneMapEntry...] per zone (12 bytes: min_value + max_value + row_count)
```

### Per-Query Storage Guides

Each guide is a concise markdown file (~30-50 lines) containing:
- **Data Encoding**: Date/decimal/string encodings
- **Table Schemas**: Rows, block size, sort order
- **Columns**: File path, C++ type, semantic type, encoding, scale factor
- **Indexes**: Index name, type, binary layout, column(s)

**Example (Q1):**
```markdown
# Q1 Storage Guide
## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor = 100
- String encoding: Dictionary-encoded as int8_t

## Tables
### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey
| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
...
```

**All 22 Guides:** Q1 through Q22, each with tables used in that query.

### Hardware Configuration

```
CPU:       64 cores (Intel Xeon)
L3 Cache:  44 MB (shared)
Memory:    376 GB
Disk:      HDD (rotational)
SIMD:      AVX2, AVX512
```

### Compilation & Execution

**Ingestion:**
```bash
g++ -O2 -std=c++17 -Wall -lpthread -o ingest ingest.cpp
./ingest <input_dir> <output_dir>
```

**Index Building:**
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp
./build_indexes <gendb_dir>
```

### Verification Checklist

✓ Date parsing self-test (parse_date("1970-01-01") = 0)
✓ Decimal parsing tests (0.04 → 4, 99.99 → 9999)
✓ Spot-check ingested dates (epoch range 8000–10000)
✓ Spot-check ingested decimals (scaled by 100)
✓ Binary file counts (73 .bin files + 11 dictionaries)
✓ Index counts (12 total: 2 zone maps, 10 hash multi-value)
✓ Storage design JSON valid and complete
✓ All 22 query guides generated and cross-verified

### Next Steps (Phase 2: Query Execution)

Phase 2 will use these artifacts to generate query-specific executors:

1. Read per-query storage guides for file paths, encodings, indexes
2. Implement parallel morsel-driven scans with zone map pruning
3. Hash join using pre-built indexes on join columns
4. Dictionary decoding and decimal scaling in inner loops
5. Partial aggregation + global merge for GROUP BY
6. Compile with `-O3 -march=native` for SIMD auto-vectorization

### References

- **Knowledge Base**: `/home/jl4492/GenDB/src/gendb/knowledge/`
  - Persistent storage: `storage/persistent-storage.md`
  - Encoding handling: `storage/encoding-handling.md`
  - Hash indexes: `indexing/hash-indexes.md`
  - Zone maps: `indexing/zone-maps.md`

- **TPC-H Schema**: `/home/jl4492/GenDB/output/tpc-h/2026-02-16T06-04-35/../schema.sql`

- **Workload Analysis**: `/home/jl4492/GenDB/output/tpc-h/2026-02-16T06-04-35/workload_analysis.json`
