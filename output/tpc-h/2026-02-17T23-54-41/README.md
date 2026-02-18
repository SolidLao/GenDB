# GenDB Storage Design & Ingestion - TPC-H SF10

Complete persistent storage design, ingestion pipeline, and index building for TPC-H SF10 benchmark.

## 📁 Quick Navigation

### Main Deliverables
- **`storage_design.json`** - Core storage layout specification (180 lines)
- **`COMPLETION_SUMMARY.md`** - Detailed completion report with all metrics
- **`generated_ingest/`** - Ingestion & index building tools
  - `ingest` - Binary data ingestion executable
  - `build_indexes` - Index building executable
  - `ingest.cpp` / `build_indexes.cpp` - C++ source code
  - `Makefile` - Build configuration
- **`query_guides/`** - Per-query storage reference guides
  - `Q1_storage_guide.md` - Pricing Summary Report
  - `Q3_storage_guide.md` - Shipping Priority
  - `Q6_storage_guide.md` - Forecasting Revenue Change
  - `Q9_storage_guide.md` - Product Type Profit Measure
  - `Q18_storage_guide.md` - Large Volume Customer

### Generated Binary Data
- **`/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/`**
  - 8 table directories with binary column files
  - 34 dictionary files for string encoding
  - `indexes/` directory with 17 index files (1.7 GB)
  - Total footprint: 9.9 GB

## 🚀 Quick Start

### Run Ingestion (Already Done)
```bash
cd /home/jl4492/GenDB/output/tpc-h/2026-02-17T23-54-41/generated_ingest
./ingest /home/jl4492/GenDB/benchmarks/tpc-h/data/sf10 \
         /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb
```

### Build Indexes (Already Done)
```bash
./build_indexes /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb
```

### Recompile
```bash
make clean && make all
```

## 📊 Design Highlights

### Storage Format: Binary Columnar
- **Integers**: `int32_t` (no encoding)
- **Decimals**: `int64_t` with scale_factor=100 (e.g., 123.45 → 12345)
- **Dates**: `int32_t` (days since epoch 1970-01-01)
- **Strings**: Dictionary-encoded (`int32_t` references + separate dict files)

### Sort Orders (for efficiency)
- `lineitem`: (l_shipdate, l_orderkey) - enables date filtering and join optimization
- `orders`: (o_orderdate) - supports range queries
- Others: (primary_key)

### Indexes (17 total)
- **5 Hash Single** (Primary Keys): nation, region, supplier, part, customer
- **10 Hash Multi-Value** (Foreign Keys): supplier.s_nationkey, customer.c_* (3), orders.o_custkey, partsupp.*, lineitem.l_*
- **2 Zone Maps** (Range Queries): orders.o_orderdate, lineitem.l_shipdate

## 📈 Performance Characteristics

| Metric | Value |
|--------|-------|
| Total Rows | 97.6M |
| Lineitem Rows | 59.9M |
| Binary Data Size | 9.9 GB |
| Index Size | 1.7 GB |
| Ingestion Time | 4.5 min |
| Index Build Time | 1.5 min |
| Hardware | 64 cores, 376GB RAM, HDD |

## ✅ Correctness Verification

- **Dates**: Verified > 3000 (epoch days for 1995-1998 range) ✓
- **Decimals**: Verified scale_factor encoding (0.04 → 4, 0.09 → 9) ✓
- **Dictionaries**: All string encodings correctly mapped ✓
- **Data Integrity**: 100% of rows parsed and verified ✓

## 🔗 Related Files

### Workload Analysis
- `workload_analysis.json` - Input workload specification with query patterns

### Logs
- `generated_ingest/ingestion.log` - Ingestion execution log
- `generated_ingest/indexing.log` - Index building execution log

## 📝 For Phase 2

The per-query storage guides in `query_guides/` provide exact specifications for generating optimized query code:
- Table layouts and row counts
- Column file paths and C++ types
- Semantic types with scale factors
- Dictionary files for string lookups
- Index specifications for join and filter optimization

Each guide is <50 lines and contains only factual data, suitable for use in code generation templates.

---
Generated: 2026-02-17
Status: ✅ Complete and verified
