---
name: gendb-storage-format
description: GenDB binary columnar storage format specification. Load when designing storage, writing ingestion code, or understanding column encodings (types, DATE encoding, DECIMAL handling, dictionary columns, zone maps, byte-packing).
user-invocable: false
---

# Skill: GenDB Storage Format

## When to Load
Storage Designer, Code Generator — understanding binary column format.

## Binary Columnar Format
GenDB stores data in binary column files under `<name>.gendb/`:
- One file per column: `<table>/<column>.bin`
- Direct mmap access: `reinterpret_cast<const T*>(mmap(...))`
- Column count: `st.st_size / sizeof(T)`

## Type Mappings
| SQL Type | C++ Type | Notes |
|----------|----------|-------|
| INTEGER | int32_t | Direct binary |
| BIGINT | int64_t | Direct binary |
| DECIMAL(p,s) | double (default) or int64_t | double: values match SQL. int64_t: multiply by scale_factor |
| DATE | int32_t | Epoch days since 1970-01-01. Values >3000. |
| CHAR/VARCHAR | int16_t (dict-encoded) | Dictionary in `<column>_dict.txt` |

## DECIMAL Encoding

### Decision Framework

| Column Characteristics | Encoding | Rationale |
|-----------------------|----------|-----------|
| Max individual value < 10^13, no sub-cent precision needed | `double` (default) | Simple, sufficient precision for accumulation |
| Max individual value >= 10^13 | `int64_t` with scale_factor | Individual values exceed double's representable range for cents |
| Queries require exact decimal arithmetic (e.g., financial compliance) | `int64_t` with scale_factor | Eliminates all FP rounding |

### Detection Guidance for Storage Designer
- Sample column values during data profiling
- If sampled max > 10^12 OR column semantic type is "monetary" with high-value entities: flag for int64_t consideration
- Document the decision in Query Guide so Code Generator knows the encoding

### Note on Code-Level Recovery
Even with double storage, code-level int64_t accumulation can recover precision for SUM/AVG when individual values are <10^13 (see aggregation-optimization skill: Precision Management). But int64_t storage is more robust for extreme values.

### double (default)
Simple. Values match SQL directly. 15-16 significant digit precision.

### int64_t with scale_factor
Exact arithmetic. stored_value = SQL_value × scale_factor.
Every threshold, divisor, comparison must account for scale.

## DATE Encoding
- Days since epoch (1970-01-01). Epoch formula: sum days for complete years, months, add day-1.
- Self-test: parse_date("1970-01-01") must return 0.
- Use date_utils.h: init_date_tables(), epoch_days_to_date_str(), date_str_to_epoch_days(),
  extract_year(), extract_month(), add_years(), add_months(), add_days().

## Dictionary-Encoded Strings
- Binary column stores int16_t codes
- Dictionary file: `<column>_dict.txt` — one string per line, line index = code value
- Load at runtime: `std::vector<std::string>` from dict file
- Filter: find matching codes in dict, filter rows by code
- Output: `dict[code].c_str()`

## Byte-Packed Columns (Compression)
- Columns with <256 distinct values: stored as uint8_t + lookup table
- Lookup table: `<column>_lookup.bin` — maps uint8_t code to original value
- 4x I/O reduction for int32_t columns on cold start

## Zone Map Format
- File: `indexes/<column>_zone_map.bin`
- Layout: [uint32_t num_blocks] then per block [T min, T max, uint32_t block_size]
- row_offset is ROW index, not byte offset
