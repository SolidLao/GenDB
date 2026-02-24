---
name: gendb-code-patterns
description: GenDB C++ code patterns and file structure templates. Load when generating or optimizing C++ query code. Covers file structure, mmap access, GENDB_PHASE timing, CSV output, compilation flags, utility headers.
user-invocable: false
---

# Skill: GenDB Code Patterns

## When to Load
Code Generator, Query Optimizer — implementation patterns for GenDB queries.

## System Utilities (MANDATORY)
- `#include "date_utils.h"`: gendb::init_date_tables(), gendb::epoch_days_to_date_str(),
  gendb::date_str_to_epoch_days(), gendb::extract_year(), gendb::extract_month(),
  gendb::add_years(), gendb::add_months(), gendb::add_days().
  NEVER write custom date conversion functions.
- `#include "timing_utils.h"`: GENDB_PHASE("name") for block-scoped RAII timing.

## GENDB_PHASE Timing Macro
RAII scoped timer. Prints `[TIMING] <name>: <ms> ms` on scope exit.
Required phases: total, data_loading, dim_filter, build_joins, main_scan, output.
Additional as needed: subquery_precompute, aggregation_merge, sort_topk.

## File Structure Template
```cpp
#include <...>           // Standard includes
#include "date_utils.h"  // GenDB system utilities (as needed)
#include "timing_utils.h"

// Helper structs, constants, functions

void run_<query_id>(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();  // if date operations needed
    { GENDB_PHASE("data_loading"); /* mmap + madvise */ }
    { GENDB_PHASE("dim_filter"); /* dimension filters */ }
    { GENDB_PHASE("build_joins"); /* hash table construction */ }
    { GENDB_PHASE("main_scan"); /* fact table scan with fused ops */ }
    { GENDB_PHASE("output"); /* CSV writing */ }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) { std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl; return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_<query_id>(gendb_dir, results_dir);
    return 0;
}
#endif
```

## mmap Data Access Pattern
```cpp
int fd = open(path, O_RDONLY);
struct stat st; fstat(fd, &st);
auto* col = reinterpret_cast<const T*>(mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
madvise((void*)col, st.st_size, MADV_SEQUENTIAL);  // for sequentially-scanned columns
posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
size_t n = st.st_size / sizeof(T);
```
For MAP_POPULATE usage decisions (hot vs cold, selective vs full scan), see data-loading skill.
Do NOT copy mmap'd data into std::vector. Do NOT use read()/fread().

## CSV Output Format
- File: `results_dir/<QUERY_ID>.csv`
- Comma delimiter (`,`), header row matching expected column names
- Numeric precision: 2 decimal places for monetary values
- Date output: YYYY-MM-DD format via gendb::epoch_days_to_date_str()

## Compilation Flags
- Standard: `g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -I<utils_path>`
- Library mode: add `-DGENDB_LIBRARY` (suppresses main())

## Key Rules
- Standalone hash structs only. NEVER `namespace std { template<> struct hash<...> }`.
- Delta-encoded columns: apply cumulative sum before use.
- Hash table init: std::fill, NEVER memset for multi-byte sentinels.
- All hash tables: bounded probing (for-loop, not while).
- NEVER read .tbl files. Only `.gendb/` binary columns via mmap.
