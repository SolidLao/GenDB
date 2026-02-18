# Date Operations: O(1) Lookup Tables

## What It Is
Precomputed lookup tables for extracting year, month, or day from epoch-day integers in O(1) time.

## When to Use
Any query that extracts year, month, or day from date columns stored as epoch-day integers (days since 1970-01-01).

## Anti-Pattern: Loop-Based Extraction
```cpp
// BAD: O(years) per row — catastrophic on large tables
int extract_year(int32_t epoch_days) {
    int year = 1970;
    while (epoch_days >= days_in_year(year)) {
        epoch_days -= days_in_year(year);
        year++;
    }
    return year;
}
```
On 60M rows with dates around year 2000, this loop executes ~30 iterations per row = 1.8 billion iterations total. This single function can dominate query execution time.

## Key Implementation Ideas

### Precomputed Year Lookup Table
```cpp
// Build once at startup — covers epoch days 0 (1970-01-01) to ~25000 (~2038)
static int16_t YEAR_TABLE[30000];   // indexed by epoch_day
static int8_t  MONTH_TABLE[30000];

void init_date_tables() {
    int year = 1970, month = 1, day_of_month = 1;
    const int days_per_month[] = {31,28,31,30,31,30,31,31,30,31,30,31};

    for (int d = 0; d < 30000; d++) {
        YEAR_TABLE[d] = year;
        MONTH_TABLE[d] = month;

        day_of_month++;
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int dim = days_per_month[month - 1] + (month == 2 && leap ? 1 : 0);
        if (day_of_month > dim) {
            day_of_month = 1;
            month++;
            if (month > 12) {
                month = 1;
                year++;
            }
        }
    }
}

// O(1) extraction — single array access
inline int extract_year(int32_t epoch_day) {
    return YEAR_TABLE[epoch_day];
}

inline int extract_month(int32_t epoch_day) {
    return MONTH_TABLE[epoch_day];
}
```

### Usage in Hot Path
```cpp
init_date_tables();  // once at startup

// In scan loop — O(1) per row
for (int64_t i = 0; i < num_rows; i++) {
    int year = YEAR_TABLE[date_col[i]];
    if (year >= 1995 && year <= 1996) {
        // process row
    }
}
```

### Alternative: Year Boundary Precomputation
When you only need to group by year, precompute year boundaries:
```cpp
// Precompute the epoch day where each year starts
int32_t year_start[100]; // year_start[i] = epoch day for Jan 1 of (1970+i)
// Then: year = binary_search(year_start, epoch_day)
// Or even simpler: just precompute the range for the years you need
```

## GenDB Utility Library
The `date_utils.h` header provides ready-to-use implementations of all date operations:
```cpp
#include "date_utils.h"

gendb::init_date_tables();  // call once at startup
int year = gendb::extract_year(epoch_day);    // O(1)
int month = gendb::extract_month(epoch_day);  // O(1)
int day = gendb::extract_day(epoch_day);      // O(1)

char buf[11];
gendb::epoch_days_to_date_str(epoch_day, buf);  // "YYYY-MM-DD"
int32_t days = gendb::date_str_to_epoch_days("1995-03-15");
```
**NEVER write custom date conversion functions.** Always use date_utils.h.

## Performance Impact
- Loop-based: ~30 iterations x 60M rows = 1.8B operations
- Lookup table: 1 array access x 60M rows = 60M operations
- Speedup: ~30x for date extraction alone
