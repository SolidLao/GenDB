#pragma once
#include <cstdint>
#include <cstring>
#include <cstdio>

namespace gendb {

// Precomputed lookup tables for O(1) date extraction from epoch-day integers.
// Covers epoch days 0 (1970-01-01) through 29999 (~2052).
// Call init_date_tables() once at startup before any date operations.

static int16_t YEAR_TABLE[30000];
static int8_t  MONTH_TABLE[30000];
static int8_t  DAY_TABLE[30000];
static bool    _date_tables_initialized = false;

inline void init_date_tables() {
    if (_date_tables_initialized) return;
    int year = 1970, month = 1, day_of_month = 1;
    const int days_per_month[] = {31,28,31,30,31,30,31,31,30,31,30,31};

    for (int d = 0; d < 30000; d++) {
        YEAR_TABLE[d] = (int16_t)year;
        MONTH_TABLE[d] = (int8_t)month;
        DAY_TABLE[d] = (int8_t)day_of_month;

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
    _date_tables_initialized = true;
}

// O(1) extraction — single array access
inline int extract_year(int32_t epoch_day)  { return YEAR_TABLE[epoch_day]; }
inline int extract_month(int32_t epoch_day) { return MONTH_TABLE[epoch_day]; }
inline int extract_day(int32_t epoch_day)   { return DAY_TABLE[epoch_day]; }

// Convert epoch days to "YYYY-MM-DD" string (writes 10 chars + null terminator)
inline void epoch_days_to_date_str(int32_t epoch_day, char* buf) {
    int y = YEAR_TABLE[epoch_day];
    int m = MONTH_TABLE[epoch_day];
    int d = DAY_TABLE[epoch_day];
    std::sprintf(buf, "%04d-%02d-%02d", y, m, d);
}

// Convert "YYYY-MM-DD" string to epoch days
inline int32_t date_str_to_epoch_days(const char* s) {
    int y = 0, m = 0, d = 0;
    // Parse YYYY-MM-DD
    y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    m = (s[5]-'0')*10 + (s[6]-'0');
    d = (s[8]-'0')*10 + (s[9]-'0');

    // Sum days for complete years from 1970 to y-1
    int32_t days = 0;
    for (int yr = 1970; yr < y; yr++) {
        bool leap = (yr % 4 == 0 && yr % 100 != 0) || (yr % 400 == 0);
        days += leap ? 366 : 365;
    }
    // Sum complete months
    const int dpm[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    for (int mo = 1; mo < m; mo++) {
        days += dpm[mo - 1] + (mo == 2 && leap ? 1 : 0);
    }
    days += (d - 1);
    return days;
}

// ---------------------------------------------------------------------------
// Date arithmetic helpers — avoid off-by-one errors in interval computation.
// All inputs/outputs are epoch days (int32_t, same encoding as storage).
// ---------------------------------------------------------------------------

// Add N calendar days to an epoch day value.
// Use for INTERVAL 'N' DAY (e.g., DATE '1998-12-01' - INTERVAL '90' DAY).
inline int32_t add_days(int32_t epoch_day, int32_t n) {
    return epoch_day + n;
}

// Add N calendar months to an epoch day value (clamps to end-of-month if needed).
// Use for INTERVAL 'N' MONTH.
inline int32_t add_months(int32_t epoch_day, int months) {
    int y = YEAR_TABLE[epoch_day];
    int m = MONTH_TABLE[epoch_day];
    int d = DAY_TABLE[epoch_day];
    m += months;
    while (m > 12) { m -= 12; y++; }
    while (m < 1)  { m += 12; y--; }
    // Clamp day to end of target month
    const int dpm[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    int max_day = dpm[m - 1] + (m == 2 && leap ? 1 : 0);
    if (d > max_day) d = max_day;
    // Reconstruct epoch day
    char buf[11];
    std::sprintf(buf, "%04d-%02d-%02d", y, m, d);
    return date_str_to_epoch_days(buf);
}

// Add N calendar years to an epoch day value.
// Use for INTERVAL 'N' YEAR (e.g., DATE '1994-01-01' + INTERVAL '1' YEAR = 1995-01-01).
inline int32_t add_years(int32_t epoch_day, int years) {
    return add_months(epoch_day, years * 12);
}

} // namespace gendb
