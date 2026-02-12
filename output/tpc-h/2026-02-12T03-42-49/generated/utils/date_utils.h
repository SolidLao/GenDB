#pragma once

#include <cstdint>
#include <ctime>
#include <string>
#include <cstdio>

namespace gendb {

// Date utilities: convert dates to/from days since epoch (1970-01-01)
// All dates stored as int32_t internally

inline int32_t date_to_days(int year, int month, int day) {
    // Algorithm: days since 1970-01-01
    // Simplified calculation assuming Gregorian calendar
    int a = (14 - month) / 12;
    int y = year - a;
    int m = month + 12 * a - 3;
    int jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400;
    int epoch_jdn = 719468; // JDN for 1970-01-01
    return jdn - epoch_jdn;
}

inline int32_t parse_date(const char* str) {
    // Parse "YYYY-MM-DD" format
    int year, month, day;
    sscanf(str, "%d-%d-%d", &year, &month, &day);
    return date_to_days(year, month, day);
}

inline std::string days_to_date_str(int32_t days) {
    // Convert days since epoch back to YYYY-MM-DD
    // Inverse of date_to_days
    int jdn = days + 719468;

    int a = jdn + 32044;
    int b = (4 * a + 3) / 146097;
    int c = a - (146097 * b) / 4;
    int d = (4 * c + 3) / 1461;
    int e = c - (1461 * d) / 4;
    int m = (5 * e + 2) / 153;

    int day = e - (153 * m + 2) / 5 + 1;
    int month = m + 3 - 12 * (m / 10);
    int year = 100 * b + d - 4800 + m / 10;

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

} // namespace gendb
