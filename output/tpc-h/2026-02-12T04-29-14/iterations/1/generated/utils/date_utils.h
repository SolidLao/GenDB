#pragma once

#include <cstdint>
#include <string>
#include <ctime>

namespace gendb {

// Convert date string (YYYY-MM-DD) to days since epoch (1970-01-01)
inline int32_t parse_date(const char* str) {
    int year, month, day;
    sscanf(str, "%d-%d-%d", &year, &month, &day);

    // Simple algorithm: count days since 1970-01-01
    // This assumes Gregorian calendar
    int days = 0;

    // Add days for complete years
    for (int y = 1970; y < year; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }

    // Add days for complete months in current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap) days_in_month[1] = 29;

    for (int m = 1; m < month; m++) {
        days += days_in_month[m - 1];
    }

    // Add remaining days
    days += day - 1;

    return days;
}

// Convert days since epoch to date string (YYYY-MM-DD)
inline std::string days_to_date_str(int32_t days) {
    // Simple conversion back
    int year = 1970;
    int remaining = days;

    while (true) {
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int year_days = leap ? 366 : 365;
        if (remaining < year_days) break;
        remaining -= year_days;
        year++;
    }

    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap) days_in_month[1] = 29;

    int month = 1;
    for (int m = 0; m < 12; m++) {
        if (remaining < days_in_month[m]) {
            month = m + 1;
            break;
        }
        remaining -= days_in_month[m];
    }

    int day = remaining + 1;

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

} // namespace gendb
