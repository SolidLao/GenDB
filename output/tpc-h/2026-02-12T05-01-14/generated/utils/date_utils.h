#pragma once

#include <cstdint>
#include <string>
#include <cstdio>
#include <ctime>

// Date utilities: convert between DATE strings and int32_t (days since epoch)
// Epoch: 1970-01-01 = day 0

namespace gendb {

// Convert YYYY-MM-DD string to days since 1970-01-01
inline int32_t parse_date(const char* str) {
    int year, month, day;
    sscanf(str, "%d-%d-%d", &year, &month, &day);

    // Days since epoch calculation
    // Simplified: doesn't handle leap years perfectly, but good enough for TPC-H
    int days = (year - 1970) * 365 + (year - 1969) / 4;  // Add leap years

    // Days in each month (non-leap year)
    static const int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += month_days[m];
    }

    // Adjust for leap year
    if (month > 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
        days += 1;
    }

    days += day - 1;  // Day of month (0-indexed)

    return static_cast<int32_t>(days);
}

// Convert days since epoch to YYYY-MM-DD string
inline std::string days_to_date_str(int32_t days) {
    // Use system time functions for accurate conversion
    time_t t = days * 86400LL;
    struct tm* tm_info = gmtime(&t);
    char buffer[16];
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d",
             tm_info->tm_year + 1900,
             tm_info->tm_mon + 1,
             tm_info->tm_mday);
    return std::string(buffer);
}

// Convert (year, month, day) to days since epoch
inline int32_t date_to_days(int year, int month, int day) {
    int days = (year - 1970) * 365 + (year - 1969) / 4;

    static const int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += month_days[m];
    }

    if (month > 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
        days += 1;
    }

    days += day - 1;

    return static_cast<int32_t>(days);
}

} // namespace gendb
