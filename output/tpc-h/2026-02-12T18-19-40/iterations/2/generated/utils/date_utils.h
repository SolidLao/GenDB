#ifndef DATE_UTILS_H
#define DATE_UTILS_H

#include <cstdint>
#include <string>
#include <sstream>

// Date conversion utilities - all dates stored as int32_t (days since epoch: 1970-01-01)

namespace date_utils {

// Convert YYYY-MM-DD string to days since epoch
inline int32_t date_to_days(const std::string& date_str) {
    // Parse YYYY-MM-DD
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days in each month (non-leap year)
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Count leap years since 1970
    auto is_leap = [](int y) { return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0); };

    int days = 0;

    // Add days for complete years
    for (int y = 1970; y < year; y++) {
        days += is_leap(y) ? 366 : 365;
    }

    // Add days for complete months in current year
    for (int m = 1; m < month; m++) {
        days += days_in_month[m];
        if (m == 2 && is_leap(year)) days++; // February in leap year
    }

    // Add remaining days
    days += day - 1; // -1 because epoch is day 0

    return static_cast<int32_t>(days);
}

// Convert days since epoch to YYYY-MM-DD string
inline std::string days_to_date(int32_t days) {
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    auto is_leap = [](int y) { return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0); };

    int year = 1970;
    int remaining = days;

    // Find year
    while (true) {
        int year_days = is_leap(year) ? 366 : 365;
        if (remaining < year_days) break;
        remaining -= year_days;
        year++;
    }

    // Find month and day
    int month = 1;
    for (month = 1; month <= 12; month++) {
        int month_days = days_in_month[month];
        if (month == 2 && is_leap(year)) month_days = 29;
        if (remaining < month_days) break;
        remaining -= month_days;
    }

    int day = remaining + 1;

    std::ostringstream oss;
    oss << year << '-';
    if (month < 10) oss << '0';
    oss << month << '-';
    if (day < 10) oss << '0';
    oss << day;

    return oss.str();
}

} // namespace date_utils

#endif // DATE_UTILS_H
