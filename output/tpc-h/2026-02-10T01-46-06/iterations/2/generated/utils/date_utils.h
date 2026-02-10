#pragma once

#include <cstdint>
#include <string>
#include <sstream>
#include <iomanip>

namespace date_utils {

// Convert date string "YYYY-MM-DD" to days since Unix epoch (1970-01-01)
inline int32_t parse_date(const std::string& date_str) {
    int year, month, day;
    char dash1, dash2;
    std::istringstream iss(date_str);
    iss >> year >> dash1 >> month >> dash2 >> day;

    // Days since epoch calculation
    // Simplified formula (ignores leap seconds, good enough for TPC-H dates)
    int days_from_epoch = 0;

    // Add days for complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        bool is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days_from_epoch += is_leap ? 366 : 365;
    }

    // Add days for complete months in current year
    const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

    for (int m = 1; m < month; ++m) {
        days_from_epoch += days_in_month[m - 1];
        if (m == 2 && is_leap) {
            days_from_epoch += 1;
        }
    }

    // Add remaining days
    days_from_epoch += day - 1;  // day 1 = 0 days offset

    return days_from_epoch;
}

// Convert days since epoch to date string "YYYY-MM-DD"
inline std::string days_to_date_str(int32_t days) {
    int year = 1970;
    int remaining_days = days;

    // Find the year
    while (true) {
        bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int days_in_year = is_leap ? 366 : 365;
        if (remaining_days < days_in_year) break;
        remaining_days -= days_in_year;
        ++year;
    }

    // Find the month
    const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

    int month = 1;
    for (month = 1; month <= 12; ++month) {
        int days_this_month = days_in_month[month - 1];
        if (month == 2 && is_leap) days_this_month = 29;

        if (remaining_days < days_this_month) break;
        remaining_days -= days_this_month;
    }

    int day = remaining_days + 1;

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << year << "-"
        << std::setw(2) << month << "-"
        << std::setw(2) << day;
    return oss.str();
}

// Fast conversion of date to days (no validation)
inline int32_t date_to_days(int year, int month, int day) {
    int days_from_epoch = 0;

    for (int y = 1970; y < year; ++y) {
        bool is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days_from_epoch += is_leap ? 366 : 365;
    }

    const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

    for (int m = 1; m < month; ++m) {
        days_from_epoch += days_in_month[m - 1];
        if (m == 2 && is_leap) {
            days_from_epoch += 1;
        }
    }

    days_from_epoch += day - 1;
    return days_from_epoch;
}

} // namespace date_utils
