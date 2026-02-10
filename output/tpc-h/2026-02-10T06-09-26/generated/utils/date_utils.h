#pragma once

#include <cstdint>
#include <string>
#include <ctime>
#include <sstream>
#include <iomanip>

namespace gendb {

// Convert date string "YYYY-MM-DD" to days since epoch (1970-01-01)
inline int32_t parse_date(const std::string& date_str) {
    // Expected format: YYYY-MM-DD
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Calculate days since epoch using a simple algorithm
    // Days from year 1970 to start of given year
    int days = 0;
    for (int y = 1970; y < year; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }

    // Add days from start of year to start of given month
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap_year) days_in_month[1] = 29;

    for (int m = 1; m < month; m++) {
        days += days_in_month[m - 1];
    }

    // Add remaining days
    days += day - 1; // -1 because day 1 is the 0th day since start of month

    return days;
}

// Convert days since epoch to date string "YYYY-MM-DD"
inline std::string days_to_date_str(int32_t days) {
    // Start from 1970-01-01
    int year = 1970;
    int remaining_days = days;

    while (true) {
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int year_days = leap ? 366 : 365;
        if (remaining_days < year_days) break;
        remaining_days -= year_days;
        year++;
    }

    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap_year) days_in_month[1] = 29;

    int month = 1;
    for (int m = 0; m < 12; m++) {
        if (remaining_days < days_in_month[m]) {
            month = m + 1;
            break;
        }
        remaining_days -= days_in_month[m];
    }

    int day = remaining_days + 1;

    std::ostringstream oss;
    oss << year << "-"
        << std::setw(2) << std::setfill('0') << month << "-"
        << std::setw(2) << std::setfill('0') << day;
    return oss.str();
}

// Convert date to days since epoch (convenience for direct values)
inline int32_t date_to_days(int year, int month, int day) {
    int days = 0;
    for (int y = 1970; y < year; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }

    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap_year) days_in_month[1] = 29;

    for (int m = 1; m < month; m++) {
        days += days_in_month[m - 1];
    }

    days += day - 1;
    return days;
}

} // namespace gendb
