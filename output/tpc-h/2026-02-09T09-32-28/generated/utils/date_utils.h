#pragma once

#include <string>
#include <sstream>
#include <iomanip>
#include <cstdint>

// Date conversion utilities (header-only)
// All dates stored as int32_t representing days since 1970-01-01

// Convert calendar date to days since epoch (1970-01-01)
inline int32_t date_to_days(int year, int month, int day) {
    // Algorithm from: https://howardhinnant.github.io/date_algorithms.html
    // Adjusted to be relative to 1970-01-01
    year -= (month <= 2) ? 1 : 0;
    int era = (year >= 0 ? year : year - 399) / 400;
    int yoe = year - era * 400;
    int doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    int days_since_0000 = era * 146097 + doe - 719468; // -719468 adjusts to 1970-01-01
    return static_cast<int32_t>(days_since_0000);
}

// Convert days since epoch back to "YYYY-MM-DD" string
inline std::string days_to_date_str(int32_t total_days) {
    // Algorithm from: https://howardhinnant.github.io/date_algorithms.html
    int z = total_days + 719468; // Adjust back to 0000-03-01
    int era = (z >= 0 ? z : z - 146096) / 146097;
    int doe = z - era * 146097;
    int yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int y = yoe + era * 400;
    int doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int mp = (5 * doy + 2) / 153;
    int d = doy - (153 * mp + 2) / 5 + 1;
    int m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2) ? 1 : 0;

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << y << '-'
        << std::setw(2) << m << '-'
        << std::setw(2) << d;
    return oss.str();
}

// Parse "YYYY-MM-DD" string to days since epoch
inline int32_t parse_date(const std::string& date_str) {
    int year, month, day;
    char dash1, dash2;
    std::istringstream iss(date_str);
    iss >> year >> dash1 >> month >> dash2 >> day;
    return date_to_days(year, month, day);
}
