#pragma once

#include <ctime>
#include <string>
#include <sstream>
#include <iomanip>

// Date utilities for TPC-H queries
// Dates are stored as int32_t (days since epoch 1970-01-01)

namespace gendb {

// Convert date string "YYYY-MM-DD" to days since epoch
inline int32_t parse_date(const std::string& date_str) {
    int year, month, day;
    char dash1, dash2;
    std::istringstream ss(date_str);
    ss >> year >> dash1 >> month >> dash2 >> day;

    // Calculate days since epoch (1970-01-01)
    // Simple approximation: count days from 1970-01-01
    struct tm timeinfo = {};
    timeinfo.tm_year = year - 1900;
    timeinfo.tm_mon = month - 1;
    timeinfo.tm_mday = day;
    timeinfo.tm_hour = 12;  // noon to avoid DST issues

    time_t t = mktime(&timeinfo);
    return static_cast<int32_t>(t / 86400);  // 86400 = seconds per day
}

// Convert days since epoch to date string "YYYY-MM-DD"
inline std::string days_to_date_str(int32_t days) {
    time_t t = static_cast<time_t>(days) * 86400;
    struct tm* timeinfo = gmtime(&t);

    std::ostringstream oss;
    oss << (timeinfo->tm_year + 1900) << "-"
        << std::setfill('0') << std::setw(2) << (timeinfo->tm_mon + 1) << "-"
        << std::setfill('0') << std::setw(2) << timeinfo->tm_mday;
    return oss.str();
}

// Convert "YYYY-MM-DD" string to int32_t days since epoch
inline int32_t date_to_days(const std::string& date_str) {
    return parse_date(date_str);
}

// Add days to a date
inline int32_t date_add_days(int32_t date, int days) {
    return date + days;
}

// Subtract days from a date
inline int32_t date_sub_days(int32_t date, int days) {
    return date - days;
}

}  // namespace gendb
