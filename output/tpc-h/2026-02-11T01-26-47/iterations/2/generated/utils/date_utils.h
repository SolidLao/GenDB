#pragma once

#include <string>
#include <ctime>
#include <sstream>
#include <iomanip>

// Date utilities for TPC-H queries
// All dates stored as int32_t (days since Unix epoch: 1970-01-01)

namespace gendb {

// Convert date string "YYYY-MM-DD" to days since epoch
inline int32_t parse_date(const std::string& date_str) {
    struct tm tm = {};
    std::istringstream ss(date_str);
    ss >> std::get_time(&tm, "%Y-%m-%d");

    // Convert to days since epoch
    time_t t = mktime(&tm);
    return static_cast<int32_t>(t / 86400);
}

// Convert days since epoch to date string "YYYY-MM-DD"
inline std::string days_to_date_str(int32_t days) {
    time_t t = static_cast<time_t>(days) * 86400;
    struct tm* tm = gmtime(&t);

    std::ostringstream ss;
    ss << std::put_time(tm, "%Y-%m-%d");
    return ss.str();
}

// Get date as int32_t (days since epoch)
inline int32_t date_to_days(int year, int month, int day) {
    struct tm tm = {};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;

    time_t t = mktime(&tm);
    return static_cast<int32_t>(t / 86400);
}

// Date arithmetic: add days to a date
inline int32_t date_add_days(int32_t date, int32_t days) {
    return date + days;
}

// Date comparison helpers
inline bool date_less_than(int32_t d1, int32_t d2) {
    return d1 < d2;
}

inline bool date_less_equal(int32_t d1, int32_t d2) {
    return d1 <= d2;
}

inline bool date_greater_than(int32_t d1, int32_t d2) {
    return d1 > d2;
}

inline bool date_between(int32_t date, int32_t min_date, int32_t max_date) {
    return date >= min_date && date < max_date;
}

} // namespace gendb
