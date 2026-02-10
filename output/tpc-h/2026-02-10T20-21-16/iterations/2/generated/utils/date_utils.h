#pragma once

#include <string>
#include <sstream>
#include <iomanip>
#include <ctime>

namespace gendb {

// Convert date string (YYYY-MM-DD) to days since epoch (1970-01-01)
inline int32_t parse_date(const std::string& date_str) {
    int year, month, day;
    char dash1, dash2;
    std::istringstream iss(date_str);
    iss >> year >> dash1 >> month >> dash2 >> day;

    std::tm tm = {};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = 12;  // Use noon to avoid DST issues

    time_t t = mktime(&tm);
    // Convert seconds to days (86400 seconds per day)
    return static_cast<int32_t>(t / 86400);
}

// Convert days since epoch to YYYY-MM-DD string
inline std::string days_to_date_str(int32_t days) {
    time_t t = static_cast<time_t>(days) * 86400;
    std::tm* tm = gmtime(&t);

    std::ostringstream oss;
    oss << std::setfill('0')
        << std::setw(4) << (tm->tm_year + 1900) << "-"
        << std::setw(2) << (tm->tm_mon + 1) << "-"
        << std::setw(2) << tm->tm_mday;
    return oss.str();
}

// Convert date to days since epoch (from year, month, day)
inline int32_t date_to_days(int year, int month, int day) {
    std::tm tm = {};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = 12;

    time_t t = mktime(&tm);
    return static_cast<int32_t>(t / 86400);
}

} // namespace gendb
