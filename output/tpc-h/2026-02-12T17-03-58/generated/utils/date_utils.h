#ifndef DATE_UTILS_H
#define DATE_UTILS_H

#include <cstdint>
#include <ctime>
#include <string>
#include <sstream>
#include <iomanip>

// Date utilities: Convert between string and int32_t (days since 1970-01-01)

namespace DateUtils {

// Convert DATE string "YYYY-MM-DD" to days since epoch (1970-01-01)
inline int32_t stringToDate(const std::string& date_str) {
    if (date_str.empty()) return 0;

    int year, month, day;
    char sep1, sep2;
    std::istringstream iss(date_str);
    iss >> year >> sep1 >> month >> sep2 >> day;

    // Use tm struct to compute days since epoch
    struct tm time_struct = {};
    time_struct.tm_year = year - 1900;
    time_struct.tm_mon = month - 1;
    time_struct.tm_mday = day;
    time_struct.tm_hour = 12; // Avoid DST issues

    time_t timestamp = timegm(&time_struct);
    return static_cast<int32_t>(timestamp / 86400); // 86400 seconds per day
}

// Convert days since epoch to DATE string "YYYY-MM-DD"
inline std::string dateToString(int32_t days) {
    time_t timestamp = static_cast<time_t>(days) * 86400;
    struct tm* time_struct = gmtime(&timestamp);

    std::ostringstream oss;
    oss << std::setfill('0')
        << std::setw(4) << (time_struct->tm_year + 1900) << '-'
        << std::setw(2) << (time_struct->tm_mon + 1) << '-'
        << std::setw(2) << time_struct->tm_mday;
    return oss.str();
}

// Helper: Parse date literals like '1998-12-01' at compile time (for queries)
constexpr int32_t parseDate(const char* date_str) {
    // Simple compile-time parser for "YYYY-MM-DD"
    int year = (date_str[0] - '0') * 1000 + (date_str[1] - '0') * 100 +
               (date_str[2] - '0') * 10 + (date_str[3] - '0');
    int month = (date_str[5] - '0') * 10 + (date_str[6] - '0');
    int day = (date_str[8] - '0') * 10 + (date_str[9] - '0');

    // Simplified epoch day calculation (assumes Gregorian calendar)
    // Days from year 1970 to year-1
    int days_from_epoch = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;

    // Days in each month (non-leap year)
    constexpr int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Add days from months
    for (int m = 1; m < month; ++m) {
        days_from_epoch += days_in_month[m];
    }

    // Add leap day if after February in a leap year
    if (month > 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
        days_from_epoch += 1;
    }

    // Add days
    days_from_epoch += day - 1;

    return days_from_epoch;
}

} // namespace DateUtils

#endif // DATE_UTILS_H
