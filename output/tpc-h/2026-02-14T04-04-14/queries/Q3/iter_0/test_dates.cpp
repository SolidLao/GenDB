#include <iostream>
#include <ctime>
#include <iomanip>
#include <string>

inline std::string epochDaysToString(int32_t days) {
    const int64_t SECONDS_PER_DAY = 86400;
    std::time_t t = days * SECONDS_PER_DAY;
    struct tm* tm_info = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_info);
    return std::string(buf);
}

int main() {
    int32_t days1 = 9204;  // 1995-03-15
    int32_t days2 = 9184;  // 1995-02-23
    
    std::cout << days1 << " -> " << epochDaysToString(days1) << std::endl;
    std::cout << days2 << " -> " << epochDaysToString(days2) << std::endl;
    
    return 0;
}
