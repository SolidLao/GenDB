#include "utils/date_utils.h"
#include <iostream>

int main() {
    int32_t date_start = gendb::date_utils::date_to_days(1994, 1, 1);
    int32_t date_end = gendb::date_utils::date_to_days(1995, 1, 1);
    std::cout << "date_start: " << date_start << std::endl;
    std::cout << "date_end: " << date_end << std::endl;
    return 0;
}
