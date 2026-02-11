#include "utils/date_utils.h"
#include <iostream>

int main() {
    int32_t date_start = date_to_days("1994-01-01");
    int32_t date_end = date_to_days("1995-01-01");
    std::cout << "date_start (1994-01-01): " << date_start << std::endl;
    std::cout << "date_end (1995-01-01): " << date_end << std::endl;
    std::cout << "Range: [" << date_start << ", " << date_end << ")" << std::endl;
    return 0;
}
