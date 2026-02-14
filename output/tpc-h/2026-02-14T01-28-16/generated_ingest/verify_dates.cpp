#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>

int main() {
    std::string path = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem_l_shipdate.bin";
    
    int fd = open(path.c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    size_t count = sb.st_size / sizeof(int32_t);
    
    int32_t* dates = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    
    std::cout << "Total dates: " << count << "\n";
    std::cout << "Min date (days since epoch): " << dates[0] << "\n";
    std::cout << "Max date (days since epoch): " << dates[count-1] << "\n";
    std::cout << "Sample dates (first 10): ";
    for (int i = 0; i < 10 && i < count; i++) {
        std::cout << dates[i] << " ";
    }
    std::cout << "\n";
    
    // Convert sample dates back to YYYY-MM-DD to verify
    auto days_to_date = [](int32_t days) {
        int year = 1970;
        while (true) {
            int days_in_year = ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) ? 366 : 365;
            if (days < days_in_year) break;
            days -= days_in_year;
            year++;
        }
        
        int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) days_in_month[1] = 29;
        
        int month = 1;
        for (int m = 0; m < 12; m++) {
            if (days < days_in_month[m]) {
                month = m + 1;
                break;
            }
            days -= days_in_month[m];
        }
        
        int day = days + 1;
        return std::to_string(year) + "-" + (month < 10 ? "0" : "") + std::to_string(month) + "-" + (day < 10 ? "0" : "") + std::to_string(day);
    };
    
    std::cout << "Min date as YYYY-MM-DD: " << days_to_date(dates[0]) << "\n";
    std::cout << "Max date as YYYY-MM-DD: " << days_to_date(dates[count-1]) << "\n";
    
    munmap(dates, sb.st_size);
    close(fd);
    
    return 0;
}
