#include <iostream>
#include <map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>
#include <iomanip>
#include <cstring>

inline std::string epochDaysToString(int32_t days) {
    std::time_t t = days * 86400;
    struct tm* tm_info = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_info);
    return std::string(buf);
}

int main() {
    int fd = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/orders.o_orderdate.sorted_idx", O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    
    const int32_t* idx_data = (const int32_t*)ptr;
    size_t num_idx_entries = sb.st_size / (2 * sizeof(int32_t));
    
    std::cout << "Sorted index analysis:\n";
    std::cout << "Total entries: " << num_idx_entries << "\n\n";
    
    // Count runs of same value
    std::map<int32_t, size_t> value_counts;
    int32_t prev_value = idx_data[0];
    for (size_t i = 1; i < num_idx_entries; ++i) {
        int32_t value = idx_data[2*i];
        if (value != prev_value) {
            value_counts[prev_value]++;
            prev_value = value;
        }
    }
    value_counts[prev_value]++;
    
    std::cout << "Value distribution (first 20):\n";
    int shown = 0;
    for (auto& pair : value_counts) {
        if (shown < 20) {
            std::cout << "Value " << pair.first << " (" << epochDaysToString(pair.first) 
                      << "): " << pair.second << " entries\n";
            shown++;
        }
    }
    
    // Find all distinct values
    std::cout << "\nTotal distinct values: " << value_counts.size() << "\n";
    
    return 0;
}
