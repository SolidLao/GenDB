#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

class MmapReader {
public:
    MmapReader(const std::string& filename) {
        fd_ = open(filename.c_str(), O_RDONLY);
        if (fd_ < 0) throw std::runtime_error("Cannot open file: " + filename);
        off_t size = lseek(fd_, 0, SEEK_END);
        if (size < 0) throw std::runtime_error("Cannot get file size: " + filename);
        size_ = static_cast<size_t>(size);
        data_ = mmap(nullptr, size_, PROT_READ, MAP_SHARED, fd_, 0);
        if (data_ == MAP_FAILED) throw std::runtime_error("Cannot mmap file: " + filename);
    }
    ~MmapReader() {
        if (data_ != nullptr) munmap(data_, size_);
        if (fd_ >= 0) close(fd_);
    }
    const void* data() const { return data_; }
    size_t size() const { return size_; }
private:
    int fd_ = -1;
    void* data_ = nullptr;
    size_t size_ = 0;
};

template <typename T>
std::vector<T> read_column(const std::string& path) {
    MmapReader reader(path);
    const T* ptr = reinterpret_cast<const T*>(reader.data());
    size_t count = reader.size() / sizeof(T);
    return std::vector<T>(ptr, ptr + count);
}

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    auto l_shipdate = read_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin");
    auto o_orderdate = read_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin");
    
    std::cout << "l_shipdate stats:\n";
    std::cout << "  Count: " << l_shipdate.size() << "\n";
    if (!l_shipdate.empty()) {
        std::cout << "  First 5: ";
        for (int i = 0; i < 5 && i < (int)l_shipdate.size(); ++i) {
            std::cout << l_shipdate[i] << " ";
        }
        std::cout << "\n";
        auto minmax = std::minmax_element(l_shipdate.begin(), l_shipdate.end());
        std::cout << "  Min/Max: " << *minmax.first << " / " << *minmax.second << "\n";
    }
    
    std::cout << "\no_orderdate stats:\n";
    std::cout << "  Count: " << o_orderdate.size() << "\n";
    if (!o_orderdate.empty()) {
        std::cout << "  First 5: ";
        for (int i = 0; i < 5 && i < (int)o_orderdate.size(); ++i) {
            std::cout << o_orderdate[i] << " ";
        }
        std::cout << "\n";
        auto minmax = std::minmax_element(o_orderdate.begin(), o_orderdate.end());
        std::cout << "  Min/Max: " << *minmax.first << " / " << *minmax.second << "\n";
    }
    
    // Check date constants
    int date_1995_03_15 = 19950315;
    std::cout << "\nDate 1995-03-15 as int: " << date_1995_03_15 << "\n";
    
    // Count how many shipdate > date_1995_03_15
    int count_shipdate_gt = 0;
    for (auto d : l_shipdate) {
        if (d > date_1995_03_15) count_shipdate_gt++;
    }
    std::cout << "Lines with l_shipdate > 19950315: " << count_shipdate_gt << "\n";
    
    // Count how many orderdate < date_1995_03_15
    int count_orderdate_lt = 0;
    for (auto d : o_orderdate) {
        if (d < date_1995_03_15) count_orderdate_lt++;
    }
    std::cout << "Orders with o_orderdate < 19950315: " << count_orderdate_lt << "\n";
    
    return 0;
}
