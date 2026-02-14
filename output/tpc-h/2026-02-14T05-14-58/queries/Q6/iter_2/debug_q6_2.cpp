#include <iostream>
#include <iomanip>
#include <fstream>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

template<typename T>
class MmapColumn {
public:
    T* data = nullptr;
    size_t count = 0;
    int fd = -1;

    ~MmapColumn() {
        if (data != nullptr) {
            munmap(data, count * sizeof(T));
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    bool load(const std::string& path, size_t expected_count) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;
        struct stat st;
        if (fstat(fd, &st) < 0) {
            close(fd);
            return false;
        }
        size_t file_size = st.st_size;
        size_t expected_size = expected_count * sizeof(T);
        if (file_size != expected_size) {
            close(fd);
            return false;
        }
        data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            close(fd);
            return false;
        }
        count = expected_count;
        madvise(data, file_size, MADV_SEQUENTIAL);
        return true;
    }

    T& operator[](size_t i) const { return data[i]; }
};

size_t getRowCount(const std::string& metadata_path) {
    std::ifstream file(metadata_path);
    if (!file.is_open()) return 0;
    std::string line;
    while (std::getline(file, line)) {
        if (line.find("rows:") == 0) {
            return std::stoul(line.substr(5));
        }
    }
    return 0;
}

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    size_t num_rows = getRowCount(gendb_dir + "/lineitem_metadata.txt");

    MmapColumn<int32_t> l_quantity, l_discount, l_shipdate;
    MmapColumn<int64_t> l_extendedprice;

    l_quantity.load(gendb_dir + "/lineitem_l_quantity.col", num_rows);
    l_extendedprice.load(gendb_dir + "/lineitem_l_extendedprice.col", num_rows);
    l_discount.load(gendb_dir + "/lineitem_l_discount.col", num_rows);
    l_shipdate.load(gendb_dir + "/lineitem_l_shipdate.col", num_rows);

    // Find some rows with discount > 0
    int found_discount = 0;
    std::cerr << "Searching for rows with discount > 0..." << std::endl;
    for (size_t i = 0; i < std::min(size_t(1000000), num_rows); i++) {
        if (l_discount[i] > 0) {
            std::cerr << "Row " << i << ": qty=" << l_quantity[i]
                      << ", price=" << l_extendedprice[i]
                      << ", discount=" << l_discount[i]
                      << ", shipdate=" << l_shipdate[i] << std::endl;
            found_discount++;
            if (found_discount >= 10) break;
        }
    }
    
    // Check discount distribution
    int discount_counts[11] = {0};
    for (size_t i = 0; i < num_rows; i++) {
        if (l_discount[i] >= 0 && l_discount[i] <= 10) {
            discount_counts[l_discount[i]]++;
        }
    }
    
    std::cerr << "\nDiscount distribution:" << std::endl;
    for (int d = 0; d <= 10; d++) {
        if (discount_counts[d] > 0) {
            std::cerr << "Discount " << d << ": " << discount_counts[d] << " rows" << std::endl;
        }
    }

    // Check shipdate range
    int32_t min_date = l_shipdate[0], max_date = l_shipdate[0];
    for (size_t i = 0; i < num_rows; i++) {
        if (l_shipdate[i] < min_date) min_date = l_shipdate[i];
        if (l_shipdate[i] > max_date) max_date = l_shipdate[i];
    }
    std::cerr << "Shipdate range: [" << min_date << ", " << max_date << "]" << std::endl;
    std::cerr << "1994-01-01 should be around 8766 (actual days since 1970)" << std::endl;
    std::cerr << "1995-01-01 should be around 9131" << std::endl;

    return 0;
}
