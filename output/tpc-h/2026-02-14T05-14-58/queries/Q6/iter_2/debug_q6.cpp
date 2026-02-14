#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
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
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            close(fd);
            fd = -1;
            return false;
        }

        size_t file_size = st.st_size;
        size_t expected_size = expected_count * sizeof(T);

        if (file_size != expected_size) {
            std::cerr << "Size mismatch for " << path << ": expected " << expected_size
                      << " bytes, got " << file_size << std::endl;
            close(fd);
            fd = -1;
            return false;
        }

        data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            fd = -1;
            data = nullptr;
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
    if (!file.is_open()) {
        std::cerr << "Failed to open metadata: " << metadata_path << std::endl;
        return 0;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.find("rows:") == 0) {
            return std::stoul(line.substr(5));
        }
    }

    std::cerr << "Could not find row count in metadata" << std::endl;
    return 0;
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argc > 1 ? argv[1] : "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    std::string metadata_path = gendb_dir + "/lineitem_metadata.txt";
    size_t num_rows = getRowCount(metadata_path);
    std::cerr << "Loaded row count: " << num_rows << std::endl;

    MmapColumn<int32_t> l_quantity;
    MmapColumn<int64_t> l_extendedprice;
    MmapColumn<int32_t> l_discount;
    MmapColumn<int32_t> l_shipdate;

    if (!l_quantity.load(gendb_dir + "/lineitem_l_quantity.col", num_rows)) {
        std::cerr << "Failed to load l_quantity\n";
        return 1;
    }
    if (!l_extendedprice.load(gendb_dir + "/lineitem_l_extendedprice.col", num_rows)) {
        std::cerr << "Failed to load l_extendedprice\n";
        return 1;
    }
    if (!l_discount.load(gendb_dir + "/lineitem_l_discount.col", num_rows)) {
        std::cerr << "Failed to load l_discount\n";
        return 1;
    }
    if (!l_shipdate.load(gendb_dir + "/lineitem_l_shipdate.col", num_rows)) {
        std::cerr << "Failed to load l_shipdate\n";
        return 1;
    }

    // Sample data: first 10 rows
    std::cerr << "Sample data (first 10 rows):" << std::endl;
    for (size_t i = 0; i < 10; i++) {
        std::cerr << "Row " << i << ": qty=" << l_quantity[i]
                  << ", price=" << l_extendedprice[i]
                  << ", discount=" << l_discount[i]
                  << ", shipdate=" << l_shipdate[i] << std::endl;
    }

    // Check date constants
    const int32_t date_1994_01_01 = 8766;
    const int32_t date_1995_01_01 = 9131;
    std::cerr << "\nDate range: [" << date_1994_01_01 << ", " << date_1995_01_01 << ")" << std::endl;

    // Discount bounds
    const int32_t discount_min = 5;
    const int32_t discount_max = 7;
    std::cerr << "Discount range: [" << discount_min << ", " << discount_max << "]" << std::endl;

    // Count matching rows
    size_t matching_count = 0;
    double total_revenue = 0.0;
    
    for (size_t i = 0; i < num_rows; i++) {
        if (l_discount[i] >= discount_min && l_discount[i] <= discount_max &&
            l_quantity[i] < 24 &&
            l_shipdate[i] >= date_1994_01_01 && l_shipdate[i] < date_1995_01_01) {
            matching_count++;
            double revenue = (static_cast<double>(l_extendedprice[i]) *
                            static_cast<double>(l_discount[i])) / 10000.0;
            total_revenue += revenue;
        }
    }

    std::cerr << "\nMatching rows: " << matching_count << std::endl;
    std::cerr << "Total revenue: " << std::fixed << std::setprecision(4) << total_revenue << std::endl;

    return 0;
}
