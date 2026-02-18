#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

struct FileMapping {
    int fd;
    void* data;
    size_t size;

    FileMapping() : fd(-1), data(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat: " << path << std::endl;
            ::close(fd);
            fd = -1;
            return false;
        }

        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            ::close(fd);
            fd = -1;
            data = nullptr;
            return false;
        }

        return true;
    }

    void close() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
        data = nullptr;
        fd = -1;
    }

    ~FileMapping() { close(); }
};

std::vector<std::string> load_string_column(const void* data, int64_t num_rows) {
    std::vector<std::string> result;
    const uint8_t* ptr = (const uint8_t*)data;

    for (int64_t i = 0; i < num_rows; ++i) {
        uint32_t len = *(uint32_t*)ptr;
        ptr += 4;

        std::string s((const char*)ptr, len);
        ptr += len;

        result.push_back(s);
    }

    return result;
}

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    // Find ASIA nations
    FileMapping fm_region_name, fm_region_regionkey;
    fm_region_regionkey.open(gendb_dir + "/region/r_regionkey.bin");
    fm_region_name.open(gendb_dir + "/region/r_name.bin");
    
    const int32_t* region_regionkey = (const int32_t*)fm_region_regionkey.data;
    auto region_names = load_string_column(fm_region_name.data, 5);
    
    int32_t asia_regionkey = -1;
    for (int i = 0; i < 5; ++i) {
        std::cout << "Region " << i << ": " << region_names[i] << " (key=" << region_regionkey[i] << ")\n";
        if (region_names[i] == "ASIA") {
            asia_regionkey = region_regionkey[i];
        }
    }
    
    std::cout << "ASIA regionkey: " << asia_regionkey << "\n";
    
    // Get nations in ASIA
    FileMapping fm_nation_nationkey, fm_nation_regionkey, fm_nation_name;
    fm_nation_nationkey.open(gendb_dir + "/nation/n_nationkey.bin");
    fm_nation_regionkey.open(gendb_dir + "/nation/n_regionkey.bin");
    fm_nation_name.open(gendb_dir + "/nation/n_name.bin");
    
    const int32_t* nation_nationkey = (const int32_t*)fm_nation_nationkey.data;
    const int32_t* nation_regionkey = (const int32_t*)fm_nation_regionkey.data;
    auto nation_names = load_string_column(fm_nation_name.data, 25);
    
    std::vector<int32_t> asia_nations;
    for (int i = 0; i < 25; ++i) {
        if (nation_regionkey[i] == asia_regionkey) {
            asia_nations.push_back(nation_nationkey[i]);
            std::cout << "  ASIA Nation: " << nation_names[i] << " (key=" << nation_nationkey[i] << ")\n";
        }
    }
    
    // Get suppliers in ASIA
    FileMapping fm_supplier_suppkey, fm_supplier_nationkey;
    fm_supplier_suppkey.open(gendb_dir + "/supplier/s_suppkey.bin");
    fm_supplier_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");
    
    const int32_t* supplier_suppkey = (const int32_t*)fm_supplier_suppkey.data;
    const int32_t* supplier_nationkey = (const int32_t*)fm_supplier_nationkey.data;
    
    std::set<int32_t> asia_nation_set(asia_nations.begin(), asia_nations.end());
    std::vector<int32_t> asia_suppliers;
    std::vector<int32_t> supp_nation;
    
    for (int i = 0; i < 100000; ++i) {
        if (asia_nation_set.count(supplier_nationkey[i]) > 0) {
            asia_suppliers.push_back(supplier_suppkey[i]);
            supp_nation.push_back(supplier_nationkey[i]);
        }
    }
    
    std::cout << "\nAsia suppliers by nation:\n";
    for (int32_t n : asia_nations) {
        int count = 0;
        for (size_t i = 0; i < supp_nation.size(); ++i) {
            if (supp_nation[i] == n) count++;
        }
        std::cout << "  Nation " << n << ": " << count << " suppliers\n";
    }
    
    return 0;
}
