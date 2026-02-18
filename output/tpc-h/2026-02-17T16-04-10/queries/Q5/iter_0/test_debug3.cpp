#include <iostream>
#include <vector>
#include <set>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

struct FileMapping {
    int fd;
    void* data;
    size_t size;

    FileMapping() : fd(-1), data(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;
        struct stat st;
        if (fstat(fd, &st) < 0) { ::close(fd); fd = -1; return false; }
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { ::close(fd); fd = -1; data = nullptr; return false; }
        return true;
    }

    void close() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) ::close(fd);
        data = nullptr;
        fd = -1;
    }

    ~FileMapping() { close(); }
};

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    // Sample lineitem rows to check ranges
    FileMapping fm_li_orderkey, fm_li_suppkey, fm_li_ep, fm_li_disc;
    fm_li_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
    fm_li_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
    fm_li_ep.open(gendb_dir + "/lineitem/l_extendedprice.bin");
    fm_li_disc.open(gendb_dir + "/lineitem/l_discount.bin");
    
    const int32_t* li_orderkey = (const int32_t*)fm_li_orderkey.data;
    const int32_t* li_suppkey = (const int32_t*)fm_li_suppkey.data;
    const int64_t* li_ep = (const int64_t*)fm_li_ep.data;
    const int64_t* li_disc = (const int64_t*)fm_li_disc.data;
    
    std::cout << "Sample lineitem rows:\n";
    for (int i = 0; i < 100; ++i) {
        if (i % 10 == 0) {
            std::cout << "Row " << i << ": orderkey=" << li_orderkey[i] 
                      << ", suppkey=" << li_suppkey[i]
                      << ", ep=" << li_ep[i]
                      << ", disc=" << li_disc[i] << "\n";
        }
    }
    
    // Count how many supplierkeys are in ASIA
    FileMapping fm_supp_suppkey, fm_supp_nk;
    fm_supp_suppkey.open(gendb_dir + "/supplier/s_suppkey.bin");
    fm_supp_nk.open(gendb_dir + "/supplier/s_nationkey.bin");
    
    const int32_t* supp_suppkey = (const int32_t*)fm_supp_suppkey.data;
    const int32_t* supp_nk = (const int32_t*)fm_supp_nk.data;
    
    std::set<int32_t> asia_supplierkeys;
    std::set<int32_t> asia_nationkeys = {8, 9, 12, 18, 21};
    
    for (int i = 0; i < 100000; ++i) {
        if (asia_nationkeys.count(supp_nk[i])) {
            asia_supplierkeys.insert(supp_suppkey[i]);
        }
    }
    
    std::cout << "\nAsia suppkey range: " << *asia_supplierkeys.begin() 
              << " to " << *asia_supplierkeys.rbegin() << "\n";
    std::cout << "Total ASIA supplier keys: " << asia_supplierkeys.size() << "\n";
    
    // Sample lineitem rows to see if any have ASIA suppliers
    int matches = 0;
    for (int i = 0; i < 100000; ++i) {
        if (asia_supplierkeys.count(li_suppkey[i])) {
            matches++;
            if (matches <= 5) {
                std::cout << "Match at row " << i << ": suppkey=" << li_suppkey[i]
                          << ", orderkey=" << li_orderkey[i] << "\n";
            }
        }
    }
    std::cout << "\nMatches in first 100K lineitem rows: " << matches << "\n";
    
    return 0;
}
