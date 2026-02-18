#include <iostream>
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
    
    const int32_t date_1994_01_01 = 8766;
    const int32_t date_1995_01_01 = 9131;
    
    // Load orders and count matches
    FileMapping fm_ord_orderkey, fm_ord_orderdate;
    fm_ord_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
    fm_ord_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
    
    const int32_t* ord_orderkey = (const int32_t*)fm_ord_orderkey.data;
    const int32_t* ord_orderdate = (const int32_t*)fm_ord_orderdate.data;
    
    std::set<int32_t> orders_in_range;
    int count = 0;
    for (int i = 0; i < 15000000; ++i) {
        if (ord_orderdate[i] >= date_1994_01_01 && ord_orderdate[i] < date_1995_01_01) {
            orders_in_range.insert(ord_orderkey[i]);
            count++;
        }
    }
    
    std::cout << "Orders in range [1994-01-01, 1995-01-01): " << count << "\n";
    std::cout << "Distinct order keys: " << orders_in_range.size() << "\n";
    
    // Sample some lineitem rows
    FileMapping fm_li_orderkey, fm_li_suppkey, fm_li_ep, fm_li_disc;
    fm_li_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
    fm_li_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
    fm_li_ep.open(gendb_dir + "/lineitem/l_extendedprice.bin");
    fm_li_disc.open(gendb_dir + "/lineitem/l_discount.bin");
    
    const int32_t* li_orderkey = (const int32_t*)fm_li_orderkey.data;
    const int32_t* li_suppkey = (const int32_t*)fm_li_suppkey.data;
    const int64_t* li_ep = (const int64_t*)fm_li_ep.data;
    const int64_t* li_disc = (const int64_t*)fm_li_disc.data;
    
    // Check first lineitem row
    if (li_orderkey[0] == 1) {
        std::cout << "\nFirst lineitem row has orderkey=1, checking if in orders:\n";
        std::cout << "  Order 1 in range? " << (orders_in_range.count(1) ? "YES" : "NO") << "\n";
    }
    
    // Sample lineitem rows and check order matches
    int li_order_matches = 0;
    for (int i = 0; i < 100000; ++i) {
        if (orders_in_range.count(li_orderkey[i])) {
            li_order_matches++;
        }
    }
    std::cout << "\nMatches in first 100K lineitem rows (order join): " << li_order_matches << "\n";
    
    return 0;
}
