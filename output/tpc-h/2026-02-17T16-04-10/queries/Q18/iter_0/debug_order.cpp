#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

template<typename T>
T* mmap_column(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return nullptr;
    struct stat sb;
    fstat(fd, &sb);
    file_size = sb.st_size;
    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return static_cast<T*>(addr == MAP_FAILED ? nullptr : addr);
}

int main() {
    size_t file_size;
    const std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    const size_t lineitem_rows = 59986052;

    auto l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", file_size);
    auto l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", file_size);

    // Find total quantity for order 42290181
    int64_t total_qty = 0;
    int count = 0;
    for (size_t i = 0; i < lineitem_rows; i++) {
        if (l_orderkey[i] == 42290181) {
            total_qty += l_quantity[i];
            count++;
        }
    }

    std::cout << "Order 42290181:\n";
    std::cout << "  Line count: " << count << "\n";
    std::cout << "  Total quantity (raw int64): " << total_qty << "\n";
    std::cout << "  Total quantity (as decimal): " << ((double)total_qty / 100.0) << "\n";
    std::cout << "  Qualifies (> 30000)? " << (total_qty > 30000 ? "YES" : "NO") << "\n";

    return 0;
}
