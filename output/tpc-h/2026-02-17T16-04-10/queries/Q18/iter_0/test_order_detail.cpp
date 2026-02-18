#include <iostream>
#include <unordered_map>
#include <unordered_set>
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
    const size_t orders_rows = 15000000;

    // Load orders data
    auto o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", file_size);
    auto o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", file_size);
    auto o_orderdate = mmap_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin", file_size);
    auto o_totalprice = mmap_column<int64_t>(gendb_dir + "/orders/o_totalprice.bin", file_size);

    // Find order 42290181
    for (size_t i = 0; i < orders_rows; i++) {
        if (o_orderkey[i] == 42290181) {
            std::cout << "Found order 42290181 at index " << i << ":\n";
            std::cout << "  o_custkey: " << o_custkey[i] << "\n";
            std::cout << "  o_orderdate: " << o_orderdate[i] << "\n";
            std::cout << "  o_totalprice: " << o_totalprice[i] << " (" << ((double)o_totalprice[i] / 100.0) << " dollars)\n";
            break;
        }
    }

    return 0;
}
