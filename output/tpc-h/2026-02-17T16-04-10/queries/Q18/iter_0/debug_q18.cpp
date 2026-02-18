#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <thread>
#include <atomic>
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
    const unsigned num_threads = std::thread::hardware_concurrency();

    auto l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", file_size);
    auto l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", file_size);

    // Aggregate by orderkey
    std::vector<std::unordered_map<int32_t, int64_t>> qty_per_thread(num_threads);
    std::atomic<size_t> counter(0);

    auto scan = [&](int tid) {
        while (true) {
            size_t start = counter.fetch_add(100000);
            if (start >= lineitem_rows) break;
            size_t end = std::min(start + 100000, lineitem_rows);
            for (size_t i = start; i < end; i++) {
                qty_per_thread[tid][l_orderkey[i]] += l_quantity[i];
            }
        }
    };

    std::vector<std::thread> threads;
    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back(scan, t);
    }
    for (auto& th : threads) th.join();

    // Merge and count HAVING > 30000
    std::unordered_set<int32_t> qualified;
    for (auto& m : qty_per_thread) {
        for (auto& kv : m) {
            if (kv.second > 30000) {
                qualified.insert(kv.first);
            }
        }
    }

    std::cout << "Qualified orders (SUM(l_quantity) > 30000): " << qualified.size() << std::endl;

    // Sample check: orderkey 42290181 should be there if ground truth is correct
    if (qualified.count(42290181)) {
        std::cout << "Order 42290181 is qualified!" << std::endl;
    } else {
        std::cout << "Order 42290181 is NOT qualified!" << std::endl;
    }

    return 0;
}
