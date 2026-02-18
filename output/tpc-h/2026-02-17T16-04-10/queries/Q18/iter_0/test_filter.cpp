#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <atomic>

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
    const unsigned num_threads = std::thread::hardware_concurrency();

    // Step 1: Get qualified orders
    auto l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", file_size);
    auto l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", file_size);

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

    std::unordered_set<int32_t> qualified_orderkeys;
    for (auto& m : qty_per_thread) {
        for (auto& kv : m) {
            if (kv.second > 30000) {
                qualified_orderkeys.insert(kv.first);
            }
        }
    }

    std::cout << "Qualified orders: " << qualified_orderkeys.size() << "\n";
    if (qualified_orderkeys.count(42290181)) {
        std::cout << "42290181 is qualified\n";
    }

    // Step 2: Filter orders
    auto o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", file_size);
    auto o_totalprice = mmap_column<int64_t>(gendb_dir + "/orders/o_totalprice.bin", file_size);

    struct OrderInfo {
        int32_t o_custkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
    };

    std::unordered_map<int32_t, OrderInfo> orders_hash;
    int64_t max_price = 0;
    for (size_t i = 0; i < orders_rows; i++) {
        if (qualified_orderkeys.count(o_orderkey[i])) {
            orders_hash[o_orderkey[i]] = {0, 0, o_totalprice[i]};
            if (o_totalprice[i] > max_price) {
                max_price = o_totalprice[i];
                std::cout << "  Found order " << o_orderkey[i] << " with price " << o_totalprice[i] << "\n";
            }
        }
    }

    std::cout << "Filtered orders: " << orders_hash.size() << "\n";
    std::cout << "Max price: " << max_price << " (" << ((double)max_price / 100.0) << ")\n";

    if (orders_hash.count(42290181)) {
        std::cout << "42290181 found in orders_hash with price " << orders_hash[42290181].o_totalprice << "\n";
    } else {
        std::cout << "42290181 NOT found in orders_hash\n";
    }

    return 0;
}
