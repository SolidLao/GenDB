// q3.cpp - TPC-H Q3: Shipping Priority Query
// Optimized with parallel hash joins, partial aggregation, and Top-K heap

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <queue>

// Date helper: convert "YYYY-MM-DD" to days since epoch
inline int32_t date_to_days(const char* date_str) {
    int year, month, day;
    sscanf(date_str, "%d-%d-%d", &year, &month, &day);

    int days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;

    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += days_in_month[m];
    }
    days += day - 1;

    if (month > 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
        days += 1;
    }

    return days;
}

// Reverse: days to date string
std::string days_to_date_str(int32_t days) {
    int year = 1970 + days / 365;
    int remaining = days - ((year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400);

    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

    int month = 1;
    int accumulated = 0;
    for (int m = 1; m <= 12; ++m) {
        int days_this_month = days_in_month[m];
        if (m == 2 && leap) days_this_month = 29;
        if (accumulated + days_this_month > remaining) {
            month = m;
            break;
        }
        accumulated += days_this_month;
    }
    int day = remaining - accumulated + 1;

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// mmap helper
template<typename T>
T* mmap_column(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }
    file_size = sb.st_size;
    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "Error: mmap failed for " << path << std::endl;
        return nullptr;
    }
    madvise(addr, file_size, MADV_SEQUENTIAL);
    return static_cast<T*>(addr);
}

// Dictionary helper - newline-separated text file
uint8_t load_dict_and_find(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    if (!f) return 255;

    std::string line;
    uint8_t idx = 0;
    while (std::getline(f, line)) {
        if (line == target) return idx;
        idx++;
    }
    return 255;
}

// Aggregation key
struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        return std::hash<int32_t>()(k.l_orderkey) ^
               (std::hash<int32_t>()(k.o_orderdate) << 1) ^
               (std::hash<int32_t>()(k.o_shippriority) << 2);
    }
};

struct ResultRow {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const ResultRow& other) const {
        // For min-heap (we want top K)
        if (revenue != other.revenue) return revenue > other.revenue;
        return o_orderdate > other.o_orderdate;
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    const int32_t date_1995_03_15 = date_to_days("1995-03-15");
    const unsigned num_threads = std::thread::hardware_concurrency();

    size_t customer_rows = 1500000;
    size_t orders_rows = 15000000;
    size_t lineitem_rows = 59986052;

    // ==================== STEP 1: Filter customer ====================
    size_t file_size;
    auto c_custkey = mmap_column<int32_t>(gendb_dir + "/customer.c_custkey.bin", file_size);
    auto c_mktsegment = mmap_column<uint8_t>(gendb_dir + "/customer.c_mktsegment.bin", file_size);
    if (!c_custkey || !c_mktsegment) return;

    uint8_t building_code = load_dict_and_find(gendb_dir + "/customer.c_mktsegment.dict", "BUILDING");

    std::vector<std::vector<int32_t>> customer_keys_per_thread(num_threads);
    std::atomic<size_t> customer_counter(0);

    auto filter_customer = [&](int thread_id) {
        const size_t morsel_size = 10000;
        while (true) {
            size_t start_idx = customer_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= customer_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, customer_rows);

            for (size_t i = start_idx; i < end_idx; i++) {
                if (c_mktsegment[i] == building_code) {
                    customer_keys_per_thread[thread_id].push_back(c_custkey[i]);
                }
            }
        }
    };

    std::vector<std::thread> threads;
    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back(filter_customer, t);
    }
    for (auto& th : threads) th.join();
    threads.clear();

    std::unordered_map<int32_t, int> qualified_customers;
    for (auto& keys : customer_keys_per_thread) {
        for (int32_t key : keys) {
            qualified_customers[key] = 1;
        }
    }
    customer_keys_per_thread.clear();

    munmap(c_custkey, file_size);
    munmap(c_mktsegment, file_size);

    // ==================== STEP 2: Scan orders and join with customer ====================
    auto o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders.o_orderkey.bin", file_size);
    auto o_custkey = mmap_column<int32_t>(gendb_dir + "/orders.o_custkey.bin", file_size);
    auto o_orderdate = mmap_column<int32_t>(gendb_dir + "/orders.o_orderdate.bin", file_size);
    auto o_shippriority = mmap_column<int32_t>(gendb_dir + "/orders.o_shippriority.bin", file_size);
    if (!o_orderkey || !o_custkey || !o_orderdate || !o_shippriority) return;

    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<std::unordered_map<int32_t, OrderInfo>> orders_maps(num_threads);
    std::atomic<size_t> orders_counter(0);

    auto scan_orders = [&](int thread_id) {
        const size_t morsel_size = 50000;
        while (true) {
            size_t start_idx = orders_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= orders_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, orders_rows);

            for (size_t i = start_idx; i < end_idx; i++) {
                if (o_orderdate[i] < date_1995_03_15 &&
                    qualified_customers.count(o_custkey[i]) > 0) {
                    orders_maps[thread_id][o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
                }
            }
        }
    };

    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back(scan_orders, t);
    }
    for (auto& th : threads) th.join();
    threads.clear();

    std::unordered_map<int32_t, OrderInfo> qualified_orders;
    for (auto& m : orders_maps) {
        for (auto& kv : m) {
            qualified_orders.insert(kv);
        }
    }
    orders_maps.clear();
    qualified_customers.clear();

    munmap(o_orderkey, file_size);
    munmap(o_custkey, file_size);
    munmap(o_orderdate, file_size);
    munmap(o_shippriority, file_size);

    // ==================== STEP 3: Scan lineitem, join with orders, aggregate ====================
    auto l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem.l_orderkey.bin", file_size);
    auto l_shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem.l_shipdate.bin", file_size);
    auto l_extendedprice = mmap_column<int64_t>(gendb_dir + "/lineitem.l_extendedprice.bin", file_size);
    auto l_discount = mmap_column<int32_t>(gendb_dir + "/lineitem.l_discount.bin", file_size);
    if (!l_orderkey || !l_shipdate || !l_extendedprice || !l_discount) return;

    std::vector<std::unordered_map<AggKey, double, AggKeyHash>> agg_maps(num_threads);
    std::atomic<size_t> lineitem_counter(0);

    auto scan_lineitem = [&](int thread_id) {
        const size_t morsel_size = 100000;
        while (true) {
            size_t start_idx = lineitem_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= lineitem_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, lineitem_rows);

            for (size_t i = start_idx; i < end_idx; i++) {
                if (l_shipdate[i] > date_1995_03_15) {
                    auto it = qualified_orders.find(l_orderkey[i]);
                    if (it != qualified_orders.end()) {
                        // Revenue = extendedprice * (1 - discount)
                        // extendedprice is int64 cents, discount is int32 (stored as value*100, e.g., 5 = 0.05)
                        // Match Q1 formula: price * (100 - disc) / 100 / 100
                        int64_t price = l_extendedprice[i];
                        int32_t disc = l_discount[i];
                        double revenue = (double)price * (100.0 - disc) / 100.0 / 100.0;

                        AggKey key{l_orderkey[i], it->second.o_orderdate, it->second.o_shippriority};
                        agg_maps[thread_id][key] += revenue;
                    }
                }
            }
        }
    };

    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back(scan_lineitem, t);
    }
    for (auto& th : threads) th.join();
    threads.clear();

    munmap(l_orderkey, file_size);
    munmap(l_shipdate, file_size);
    munmap(l_extendedprice, file_size);
    munmap(l_discount, file_size);

    // ==================== STEP 4: Find Top-K ====================
    std::priority_queue<ResultRow> topk_heap;
    const size_t K = 10;

    for (auto& local_map : agg_maps) {
        for (auto& kv : local_map) {
            ResultRow row{kv.first.l_orderkey, kv.second, kv.first.o_orderdate, kv.first.o_shippriority};

            if (topk_heap.size() < K) {
                topk_heap.push(row);
            } else if (row < topk_heap.top()) {
                topk_heap.pop();
                topk_heap.push(row);
            }
        }
    }

    std::vector<ResultRow> results;
    while (!topk_heap.empty()) {
        results.push_back(topk_heap.top());
        topk_heap.pop();
    }

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

    // ==================== OUTPUT ====================
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (const auto& row : results) {
            out << row.l_orderkey << ","
                << std::fixed << std::setprecision(2) << row.revenue << ","
                << days_to_date_str(row.o_orderdate) << ","
                << row.o_shippriority << "\n";
        }
        out.close();
    }

    std::cout << "Q3: " << results.size() << " rows, "
              << std::fixed << std::setprecision(2) << elapsed_ms << " ms" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
