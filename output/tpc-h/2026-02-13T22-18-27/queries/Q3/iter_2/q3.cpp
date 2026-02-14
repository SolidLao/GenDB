// q3.cpp - TPC-H Q3: Shipping Priority Query
// Optimized with parallel hash joins, partial aggregation, zone maps, and SIMD

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
#include <immintrin.h>  // AVX2/SSE intrinsics

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

// Zone map structure - each zone has start_offset, min, max
struct ZoneMapEntry {
    size_t start_offset;  // Starting row index of this zone
    int32_t min_val;
    int32_t max_val;
};

// Load zone map from .zonemap.idx file
// Format: num_zones (uint64_t), then for each zone: start_offset (uint64_t), min (int32_t), max (int32_t)
std::vector<ZoneMapEntry> load_zonemap(const std::string& zonemap_path) {
    std::vector<ZoneMapEntry> zones;
    int fd = open(zonemap_path.c_str(), O_RDONLY);
    if (fd < 0) return zones;

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return zones;
    }

    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) return zones;

    const uint8_t* data = static_cast<const uint8_t*>(addr);
    size_t offset = 0;

    // Read number of zones
    if (sb.st_size < 8) {
        munmap(addr, sb.st_size);
        return zones;
    }
    uint64_t num_zones = *reinterpret_cast<const uint64_t*>(data);
    offset += 8;

    // Each zone entry: start_offset (8 bytes), min (4 bytes), max (4 bytes) = 16 bytes
    zones.reserve(num_zones);
    for (uint64_t i = 0; i < num_zones && offset + 16 <= sb.st_size; i++) {
        ZoneMapEntry entry;
        entry.start_offset = *reinterpret_cast<const uint64_t*>(data + offset);
        offset += 8;
        entry.min_val = *reinterpret_cast<const int32_t*>(data + offset);
        offset += 4;
        entry.max_val = *reinterpret_cast<const int32_t*>(data + offset);
        offset += 4;
        zones.push_back(entry);
    }

    munmap(addr, sb.st_size);
    return zones;
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
        // Improved hash function - multiplicative hash with better mixing
        size_t h = k.l_orderkey;
        h ^= k.o_orderdate + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= k.o_shippriority + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
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

    // Pre-size hash table to avoid rehashing (expect ~20% of 1.5M customers = 300K)
    std::unordered_map<int32_t, int> qualified_customers;
    qualified_customers.reserve(350000);
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

    // Load zone map for o_orderdate to skip blocks
    auto orders_zonemap = load_zonemap(gendb_dir + "/orders.o_orderdate.zonemap.idx");

    // Build zone skip bitmap: skip zones where min_val >= 1995-03-15
    // (We want orderdate < 1995-03-15, so skip zones where ALL dates are >= threshold)
    const size_t zone_size = 100000;  // Zone maps built on 100K row blocks
    std::vector<bool> orders_zone_skip(orders_zonemap.size(), false);
    for (size_t z = 0; z < orders_zonemap.size(); z++) {
        if (orders_zonemap[z].min_val >= date_1995_03_15) {
            orders_zone_skip[z] = true;
        }
    }

    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<std::unordered_map<int32_t, OrderInfo>> orders_maps(num_threads);
    // Pre-size each thread's hash table
    for (auto& m : orders_maps) {
        m.reserve(200000);
    }
    std::atomic<size_t> orders_counter(0);

    auto scan_orders = [&](int thread_id) {
        const size_t morsel_size = 50000;
        while (true) {
            size_t start_idx = orders_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= orders_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, orders_rows);

            // Check if this entire morsel can be skipped
            size_t start_zone = start_idx / zone_size;
            size_t end_zone = (end_idx > 0 ? (end_idx - 1) / zone_size : 0);

            // If entire morsel is in one skippable zone, skip it
            if (start_zone == end_zone && start_zone < orders_zone_skip.size() && orders_zone_skip[start_zone]) {
                continue;
            }

            for (size_t i = start_idx; i < end_idx; i++) {
                // Skip rows in skippable zones
                size_t row_zone = i / zone_size;
                if (row_zone < orders_zone_skip.size() && orders_zone_skip[row_zone]) {
                    continue;
                }

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

    // Pre-size global orders hash table
    std::unordered_map<int32_t, OrderInfo> qualified_orders;
    qualified_orders.reserve(3000000);
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

    // Load zone map for l_shipdate to skip blocks
    auto lineitem_zonemap = load_zonemap(gendb_dir + "/lineitem.l_shipdate.zonemap.idx");

    // Build zone skip bitmap: skip zones where max_val <= 1995-03-15
    // (We want shipdate > 1995-03-15, so skip zones where ALL dates are <= threshold)
    std::vector<bool> lineitem_zone_skip(lineitem_zonemap.size(), false);
    for (size_t z = 0; z < lineitem_zonemap.size(); z++) {
        if (lineitem_zonemap[z].max_val <= date_1995_03_15) {
            lineitem_zone_skip[z] = true;
        }
    }

    std::vector<std::unordered_map<AggKey, double, AggKeyHash>> agg_maps(num_threads);
    // Pre-size each thread's aggregation map (expect ~1M distinct order keys distributed across threads)
    for (auto& m : agg_maps) {
        m.reserve(150000);
    }
    std::atomic<size_t> lineitem_counter(0);

    auto scan_lineitem = [&](int thread_id) {
        const size_t morsel_size = 100000;

        // Pre-allocate SIMD threshold vector for AVX2 (8 x int32)
        __m256i date_threshold_vec = _mm256_set1_epi32(date_1995_03_15);

        while (true) {
            size_t start_idx = lineitem_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= lineitem_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, lineitem_rows);

            size_t i = start_idx;

            // SIMD-accelerated date filtering (process 8 dates at a time)
            size_t simd_end = start_idx + ((end_idx - start_idx) / 8) * 8;
            for (; i < simd_end; i += 8) {
                // Load 8 shipdates
                __m256i dates = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&l_shipdate[i]));

                // Compare: dates > threshold
                __m256i cmp = _mm256_cmpgt_epi32(dates, date_threshold_vec);

                // Extract mask (each bit represents one element)
                int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));

                // Process matching elements
                for (int j = 0; j < 8; j++) {
                    if (mask & (1 << j)) {
                        auto it = qualified_orders.find(l_orderkey[i + j]);
                        if (it != qualified_orders.end()) {
                            int64_t price = l_extendedprice[i + j];
                            int32_t disc = l_discount[i + j];
                            double revenue = (double)price * (100.0 - disc) / 100.0 / 100.0;

                            AggKey key{l_orderkey[i + j], it->second.o_orderdate, it->second.o_shippriority};
                            agg_maps[thread_id][key] += revenue;
                        }
                    }
                }
            }

            // Handle remaining elements (tail that doesn't fit in SIMD width)
            for (; i < end_idx; i++) {
                if (l_shipdate[i] > date_1995_03_15) {
                    auto it = qualified_orders.find(l_orderkey[i]);
                    if (it != qualified_orders.end()) {
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

    // ==================== STEP 4: Merge partial aggregations (CRITICAL FIX) ====================
    // Each thread has partial aggregations - must merge them before Top-K
    std::unordered_map<AggKey, double, AggKeyHash> global_agg;
    global_agg.reserve(500000);  // Pre-size for merged results

    for (auto& local_map : agg_maps) {
        for (auto& kv : local_map) {
            global_agg[kv.first] += kv.second;
        }
    }
    agg_maps.clear();

    // ==================== STEP 5: Find Top-K ====================
    std::priority_queue<ResultRow> topk_heap;
    const size_t K = 10;

    for (auto& kv : global_agg) {
        ResultRow row{kv.first.l_orderkey, kv.second, kv.first.o_orderdate, kv.first.o_shippriority};

        if (topk_heap.size() < K) {
            topk_heap.push(row);
        } else if (row < topk_heap.top()) {
            topk_heap.pop();
            topk_heap.push(row);
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
