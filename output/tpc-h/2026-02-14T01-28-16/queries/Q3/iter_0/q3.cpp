// q3.cpp - Self-contained TPC-H Q3: Shipping Priority
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <queue>
#include <thread>
#include <atomic>
#include <mutex>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <cmath>

// JSON parsing (minimal)
#include <sstream>

// Result structure for Top-K
struct Q3Result {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const Q3Result& other) const {
        // Max-heap: reverse order (for min-heap of top elements)
        if (std::abs(revenue - other.revenue) > 1e-9) return revenue < other.revenue;
        return o_orderdate > other.o_orderdate;
    }
};

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

namespace std {
    template<>
    struct hash<AggKey> {
        size_t operator()(const AggKey& k) const {
            return hash<int32_t>()(k.l_orderkey) ^
                   (hash<int32_t>()(k.o_orderdate) << 1) ^
                   (hash<int32_t>()(k.o_shippriority) << 2);
        }
    };
}

// Zone map structure
struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
};

// Memory-mapped column
template<typename T>
struct MMapColumn {
    T* data = nullptr;
    size_t count = 0;
    int fd = -1;
    size_t file_size = 0;

    void open(const std::string& path, size_t expected_count) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            ::close(fd);
            return;
        }

        file_size = sb.st_size;
        count = file_size / sizeof(T);

        void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (addr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            ::close(fd);
            return;
        }

        madvise(addr, file_size, MADV_SEQUENTIAL);
        data = static_cast<T*>(addr);
    }

    ~MMapColumn() {
        if (data) {
            munmap(data, file_size);
            ::close(fd);
        }
    }
};

// Date conversion
std::string format_date(int32_t days_since_epoch) {
    // Simple conversion: days since 1970-01-01
    int32_t year = 1970;
    int32_t days = days_since_epoch;

    // Approximate year
    year += days / 365;
    days %= 365;

    // Refine (handle leap years)
    auto is_leap = [](int y) { return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0); };

    int32_t epoch_days = 0;
    for (int y = 1970; y < year; ++y) {
        epoch_days += is_leap(y) ? 366 : 365;
    }

    while (epoch_days > days_since_epoch) {
        --year;
        epoch_days -= is_leap(year) ? 366 : 365;
    }

    while (epoch_days + (is_leap(year) ? 366 : 365) <= days_since_epoch) {
        epoch_days += is_leap(year) ? 366 : 365;
        ++year;
    }

    days = days_since_epoch - epoch_days;

    // Month/day calculation
    int days_in_month[] = {31, is_leap(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int month = 1;
    for (int m = 0; m < 12; ++m) {
        if (days < days_in_month[m]) {
            month = m + 1;
            break;
        }
        days -= days_in_month[m];
    }
    int day = days + 1;

    char buf[12];
    snprintf(buf, 12, "%04d-%02d-%02d", year, month, day);
    return buf;
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load metadata (dictionaries)
    std::unordered_map<uint8_t, std::string> c_mktsegment_dict;
    {
        std::ifstream meta_file(gendb_dir + "/metadata.json");
        std::string line, content;
        while (std::getline(meta_file, line)) content += line;

        // Parse customer_c_mktsegment dictionary (simple string search)
        size_t pos = content.find("\"customer_c_mktsegment\"");
        if (pos != std::string::npos) {
            size_t start = content.find("[", pos);
            size_t end = content.find("]", start);
            std::string dict_str = content.substr(start + 1, end - start - 1);

            uint8_t code = 0;
            size_t i = 0;
            while (i < dict_str.size()) {
                if (dict_str[i] == '"') {
                    size_t j = dict_str.find('"', i + 1);
                    c_mktsegment_dict[code++] = dict_str.substr(i + 1, j - i - 1);
                    i = j + 1;
                } else {
                    ++i;
                }
            }
        }
    }

    // Find BUILDING code
    uint8_t building_code = 0;
    for (const auto& [code, val] : c_mktsegment_dict) {
        if (val == "BUILDING") {
            building_code = code;
            break;
        }
    }

    // Load customer columns
    MMapColumn<int32_t> c_custkey;
    MMapColumn<uint8_t> c_mktsegment;

    c_custkey.open(gendb_dir + "/customer_c_custkey.bin", 1500000);
    c_mktsegment.open(gendb_dir + "/customer_c_mktsegment.bin", 1500000);

    size_t customer_count = c_custkey.count;

    // Filter customer (c_mktsegment = 'BUILDING')
    std::vector<int32_t> filtered_custkeys;
    filtered_custkeys.reserve(customer_count / 5);  // ~20% selectivity

    for (size_t i = 0; i < customer_count; ++i) {
        if (c_mktsegment.data[i] == building_code) {
            filtered_custkeys.push_back(c_custkey.data[i]);
        }
    }

    // Build hash table on filtered customers
    std::unordered_map<int32_t, bool> customer_ht;
    customer_ht.reserve(filtered_custkeys.size() * 1.5);
    for (int32_t ck : filtered_custkeys) {
        customer_ht[ck] = true;
    }

    // Load orders columns
    MMapColumn<int32_t> o_orderkey;
    MMapColumn<int32_t> o_custkey;
    MMapColumn<int32_t> o_orderdate;
    MMapColumn<int32_t> o_shippriority;

    o_orderkey.open(gendb_dir + "/orders_o_orderkey.bin", 15000000);
    o_custkey.open(gendb_dir + "/orders_o_custkey.bin", 15000000);
    o_orderdate.open(gendb_dir + "/orders_o_orderdate.bin", 15000000);
    o_shippriority.open(gendb_dir + "/orders_o_shippriority.bin", 15000000);

    size_t orders_count = o_orderkey.count;

    // Load orders zone map
    std::vector<ZoneMap> o_zonemap;
    {
        std::ifstream zm_file(gendb_dir + "/orders_o_orderdate_zonemap.idx", std::ios::binary);
        if (zm_file) {
            zm_file.seekg(0, std::ios::end);
            size_t size = zm_file.tellg();
            zm_file.seekg(0);
            size_t num_zones = size / sizeof(ZoneMap);
            o_zonemap.resize(num_zones);
            zm_file.read(reinterpret_cast<char*>(o_zonemap.data()), size);
        }
    }

    // Filter orders (o_orderdate < 1995-03-15 = 9204) AND join with customer
    const int32_t date_threshold = 9204;
    const size_t block_size = 100000;

    struct OrderJoinResult {
        int32_t o_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<OrderJoinResult> order_join_results;
    order_join_results.reserve(orders_count / 5);

    for (size_t block_start = 0; block_start < orders_count; block_start += block_size) {
        size_t block_idx = block_start / block_size;

        // Zone map pruning
        if (block_idx < o_zonemap.size()) {
            if (o_zonemap[block_idx].min_val >= date_threshold) {
                continue;  // Skip entire block
            }
        }

        size_t block_end = std::min(block_start + block_size, orders_count);
        for (size_t i = block_start; i < block_end; ++i) {
            if (o_orderdate.data[i] < date_threshold &&
                customer_ht.count(o_custkey.data[i]) > 0) {
                order_join_results.push_back({
                    o_orderkey.data[i],
                    o_orderdate.data[i],
                    o_shippriority.data[i]
                });
            }
        }
    }

    // Build hash table on order join results
    std::unordered_map<int32_t, std::vector<std::pair<int32_t, int32_t>>> order_ht;
    order_ht.reserve(order_join_results.size() * 1.5);

    for (const auto& oj : order_join_results) {
        order_ht[oj.o_orderkey].push_back({oj.o_orderdate, oj.o_shippriority});
    }

    // Load lineitem columns
    MMapColumn<int32_t> l_orderkey;
    MMapColumn<double> l_extendedprice;
    MMapColumn<double> l_discount;
    MMapColumn<int32_t> l_shipdate;

    l_orderkey.open(gendb_dir + "/lineitem_l_orderkey.bin", 59986052);
    l_extendedprice.open(gendb_dir + "/lineitem_l_extendedprice.bin", 59986052);
    l_discount.open(gendb_dir + "/lineitem_l_discount.bin", 59986052);
    l_shipdate.open(gendb_dir + "/lineitem_l_shipdate.bin", 59986052);

    size_t lineitem_count = l_orderkey.count;

    // Load lineitem zone map
    std::vector<ZoneMap> l_zonemap;
    {
        std::ifstream zm_file(gendb_dir + "/lineitem_l_shipdate_zonemap.idx", std::ios::binary);
        if (zm_file) {
            zm_file.seekg(0, std::ios::end);
            size_t size = zm_file.tellg();
            zm_file.seekg(0);
            size_t num_zones = size / sizeof(ZoneMap);
            l_zonemap.resize(num_zones);
            zm_file.read(reinterpret_cast<char*>(l_zonemap.data()), size);
        }
    }

    // Parallel aggregation with join and filter
    const unsigned num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 50000;
    std::atomic<size_t> morsel_idx{0};

    std::vector<std::unordered_map<AggKey, double>> local_aggs(num_threads);

    auto worker = [&](unsigned tid) {
        auto& local_map = local_aggs[tid];
        local_map.reserve(10000);

        while (true) {
            size_t block_start = morsel_idx.fetch_add(1) * morsel_size;
            if (block_start >= lineitem_count) break;

            size_t block_idx = block_start / 100000;

            // Zone map pruning (l_shipdate > 9204)
            if (block_idx < l_zonemap.size()) {
                if (l_zonemap[block_idx].max_val <= date_threshold) {
                    continue;  // Skip entire block
                }
            }

            size_t block_end = std::min(block_start + morsel_size, lineitem_count);

            for (size_t i = block_start; i < block_end; ++i) {
                if (l_shipdate.data[i] > date_threshold) {
                    int32_t ok = l_orderkey.data[i];
                    auto it = order_ht.find(ok);
                    if (it != order_ht.end()) {
                        double revenue = l_extendedprice.data[i] * (1.0 - l_discount.data[i]);

                        for (const auto& [od, sp] : it->second) {
                            AggKey key{ok, od, sp};
                            local_map[key] += revenue;
                        }
                    }
                }
            }
        }
    };

    std::vector<std::thread> threads;
    for (unsigned t = 0; t < num_threads; ++t) {
        threads.emplace_back(worker, t);
    }
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local results
    std::unordered_map<AggKey, double> global_agg;
    for (const auto& local_map : local_aggs) {
        for (const auto& [key, rev] : local_map) {
            global_agg[key] += rev;
        }
    }

    // Top-K using priority queue (min-heap of size 10)
    auto cmp = [](const Q3Result& a, const Q3Result& b) {
        if (std::abs(a.revenue - b.revenue) > 1e-9) return a.revenue < b.revenue;
        return a.o_orderdate > b.o_orderdate;
    };
    std::priority_queue<Q3Result, std::vector<Q3Result>, decltype(cmp)> topk(cmp);

    for (const auto& [key, rev] : global_agg) {
        Q3Result res{key.l_orderkey, rev, key.o_orderdate, key.o_shippriority};

        if (topk.size() < 10) {
            topk.push(res);
        } else if (res.revenue > topk.top().revenue ||
                   (std::abs(res.revenue - topk.top().revenue) < 1e-9 &&
                    res.o_orderdate < topk.top().o_orderdate)) {
            topk.pop();
            topk.push(res);
        }
    }

    // Extract results and sort
    std::vector<Q3Result> results;
    while (!topk.empty()) {
        results.push_back(topk.top());
        topk.pop();
    }

    std::sort(results.begin(), results.end(), [](const Q3Result& a, const Q3Result& b) {
        if (std::abs(a.revenue - b.revenue) > 1e-9) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    });

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Output results
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (const auto& r : results) {
            out << r.l_orderkey << ","
                << std::fixed << std::setprecision(2) << r.revenue << ","
                << format_date(r.o_orderdate) << ","
                << r.o_shippriority << "\n";
        }
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
