#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <map>

namespace {

// Constants for date encoding (days since 1970-01-01)
// 1995-03-15 = 9204 days
constexpr int32_t DATE_1995_03_15 = 9204;

// Convert epoch days to YYYY-MM-DD format
inline std::string epochDaysToString(int32_t days) {
    // Reference: 1970-01-01 is day 0
    // Each year: 365 or 366 days (leap year)
    // Leap year rule: divisible by 4, except century years (divisible by 100) unless divisible by 400

    const int DAYS_IN_MONTH[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int year = 1970;
    int remaining = days;

    // Calculate year
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (remaining < days_in_year) break;
        remaining -= days_in_year;
        year++;
    }

    // Calculate month and day
    int month = 0;
    bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    while (month < 12) {
        int days_in_month = DAYS_IN_MONTH[month];
        if (month == 1 && is_leap) days_in_month = 29;
        if (remaining < days_in_month) break;
        remaining -= days_in_month;
        month++;
    }

    int day = remaining + 1;  // Days are 1-indexed

    char buffer[11];
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year, month + 1, day);
    return std::string(buffer);
}

struct AggregateKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggregateKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct AggregateKeyHash {
    size_t operator()(const AggregateKey& k) const {
        return (((size_t)k.l_orderkey << 32) | k.o_orderdate) ^ ((size_t)k.o_shippriority << 16);
    }
};

struct AggregateResult {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const AggregateResult& other) const {
        if (other.revenue != revenue) {
            return revenue > other.revenue;  // DESC
        }
        return o_orderdate < other.o_orderdate;  // ASC
    }
};

// Helper to mmap a file
void* mmapFile(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file: " << path << std::endl;
        return nullptr;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return nullptr;
    }

    size = st.st_size;
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return ptr;
}

// Load zone map and return relevant row ranges
// Note: Zone map format appears to need clarification, so we'll scan all rows for correctness
std::vector<std::pair<uint64_t, uint64_t>> loadZoneMap(const std::string& path, int32_t threshold, uint64_t total_rows) {
    // For now, just return all rows - zone map format needs investigation
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    ranges.push_back({0, total_rows});
    return ranges;
}

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // ===== Step 1: Load dictionaries =====
    std::unordered_map<uint8_t, std::string> mktsegment_dict;
    mktsegment_dict[0] = "BUILDING";
    mktsegment_dict[1] = "AUTOMOBILE";
    mktsegment_dict[2] = "MACHINERY";
    mktsegment_dict[3] = "HOUSEHOLD";
    mktsegment_dict[4] = "FURNITURE";

    // ===== Step 2: Load customer table =====
    size_t customer_size = 0;
    const int32_t* c_custkey_data = (const int32_t*)mmapFile(
        gendb_dir + "/customer/c_custkey.col", customer_size);
    size_t customer_rows = customer_size / sizeof(int32_t);

    size_t mktseg_size = 0;
    const uint8_t* c_mktsegment_data = (const uint8_t*)mmapFile(
        gendb_dir + "/customer/c_mktsegment.col", mktseg_size);

    // Filter customers by mktsegment = 'BUILDING' (code 0)
    std::unordered_map<int32_t, std::vector<size_t>> customer_building;
    for (size_t i = 0; i < customer_rows; ++i) {
        if (c_mktsegment_data[i] == 0) {  // BUILDING
            customer_building[c_custkey_data[i]].push_back(i);
        }
    }

    // ===== Step 3: Load orders table =====
    size_t orders_size = 0;
    const int32_t* o_orderkey_data = (const int32_t*)mmapFile(
        gendb_dir + "/orders/o_orderkey.col", orders_size);
    size_t orders_rows = orders_size / sizeof(int32_t);

    size_t custkey_size = 0;
    const int32_t* o_custkey_data = (const int32_t*)mmapFile(
        gendb_dir + "/orders/o_custkey.col", custkey_size);

    size_t orderdate_size = 0;
    const int32_t* o_orderdate_data = (const int32_t*)mmapFile(
        gendb_dir + "/orders/o_orderdate.col", orderdate_size);

    size_t shippriority_size = 0;
    const int32_t* o_shippriority_data = (const int32_t*)mmapFile(
        gendb_dir + "/orders/o_shippriority.col", shippriority_size);

    // Build hash table: orderkey -> (orderdate, shippriority) for matching customers with o_orderdate < 1995-03-15
    std::unordered_map<int32_t, std::vector<std::pair<int32_t, int32_t>>> orders_by_orderkey;
    for (size_t i = 0; i < orders_rows; ++i) {
        int32_t custkey = o_custkey_data[i];
        int32_t orderdate = o_orderdate_data[i];

        // Filter: customer is in BUILDING segment AND orderdate < 1995-03-15
        if (customer_building.count(custkey) && orderdate < DATE_1995_03_15) {
            int32_t orderkey = o_orderkey_data[i];
            int32_t shippriority = o_shippriority_data[i];
            orders_by_orderkey[orderkey].push_back({orderdate, shippriority});
        }
    }

    // ===== Step 4: Load lineitem table =====

    size_t lineitem_orderkey_size = 0;
    const int32_t* l_orderkey_data = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem/l_orderkey.col", lineitem_orderkey_size);

    size_t shipdate_size = 0;
    const int32_t* l_shipdate_data = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem/l_shipdate.col", shipdate_size);
    size_t lineitem_rows = shipdate_size / sizeof(int32_t);

    size_t extendedprice_size = 0;
    const double* l_extendedprice_data = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_extendedprice.col", extendedprice_size);

    size_t discount_size = 0;
    const double* l_discount_data = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_discount.col", discount_size);

    // Global aggregation map
    std::unordered_map<AggregateKey, double, AggregateKeyHash> global_agg;
    std::mutex agg_mutex;

    // ===== Step 5: Parallel scan of lineitem =====
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::unordered_map<AggregateKey, double, AggregateKeyHash> local_agg;

            // Partition lineitem into morsels across threads
            for (uint64_t i = t * morsel_size; i < lineitem_rows; i += num_threads * morsel_size) {
                uint64_t end = std::min(i + morsel_size, lineitem_rows);
                for (uint64_t j = i; j < end; ++j) {
                    int32_t orderkey = l_orderkey_data[j];
                    int32_t shipdate = l_shipdate_data[j];

                    // Filter: shipdate > 1995-03-15
                    if (shipdate > DATE_1995_03_15 && orders_by_orderkey.count(orderkey)) {
                        double extendedprice = l_extendedprice_data[j];
                        double discount = l_discount_data[j];
                        double revenue = extendedprice * (1.0 - discount);

                        // Get order info
                        auto& order_list = orders_by_orderkey[orderkey];
                        for (auto& [orderdate, shippriority] : order_list) {
                            AggregateKey key{orderkey, orderdate, shippriority};
                            local_agg[key] += revenue;
                        }
                    }
                }
            }

            // Merge local results into global
            {
                std::lock_guard<std::mutex> lock(agg_mutex);
                for (auto& [key, value] : local_agg) {
                    global_agg[key] += value;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // ===== Step 6: Convert to results and sort =====
    std::vector<AggregateResult> results;
    results.reserve(global_agg.size());

    for (auto& [key, revenue] : global_agg) {
        results.push_back({
            key.l_orderkey,
            revenue,
            key.o_orderdate,
            key.o_shippriority
        });
    }

    std::sort(results.begin(), results.end());

    // ===== Step 7: Write results (if output dir specified) =====
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/Q3.csv");
        outfile << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

        size_t limit = std::min(results.size(), (size_t)10);
        for (size_t i = 0; i < limit; ++i) {
            outfile << results[i].l_orderkey << ","
                    << std::fixed << std::setprecision(4) << results[i].revenue << ","
                    << epochDaysToString(results[i].o_orderdate) << ","
                    << results[i].o_shippriority << "\n";
        }
        outfile.close();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Query returned " << results.size() << " rows" << std::endl;
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;
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
