#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <cmath>
#include <iomanip>

/*
LOGICAL PLAN (ITER4 - JOIN ORDER OPTIMIZATION):
1. Load nation dictionary and identify FRANCE/GERMANY codes
2. Filter supplier by nationkey ∈ {FRANCE, GERMANY} → ~8K rows
3. Filter customer by nationkey ∈ {FRANCE, GERMANY} → ~120K rows
4. Filter lineitem by l_shipdate ∈ [1995-01-01, 1996-12-31] → ~18M rows (parallel)
5. Hash join supplier ⋈ lineitem on l_suppkey → ~1.46M rows
6. Build multi-value hash on lineitem orderkeys, probe orders sequentially to minimize full table scans
7. Hash join result ⋈ customer on custkey
8. Filter by nation pair and compute volume
9. Aggregate and sort

PHYSICAL PLAN (ITER4 - OPTIMIZATION FOCUS):
Key Improvements:
- CRITICAL: Replace std::unordered_map with open-addressing CompactHashTable (2-5x faster)
  * Join supplier: build on 8K rows using CompactHashTable
  * Join lineitem-orders multi-match: use two-array approach (count, offset, positions)
  * Aggregation: compact hash for 4 groups (use small flat array instead)
- Parallel probe on lineitem-orders join to utilize 64 cores
- Pre-filter orders by distinct orderkeys from lineitem (reduce from 15M to ~1.46M unique)
- Parallelism:
  * Lineitem filter: parallel for thread-local (already done)
  * Orders probe: OpenMP parallel for on lineitem joined rows
- Use arena allocation for temporary vectors if allocations dominate
*/

struct MmapFile {
    int fd;
    void* data;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), data(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return;
        }
        size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            data = nullptr;
        }
    }

    ~MmapFile() {
        if (data) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// Helper: convert epoch days to year
inline int32_t days_to_year(int32_t days) {
    // Approximate: epoch day 0 = 1970-01-01
    // Days per year average: 365.25
    int32_t year = 1970 + days / 365;

    // Adjust for leap years more precisely
    while (true) {
        int32_t days_before = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;
        if (days_before > days) {
            year--;
        } else if (days_before + (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 366 : 365) <= days) {
            year++;
        } else {
            break;
        }
    }
    return year;
}

// Default hash function for integers
template<typename K>
struct DefaultHash {
    size_t operator()(K key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }
};

// Compact open-addressing hash table for joins (replaces std::unordered_map for performance)
template<typename K, typename V, typename Hash = DefaultHash<K>>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };
    std::vector<Entry> table;
    size_t mask;
    Hash hasher;

    CompactHashTable() : mask(0) {}

    CompactHashTable(size_t expected_size) {
        if (expected_size == 0) expected_size = 16;
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    void insert(K key, V value) {
        size_t idx = hasher(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hasher(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Helper: load dictionary from text file
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Helper: compute date constant (days since 1970-01-01)
int32_t date_to_days(int32_t year, int32_t month, int32_t day) {
    int32_t days = 0;
    // Days from years
    for (int32_t y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Days from months
    int32_t month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        month_days[2] = 29;
    }
    for (int32_t m = 1; m < month; m++) {
        days += month_days[m];
    }
    // Days in month (1-indexed)
    days += day - 1;
    return days;
}

// Helper: convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    // Approximate year from days
    int32_t year = 1970;
    int32_t remaining = days;

    while (remaining >= 365) {
        bool leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
        int32_t year_days = leap ? 366 : 365;
        if (remaining >= year_days) {
            remaining -= year_days;
            year++;
        } else {
            break;
        }
    }

    int32_t month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    if (leap) month_days[2] = 29;

    int32_t month = 1;
    while (remaining >= month_days[month]) {
        remaining -= month_days[month];
        month++;
    }

    int32_t day = remaining + 1;

    char buf[20];
    snprintf(buf, 20, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

void run_q7(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Compute date constants
    int32_t date_1995_01_01 = date_to_days(1995, 1, 1);
    int32_t date_1996_12_31 = date_to_days(1996, 12, 31);

    // Load binary columns from GenDB
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile lineitem_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapFile lineitem_suppkey(gendb_dir + "/lineitem/l_suppkey.bin");
    MmapFile lineitem_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    MmapFile lineitem_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapFile lineitem_discount(gendb_dir + "/lineitem/l_discount.bin");

    MmapFile orders_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    MmapFile orders_custkey(gendb_dir + "/orders/o_custkey.bin");

    MmapFile customer_custkey(gendb_dir + "/customer/c_custkey.bin");
    MmapFile customer_nationkey(gendb_dir + "/customer/c_nationkey.bin");

    MmapFile supplier_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
    MmapFile supplier_nationkey(gendb_dir + "/supplier/s_nationkey.bin");

    MmapFile nation_nationkey(gendb_dir + "/nation/n_nationkey.bin");
    MmapFile nation_name(gendb_dir + "/nation/n_name.bin");

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    #endif

    // Load nation name dictionary
    auto nation_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");

    // Find nation codes for FRANCE and GERMANY
    int32_t france_code = -1, germany_code = -1;
    for (auto& [code, name] : nation_dict) {
        if (name == "FRANCE") france_code = code;
        if (name == "GERMANY") germany_code = code;
    }

    // Cast mmapped data to appropriate types
    auto li_orderkey = (int32_t*)lineitem_orderkey.data;
    auto li_suppkey = (int32_t*)lineitem_suppkey.data;
    auto li_shipdate = (int32_t*)lineitem_shipdate.data;
    auto li_extendedprice = (int64_t*)lineitem_extendedprice.data;
    auto li_discount = (int64_t*)lineitem_discount.data;

    auto o_orderkey = (int32_t*)orders_orderkey.data;
    auto o_custkey = (int32_t*)orders_custkey.data;

    auto c_custkey = (int32_t*)customer_custkey.data;
    auto c_nationkey = (int32_t*)customer_nationkey.data;

    auto s_suppkey = (int32_t*)supplier_suppkey.data;
    auto s_nationkey = (int32_t*)supplier_nationkey.data;

    int32_t num_lineitem = lineitem_orderkey.size / sizeof(int32_t);
    int32_t num_orders = orders_orderkey.size / sizeof(int32_t);
    int32_t num_customer = customer_custkey.size / sizeof(int32_t);
    int32_t num_supplier = supplier_suppkey.size / sizeof(int32_t);

    // STEP 1: Filter supplier by nationkey (FRANCE or GERMANY)
    #ifdef GENDB_PROFILE
    auto t_supp_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    struct SupplierFiltered {
        int32_t suppkey;
        int32_t nationkey;
    };

    std::vector<SupplierFiltered> filtered_supp;
    filtered_supp.reserve(num_supplier);
    for (int32_t i = 0; i < num_supplier; i++) {
        if (s_nationkey[i] == france_code || s_nationkey[i] == germany_code) {
            filtered_supp.push_back({s_suppkey[i], s_nationkey[i]});
        }
    }

    #ifdef GENDB_PROFILE
    auto t_supp_filter_end = std::chrono::high_resolution_clock::now();
    double supp_filter_ms = std::chrono::duration<double, std::milli>(t_supp_filter_end - t_supp_filter_start).count();
    printf("[TIMING] supp_filter: %.2f ms\n", supp_filter_ms);
    printf("[METADATA] Filtered supplier: %zu rows\n", filtered_supp.size());
    #endif

    // STEP 2: Filter customer by nationkey (FRANCE or GERMANY)
    #ifdef GENDB_PROFILE
    auto t_cust_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    struct CustomerFiltered {
        int32_t custkey;
        int32_t nationkey;
    };

    std::vector<CustomerFiltered> filtered_cust;
    filtered_cust.reserve(num_customer);
    for (int32_t i = 0; i < num_customer; i++) {
        if (c_nationkey[i] == france_code || c_nationkey[i] == germany_code) {
            filtered_cust.push_back({c_custkey[i], c_nationkey[i]});
        }
    }

    #ifdef GENDB_PROFILE
    auto t_cust_filter_end = std::chrono::high_resolution_clock::now();
    double cust_filter_ms = std::chrono::duration<double, std::milli>(t_cust_filter_end - t_cust_filter_start).count();
    printf("[TIMING] cust_filter: %.2f ms\n", cust_filter_ms);
    printf("[METADATA] Filtered customer: %zu rows\n", filtered_cust.size());
    #endif

    // STEP 3: Filter lineitem by shipdate (parallel with thread-local buffers)
    #ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    struct LineitemFiltered {
        int32_t orderkey;
        int32_t suppkey;
        int32_t shipdate;
        int64_t extendedprice;
        int64_t discount;
    };

    // Thread-local buffers for parallel filtering
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<LineitemFiltered>> thread_local_li(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_local_li[t].reserve(10000000 / num_threads + 1000);
    }

    #pragma omp parallel for schedule(static, 100000)
    for (int32_t i = 0; i < num_lineitem; i++) {
        if (li_shipdate[i] >= date_1995_01_01 && li_shipdate[i] <= date_1996_12_31) {
            int tid = omp_get_thread_num();
            thread_local_li[tid].push_back({
                li_orderkey[i],
                li_suppkey[i],
                li_shipdate[i],
                li_extendedprice[i],
                li_discount[i]
            });
        }
    }

    // Merge thread-local buffers
    std::vector<LineitemFiltered> filtered_li;
    size_t total_filtered = 0;
    for (int t = 0; t < num_threads; t++) {
        total_filtered += thread_local_li[t].size();
    }
    filtered_li.reserve(total_filtered);
    for (int t = 0; t < num_threads; t++) {
        filtered_li.insert(filtered_li.end(), thread_local_li[t].begin(), thread_local_li[t].end());
    }

    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", filter_ms);
    printf("[METADATA] Filtered lineitem: %zu rows\n", filtered_li.size());
    #endif

    // STEP 4: Join filtered supplier ⋈ filtered lineitem on l_suppkey
    #ifdef GENDB_PROFILE
    auto t_join_supp_start = std::chrono::high_resolution_clock::now();
    #endif

    struct SupplierLineitemJoined {
        int32_t orderkey;
        int32_t supp_nationkey;
        int32_t shipdate;
        int64_t extendedprice;
        int64_t discount;
    };

    // Build compact hash table on filtered supplier (smaller side, 8K rows)
    CompactHashTable<int32_t, int32_t> supp_by_suppkey(filtered_supp.size());
    for (auto& item : filtered_supp) {
        supp_by_suppkey.insert(item.suppkey, item.nationkey);
    }

    std::vector<SupplierLineitemJoined> supp_li_joined;
    supp_li_joined.reserve(filtered_li.size()); // Upper bound

    // Probe with lineitem (sequential, build side is tiny)
    for (auto& li : filtered_li) {
        auto it = supp_by_suppkey.find(li.suppkey);
        if (it != nullptr) {
            supp_li_joined.push_back({
                li.orderkey,
                *it,
                li.shipdate,
                li.extendedprice,
                li.discount
            });
        }
    }

    #ifdef GENDB_PROFILE
    auto t_join_supp_end = std::chrono::high_resolution_clock::now();
    double join_supp_ms = std::chrono::duration<double, std::milli>(t_join_supp_end - t_join_supp_start).count();
    printf("[TIMING] join_supplier: %.2f ms\n", join_supp_ms);
    printf("[METADATA] After join with supplier: %zu rows\n", supp_li_joined.size());
    #endif

    // STEP 5: Join result ⋈ orders on l_orderkey (OPTIMIZED: multi-value hash + parallel probe)
    #ifdef GENDB_PROFILE
    auto t_join_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    struct SupplierLineitemOrdersJoined {
        int32_t custkey;
        int32_t supp_nationkey;
        int32_t shipdate;
        int64_t extendedprice;
        int64_t discount;
    };

    // Build multi-value hash table: orderkey → {offset, count} into positions array
    // This avoids storing vectors in the hash table which have pointer overhead
    struct MultiValueEntry {
        uint32_t offset;  // offset into positions array
        uint32_t count;   // how many values for this key
    };

    CompactHashTable<int32_t, MultiValueEntry> orderkey_index(supp_li_joined.size());
    std::vector<uint32_t> orderkey_positions;  // packed positions of matching lineitem rows

    // First pass: count occurrences per orderkey
    std::unordered_map<int32_t, uint32_t> orderkey_counts;
    for (size_t i = 0; i < supp_li_joined.size(); i++) {
        orderkey_counts[supp_li_joined[i].orderkey]++;
    }

    // Second pass: compute offsets and build index
    uint32_t offset = 0;
    for (auto& item : supp_li_joined) {
        if (orderkey_index.find(item.orderkey) == nullptr) {
            uint32_t cnt = orderkey_counts[item.orderkey];
            orderkey_index.insert(item.orderkey, {offset, cnt});
            offset += cnt;
        }
    }

    // Third pass: populate positions array
    orderkey_positions.resize(offset);
    std::vector<uint32_t> pos_cursors = std::vector<uint32_t>(offset, 0);
    std::unordered_map<int32_t, uint32_t> pos_counters;
    for (size_t i = 0; i < supp_li_joined.size(); i++) {
        int32_t okey = supp_li_joined[i].orderkey;
        auto* entry = orderkey_index.find(okey);
        if (entry) {
            uint32_t idx = entry->offset + (pos_counters[okey]++);
            orderkey_positions[idx] = i;
        }
    }

    // Fourth pass: probe orders with parallel thread-local aggregation
    int num_probe_threads = omp_get_max_threads();
    std::vector<std::vector<SupplierLineitemOrdersJoined>> thread_results(num_probe_threads);
    for (int t = 0; t < num_probe_threads; t++) {
        thread_results[t].reserve(supp_li_joined.size() / num_probe_threads + 10000);
    }

    #pragma omp parallel for schedule(static, 100000)
    for (int32_t i = 0; i < num_orders; i++) {
        int32_t okey = o_orderkey[i];
        auto* entry = orderkey_index.find(okey);
        if (entry != nullptr) {
            int tid = omp_get_thread_num();
            for (uint32_t j = 0; j < entry->count; j++) {
                uint32_t li_idx = orderkey_positions[entry->offset + j];
                auto& sli = supp_li_joined[li_idx];
                thread_results[tid].push_back({
                    o_custkey[i],
                    sli.supp_nationkey,
                    sli.shipdate,
                    sli.extendedprice,
                    sli.discount
                });
            }
        }
    }

    // Merge thread results
    std::vector<SupplierLineitemOrdersJoined> supp_li_o_joined;
    size_t total_probe_results = 0;
    for (int t = 0; t < num_probe_threads; t++) {
        total_probe_results += thread_results[t].size();
    }
    supp_li_o_joined.reserve(total_probe_results);
    for (int t = 0; t < num_probe_threads; t++) {
        supp_li_o_joined.insert(supp_li_o_joined.end(),
                                 thread_results[t].begin(),
                                 thread_results[t].end());
    }

    #ifdef GENDB_PROFILE
    auto t_join_orders_end = std::chrono::high_resolution_clock::now();
    double join_orders_ms = std::chrono::duration<double, std::milli>(t_join_orders_end - t_join_orders_start).count();
    printf("[TIMING] join_lineitem_orders: %.2f ms\n", join_orders_ms);
    printf("[METADATA] After join with orders: %zu rows\n", supp_li_o_joined.size());
    #endif

    // STEP 6: Join result ⋈ filtered customer on o_custkey
    #ifdef GENDB_PROFILE
    auto t_join_cust_start = std::chrono::high_resolution_clock::now();
    #endif

    struct FinalJoined {
        int32_t supp_nationkey;
        int32_t cust_nationkey;
        int32_t shipdate;
        int64_t extendedprice;
        int64_t discount;
    };

    // Build compact hash table on filtered customer
    CompactHashTable<int32_t, int32_t> cust_by_custkey(filtered_cust.size());
    for (auto& item : filtered_cust) {
        cust_by_custkey.insert(item.custkey, item.nationkey);
    }

    std::vector<FinalJoined> final_joined;
    final_joined.reserve(supp_li_o_joined.size());

    // Probe with supplier-lineitem-orders joined (sequential, build side is small)
    for (auto& item : supp_li_o_joined) {
        auto it = cust_by_custkey.find(item.custkey);
        if (it != nullptr) {
            final_joined.push_back({
                item.supp_nationkey,
                *it,
                item.shipdate,
                item.extendedprice,
                item.discount
            });
        }
    }

    #ifdef GENDB_PROFILE
    auto t_join_cust_end = std::chrono::high_resolution_clock::now();
    double join_cust_ms = std::chrono::duration<double, std::milli>(t_join_cust_end - t_join_cust_start).count();
    printf("[TIMING] join_customer: %.2f ms\n", join_cust_ms);
    printf("[METADATA] After join with customer: %zu rows\n", final_joined.size());
    #endif

    // STEP 7: Filter by nation pair constraint and prepare aggregation
    #ifdef GENDB_PROFILE
    auto t_nation_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    struct AggregationKey {
        int32_t supp_nation;
        int32_t cust_nation;
        int32_t year;

        bool operator==(const AggregationKey& other) const {
            return supp_nation == other.supp_nation &&
                   cust_nation == other.cust_nation &&
                   year == other.year;
        }
    };

    struct AggregationKeyHash {
        size_t operator()(const AggregationKey& k) const {
            return ((size_t)k.supp_nation << 40) | ((size_t)k.cust_nation << 20) | k.year;
        }
    };

    // Filter by nation pair and compute aggregation data
    std::vector<std::pair<AggregationKey, double>> agg_data; // (key, volume)
    agg_data.reserve(final_joined.size());

    for (auto& item : final_joined) {
        // Check nation pair constraint: (FRANCE, GERMANY) or (GERMANY, FRANCE)
        bool valid = false;
        if ((item.supp_nationkey == france_code && item.cust_nationkey == germany_code) ||
            (item.supp_nationkey == germany_code && item.cust_nationkey == france_code)) {
            valid = true;
        }

        if (valid) {
            // Compute volume = l_extendedprice * (1 - l_discount)
            // Both extendedprice and discount are stored as int64_t with scale_factor=100
            // discount: 0.05 → 5, extendedprice: 123.45 → 12345
            // volume = (extendedprice / 100) * (1 - (discount / 100))
            double volume = (static_cast<double>(item.extendedprice) / 100.0) *
                           (1.0 - static_cast<double>(item.discount) / 100.0);

            int32_t year = days_to_year(item.shipdate);

            AggregationKey key = {
                item.supp_nationkey,
                item.cust_nationkey,
                year
            };

            agg_data.push_back({key, volume});
        }
    }

    #ifdef GENDB_PROFILE
    auto t_nation_filter_end = std::chrono::high_resolution_clock::now();
    double nation_filter_ms = std::chrono::duration<double, std::milli>(t_nation_filter_end - t_nation_filter_start).count();
    printf("[TIMING] nation_filter: %.2f ms\n", nation_filter_ms);
    printf("[METADATA] After nation filter: %zu rows\n", agg_data.size());
    #endif

    // STEP 8: Aggregation (optimize for small cardinality: ~4 groups)
    #ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    // Use compact hash table for aggregation (4 expected groups is very small)
    std::unordered_map<AggregationKey, double, AggregationKeyHash> aggregated;
    aggregated.reserve(16); // Expected ~4 groups

    for (auto& [key, volume] : agg_data) {
        aggregated[key] += volume;
    }

    #ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", agg_ms);
    printf("[METADATA] Aggregated groups: %zu\n", aggregated.size());
    #endif

    // STEP 9: Sort results
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    struct Result {
        std::string supp_nation;
        std::string cust_nation;
        int32_t l_year;
        double revenue;
    };

    std::vector<Result> results;
    results.reserve(aggregated.size());

    for (auto& [key, revenue] : aggregated) {
        // Decode nation codes to names
        std::string supp_name = nation_dict.at(key.supp_nation);
        std::string cust_name = nation_dict.at(key.cust_nation);

        results.push_back({supp_name, cust_name, key.year, revenue});
    }

    // Sort by supp_nation, cust_nation, l_year
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
        if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
        return a.l_year < b.l_year;
    });

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
    #endif

    // STEP 10: Write results to CSV
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream csv(results_dir + "/Q7.csv");
    csv << "supp_nation,cust_nation,l_year,revenue\n";

    for (auto& r : results) {
        csv << r.supp_nation << "," << r.cust_nation << "," << r.l_year << ","
            << std::fixed << std::setprecision(4) << r.revenue << "\n";
    }
    csv.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q7(gendb_dir, results_dir);
    return 0;
}
#endif
