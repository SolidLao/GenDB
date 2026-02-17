/*****************************************************************************
 * Q7: Volume Shipping Query - Iteration 1
 *
 * OPTIMIZATION PLAN:
 * Primary bottleneck: load_data (929ms / 92%)
 * - Building customer_nation hash map: ~120K filtered entries from 1.5M
 * - Building order_customer hash map: ~15M entries
 * - Solution: Use pre-built mmap indexes to eliminate build time
 *
 * LOGICAL PLAN:
 * 1. Filter nation to FRANCE and GERMANY (25 → 2 rows)
 * 2. Build direct arrays indexed by nationkey for nation names
 * 3. Join supplier with filtered nations → boolean filter array
 * 4. Scan lineitem with l_shipdate filter [1995-01-01, 1996-12-31]
 *    - Use zone map to skip blocks outside date range
 * 5. For each qualifying lineitem row:
 *    - Lookup supplier nation via s_suppkey (direct array)
 *    - Lookup order → customer via pre-built customer_custkey_hash index
 *    - Lookup customer nation (direct array or pre-filtered set)
 *    - Check nation pair matches (FRANCE,GERMANY) or (GERMANY,FRANCE)
 *    - Extract year from l_shipdate
 *    - Accumulate revenue in compact hash table keyed by (supp_nation, cust_nation, year)
 * 6. Sort by (supp_nation, cust_nation, year) and output
 *
 * PHYSICAL PLAN (ITERATION 1):
 * - Nation: direct array lookup (25 elements)
 * - Supplier: direct array filter indexed by s_suppkey (100K elements)
 * - Customer: mmap pre-built customer_custkey_hash index (1.5M entries, 0ms build)
 * - Orders: load o_orderkey/o_custkey columns, build compact hash (faster than unordered_map)
 * - Lineitem: parallel scan with zone map pruning on l_shipdate
 * - Date extraction: precomputed lookup table (O(1) year extraction)
 * - Aggregation: open-addressing hash table (~8 groups expected)
 * - Parallelism: parallel lineitem scan with thread-local aggregation
 *
 * KEY OPTIMIZATIONS:
 * - Pre-built index loading via mmap (eliminates customer hash build time)
 * - Open-addressing hash table for orders join (2-5x faster than std::unordered_map)
 * - Open-addressing hash table for aggregation (2-5x faster than std::unordered_map)
 * - Filter customer nationkey during lookup (avoid pre-filtering entire table)
 ******************************************************************************/

#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// Date lookup table for O(1) year extraction
static int16_t YEAR_TABLE[30000];

void init_date_table() {
    int year = 1970, month = 1, day_of_month = 1;
    const int days_per_month[] = {31,28,31,30,31,30,31,31,30,31,30,31};

    for (int d = 0; d < 30000; d++) {
        YEAR_TABLE[d] = year;

        day_of_month++;
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int dim = days_per_month[month - 1] + (month == 2 && leap ? 1 : 0);
        if (day_of_month > dim) {
            day_of_month = 1;
            month++;
            if (month > 12) {
                month = 1;
                year++;
            }
        }
    }
}

// Memory-mapped file helper
template<typename T>
T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    count = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        exit(1);
    }
    return static_cast<T*>(addr);
}

// Load nation names (length-prefixed binary strings)
std::vector<std::string> load_nation_names(const std::string& gendb_dir) {
    std::vector<std::string> names;
    std::ifstream f(gendb_dir + "/nation/n_name.bin", std::ios::binary);
    if (!f.is_open()) {
        std::cerr << "Failed to open nation names" << std::endl;
        exit(1);
    }

    while (f) {
        uint32_t len;
        if (!f.read(reinterpret_cast<char*>(&len), sizeof(uint32_t))) break;
        if (len == 0 || len > 10000) break; // sanity check

        std::string s(len, '\0');
        if (!f.read(&s[0], len)) break;
        names.push_back(s);
    }

    return names;
}

// Compact open-addressing hash table for int32_t → int32_t mapping
struct CompactHashTable {
    struct Entry {
        int32_t key;
        int32_t value;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
        for (auto& e : table) e.occupied = false;
    }

    size_t hash(int32_t key) const {
        // Fibonacci hashing for good distribution
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key, int32_t value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    int32_t* find(int32_t key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Compact hash table for aggregation (composite key)
struct AggKey {
    int32_t supp_nation_idx;  // Index into nation_names array (using int32_t to match nationkey type)
    int32_t cust_nation_idx;
    int16_t year;

    bool operator==(const AggKey& o) const {
        return year == o.year && supp_nation_idx == o.supp_nation_idx && cust_nation_idx == o.cust_nation_idx;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        uint64_t combined = ((uint64_t)k.supp_nation_idx << 32) | ((uint64_t)k.cust_nation_idx << 16) | (uint64_t)k.year;
        return combined * 0x9E3779B97F4A7C15ULL;
    }
};

struct CompactAggTable {
    struct Entry {
        AggKey key;
        int64_t value;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactAggTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
        for (auto& e : table) e.occupied = false;
    }

    size_t hash(const AggKey& key) const {
        AggKeyHash h;
        return h(key);
    }

    void add(const AggKey& key, int64_t value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value += value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    std::vector<std::pair<AggKey, int64_t>> get_all() const {
        std::vector<std::pair<AggKey, int64_t>> result;
        for (const auto& e : table) {
            if (e.occupied) result.push_back({e.key, e.value});
        }
        return result;
    }
};

// Pre-built hash index loader for customer_custkey_hash
struct CustomerHashIndex {
    struct Entry {
        int32_t key;
        uint32_t position;
    };

    Entry* entries;
    uint32_t num_entries;
    uint32_t table_size;
    size_t mask;

    CustomerHashIndex(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        struct stat sb;
        fstat(fd, &sb);
        void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (addr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            exit(1);
        }

        uint32_t* header = static_cast<uint32_t*>(addr);
        num_entries = header[0];
        table_size = header[1];
        mask = table_size - 1;
        entries = reinterpret_cast<Entry*>(header + 2);
    }

    size_t hash(int32_t key) const {
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    uint32_t* find(int32_t key) {
        size_t idx = hash(key) & mask;
        while (entries[idx].key != 0 || entries[idx].position != 0) {
            if (entries[idx].key == key) return &entries[idx].position;
            idx = (idx + 1) & mask;
            if (idx >= table_size) idx = 0;
        }
        return nullptr;
    }
};

void run_q7(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    init_date_table();

    // Date range: 1995-01-01 to 1996-12-31
    const int32_t date_min = 9131;
    const int32_t date_max = 9861;

    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load nation
    size_t nation_count;
    int32_t* n_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    std::vector<std::string> n_name = load_nation_names(gendb_dir);

    // Find FRANCE and GERMANY nationkeys
    int32_t france_key = -1, germany_key = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (n_name[i] == "FRANCE") france_key = n_nationkey[i];
        if (n_name[i] == "GERMANY") germany_key = n_nationkey[i];
    }
    if (france_key < 0 || germany_key < 0) {
        std::cerr << "Nation keys not found" << std::endl;
        exit(1);
    }

    // Build nation name lookup array (direct indexing by nationkey)
    std::string nation_names[25];
    bool nation_is_target[25] = {};
    for (size_t i = 0; i < nation_count; i++) {
        nation_names[n_nationkey[i]] = n_name[i];
        if (n_nationkey[i] == france_key || n_nationkey[i] == germany_key) {
            nation_is_target[n_nationkey[i]] = true;
        }
    }

    // Load supplier
    size_t supplier_count;
    int32_t* s_suppkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Build supplier→nation mapping (direct array indexed by s_suppkey)
    int32_t supplier_nation[100001] = {}; // s_suppkey is 1-based
    bool supplier_is_target[100001] = {};
    for (size_t i = 0; i < supplier_count; i++) {
        supplier_nation[s_suppkey[i]] = s_nationkey[i];
        if (nation_is_target[s_nationkey[i]]) {
            supplier_is_target[s_suppkey[i]] = true;
        }
    }

    // Load customer (just c_nationkey column - custkey comes from index lookup)
    size_t customer_count;
    int32_t* c_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // Load pre-built customer_custkey_hash index
    CustomerHashIndex customer_hash(gendb_dir + "/indexes/customer_custkey_hash.bin");

    // Load orders
    size_t orders_count;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    // Build compact hash table for orders (o_orderkey → o_custkey)
    // Only for customers with target nations
    CompactHashTable order_customer(orders_count / 4);
    for (size_t i = 0; i < orders_count; i++) {
        // Look up customer's position
        uint32_t* cust_pos = customer_hash.find(o_custkey[i]);
        if (cust_pos != nullptr) {
            // Check if customer has target nation
            if (nation_is_target[c_nationkey[*cust_pos]]) {
                order_customer.insert(o_orderkey[i], o_custkey[i]);
            }
        }
    }

    // Load lineitem
    size_t lineitem_count;
    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count);
    int32_t* l_suppkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_count);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", load_ms);
    #endif

    // Load zone map index for lineitem l_shipdate
    #ifdef GENDB_PROFILE
    auto t_zone_start = std::chrono::high_resolution_clock::now();
    #endif

    int fd = open((gendb_dir + "/indexes/lineitem_shipdate_zone.bin").c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    void* zone_addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    uint32_t* zone_ptr = static_cast<uint32_t*>(zone_addr);
    uint32_t num_zones = zone_ptr[0];
    struct ZoneEntry { int32_t min_val; int32_t max_val; };
    ZoneEntry* zones = reinterpret_cast<ZoneEntry*>(zone_ptr + 1);

    const size_t block_size = 100000;
    std::vector<bool> block_active(num_zones, false);
    for (uint32_t z = 0; z < num_zones; z++) {
        if (zones[z].max_val >= date_min && zones[z].min_val <= date_max) {
            block_active[z] = true;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_zone_end = std::chrono::high_resolution_clock::now();
    double zone_ms = std::chrono::duration<double, std::milli>(t_zone_end - t_zone_start).count();
    printf("[TIMING] zone_map_setup: %.2f ms\n", zone_ms);
    #endif

    // Parallel scan and aggregation
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    int num_threads = omp_get_max_threads();
    std::vector<CompactAggTable> thread_aggs;
    for (int i = 0; i < num_threads; i++) {
        thread_aggs.emplace_back(16);  // Small expected size per thread
    }

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_agg = thread_aggs[tid];

        #pragma omp for schedule(dynamic, 10)
        for (uint32_t z = 0; z < num_zones; z++) {
            if (!block_active[z]) continue;

            size_t start = z * block_size;
            size_t end = std::min(start + block_size, lineitem_count);

            for (size_t i = start; i < end; i++) {
                // Filter by date
                if (l_shipdate[i] < date_min || l_shipdate[i] > date_max) continue;

                // Check supplier nation
                int32_t suppkey = l_suppkey[i];
                if (!supplier_is_target[suppkey]) continue;
                int32_t supp_nation_key = supplier_nation[suppkey];

                // Lookup order → customer
                int32_t* custkey_ptr = order_customer.find(l_orderkey[i]);
                if (custkey_ptr == nullptr) continue;
                int32_t custkey = *custkey_ptr;

                // Lookup customer nation via pre-built hash index
                uint32_t* cust_pos = customer_hash.find(custkey);
                if (cust_pos == nullptr) continue;
                int32_t cust_nation_key = c_nationkey[*cust_pos];

                // Check nation pair filter
                if (!((supp_nation_key == france_key && cust_nation_key == germany_key) ||
                      (supp_nation_key == germany_key && cust_nation_key == france_key))) {
                    continue;
                }

                // Extract year
                int16_t year = YEAR_TABLE[l_shipdate[i]];

                // Compute volume
                int64_t volume = l_extendedprice[i] * (100 - l_discount[i]);

                // Aggregate
                AggKey key{supp_nation_key, cust_nation_key, year};
                local_agg.add(key, volume);
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_join: %.2f ms\n", scan_ms);
    #endif

    // Merge thread-local aggregations
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    CompactAggTable final_agg(64);
    for (auto& local : thread_aggs) {
        auto local_results = local.get_all();
        for (auto& [k, v] : local_results) {
            final_agg.add(k, v);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", merge_ms);
    #endif

    // Sort results
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    struct ResultRow {
        std::string supp_nation;
        std::string cust_nation;
        int16_t l_year;
        int64_t revenue;
    };

    std::vector<ResultRow> results;
    auto final_results = final_agg.get_all();
    results.reserve(final_results.size());
    for (auto& [k, v] : final_results) {
        results.push_back({nation_names[k.supp_nation_idx], nation_names[k.cust_nation_idx], k.year, v});
    }

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
        if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
        return a.l_year < b.l_year;
    });

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif

    // Write output
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream out(results_dir + "/Q7.csv");
    out << "supp_nation,cust_nation,l_year,revenue\n";
    for (auto& r : results) {
        double revenue_decimal = r.revenue / 10000.0;
        out << r.supp_nation << "," << r.cust_nation << "," << r.l_year << ","
            << std::fixed << std::setprecision(2) << revenue_decimal << "\n";
    }
    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    std::cout << "Q7 completed: " << results.size() << " rows\n";
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
