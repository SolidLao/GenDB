/*****************************************************************************
 * Q7: Volume Shipping Query - Iteration 2
 *
 * OPTIMIZATIONS APPLIED:
 * 1. Load pre-built customer_custkey_hash index (mmap, avoid building 1.5M hash table)
 * 2. Build order→custkey hash only for target customers (reduce from 15M to ~900K)
 * 3. Direct array for suppliers (100K, avoid hash overhead)
 * 4. Compact open-addressing hash table for aggregation (replace std::unordered_map)
 * 5. Late materialization of nation names (defer until output)
 * 6. Use multiply-shift hash function (avoid std::hash identity function on integers)
 *
 * LOGICAL PLAN:
 * 1. Identify FRANCE/GERMANY nationkeys
 * 2. Build supplier nationkey direct array indexed by s_suppkey
 * 3. Load customer_custkey_hash pre-built index, filter to target nations
 * 4. Build order→custkey hash (filtered to target customers only)
 * 5. Scan lineitem with zone map, fused filter+join+aggregation
 * 6. Materialize nation names, sort, output
 *
 * TARGET: Reduce load_data from 929ms to <200ms by using pre-built indexes
 *****************************************************************************/

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

// Load nation names
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
        if (len == 0 || len > 10000) break;

        std::string s(len, '\0');
        if (!f.read(&s[0], len)) break;
        names.push_back(s);
    }

    return names;
}

// Compact open-addressing hash table (Robin Hood hashing)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint8_t dist;
        bool occupied;
    };
    std::vector<Entry> table;
    size_t mask;

    explicit CompactHashTable(size_t expected = 0) {
        if (expected > 0) {
            size_t cap = 1;
            while (cap < expected * 4 / 3) cap <<= 1;
            table.resize(cap);
            mask = cap - 1;
        }
    }

    inline size_t hash_key(K key) const {
        // Multiply-shift hash (avoid std::hash identity on int32_t)
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    void insert(K key, V value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
    }

    V* find(K key) {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }
};

// Aggregation key (use int8_t for nation IDs to save space)
struct AggKey {
    int8_t supp_nation_id;
    int8_t cust_nation_id;
    int16_t year;

    bool operator==(const AggKey& o) const {
        return supp_nation_id == o.supp_nation_id &&
               cust_nation_id == o.cust_nation_id &&
               year == o.year;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        // Pack into 32-bit integer for fast hashing
        uint32_t packed = ((uint32_t)(uint8_t)k.supp_nation_id << 24) |
                          ((uint32_t)(uint8_t)k.cust_nation_id << 16) |
                          ((uint32_t)(uint16_t)k.year);
        return (uint64_t)packed * 0x9E3779B97F4A7C15ULL >> 32;
    }
};

// Load pre-built customer hash index
struct CustomerHashIndex {
    struct Entry {
        int32_t key;
        uint32_t position;
    };
    Entry* entries;
    uint32_t table_size;
    size_t mask;

    void load(const std::string& index_path) {
        int fd = open(index_path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open customer index" << std::endl;
            exit(1);
        }
        struct stat sb;
        fstat(fd, &sb);
        void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (addr == MAP_FAILED) {
            std::cerr << "Failed to mmap customer index" << std::endl;
            exit(1);
        }

        uint32_t* header = static_cast<uint32_t*>(addr);
        // uint32_t num_entries = header[0];  // unused
        table_size = header[1];
        entries = reinterpret_cast<Entry*>(header + 2);
        mask = table_size - 1;
    }

    inline uint32_t* find(int32_t key) const {
        size_t pos = (((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (entries[pos].key != 0) {
            if (entries[pos].key == key) {
                return const_cast<uint32_t*>(&entries[pos].position);
            }
            pos = (pos + 1) & mask;
        }
        return nullptr;
    }
};

void run_q7(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    init_date_table();

    const int32_t date_min = 9131;  // 1995-01-01
    const int32_t date_max = 9861;  // 1996-12-31

    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load nation
    size_t nation_count;
    int32_t* n_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    std::vector<std::string> n_name = load_nation_names(gendb_dir);

    // Find FRANCE and GERMANY
    int8_t france_id = -1, germany_id = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (n_name[i] == "FRANCE") france_id = static_cast<int8_t>(i);
        if (n_name[i] == "GERMANY") germany_id = static_cast<int8_t>(i);
    }
    if (france_id < 0 || germany_id < 0) {
        std::cerr << "Nation not found" << std::endl;
        exit(1);
    }

    // Build nation lookup: nationkey → nation_id
    int8_t nation_lookup[25];
    bool nation_is_target[25] = {};
    for (size_t i = 0; i < nation_count; i++) {
        nation_lookup[n_nationkey[i]] = static_cast<int8_t>(i);
        if (static_cast<int8_t>(i) == france_id || static_cast<int8_t>(i) == germany_id) {
            nation_is_target[n_nationkey[i]] = true;
        }
    }

    // Load supplier
    size_t supplier_count;
    int32_t* s_suppkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Build direct array: s_suppkey → nationkey
    int32_t supplier_nation[100001];
    memset(supplier_nation, 0, sizeof(supplier_nation));
    for (size_t i = 0; i < supplier_count; i++) {
        supplier_nation[s_suppkey[i]] = s_nationkey[i];
    }

    // Load customer columns
    size_t customer_count;
    // c_custkey not needed (using pre-built index)
    mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // Load pre-built customer hash index
    CustomerHashIndex cust_index;
    cust_index.load(gendb_dir + "/indexes/customer_custkey_hash.bin");

    // Build customer nationkey array indexed by position
    std::vector<int32_t> customer_nation_by_pos(customer_count);
    for (size_t i = 0; i < customer_count; i++) {
        customer_nation_by_pos[i] = c_nationkey[i];
    }

    // Load orders
    size_t orders_count;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    // Build order→custkey hash (only for target nation customers)
    CompactHashTable<int32_t, int32_t> order_customer(1000000);
    for (size_t i = 0; i < orders_count; i++) {
        uint32_t* cust_pos = cust_index.find(o_custkey[i]);
        if (cust_pos != nullptr) {
            int32_t cust_nation = customer_nation_by_pos[*cust_pos];
            if (nation_is_target[cust_nation]) {
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

    // Load zone map
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

    // Use flat array for aggregation (only 8 groups expected)
    // Key encoding: (supp_id * 4 + cust_id) * 3 + (year - 1995)
    // Max: (1 * 4 + 1) * 3 + 1 = 16 slots
    std::vector<std::vector<int64_t>> thread_aggs(num_threads, std::vector<int64_t>(16, 0));

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
                int32_t shipdate = l_shipdate[i];
                if (shipdate < date_min || shipdate > date_max) continue;

                // Check supplier nation
                int32_t supp_nation_key = supplier_nation[l_suppkey[i]];
                if (!nation_is_target[supp_nation_key]) continue;

                // Lookup order → customer
                int32_t* custkey_ptr = order_customer.find(l_orderkey[i]);
                if (custkey_ptr == nullptr) continue;

                // Lookup customer nation via pre-built index
                uint32_t* cust_pos = cust_index.find(*custkey_ptr);
                if (cust_pos == nullptr) continue;
                int32_t cust_nation_key = customer_nation_by_pos[*cust_pos];

                // Check nation pair filter
                int8_t supp_id = nation_lookup[supp_nation_key];
                int8_t cust_id = nation_lookup[cust_nation_key];

                if (!((supp_id == france_id && cust_id == germany_id) ||
                      (supp_id == germany_id && cust_id == france_id))) {
                    continue;
                }

                // Extract year
                int16_t year = YEAR_TABLE[shipdate];

                // Compute volume
                int64_t volume = l_extendedprice[i] * (100 - l_discount[i]);

                // Flat array aggregation
                int slot = ((supp_id == france_id ? 0 : 1) * 4 +
                           (cust_id == france_id ? 0 : 1)) * 3 + (year - 1995);
                local_agg[slot] += volume;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_join: %.2f ms\n", scan_ms);
    #endif

    // Merge aggregations
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<int64_t> final_agg(16, 0);
    for (auto& local : thread_aggs) {
        for (size_t i = 0; i < 16; i++) {
            final_agg[i] += local[i];
        }
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", merge_ms);
    #endif

    // Build results
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
    for (int supp_bit = 0; supp_bit < 2; supp_bit++) {
        for (int cust_bit = 0; cust_bit < 2; cust_bit++) {
            for (int year_offset = 0; year_offset < 3; year_offset++) {
                int slot = (supp_bit * 4 + cust_bit) * 3 + year_offset;
                if (final_agg[slot] > 0) {
                    int8_t supp_id = (supp_bit == 0) ? france_id : germany_id;
                    int8_t cust_id = (cust_bit == 0) ? france_id : germany_id;
                    results.push_back({
                        n_name[supp_id],
                        n_name[cust_id],
                        (int16_t)(1995 + year_offset),
                        final_agg[slot]
                    });
                }
            }
        }
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
