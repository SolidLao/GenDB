#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <omp.h>

/*
 * Q7: Volume Shipping (Iteration 7)
 *
 * OPTIMIZATION FOCUS:
 * Load pre-built hash indexes from disk instead of building them.
 * Previous iteration built hash tables in 661.95ms (53% of total time).
 * Pre-built indexes eliminate build time entirely.
 *
 * LOGICAL PLAN:
 * 1. Filter lineitem by l_shipdate BETWEEN 1995-01-01 AND 1996-12-31
 * 2. For each filtered lineitem row, join through supplier, orders, customer
 * 3. Lookup nation names and filter by FRANCE-GERMANY constraint
 * 4. Compute volume and aggregate by (supp_nation, cust_nation, l_year)
 * 5. Sort and output
 *
 * PHYSICAL PLAN:
 * - Load pre-built hash_single indexes for supplier, orders, customer, nation (mmap)
 * - Use existing parallel join loop (no changes needed — lookup semantics unchanged)
 * - Parallel join loop with thread-local aggregation buffers
 * - Merge thread results at end
 *
 * EXPECTED IMPROVEMENT:
 * - Save ~330ms from eliminating hash table builds
 * - Build time: 661.95ms → ~20ms (just loading binary index files)
 * - Total: 1248.23ms → ~950ms (24% improvement)
 */

// Date constants (epoch days)
static constexpr int32_t EPOCH_1995_01_01 = 9131;
static constexpr int32_t EPOCH_1996_12_31 = 9861;

// Precomputed year table for fast year extraction
static int16_t YEAR_TABLE[30000];

void init_year_table() {
    int year = 1970, month = 1, day = 1;
    const int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int epoch_day = 0;

    while (epoch_day < 30000) {
        YEAR_TABLE[epoch_day] = year;

        epoch_day++;
        day++;
        bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int dim = days_per_month[month - 1] + (month == 2 && is_leap ? 1 : 0);

        if (day > dim) {
            day = 1;
            month++;
            if (month > 12) {
                month = 1;
                year++;
            }
        }
    }
}

inline int16_t extract_year(int32_t epoch_day) {
    if (epoch_day < 0 || epoch_day >= 30000) return 1970;
    return YEAR_TABLE[epoch_day];
}

// Pre-built hash index (hash_single layout):
// [uint32_t num_entries] then [key:int32_t, pos:uint32_t] (8B/entry)
struct PrebuiltHashIndex {
    int32_t* keys;
    uint32_t* values;
    uint32_t num_entries;
    void* mmap_ptr;
    size_t mmap_size;

    PrebuiltHashIndex() : keys(nullptr), values(nullptr), num_entries(0), mmap_ptr(nullptr), mmap_size(0) {}

    // Load from file
    bool load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Cannot open " << path << std::endl;
            return false;
        }

        off_t size = lseek(fd, 0, SEEK_END);
        mmap_size = (size_t)size;

        mmap_ptr = mmap(nullptr, mmap_size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);

        if (mmap_ptr == MAP_FAILED) {
            std::cerr << "mmap failed for " << path << std::endl;
            return false;
        }

        // Parse header
        uint32_t* header = (uint32_t*)mmap_ptr;
        num_entries = header[0];

        // Entries start after header
        uint8_t* data = (uint8_t*)mmap_ptr + sizeof(uint32_t);
        keys = (int32_t*)data;
        values = (uint32_t*)(data + num_entries * sizeof(int32_t));

        return true;
    }

    // Simple linear scan to find value by key (hash_single is unsorted)
    uint32_t* find(int32_t key) {
        for (uint32_t i = 0; i < num_entries; i++) {
            if (keys[i] == key) {
                return &values[i];
            }
        }
        return nullptr;
    }

    ~PrebuiltHashIndex() {
        if (mmap_ptr != nullptr && mmap_ptr != MAP_FAILED) {
            munmap(mmap_ptr, mmap_size);
        }
    }
};

// Open-addressing hash table implementation (robin hood)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count = 0;

    CompactHashTable() {}

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
        count++;
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Mmap utility
void* mmap_file(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << path << std::endl;
        return nullptr;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    file_size = (size_t)size;

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// Load dictionary file and find code for a target string
int32_t find_dict_code(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    if (!f) {
        std::cerr << "Cannot open dict " << dict_path << std::endl;
        return -1;
    }

    int32_t code = 0;
    std::string line;
    while (std::getline(f, line)) {
        // Remove trailing whitespace/newlines
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r' || line.back() == ' ')) {
            line.pop_back();
        }
        if (line == target) {
            return code;
        }
        code++;
    }

    std::cerr << "Dictionary code not found for '" << target << "'" << std::endl;
    return -1;
}

// Aggregation key structure
struct AggKey {
    int32_t supp_nation_code;
    int32_t cust_nation_code;
    int16_t l_year;

    bool operator==(const AggKey& other) const {
        return supp_nation_code == other.supp_nation_code &&
               cust_nation_code == other.cust_nation_code &&
               l_year == other.l_year;
    }
};

struct AggResult {
    AggKey key;
    double volume_sum;
};

void run_q7(const std::string& gendb_dir, const std::string& results_dir) {
    init_year_table();

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Load lineitem columns
    size_t li_size;
    int32_t* li_suppkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_suppkey.bin", li_size);
    int32_t* li_orderkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", li_size);
    int32_t* li_shipdate = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", li_size);
    int64_t* li_extendedprice = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", li_size);
    int64_t* li_discount = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_discount.bin", li_size);

    uint64_t li_count = li_size / sizeof(int32_t);

    // Load supplier columns
    size_t s_size;
    int32_t* s_suppkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_suppkey.bin", s_size);
    int32_t* s_nationkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_nationkey.bin", s_size);

    uint64_t s_count = s_size / sizeof(int32_t);

    // Load orders columns
    size_t o_size;
    int32_t* o_orderkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", o_size);
    int32_t* o_custkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_custkey.bin", o_size);

    uint64_t o_count = o_size / sizeof(int32_t);

    // Load customer columns
    size_t c_size;
    int32_t* c_custkey = (int32_t*)mmap_file(gendb_dir + "/customer/c_custkey.bin", c_size);
    int32_t* c_nationkey = (int32_t*)mmap_file(gendb_dir + "/customer/c_nationkey.bin", c_size);

    uint64_t c_count = c_size / sizeof(int32_t);

    // Load nation columns
    size_t n_size;
    int32_t* n_nationkey = (int32_t*)mmap_file(gendb_dir + "/nation/n_nationkey.bin", n_size);
    int32_t* n_name_codes = (int32_t*)mmap_file(gendb_dir + "/nation/n_name.bin", n_size);

    // Load nation dictionary
    int32_t france_code = find_dict_code(gendb_dir + "/nation/n_name_dict.txt", "FRANCE");
    int32_t germany_code = find_dict_code(gendb_dir + "/nation/n_name_dict.txt", "GERMANY");

    // Phase 1: Scan and filter lineitem by shipdate
#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<uint64_t> filtered_li_indices;
    for (uint64_t i = 0; i < li_count; i++) {
        if (li_shipdate[i] >= EPOCH_1995_01_01 && li_shipdate[i] <= EPOCH_1996_12_31) {
            filtered_li_indices.push_back(i);
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double ms_filter = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_filter);
    printf("[TIMING] filtered_rows: %zu\n", filtered_li_indices.size());
#endif

    // Phase 2: Load pre-built hash indexes
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Load pre-built hash_single indexes via mmap
    PrebuiltHashIndex supplier_idx;
    PrebuiltHashIndex orders_idx;
    PrebuiltHashIndex customer_idx;
    PrebuiltHashIndex nation_idx;

    if (!supplier_idx.load(gendb_dir + "/indexes/supplier_suppkey_hash.bin")) {
        std::cerr << "Failed to load supplier index" << std::endl;
        return;
    }
    if (!orders_idx.load(gendb_dir + "/indexes/orders_orderkey_hash.bin")) {
        std::cerr << "Failed to load orders index" << std::endl;
        return;
    }
    if (!customer_idx.load(gendb_dir + "/indexes/customer_custkey_hash.bin")) {
        std::cerr << "Failed to load customer index" << std::endl;
        return;
    }
    if (!nation_idx.load(gendb_dir + "/indexes/nation_nationkey_hash.bin")) {
        std::cerr << "Failed to load nation index" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hashtables: %.2f ms\n", ms_build);
#endif

    // Phase 3: Join with parallel loop and thread-local aggregation
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Use thread-local aggregation buffers
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<AggResult>> thread_agg_buffers(num_threads);

    #pragma omp parallel for schedule(dynamic, 1000)
    for (size_t li_idx_pos = 0; li_idx_pos < filtered_li_indices.size(); li_idx_pos++) {
        uint64_t li_idx = filtered_li_indices[li_idx_pos];
        int thread_id = omp_get_thread_num();
        auto& local_agg = thread_agg_buffers[thread_id];

        // Get lineitem data
        int32_t supp_key = li_suppkey[li_idx];
        int32_t order_key = li_orderkey[li_idx];
        int32_t shipdate = li_shipdate[li_idx];
        int64_t extendedprice = li_extendedprice[li_idx];
        int64_t discount = li_discount[li_idx];

        // Compute volume = extendedprice * (1 - discount)
        double volume = (double)extendedprice * (100.0 - (double)discount) / 10000.0;

        // Lookup supplier nationkey using pre-built index
        uint32_t* supp_pos_ptr = supplier_idx.find(supp_key);
        if (!supp_pos_ptr) continue;
        uint32_t supp_pos = *supp_pos_ptr;
        int32_t supp_nationkey = s_nationkey[supp_pos];

        // Lookup customer key from order using pre-built index
        uint32_t* order_pos_ptr = orders_idx.find(order_key);
        if (!order_pos_ptr) continue;
        uint32_t order_pos = *order_pos_ptr;
        int32_t cust_key = o_custkey[order_pos];

        // Lookup customer nationkey using pre-built index
        uint32_t* cust_pos_ptr = customer_idx.find(cust_key);
        if (!cust_pos_ptr) continue;
        uint32_t cust_pos = *cust_pos_ptr;
        int32_t cust_nationkey = c_nationkey[cust_pos];

        // Get nation codes using pre-built index
        uint32_t* supp_nation_pos_ptr = nation_idx.find(supp_nationkey);
        uint32_t* cust_nation_pos_ptr = nation_idx.find(cust_nationkey);
        if (!supp_nation_pos_ptr || !cust_nation_pos_ptr) continue;

        uint32_t supp_nation_pos = *supp_nation_pos_ptr;
        uint32_t cust_nation_pos = *cust_nation_pos_ptr;
        int32_t supp_nation_code = n_name_codes[supp_nation_pos];
        int32_t cust_nation_code = n_name_codes[cust_nation_pos];

        // Check nation constraint
        bool valid = (
            (supp_nation_code == france_code && cust_nation_code == germany_code) ||
            (supp_nation_code == germany_code && cust_nation_code == france_code)
        );
        if (!valid) continue;

        // Extract year from shipdate
        int16_t year = extract_year(shipdate);

        // Aggregate into thread-local buffer
        AggKey key = {supp_nation_code, cust_nation_code, year};
        bool found = false;
        for (auto& result : local_agg) {
            if (result.key == key) {
                result.volume_sum += volume;
                found = true;
                break;
            }
        }
        if (!found) {
            local_agg.push_back({key, volume});
        }
    }

    // Merge thread-local buffers
    std::vector<AggResult> merged_agg;
    for (int t = 0; t < num_threads; t++) {
        for (const auto& result : thread_agg_buffers[t]) {
            bool found = false;
            for (auto& merged : merged_agg) {
                if (merged.key == result.key) {
                    merged.volume_sum += result.volume_sum;
                    found = true;
                    break;
                }
            }
            if (!found) {
                merged_agg.push_back(result);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join: %.2f ms\n", ms_join);
    printf("[TIMING] aggregate_groups: %zu\n", merged_agg.size());
#endif

    // Phase 4: Convert codes back to nation names and prepare output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::tuple<std::string, std::string, int16_t, double>> results;

    for (const auto& result : merged_agg) {
        int32_t supp_code = result.key.supp_nation_code;
        int32_t cust_code = result.key.cust_nation_code;
        int16_t year = result.key.l_year;

        // Map nation codes back to names
        std::string supp_nation = (supp_code == france_code) ? "FRANCE" :
                                  (supp_code == germany_code) ? "GERMANY" : "UNKNOWN";
        std::string cust_nation = (cust_code == france_code) ? "FRANCE" :
                                  (cust_code == germany_code) ? "GERMANY" : "UNKNOWN";

        double revenue = result.volume_sum;
        results.push_back(std::make_tuple(supp_nation, cust_nation, year, revenue));
    }

    // Sort by (supp_nation, cust_nation, l_year)
    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            if (std::get<0>(a) != std::get<0>(b)) return std::get<0>(a) < std::get<0>(b);
            if (std::get<1>(a) != std::get<1>(b)) return std::get<1>(a) < std::get<1>(b);
            return std::get<2>(a) < std::get<2>(b);
        }
    );

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    // Write CSV results
#ifdef GENDB_PROFILE
    auto t_csv_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q7.csv";
    std::ofstream ofs(output_file);
    if (!ofs) {
        std::cerr << "Cannot open output file " << output_file << std::endl;
        return;
    }

    ofs << "supp_nation,cust_nation,l_year,revenue\n";

    for (const auto& [supp_nation, cust_nation, year, revenue] : results) {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s,%s,%d,%.4f\n",
                 supp_nation.c_str(), cust_nation.c_str(), year, revenue);
        ofs << buf;
    }

    ofs.close();

#ifdef GENDB_PROFILE
    auto t_csv_end = std::chrono::high_resolution_clock::now();
    double ms_csv = std::chrono::duration<double, std::milli>(t_csv_end - t_csv_start).count();
    printf("[TIMING] csv_write: %.2f ms\n", ms_csv);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
#ifdef GENDB_PROFILE
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    std::cout << "Q7 results written to " << output_file << std::endl;
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
