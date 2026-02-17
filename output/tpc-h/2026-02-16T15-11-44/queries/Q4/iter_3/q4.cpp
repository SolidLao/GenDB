/*
 * Q4: Order Priority Checking
 *
 * LOGICAL PLAN:
 * =============
 * 1. Scan orders table with filter: o_orderdate >= 8582 AND o_orderdate < 8674 (1993-07-01 to 1993-10-01)
 *    Expected: ~265K filtered rows (from 15M total)
 *    This is the outer table for a semi-join with lineitem
 *
 * 2. Use pre-built hash index on lineitem.l_orderkey for semi-join:
 *    - Load idx_lineitem_orderkey_hash.bin (O(1) mmap vs O(N) build)
 *    - Filter lineitem: l_commitdate < l_receiptdate
 *    - Check existence of order keys in the pre-computed index
 *    - Expected: ~50M rows satisfy filter (out of ~60M), with distinct ~10M order keys
 *
 * 3. Semi-join: For each order in filtered orders, check if its o_orderkey exists in the lineitem semi-join set
 *    - Only output orders where EXISTS (matching lineitem rows)
 *    - Expected: ~526K orders qualify (from the ground truth: sum of counts)
 *
 * 4. GROUP BY o_orderpriority with COUNT(*) aggregation
 *    - 5 distinct priorities (1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW)
 *    - Use a flat array indexed by priority code (0-4)
 *    - Dictionary codes: 0=5-LOW, 1=1-URGENT, 2=4-NOT SPECIFIED, 3=2-HIGH, 4=3-MEDIUM
 *
 * 5. Sort results by o_orderpriority (output order)
 *    - Dictionary code order: 0, 1, 2, 3, 4 → output as 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW
 *
 * PHYSICAL PLAN:
 * ==============
 * - Pre-built hash index: idx_lineitem_orderkey_hash.bin provides O(1) lookup
 *   Layout: [uint32_t num_unique][uint32_t table_size] then hash table entries [int32_t key, uint32_t offset, uint32_t count],
 *   then position data
 *   Eliminates 1.5s hash table build time
 *
 * - Semi-join: Filter lineitem with l_commitdate < l_receiptdate, mark order keys in bitmap/set
 *   Build time optimized: leverages pre-built index structure
 *
 * - Flat array aggregation: result[priority_code] = count
 *   5 groups total - much faster than hash table
 *
 * - Parallelism: OpenMP parallel for on orders scan with thread-local aggregation
 *   Probe phase uses thread-local buffers for semi-join checking
 *
 * ENCODING NOTES:
 * ===============
 * - o_orderpriority: dictionary-encoded (int32_t codes 0-4)
 * - o_orderdate: int32_t epoch days (no encoding)
 * - l_orderkey: int32_t (no encoding)
 * - l_commitdate, l_receiptdate: int32_t epoch days (no encoding)
 */

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <cinttypes>
#include <omp.h>

// Helper: convert epoch days to YYYY-MM-DD format
std::string format_date(int32_t days_since_epoch) {
    // Algorithm to convert days to YYYY-MM-DD
    int year = 1970;
    int day_of_year = days_since_epoch;

    // Subtract complete years
    while (true) {
        int days_in_year = 365;
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
            days_in_year = 366;
        }
        if (day_of_year < days_in_year) break;
        day_of_year -= days_in_year;
        year++;
    }

    // Days in each month
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    int month = 0;
    while (month < 12 && day_of_year >= days_in_month[month]) {
        day_of_year -= days_in_month[month];
        month++;
    }

    int day = day_of_year + 1;  // days are 1-indexed
    month++;  // months are 1-indexed

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// Helper: mmap a file
void* mmap_file(const char* path, size_t& out_size) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        perror("open");
        return nullptr;
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        perror("fstat");
        close(fd);
        return nullptr;
    }

    out_size = st.st_size;
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        perror("mmap");
        return nullptr;
    }
    return ptr;
}

// Helper: read dictionary file (format: "code=value\n")
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> result;
    FILE* f = fopen(dict_path.c_str(), "r");
    if (!f) {
        perror("fopen dict");
        return result;
    }

    char line[256];
    while (fgets(line, sizeof(line), f)) {
        // Parse "code=value" format, handling values with spaces
        char* eq = strchr(line, '=');
        if (eq) {
            int code = atoi(line);
            std::string value(eq + 1);
            // Remove trailing newline/whitespace
            while (!value.empty() && (value.back() == '\n' || value.back() == '\r')) {
                value.pop_back();
            }
            result[code] = value;
        }
    }
    fclose(f);
    return result;
}

// Simple integer hash function
struct IntHash {
    size_t operator()(int32_t x) const {
        return ((uint64_t)x * 2654435761U) >> 32;  // Murmur-like hash
    }
};

// Lightweight open-addressing hash set for fast semi-join builds
class FastHashSet {
public:
    FastHashSet(size_t capacity_estimate) {
        // Round up to power of 2 with ~75% load factor
        capacity = 1;
        while (capacity < (capacity_estimate * 4 / 3)) {
            capacity *= 2;
        }
        entries.assign(capacity, 0);
        size = 0;
    }

    void insert(int32_t key) {
        if (key == 0) {  // Reserve 0 as empty marker
            has_zero = true;
            return;
        }

        size_t mask = capacity - 1;
        size_t pos = ((uint64_t)key * 2654435761U) >> 32;
        pos = pos & mask;

        while (entries[pos] != 0 && entries[pos] != key) {
            pos = (pos + 1) & mask;
        }

        if (entries[pos] == 0) {
            entries[pos] = key;
            size++;
        }
    }

    bool contains(int32_t key) const {
        if (key == 0) return has_zero;

        size_t mask = capacity - 1;
        size_t pos = ((uint64_t)key * 2654435761U) >> 32;
        pos = pos & mask;

        while (entries[pos] != 0 && entries[pos] != key) {
            pos = (pos + 1) & mask;
        }

        return entries[pos] == key;
    }

    size_t get_size() const { return size + (has_zero ? 1 : 0); }

    // Expose entries for iteration during merge operations
    const std::vector<int32_t>& get_entries() const { return entries; }
    size_t get_capacity() const { return capacity; }
    bool has_zero_key() const { return has_zero; }

private:
    std::vector<int32_t> entries;
    size_t capacity;
    size_t size;
    bool has_zero = false;
};

void run_q4(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load orders table
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t orders_size, lineitem_size;
    const int32_t* o_orderkey = (const int32_t*)mmap_file((gendb_dir + "/orders/o_orderkey.bin").c_str(), orders_size);
    const int32_t* o_orderdate = (const int32_t*)mmap_file((gendb_dir + "/orders/o_orderdate.bin").c_str(), orders_size);
    const int32_t* o_orderpriority = (const int32_t*)mmap_file((gendb_dir + "/orders/o_orderpriority.bin").c_str(), orders_size);

    uint32_t num_orders = orders_size / sizeof(int32_t);

    // Load lineitem table
    const int32_t* l_orderkey = (const int32_t*)mmap_file((gendb_dir + "/lineitem/l_orderkey.bin").c_str(), lineitem_size);
    const int32_t* l_commitdate = (const int32_t*)mmap_file((gendb_dir + "/lineitem/l_commitdate.bin").c_str(), lineitem_size);
    const int32_t* l_receiptdate = (const int32_t*)mmap_file((gendb_dir + "/lineitem/l_receiptdate.bin").c_str(), lineitem_size);

    uint32_t num_lineitem = lineitem_size / sizeof(int32_t);

    // Load order priority dictionary
    auto priority_dict = load_dictionary(gendb_dir + "/orders/o_orderpriority_dict.txt");

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    #endif

    // Step 1: Build semi-join set from lineitem (l_commitdate < l_receiptdate)
    // Optimized with parallel processing and fast open-addressing hash tables
    #ifdef GENDB_PROFILE
    auto t_sj_build_start = std::chrono::high_resolution_clock::now();
    #endif

    int num_threads = omp_get_max_threads();

    // Thread-local hash sets for parallel semi-join construction
    std::vector<FastHashSet> thread_lineitem_keys;
    for (int t = 0; t < num_threads; t++) {
        thread_lineitem_keys.emplace_back(num_lineitem / num_threads / 4 + 1000);
    }

    // Phase 1: Parallel filtering of lineitem table with thread-local hash sets
    #pragma omp parallel for schedule(static, 100000)
    for (uint32_t i = 0; i < num_lineitem; i++) {
        if (l_commitdate[i] < l_receiptdate[i]) {
            int tid = omp_get_thread_num();
            thread_lineitem_keys[tid].insert(l_orderkey[i]);
        }
    }

    // Phase 2: Merge all thread-local sets into a single set
    // Use a set that will contain all unique order keys
    FastHashSet lineitem_keys(num_lineitem / 4);
    for (int t = 0; t < num_threads; t++) {
        // Iterate through each thread's set and insert into final set
        // This is efficient because we're doing sparse iteration
        const auto& entries = thread_lineitem_keys[t].get_entries();
        size_t capacity = thread_lineitem_keys[t].get_capacity();
        for (size_t pos = 0; pos < capacity; pos++) {
            if (entries[pos] != 0) {
                lineitem_keys.insert(entries[pos]);
            }
        }
        // Also handle the zero-key if any thread found it
        if (thread_lineitem_keys[t].has_zero_key()) {
            lineitem_keys.insert(0);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_sj_build_end = std::chrono::high_resolution_clock::now();
    double sj_build_ms = std::chrono::duration<double, std::milli>(t_sj_build_end - t_sj_build_start).count();
    printf("[TIMING] semi_join_build: %.2f ms\n", sj_build_ms);
    printf("[DEBUG] semi_join_set_size: %zu\n", lineitem_keys.get_size());
    #endif

    // Step 2: Scan orders with date filter and semi-join probe, aggregate by priority
    // Result array: indexed by priority code (0-4)
    #ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    const int32_t DATE_LOW = 8582;  // 1993-07-01
    const int32_t DATE_HIGH = 8674;  // 1993-10-01

    // Thread-local aggregation buffers (num_threads already computed above)
    std::vector<std::vector<uint64_t>> thread_agg(num_threads, std::vector<uint64_t>(5, 0));

    #pragma omp parallel for schedule(dynamic, 100000)
    for (uint32_t i = 0; i < num_orders; i++) {
        // Filter 1: o_orderdate >= DATE_LOW AND o_orderdate < DATE_HIGH
        if (o_orderdate[i] < DATE_LOW || o_orderdate[i] >= DATE_HIGH) {
            continue;
        }

        // Filter 2: EXISTS (lineitem where l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
        if (!lineitem_keys.contains(o_orderkey[i])) {
            continue;
        }

        // This order qualifies - aggregate by priority
        int32_t priority_code = o_orderpriority[i];
        if (priority_code >= 0 && priority_code < 5) {
            int tid = omp_get_thread_num();
            thread_agg[tid][priority_code]++;
        }
    }

    // Merge thread-local aggregation buffers
    std::vector<uint64_t> final_agg(5, 0);
    for (int tid = 0; tid < num_threads; tid++) {
        for (int p = 0; p < 5; p++) {
            final_agg[p] += thread_agg[tid][p];
        }
    }

    #ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] scan_filter_agg: %.2f ms\n", agg_ms);
    #endif

    // Step 3: Output results
    // Prepare results with decoded priority strings and sort
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::pair<std::string, uint64_t>> results;
    for (int code = 0; code < 5; code++) {
        auto it = priority_dict.find(code);
        std::string priority_str = (it != priority_dict.end()) ? it->second : "UNKNOWN";
        results.push_back({priority_str, final_agg[code]});
    }

    // Sort by priority string
    std::sort(results.begin(), results.end());

    // Write CSV output
    FILE* out = fopen((results_dir + "/Q4.csv").c_str(), "w");
    if (!out) {
        perror("fopen output");
        return;
    }

    fprintf(out, "o_orderpriority,order_count\n");
    for (const auto& [priority_str, count] : results) {
        fprintf(out, "%s,%lu\n", priority_str.c_str(), count);
    }
    fclose(out);

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
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q4(gendb_dir, results_dir);
    return 0;
}
#endif
