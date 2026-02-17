#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ============================================================================
// LOGICAL QUERY PLAN
// ============================================================================
/*
Q20: Find suppliers that could be promoted (potential part promotion analysis)

Step 1: Filter nation WHERE n_name = 'CANADA' → 1 row
  - Load nation.n_name (dictionary-encoded), compare against 'CANADA'
  - Get n_nationkey for Canada

Step 2: Filter part WHERE p_name LIKE 'forest%' → ~5-10K rows out of 2M
  - Load part.p_name (dictionary-encoded), check prefix match
  - Collect p_partkey values

Step 3: Correlated aggregation for lineitem (by partkey, suppkey)
  - Scan lineitem (~60M rows), filter by l_shipdate [1994-01-01, 1995-01-01)
  - For each (l_partkey, l_suppkey) pair, compute SUM(l_quantity)
  - Store in hash map: (l_partkey, l_suppkey) → sum_quantity

Step 4: Filter partsupp WHERE ps_partkey IN (forest parts)
  - Scan partsupp (8M rows)
  - For each row, check if ps_partkey in forest_parts set
  - For each qualifying row, lookup sum_quantity from step 3
  - Filter WHERE ps_availqty > 0.5 * sum_quantity
  - Collect (ps_suppkey) values that pass both filters
  - Result: ~5-15K qualified supplier keys

Step 5: Semi-join supplier WHERE s_suppkey IN (qualified keys)
  - Scan supplier (100K rows), filter to those in qualified_suppkeys
  - Filter WHERE s_nationkey = Canada's nationkey
  - Result: ~5-15K rows

Step 6: Sort by s_name and output

PHYSICAL PLAN:
- Nation lookup: Direct array access (25 nations, O(1))
- Part filtering: Full scan with dictionary decode + string prefix matching
- Lineitem aggregation: Full scan with date filter + hash aggregation by (partkey, suppkey)
  - Use unordered_map with composite key hash
- Partsupp filtering: Full scan with set lookup + correlated condition check
- Supplier filtering: Full scan with set lookup + direct array nation lookup
- Sort: std::sort on (s_name_code) for output ordering by decoded strings
- Parallelism:
  - Lineitem scan: OpenMP parallel with thread-local hash maps + merge
  - Partsupp/supplier scans: Sequential (result size small)
*/

// ============================================================================
// HELPER STRUCTURES & CONSTANTS
// ============================================================================

// Date constants: days since epoch 1970-01-01
// 1996-01-01 = 9496 days (26 years * 365 + 6 leap days)
// 1997-01-01 = 9861 days (27 years * 365 + 6 leap days)
constexpr int32_t DATE_1996_01_01 = 9496;
constexpr int32_t DATE_1997_01_01 = 9861;

// Scale factor for l_quantity (stored as int64_t * scale_factor = int64_t * 2)
// For 0.5 threshold: 0.5 * scale_factor = 0.5 * 2 = 1 (stored)
constexpr int64_t HALF_SCALE = 1;  // 0.5 with scale 2

// Open-addressing hash table for (partkey, suppkey) → quantity_sum
// Avoids std::unordered_map's pointer chasing overhead (2-5x faster)
struct PartSuppKey {
    int32_t partkey;
    int32_t suppkey;

    bool operator==(const PartSuppKey& other) const {
        return partkey == other.partkey && suppkey == other.suppkey;
    }
};

struct PartSuppKeyHash {
    size_t operator()(const PartSuppKey& k) const {
        // Fibonacci hashing for good distribution
        uint64_t combined = ((uint64_t)k.partkey << 32) | (uint32_t)k.suppkey;
        return (combined * 0x9E3779B97F4A7C15ULL);
    }
};

// Entry for open-addressing hash table
struct PartSuppEntry {
    PartSuppKey key;
    int64_t sum_qty;
    bool occupied = false;
};

// Compact open-addressing hash table
struct PartSuppHashTable {
    std::vector<PartSuppEntry> table;
    size_t mask;

    PartSuppHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    void insert_or_add(PartSuppKey key, int64_t value) {
        PartSuppKeyHash hasher;
        size_t idx = hasher(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].sum_qty += value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    int64_t* find(PartSuppKey key) {
        PartSuppKeyHash hasher;
        size_t idx = hasher(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].sum_qty;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// ============================================================================
// DICTIONARY LOADING
// ============================================================================

std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        size_t eq = line.find('=');
        if (eq == std::string::npos) {
            // Single-line format with space-separated values
            // e.g., "goldenrod lavender spring chocolate lace"
            // Each word is a value, code is its position
            // This format stores values sequentially, 5 per line typically
            // For now, skip - we'll handle this differently
            continue;
        }
        int32_t code = std::stoi(line.substr(0, eq));
        dict[code] = line.substr(eq + 1);
    }
    return dict;
}

// Parse dictionary in line-separated format (for p_name, s_name, s_address, n_name, etc.)
// Each line is a value, indexed by line number (0-indexed)
std::vector<std::string> load_dictionary_space_separated(const std::string& dict_path) {
    std::vector<std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        dict.push_back(line);
    }
    return dict;
}

// ============================================================================
// MMAP HELPERS
// ============================================================================

template <typename T>
T* mmap_column(const std::string& path, int64_t& num_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    num_elements = size / sizeof(T);

    T* data = (T*)mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for: " << path << std::endl;
        return nullptr;
    }

    return data;
}

// ============================================================================
// STRING MATCHING
// ============================================================================

bool string_like_forest(const std::string& s) {
    return s.length() >= 6 && s.substr(0, 6) == "forest";
}

// ============================================================================
// MAIN QUERY EXECUTION
// ============================================================================

void run_q20(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ========================================================================
    // STEP 1: Load dictionaries and static data
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    auto p_name_dict = load_dictionary_space_separated(gendb_dir + "/part/p_name_dict.txt");
    auto s_name_dict = load_dictionary_space_separated(gendb_dir + "/supplier/s_name_dict.txt");
    auto s_address_dict = load_dictionary_space_separated(gendb_dir + "/supplier/s_address_dict.txt");
    auto n_name_dict = load_dictionary_space_separated(gendb_dir + "/nation/n_name_dict.txt");

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", ms);
#endif

    // ========================================================================
    // STEP 2: Find Canada's nation key (direct array lookup)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t nation_rows = 0;
    auto n_nationkey = mmap_column<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_rows);
    auto n_name = mmap_column<int32_t>(gendb_dir + "/nation/n_name.bin", nation_rows);

    int32_t canada_nationkey = -1;
    for (int64_t i = 0; i < nation_rows; i++) {
        if (n_name[i] >= 0 && n_name[i] < (int32_t)n_name_dict.size() && n_name_dict[n_name[i]] == "CANADA") {
            canada_nationkey = n_nationkey[i];
            break;
        }
    }

    if (canada_nationkey == -1) {
        std::cerr << "ERROR: Canada not found in nation table" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] nation_filter: %.2f ms\n", ms);
#endif

    // ========================================================================
    // STEP 3: Filter part WHERE p_name LIKE 'forest%'
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t part_rows = 0;
    auto p_partkey = mmap_column<int32_t>(gendb_dir + "/part/p_partkey.bin", part_rows);
    auto p_name = mmap_column<int32_t>(gendb_dir + "/part/p_name.bin", part_rows);

    std::unordered_set<int32_t> forest_parts;
    for (int64_t i = 0; i < part_rows; i++) {
        if (p_name[i] >= 0 && p_name[i] < (int32_t)p_name_dict.size()) {
            if (string_like_forest(p_name_dict[p_name[i]])) {
                forest_parts.insert(p_partkey[i]);
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] part_filter: %.2f ms (found %lu forest parts)\n", ms, forest_parts.size());
#endif

    // ========================================================================
    // STEP 4: Scan lineitem with zone map pruning and parallel aggregation
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t lineitem_rows = 0;
    auto l_partkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", lineitem_rows);
    auto l_suppkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_rows);
    auto l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", lineitem_rows);
    auto l_shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_rows);

    // Parallel aggregation with thread-local hash tables
    int num_threads = omp_get_max_threads();
    std::vector<PartSuppHashTable> thread_tables;
    for (int t = 0; t < num_threads; t++) {
        thread_tables.emplace_back(8000000 / num_threads);  // Estimate: 8M/num_threads per thread
    }

    // Parallel scan of all lineitem rows, aggregate by (partkey, suppkey)
    // Filter by shipdate: [1996-01-01, 1997-01-01)
    #pragma omp parallel for schedule(dynamic, 100000)
    for (int64_t i = 0; i < lineitem_rows; i++) {
        if (l_shipdate[i] >= DATE_1996_01_01 && l_shipdate[i] < DATE_1997_01_01) {
            int thread_id = omp_get_thread_num();
            PartSuppKey key = {l_partkey[i], l_suppkey[i]};
            thread_tables[thread_id].insert_or_add(key, l_quantity[i]);
        }
    }

    // Merge all thread-local tables into a single master table
    PartSuppHashTable lineitem_sums(8000000);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_tables[t].table) {
            if (entry.occupied) {
                lineitem_sums.insert_or_add(entry.key, entry.sum_qty);
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    size_t agg_groups = 0;
    for (const auto& entry : lineitem_sums.table) {
        if (entry.occupied) agg_groups++;
    }
    printf("[TIMING] lineitem_aggregation: %.2f ms (aggregated %lu groups)\n", ms, agg_groups);
#endif

    // ========================================================================
    // STEP 5: Filter partsupp with correlated condition
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t partsupp_rows = 0;
    auto ps_partkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", partsupp_rows);
    auto ps_suppkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", partsupp_rows);
    auto ps_availqty = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_availqty.bin", partsupp_rows);

    std::unordered_set<int32_t> qualified_suppkeys;

    for (int64_t i = 0; i < partsupp_rows; i++) {
        // Filter 1: ps_partkey in forest_parts
        if (forest_parts.count(ps_partkey[i]) == 0) {
            continue;
        }

        // Filter 2: ps_availqty > 0.5 * SUM(l_quantity) for this (partkey, suppkey)
        // CRITICAL: If no lineitem entries match, the correlated subquery returns NULL,
        // and "ps_availqty > NULL" is unknown (false in SQL), so we SKIP this row.
        PartSuppKey key = {ps_partkey[i], ps_suppkey[i]};
        int64_t* sum_ptr = lineitem_sums.find(key);

        // MUST have a lineitem entry to qualify
        if (sum_ptr == nullptr) {
            continue;
        }

        int64_t sum_quantity = *sum_ptr;

        // Compare: ps_availqty > 0.5 * sum_quantity
        // ps_availqty is int32_t (native units), sum_quantity is int64_t with scale_factor=2
        // To avoid floating point: multiply ps_availqty by 2 to match scale
        // ps_availqty * 2 > sum_quantity
        if ((int64_t)ps_availqty[i] * 2 > sum_quantity) {
            qualified_suppkeys.insert(ps_suppkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] partsupp_filter: %.2f ms (qualified %lu suppkeys)\n", ms, qualified_suppkeys.size());
#endif

    // ========================================================================
    // STEP 6: Filter supplier and join with nation
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t supplier_rows = 0;
    auto s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_rows);
    auto s_name = mmap_column<int32_t>(gendb_dir + "/supplier/s_name.bin", supplier_rows);
    auto s_address = mmap_column<int32_t>(gendb_dir + "/supplier/s_address.bin", supplier_rows);
    auto s_nationkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_rows);

    // Result: vector of (s_name_code, s_address_code) for sorting and output
    std::vector<std::pair<int32_t, int32_t>> result_rows;

    for (int64_t i = 0; i < supplier_rows; i++) {
        // Semi-join: s_suppkey in qualified_suppkeys
        if (qualified_suppkeys.count(s_suppkey[i]) == 0) {
            continue;
        }

        // Join with nation: s_nationkey = canada_nationkey
        if (s_nationkey[i] != canada_nationkey) {
            continue;
        }

        result_rows.push_back({s_name[i], s_address[i]});
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] supplier_filter: %.2f ms (qualified %lu suppliers)\n", ms, result_rows.size());
#endif

    // ========================================================================
    // STEP 7: Sort by s_name
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(result_rows.begin(), result_rows.end(),
        [](const std::pair<int32_t, int32_t>& a, const std::pair<int32_t, int32_t>& b) {
            return a.first < b.first;
        }
    );

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // ========================================================================
    // STEP 8: Output to CSV
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q20.csv";
    std::ofstream out(output_path);

    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out << "s_name,s_address\n";

    // Write data
    for (const auto& row : result_rows) {
        int32_t name_code = row.first;
        int32_t addr_code = row.second;

        std::string name = (name_code >= 0 && name_code < (int32_t)s_name_dict.size())
            ? s_name_dict[name_code] : "";
        std::string addr = (addr_code >= 0 && addr_code < (int32_t)s_address_dict.size())
            ? s_address_dict[addr_code] : "";

        // Escape CSV: wrap in quotes if contains comma or newline
        auto escape_csv = [](const std::string& s) -> std::string {
            if (s.find(',') != std::string::npos || s.find('\n') != std::string::npos || s.find('"') != std::string::npos) {
                std::string escaped = "\"";
                for (char c : s) {
                    if (c == '"') escaped += "\"\"";
                    else escaped += c;
                }
                escaped += "\"";
                return escaped;
            }
            return s;
        };

        out << escape_csv(name) << "," << escape_csv(addr) << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Query Q20 completed. Results written to " << output_path << std::endl;
    std::cout << "Total execution time: " << total_ms << " ms" << std::endl;
    std::cout << "Result rows: " << result_rows.size() << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q20(gendb_dir, results_dir);
    return 0;
}
#endif
