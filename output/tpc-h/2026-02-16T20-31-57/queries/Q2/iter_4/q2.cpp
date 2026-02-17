#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <sstream>
#include <iomanip>
#include <omp.h>
#include <atomic>

// ============================================================================
// LOGICAL QUERY PLAN FOR Q2:
// ============================================================================
// Step 1: Load region table, filter by r_name = 'EUROPE'
//         Expected: 1 row (regionkey = 0 or similar)
// Step 2: Load nation table, filter by n_regionkey = EUROPE_regionkey
//         Expected: ~5 nations
// Step 3: Load supplier table, filter by s_nationkey IN (qualified nations)
//         Expected: ~4000 suppliers (100K / 25 nations × 5 qualified nations)
// Step 4: Load part table, filter by p_size = 15 AND p_type LIKE '%BRASS'
//         Expected: ~2000 parts (2M rows × selectivity ~0.1%)
// Step 5: Compute SUBQUERY (correlated on p_partkey):
//         SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region
//         WHERE ps_partkey = p_partkey AND s_suppkey = ps_suppkey
//         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
//         AND r_name = 'EUROPE'
//         Pre-compute: For each qualified (partkey, suppkey), find minimum ps_supplycost
//         Result: hash map (partkey → min_cost)
// Step 6: Hash join: filtered partsupp with qualified suppliers on suppkey
//         Expected: ~20K-40K rows (4000 suppliers × avg 4 supplycost per supplier)
// Step 7: Hash join: filtered part with join result on partkey
//         Expected: matching partsupp rows for qualified parts
// Step 8: Filter by ps_supplycost = min_cost_for_part
//         Expected: 1-4 rows per part (best suppliers)
// Step 9: Join with nation and region for names (via suppkey)
// Step 10: Sort by s_acctbal DESC, n_name, s_name, p_partkey
// Step 11: LIMIT 100
//
// PHYSICAL PLAN:
// - Use direct array lookup for region (5 entries) and nation (25 entries)
// - Use hash tables for supplier → nationkey and partsupp lookups
// - Subquery: pre-compute minimum ps_supplycost by partkey using single pass
// - Main: Scan filtered part + partsupp, use subquery map for filtering

// ============================================================================
// HELPER STRUCTURES AND FUNCTIONS
// ============================================================================

struct OutputRow {
    int64_t s_acctbal;
    std::string s_name;
    std::string n_name;
    int32_t p_partkey;
    std::string p_mfgr;
    std::string s_address;
    std::string s_phone;
    std::string s_comment;
};

// Load dictionary
std::vector<std::string> load_dictionary(const std::string& dict_file) {
    std::vector<std::string> dict;
    std::ifstream f(dict_file);
    std::string line;
    while (std::getline(f, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Find dictionary code for a value
int32_t find_dict_code(const std::vector<std::string>& dict, const std::string& value) {
    for (size_t i = 0; i < dict.size(); i++) {
        if (dict[i] == value) return i;
    }
    return -1;
}

// String matching for LIKE
bool string_ends_with(const std::string& str, const std::string& suffix) {
    if (str.size() < suffix.size()) return false;
    return str.substr(str.size() - suffix.size()) == suffix;
}

// Mmap helper
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << path << std::endl;
        return nullptr;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    size = file_size;
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return ptr;
}

// ============================================================================
// MAIN QUERY EXECUTION
// ============================================================================

void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // PRE-LOAD ALL DICTIONARIES IN PARALLEL (before main query starts)
    // This hides I/O latency by starting loads early
    // ========================================================================
    std::vector<std::string> region_name_dict, nation_name_dict;
    std::vector<std::string> supp_name_dict, supp_address_dict, supp_phone_dict, supp_comment_dict;
    std::vector<std::string> part_type_dict, part_mfgr_dict;

    #pragma omp parallel sections
    {
        #pragma omp section
        region_name_dict = load_dictionary(gendb_dir + "/region/r_name_dict.txt");
        #pragma omp section
        nation_name_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");
        #pragma omp section
        supp_name_dict = load_dictionary(gendb_dir + "/supplier/s_name_dict.txt");
        #pragma omp section
        supp_address_dict = load_dictionary(gendb_dir + "/supplier/s_address_dict.txt");
        #pragma omp section
        supp_phone_dict = load_dictionary(gendb_dir + "/supplier/s_phone_dict.txt");
        #pragma omp section
        supp_comment_dict = load_dictionary(gendb_dir + "/supplier/s_comment_dict.txt");
        #pragma omp section
        part_type_dict = load_dictionary(gendb_dir + "/part/p_type_dict.txt");
        #pragma omp section
        part_mfgr_dict = load_dictionary(gendb_dir + "/part/p_mfgr_dict.txt");
    }

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LOAD REGION TABLE (5 rows, filter by r_name = 'EUROPE')
    // ========================================================================
    int32_t europe_code = find_dict_code(region_name_dict, "EUROPE");

    size_t region_size;
    auto* region_name_ptr = (int32_t*)mmap_file(gendb_dir + "/region/r_name.bin", region_size);
    int32_t* region_names = (int32_t*)region_name_ptr;
    int32_t europe_regionkey = -1;
    for (int i = 0; i < 5; i++) {
        if (region_names[i] == europe_code) {
            europe_regionkey = i;
            break;
        }
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] region_filter: %.2f ms\n", ms);
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LOAD NATION TABLE (25 rows, filter by n_regionkey = EUROPE_regionkey)
    // ========================================================================
    size_t nation_regionkey_size;
    auto* nation_regionkey_ptr = (int32_t*)mmap_file(gendb_dir + "/nation/n_regionkey.bin", nation_regionkey_size);
    int32_t* nation_regionkeys = (int32_t*)nation_regionkey_ptr;

    std::vector<int32_t> qualified_nations;
    for (int i = 0; i < 25; i++) {
        if (nation_regionkeys[i] == europe_regionkey) {
            qualified_nations.push_back(i);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] nation_filter: %.2f ms\n", ms);
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LOAD SUPPLIER TABLE (100K rows)
    // Filter by s_nationkey IN (qualified_nations)
    // ========================================================================
    size_t supp_suppkey_size, supp_nationkey_size, supp_acctbal_size;
    auto* supp_suppkey_ptr = (int32_t*)mmap_file(gendb_dir + "/supplier/s_suppkey.bin", supp_suppkey_size);
    auto* supp_nationkey_ptr = (int32_t*)mmap_file(gendb_dir + "/supplier/s_nationkey.bin", supp_nationkey_size);
    auto* supp_acctbal_ptr = (int64_t*)mmap_file(gendb_dir + "/supplier/s_acctbal.bin", supp_acctbal_size);

    int32_t* supp_suppkeys = (int32_t*)supp_suppkey_ptr;
    int32_t* supp_nationkeys = (int32_t*)supp_nationkey_ptr;
    int64_t* supp_acctbals = (int64_t*)supp_acctbal_ptr;

    // Build a fast lookup for qualified nations (only 25 possible, use direct indexing)
    std::vector<bool> is_qualified_nation(25, false);
    for (int32_t n : qualified_nations) {
        is_qualified_nation[n] = true;
    }

    // Thread-local filtering
    int num_threads_supp = omp_get_max_threads();
    std::vector<std::vector<uint32_t>> local_qualified_suppliers(num_threads_supp);
    for (int t = 0; t < num_threads_supp; t++) {
        local_qualified_suppliers[t].reserve(10000 / num_threads_supp + 100);
    }

    #pragma omp parallel for schedule(dynamic, 10000)
    for (uint32_t i = 0; i < 100000; i++) {
        if (supp_nationkeys[i] < 25 && is_qualified_nation[supp_nationkeys[i]]) {
            int tid = omp_get_thread_num();
            local_qualified_suppliers[tid].push_back(i);
        }
    }

    std::vector<uint32_t> qualified_suppliers;
    qualified_suppliers.reserve(10000);
    for (int t = 0; t < num_threads_supp; t++) {
        qualified_suppliers.insert(qualified_suppliers.end(),
                                   local_qualified_suppliers[t].begin(),
                                   local_qualified_suppliers[t].end());
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] supplier_filter: %.2f ms\n", ms);
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LOAD PART TABLE (2M rows)
    // Filter by p_size = 15 AND p_type LIKE '%BRASS'
    // ========================================================================

    // Find all dictionary codes for types ending with "BRASS"
    // Use a simple vector instead of hash set for faster lookup
    std::vector<bool> is_brass_code(part_type_dict.size(), false);
    for (size_t i = 0; i < part_type_dict.size(); i++) {
        if (string_ends_with(part_type_dict[i], "BRASS")) {
            is_brass_code[i] = true;
        }
    }

    size_t part_partkey_size, part_size_size, part_type_size;
    auto* part_partkey_ptr = (int32_t*)mmap_file(gendb_dir + "/part/p_partkey.bin", part_partkey_size);
    auto* part_size_ptr = (int32_t*)mmap_file(gendb_dir + "/part/p_size.bin", part_size_size);
    auto* part_type_ptr = (int32_t*)mmap_file(gendb_dir + "/part/p_type.bin", part_type_size);

    int32_t* part_partkeys = (int32_t*)part_partkey_ptr;
    int32_t* part_sizes = (int32_t*)part_size_ptr;
    int32_t* part_types = (int32_t*)part_type_ptr;

    // Thread-local filtering to avoid critical section
    int num_threads_part = omp_get_max_threads();
    std::vector<std::vector<uint32_t>> local_qualified_parts(num_threads_part);
    for (int t = 0; t < num_threads_part; t++) {
        local_qualified_parts[t].reserve(5000 / num_threads_part + 100);
    }

    #pragma omp parallel for schedule(dynamic, 100000)
    for (uint32_t i = 0; i < 2000000; i++) {
        if (part_sizes[i] == 15) {
            int32_t type_code = part_types[i];
            if (type_code >= 0 && type_code < (int32_t)is_brass_code.size() && is_brass_code[type_code]) {
                int tid = omp_get_thread_num();
                local_qualified_parts[tid].push_back(i);
            }
        }
    }

    std::vector<uint32_t> qualified_parts;
    qualified_parts.reserve(5000);
    for (int t = 0; t < num_threads_part; t++) {
        qualified_parts.insert(qualified_parts.end(),
                               local_qualified_parts[t].begin(),
                               local_qualified_parts[t].end());
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] part_filter: %.2f ms\n", ms);
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LOAD PARTSUPP TABLE (8M rows)
    // Use PRE-BUILT HASH INDEX for partsupp_partkey to avoid full scan
    // ========================================================================
    size_t ps_suppkey_size, ps_supplycost_size;
    auto* ps_suppkey_ptr = (int32_t*)mmap_file(gendb_dir + "/partsupp/ps_suppkey.bin", ps_suppkey_size);
    auto* ps_supplycost_ptr = (int64_t*)mmap_file(gendb_dir + "/partsupp/ps_supplycost.bin", ps_supplycost_size);

    int32_t* ps_suppkeys = (int32_t*)ps_suppkey_ptr;
    int64_t* ps_supplycosts = (int64_t*)ps_supplycost_ptr;

    // Load pre-built partsupp_partkey_hash index
    size_t partsupp_hash_size;
    auto* partsupp_hash_ptr = mmap_file(gendb_dir + "/indexes/partsupp_partkey_hash.bin", partsupp_hash_size);

    // Parse the multi-value hash index
    uint32_t* hash_header = (uint32_t*)partsupp_hash_ptr;
    uint32_t table_size = hash_header[1];

    // Hash table entries: [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot)
    struct HashEntry { int32_t key; uint32_t offset; uint32_t count; };
    HashEntry* hash_table = (HashEntry*)(hash_header + 2);

    // Positions array starts after hash table
    uint32_t* positions_array = (uint32_t*)(hash_table + table_size);

    // ========================================================================
    // SUBQUERY COMPUTATION USING PRE-BUILT INDEX:
    // For each qualified partkey, lookup via hash index instead of scanning all 8M rows
    // ========================================================================
    std::unordered_set<int32_t> qualified_suppkey_set;
    for (uint32_t idx : qualified_suppliers) {
        qualified_suppkey_set.insert(supp_suppkeys[idx]);
    }

    // Vector for min costs indexed by partkey
    std::vector<int64_t> min_supplycost_by_partkey(2000001, INT64_MAX);

    // Build map: partkey → min cost supplier (for later filtering)
    std::unordered_map<int32_t, std::vector<std::pair<int32_t, int64_t>>> partsupp_by_partkey;
    partsupp_by_partkey.reserve(qualified_parts.size());

    // Compute min cost for each qualified partkey using the pre-built index
    // This is MUCH faster than scanning all 8M rows
    for (uint32_t part_idx : qualified_parts) {
        int32_t pk = part_partkeys[part_idx];

        // Hash lookup in pre-built index
        // Simple linear probe hash table lookup
        uint32_t h = ((uint32_t)pk * 2654435761u) & (table_size - 1);
        int64_t local_min = INT64_MAX;

        // Find the hash entry for this partkey
        for (uint32_t probe = 0; probe < table_size; probe++) {
            uint32_t idx = (h + probe) & (table_size - 1);
            if (hash_table[idx].key == pk) {
                // Found: retrieve positions for this partkey
                uint32_t offset = hash_table[idx].offset;
                uint32_t count = hash_table[idx].count;

                std::vector<std::pair<int32_t, int64_t>>& ps_list = partsupp_by_partkey[pk];

                // Iterate through all partsupp entries for this partkey
                for (uint32_t j = 0; j < count; j++) {
                    uint32_t ps_row = positions_array[offset + j];
                    int32_t sk = ps_suppkeys[ps_row];
                    int64_t sc = ps_supplycosts[ps_row];

                    // Filter by qualified supplier
                    if (qualified_suppkey_set.count(sk)) {
                        ps_list.push_back({sk, sc});
                        if (sc < local_min) {
                            local_min = sc;
                        }
                    }
                }

                if (local_min < INT64_MAX) {
                    min_supplycost_by_partkey[pk] = local_min;
                }
                break;
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] subquery_and_index: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Load supplier and part column data (NOT dictionaries — those loaded early)
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    size_t supp_name_size, supp_address_size, supp_phone_size, supp_comment_size;
    size_t part_mfgr_size, nation_name_size;
    int32_t* supp_names, * supp_addresses, * supp_phones, * supp_comments;
    int32_t* part_mfgrs, * nation_names;

    #pragma omp parallel sections
    {
        #pragma omp section
        {
            auto* supp_name_ptr = (int32_t*)mmap_file(gendb_dir + "/supplier/s_name.bin", supp_name_size);
            supp_names = (int32_t*)supp_name_ptr;
        }
        #pragma omp section
        {
            auto* supp_address_ptr = (int32_t*)mmap_file(gendb_dir + "/supplier/s_address.bin", supp_address_size);
            supp_addresses = (int32_t*)supp_address_ptr;
        }
        #pragma omp section
        {
            auto* supp_phone_ptr = (int32_t*)mmap_file(gendb_dir + "/supplier/s_phone.bin", supp_phone_size);
            supp_phones = (int32_t*)supp_phone_ptr;
        }
        #pragma omp section
        {
            auto* supp_comment_ptr = (int32_t*)mmap_file(gendb_dir + "/supplier/s_comment.bin", supp_comment_size);
            supp_comments = (int32_t*)supp_comment_ptr;
        }
        #pragma omp section
        {
            auto* part_mfgr_ptr = (int32_t*)mmap_file(gendb_dir + "/part/p_mfgr.bin", part_mfgr_size);
            part_mfgrs = (int32_t*)part_mfgr_ptr;
        }
        #pragma omp section
        {
            auto* nation_name_ptr = (int32_t*)mmap_file(gendb_dir + "/nation/n_name.bin", nation_name_size);
            nation_names = (int32_t*)nation_name_ptr;
        }
    }

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dicts: %.2f ms\n", dict_ms);
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // Main query: Build output rows
    // ========================================================================
    std::vector<OutputRow> results;
    results.reserve(10000);

    for (uint32_t part_idx : qualified_parts) {
        int32_t part_key = part_partkeys[part_idx];

        // Find minimum supplycost for this part (direct vector lookup)
        if (part_key < 0 || part_key >= 2000001) continue;
        int64_t min_cost = min_supplycost_by_partkey[part_key];
        if (min_cost == INT64_MAX) continue;  // No matching partsupp found

        // Find all partsupp entries matching this partkey and min_cost
        auto ps_it = partsupp_by_partkey.find(part_key);
        if (ps_it == partsupp_by_partkey.end()) continue;

        for (const auto& ps_pair : ps_it->second) {
            int32_t supp_key = ps_pair.first;
            int64_t cost = ps_pair.second;

            if (cost != min_cost) continue;

            // Find supplier index
            uint32_t supp_idx = supp_key - 1;  // suppkey is 1-indexed
            if (supp_idx >= 100000) continue;

            // Build output row
            OutputRow row;
            row.s_acctbal = supp_acctbals[supp_idx];
            row.s_name = supp_name_dict[supp_names[supp_idx]];
            row.n_name = nation_name_dict[nation_names[supp_nationkeys[supp_idx]]];
            row.p_partkey = part_key;
            row.p_mfgr = part_mfgr_dict[part_mfgrs[part_idx]];
            row.s_address = supp_address_dict[supp_addresses[supp_idx]];
            row.s_phone = supp_phone_dict[supp_phones[supp_idx]];
            row.s_comment = supp_comment_dict[supp_comments[supp_idx]];

            results.push_back(row);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_results: %.2f ms\n", ms);
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // SORT BY: s_acctbal DESC, n_name, s_name, p_partkey
    // ========================================================================
    std::sort(results.begin(), results.end(), [](const OutputRow& a, const OutputRow& b) {
        if (a.s_acctbal != b.s_acctbal) return a.s_acctbal > b.s_acctbal;
        if (a.n_name != b.n_name) return a.n_name < b.n_name;
        if (a.s_name != b.s_name) return a.s_name < b.s_name;
        return a.p_partkey < b.p_partkey;
    });

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LIMIT 100
    // ========================================================================
    if (results.size() > 100) {
        results.resize(100);
    }

    // ========================================================================
    // WRITE RESULTS TO CSV
    // ========================================================================
    std::string output_file = results_dir + "/Q2.csv";
    std::ofstream out(output_file);

    // Helper to escape and quote CSV fields
    auto escape_csv_field = [](const std::string& field) -> std::string {
        // Check if field needs quoting (contains comma, quote, or newline)
        bool needs_quote = false;
        for (char c : field) {
            if (c == ',' || c == '"' || c == '\n' || c == '\r') {
                needs_quote = true;
                break;
            }
        }

        if (!needs_quote) {
            return field;
        }

        // Quote and escape
        std::string result = "\"";
        for (char c : field) {
            if (c == '"') result += "\"\"";  // escape quote by doubling
            else result += c;
        }
        result += "\"";
        return result;
    };

    // Write header
    out << "s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment\r\n";

    // Write data rows
    for (const auto& row : results) {
        out << std::fixed << std::setprecision(2) << (double)row.s_acctbal / 100.0 << ",";
        out << escape_csv_field(row.s_name) << ",";
        out << escape_csv_field(row.n_name) << ",";
        out << row.p_partkey << ",";
        out << escape_csv_field(row.p_mfgr) << ",";
        out << escape_csv_field(row.s_address) << ",";
        out << escape_csv_field(row.s_phone) << ",";
        out << escape_csv_field(row.s_comment) << "\r\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
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
    run_q2(gendb_dir, results_dir);
    return 0;
}
#endif
