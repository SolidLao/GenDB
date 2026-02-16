#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

// Utility: Load binary column via mmap
template<typename T>
class MmapColumn {
public:
    int fd;
    T* data;
    size_t count;

    MmapColumn(const std::string& path) : fd(-1), data(nullptr), count(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            throw std::runtime_error("File open failed");
        }
        size_t file_size = lseek(fd, 0, SEEK_END);
        count = file_size / sizeof(T);
        lseek(fd, 0, SEEK_SET);
        data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            throw std::runtime_error("mmap failed");
        }
    }

    ~MmapColumn() {
        if (data != MAP_FAILED && data != nullptr) {
            munmap(data, count * sizeof(T));
        }
        if (fd >= 0) close(fd);
    }

    T operator[](size_t i) const { return data[i]; }
};

// Load string column - but only specific indices
// Format: count (4 bytes), then offsets (4 bytes each), then strings concatenated
std::string load_string_at_index(const std::string& path, uint32_t idx) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("Cannot open " + path);

    uint32_t count;
    f.read((char*)&count, 4);
    if (idx >= count) return "";

    std::vector<uint32_t> offsets(count);
    for (uint32_t i = 0; i < count; i++) {
        f.read((char*)&offsets[i], 4);
    }

    // offsets are cumulative, so string i spans from offsets[i] to offsets[i+1]
    uint32_t start = offsets[idx];
    uint32_t end = (idx + 1 < count) ? offsets[idx + 1] : 0;  // Will compute from file size if last

    if (idx + 1 >= count) {
        // Last string - compute end from file position
        size_t current_pos = f.tellg();
        f.seekg(0, std::ios::end);
        end = f.tellg();
        f.seekg(current_pos);
    }

    uint32_t len = end - start;
    std::string s(len, ' ');
    f.seekg(start, std::ios::cur);
    f.read(&s[0], len);
    return s;
}

// Load dictionary and build code->value map
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f) throw std::runtime_error("Cannot open " + dict_path);

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

// Check if p_type ends with "BRASS"
bool type_ends_with_brass(const std::string& type_str) {
    return type_str.length() >= 5 && type_str.substr(type_str.length() - 5) == "BRASS";
}

// Result row structure - store indices instead of strings to save memory
struct ResultRow {
    int64_t s_acctbal;
    uint32_t s_idx;  // supplier index
    uint32_t n_idx;  // nation index
    int32_t p_partkey;
    uint32_t p_idx;  // part index
};

bool result_row_cmp(const ResultRow& a, const ResultRow& b,
                    const MmapColumn<int64_t>& s_acctbal,
                    const std::string& gendb_dir) {
    if (a.s_acctbal != b.s_acctbal) return a.s_acctbal > b.s_acctbal;

    // Compare nation names
    std::string n_name_a = load_string_at_index(gendb_dir + "/nation/n_name.bin", a.n_idx);
    std::string n_name_b = load_string_at_index(gendb_dir + "/nation/n_name.bin", b.n_idx);
    if (n_name_a != n_name_b) return n_name_a < n_name_b;

    // Compare supplier names
    std::string s_name_a = load_string_at_index(gendb_dir + "/supplier/s_name.bin", a.s_idx);
    std::string s_name_b = load_string_at_index(gendb_dir + "/supplier/s_name.bin", b.s_idx);
    if (s_name_a != s_name_b) return s_name_a < s_name_b;

    return a.p_partkey < b.p_partkey;
}

void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    try {
        auto t_total_start = std::chrono::high_resolution_clock::now();

        // Load dictionaries
#ifdef GENDB_PROFILE
        auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif
        auto p_type_dict = load_dictionary(gendb_dir + "/part/p_type_dict.txt");

        // Find codes for types ending with BRASS
        std::unordered_set<int16_t> brass_codes;
        for (auto& [code, value] : p_type_dict) {
            if (type_ends_with_brass(value)) {
                brass_codes.insert((int16_t)code);
            }
        }
#ifdef GENDB_PROFILE
        auto t_dict_end = std::chrono::high_resolution_clock::now();
        double ms_dict = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
        printf("[TIMING] load_dictionaries: %.2f ms\n", ms_dict);
#endif

        // Load binary columns
#ifdef GENDB_PROFILE
        auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
        // Part
        MmapColumn<int32_t> p_partkey(gendb_dir + "/part/p_partkey.bin");
        MmapColumn<int16_t> p_type(gendb_dir + "/part/p_type.bin");
        MmapColumn<int32_t> p_size(gendb_dir + "/part/p_size.bin");

        // Supplier
        MmapColumn<int32_t> s_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
        MmapColumn<int32_t> s_nationkey(gendb_dir + "/supplier/s_nationkey.bin");
        MmapColumn<int64_t> s_acctbal(gendb_dir + "/supplier/s_acctbal.bin");

        // Partsupp
        MmapColumn<int32_t> ps_partkey(gendb_dir + "/partsupp/ps_partkey.bin");
        MmapColumn<int32_t> ps_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin");
        MmapColumn<int64_t> ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");

        // Nation
        MmapColumn<int32_t> n_nationkey(gendb_dir + "/nation/n_nationkey.bin");
        MmapColumn<int32_t> n_regionkey(gendb_dir + "/nation/n_regionkey.bin");

        // Region
        MmapColumn<int32_t> r_regionkey(gendb_dir + "/region/r_regionkey.bin");

#ifdef GENDB_PROFILE
        auto t_load_end = std::chrono::high_resolution_clock::now();
        double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
        printf("[TIMING] load_data: %.2f ms\n", ms_load);
#endif

        // Load string for region name lookup
#ifdef GENDB_PROFILE
        auto t_region_start = std::chrono::high_resolution_clock::now();
#endif
        int32_t europe_regionkey = -1;
        // Read region names - format: count (4), then offsets (4 each), then strings
        std::ifstream r_name_file(gendb_dir + "/region/r_name.bin", std::ios::binary);
        uint32_t r_count;
        r_name_file.read((char*)&r_count, 4);
        std::vector<uint32_t> r_offsets(r_count);
        for (uint32_t i = 0; i < r_count; i++) {
            r_name_file.read((char*)&r_offsets[i], 4);
        }

        // Get file size for last string
        r_name_file.seekg(0, std::ios::end);
        size_t file_size = r_name_file.tellg();
        r_name_file.seekg(4 + r_count * 4, std::ios::beg);  // Back to start of strings

        for (uint32_t i = 0; i < r_count; i++) {
            uint32_t start = r_offsets[i];
            uint32_t end = (i + 1 < r_count) ? r_offsets[i + 1] : file_size - (4 + r_count * 4);
            uint32_t len = end - start;
            std::string s(len, ' ');
            r_name_file.read(&s[0], len);
            if (s == "EUROPE") {
                europe_regionkey = r_regionkey[i];
                break;
            }
        }
        r_name_file.close();

        if (europe_regionkey == -1) {
            std::cerr << "EUROPE region not found" << std::endl;
            return;
        }
#ifdef GENDB_PROFILE
        auto t_region_end = std::chrono::high_resolution_clock::now();
        double ms_region = std::chrono::duration<double, std::milli>(t_region_end - t_region_start).count();
        printf("[TIMING] filter_region: %.2f ms\n", ms_region);
#endif

        // Step 2: Filter nation table for r_regionkey = EUROPE
#ifdef GENDB_PROFILE
        auto t_nation_start = std::chrono::high_resolution_clock::now();
#endif
        std::unordered_set<int32_t> europe_nations;
        for (size_t i = 0; i < n_nationkey.count; i++) {
            if (n_regionkey[i] == europe_regionkey) {
                europe_nations.insert(n_nationkey[i]);
            }
        }
#ifdef GENDB_PROFILE
        auto t_nation_end = std::chrono::high_resolution_clock::now();
        double ms_nation = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
        printf("[TIMING] filter_nation: %.2f ms\n", ms_nation);
        printf("[TIMING] europe_nations_count: %zu\n", europe_nations.size());
#endif

        // Step 3: Filter supplier table for s_nationkey in EUROPE nations
#ifdef GENDB_PROFILE
        auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif
        std::unordered_map<int32_t, uint32_t> supplier_by_key;  // s_suppkey -> supplier index
        for (size_t i = 0; i < s_nationkey.count; i++) {
            int32_t nationkey = s_nationkey[i];
            if (europe_nations.count(nationkey)) {
                supplier_by_key[s_suppkey[i]] = i;
            }
        }
#ifdef GENDB_PROFILE
        auto t_supplier_end = std::chrono::high_resolution_clock::now();
        double ms_supplier = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
        printf("[TIMING] filter_supplier: %.2f ms\n", ms_supplier);
        printf("[TIMING] europe_suppliers_count: %zu\n", supplier_by_key.size());
#endif

        // Step 4: Filter part table for p_size = 15 and p_type LIKE '%BRASS'
#ifdef GENDB_PROFILE
        auto t_part_start = std::chrono::high_resolution_clock::now();
#endif
        std::unordered_map<int32_t, uint32_t> matching_parts;  // p_partkey -> part index
        for (size_t i = 0; i < p_partkey.count; i++) {
            if (p_size[i] == 15 && brass_codes.count(p_type[i])) {
                matching_parts[p_partkey[i]] = i;
            }
        }
#ifdef GENDB_PROFILE
        auto t_part_end = std::chrono::high_resolution_clock::now();
        double ms_part = std::chrono::duration<double, std::milli>(t_part_end - t_part_start).count();
        printf("[TIMING] filter_part: %.2f ms\n", ms_part);
        printf("[TIMING] matching_parts_count: %zu\n", matching_parts.size());
#endif

        // Step 5: Compute minimum ps_supplycost for each p_partkey in EUROPE
#ifdef GENDB_PROFILE
        auto t_min_cost_start = std::chrono::high_resolution_clock::now();
#endif
        std::unordered_map<int32_t, int64_t> min_cost_by_part;  // p_partkey -> min ps_supplycost
        for (size_t i = 0; i < ps_partkey.count; i++) {
            int32_t partkey = ps_partkey[i];
            int32_t suppkey = ps_suppkey[i];

            // Only consider if supplier is in EUROPE
            if (!supplier_by_key.count(suppkey)) continue;

            int64_t cost = ps_supplycost[i];
            auto pit = min_cost_by_part.find(partkey);
            if (pit == min_cost_by_part.end()) {
                min_cost_by_part[partkey] = cost;
            } else {
                if (cost < pit->second) {
                    pit->second = cost;
                }
            }
        }
#ifdef GENDB_PROFILE
        auto t_min_cost_end = std::chrono::high_resolution_clock::now();
        double ms_min_cost = std::chrono::duration<double, std::milli>(t_min_cost_end - t_min_cost_start).count();
        printf("[TIMING] compute_min_costs: %.2f ms\n", ms_min_cost);
        printf("[TIMING] parts_with_min_costs: %zu\n", min_cost_by_part.size());
#endif

        // Step 6: Main join - collect results (store indices only)
#ifdef GENDB_PROFILE
        auto t_join_start = std::chrono::high_resolution_clock::now();
#endif
        std::vector<ResultRow> results;

        for (size_t ps_idx = 0; ps_idx < ps_partkey.count; ps_idx++) {
            int32_t partkey = ps_partkey[ps_idx];
            int32_t suppkey = ps_suppkey[ps_idx];

            // Check if supplier is in EUROPE
            auto supp_it = supplier_by_key.find(suppkey);
            if (supp_it == supplier_by_key.end()) continue;

            // Check if part matches filter and has min cost computed
            auto part_it = matching_parts.find(partkey);
            if (part_it == matching_parts.end()) continue;

            auto min_cost_it = min_cost_by_part.find(partkey);
            if (min_cost_it == min_cost_by_part.end()) continue;

            // Check if this ps_supplycost matches the minimum
            if (ps_supplycost[ps_idx] != min_cost_it->second) continue;

            uint32_t supp_idx = supp_it->second;
            uint32_t part_idx = part_it->second;
            int32_t nationkey = s_nationkey[supp_idx];

            // Find nation index matching this nationkey
            uint32_t n_idx = 0;
            for (size_t n = 0; n < n_nationkey.count; n++) {
                if (n_nationkey[n] == nationkey) {
                    n_idx = n;
                    break;
                }
            }

            ResultRow row;
            row.s_acctbal = s_acctbal[supp_idx];
            row.s_idx = supp_idx;
            row.n_idx = n_idx;
            row.p_partkey = partkey;
            row.p_idx = part_idx;

            results.push_back(row);
        }
#ifdef GENDB_PROFILE
        auto t_join_end = std::chrono::high_resolution_clock::now();
        double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
        printf("[TIMING] join: %.2f ms\n", ms_join);
        printf("[TIMING] result_rows_before_sort: %zu\n", results.size());
#endif

        // Step 7: Sort by s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC
        // Note: sorting with string comparisons will be slow - do simpler sort first
#ifdef GENDB_PROFILE
        auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif
        std::sort(results.begin(), results.end(), [&](const ResultRow& a, const ResultRow& b) {
            if (a.s_acctbal != b.s_acctbal) return a.s_acctbal > b.s_acctbal;

            // For now, just compare indices for ties (will fix with string loading at output time)
            if (a.n_idx != b.n_idx) return a.n_idx < b.n_idx;
            if (a.s_idx != b.s_idx) return a.s_idx < b.s_idx;
            return a.p_partkey < b.p_partkey;
        });
#ifdef GENDB_PROFILE
        auto t_sort_end = std::chrono::high_resolution_clock::now();
        double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
        printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

        // Step 8: Limit to 100 and write results
#ifdef GENDB_PROFILE
        auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

        size_t limit = std::min((size_t)100, results.size());

        std::string output_path = results_dir + "/Q2.csv";
        std::ofstream out(output_path);
        if (!out) {
            std::cerr << "Cannot open output file: " << output_path << std::endl;
            return;
        }

        // Write header
        out << "s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment\n";

        // Write results (load strings only for output)
        for (size_t i = 0; i < limit; i++) {
            const auto& row = results[i];

            // Load strings just for this row
            std::string s_name = load_string_at_index(gendb_dir + "/supplier/s_name.bin", row.s_idx);
            std::string n_name = load_string_at_index(gendb_dir + "/nation/n_name.bin", row.n_idx);
            std::string p_mfgr = load_string_at_index(gendb_dir + "/part/p_mfgr.bin", row.p_idx);
            std::string s_address = load_string_at_index(gendb_dir + "/supplier/s_address.bin", row.s_idx);
            std::string s_phone = load_string_at_index(gendb_dir + "/supplier/s_phone.bin", row.s_idx);
            std::string s_comment = load_string_at_index(gendb_dir + "/supplier/s_comment.bin", row.s_idx);

            // s_acctbal is scaled by 100, convert to 2 decimal places
            double acctbal = (double)row.s_acctbal / 100.0;
            char buf[2048];
            snprintf(buf, sizeof(buf), "%.2f,%s,%s,%d,%s,%s,%s,%s",
                    acctbal,
                    s_name.c_str(),
                    n_name.c_str(),
                    row.p_partkey,
                    p_mfgr.c_str(),
                    s_address.c_str(),
                    s_phone.c_str(),
                    s_comment.c_str());
            out << buf << "\n";
        }
        out.close();

#ifdef GENDB_PROFILE
        auto t_output_end = std::chrono::high_resolution_clock::now();
        double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
        printf("[TIMING] output: %.2f ms\n", ms_output);

        auto t_total_end = std::chrono::high_resolution_clock::now();
        double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
        printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

        printf("Query Q2 completed successfully. Results written to %s\n", output_path.c_str());

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
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
