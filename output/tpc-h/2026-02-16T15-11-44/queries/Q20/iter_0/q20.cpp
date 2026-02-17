#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

/*
================================================================================
                        LOGICAL QUERY PLAN FOR Q20
================================================================================

Step 1 (Logical): Table filtering and cardinality estimation
  - part: WHERE p_name LIKE 'forest%' → ~200 rows (0.01% of 2M)
  - lineitem: WHERE l_shipdate >= 1994-01-01 AND l_shipdate < 1995-01-01
             → ~20M rows (33% of 60M)
  - supplier: No single-table predicate (will join with nation)
  - partsupp: Will be filtered by qualified part/lineitem pairs
  - nation: WHERE n_name = 'CANADA' → 1 row (4% of 25)

Step 2 (Logical): Join graph and ordering
  1. Filter part by p_name LIKE 'forest%' → 200 rows
  2. Pre-compute lineitem aggregation by (l_partkey, l_suppkey) for 1994-01-01 to 1995-01-01
     Groups: ~15M (many (part,supp) pairs have lineitem)
  3. Qualify partsupp: ps_availqty > 0.5 * SUM(l_quantity)
     Result: ~50-100 rows (0.001% of 8M)
  4. Filter nation by n_name = 'CANADA' → 1 row
  5. Semi-join supplier with nation (s_nationkey = n_nationkey) → ~4000 rows
  6. Semi-join supplier with qualified partsupp (s_suppkey IN qualified ps_suppkey)
     Result: ~1800 rows

Step 3 (Logical): Subquery decorrelation
  - Correlated subquery: 0.5 * SUM(l_quantity) WHERE l_partkey=ps_partkey AND l_suppkey=ps_suppkey
  - Decorrelation: Pre-compute all (l_partkey, l_suppkey) → SUM(l_quantity) into hash map
  - At partsupp level: lookup the sum and compare ps_availqty > 0.5*sum

Step 4 (Physical): Operator selection
  - Part scan+filter: Full scan, dictionary lookup for p_name codes matching 'forest%'
  - Lineitem aggregation: Parallel scan by (l_partkey, l_suppkey), hash aggregation
    Data structure: hash_map<pair<int32_t,int32_t>, int64_t> (count: ~15M groups)
  - Partsupp filtering: Iterate, check if (ps_partkey, ps_suppkey) is in part and if
                        ps_availqty > 0.5*sum
  - Nation+Supplier: Build hash set of Canada's suppliers from nation lookup
  - Final join: Hash set of qualified s_suppkey, probe supplier

Step 5 (Physical): Data structures
  - Lineitem aggregation: std::unordered_map<(partkey,suppkey), quantity_sum>
    (Pre-size to 15M entries)
  - Part p_name → codes: Hash set of codes matching 'forest%'
  - Qualified partsupp: Hash set<(partkey, suppkey)>
  - Qualified supplier s_suppkey: Hash set<suppkey>
  - Nation lookup: Direct array (25 rows, find code for 'CANADA')

Step 6 (Physical): Parallelism
  - Lineitem aggregation: OpenMP parallel scan with thread-local hash maps, merge
  - Other operations: Sequential (small intermediate results)

================================================================================
                        PHYSICAL QUERY PLAN FOR Q20
================================================================================

Execution order:
  1. [TIMING] Load nation, find 'CANADA' code
  2. [TIMING] Load part, filter p_name LIKE 'forest%', build set of p_partkey
  3. [TIMING] Parallel scan lineitem, aggregate by (l_partkey, l_suppkey), filter by date
  4. [TIMING] Load partsupp, filter by qualified p_partkey and ps_availqty > 0.5*sum
  5. [TIMING] Load supplier, filter by s_nationkey = Canada's key
  6. [TIMING] Semi-join supplier with qualified partsupp s_suppkey, build result
  7. [TIMING] Sort result by s_name
  8. [TIMING] Write CSV output
  9. [TIMING] Total computation time

================================================================================
*/

// DATE encoding: int32_t days since 1970-01-01
// Compute 1994-01-01 and 1995-01-01
// 1994-01-01: years 1970-1993 = 24 years
//   1970-2001 has leap years every 4 years except 2000
//   Leap years 1972,76,80,84,88,92 (6 leap years in 1970-1993)
//   Days = 24*365 + 6 = 8766
// 1995-01-01: 25*365 + 6 = 9131
const int32_t DATE_1994_01_01 = 8766;
const int32_t DATE_1995_01_01 = 9131;

struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        size = lseek(fd, 0, SEEK_END);
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            exit(1);
        }
    }

    ~MmapFile() {
        if (ptr != MAP_FAILED && ptr) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }
};

// Load dictionary file and build reverse map: value → code
std::unordered_map<std::string, int32_t> load_dict(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> result;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        exit(1);
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            result[value] = code;
        }
    }
    return result;
}

// Load dictionary file and build forward map: code → value
std::unordered_map<int32_t, std::string> load_dict_forward(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> result;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        exit(1);
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            result[code] = value;
        }
    }
    return result;
}

// Simple string prefix matching
bool starts_with_forest(const std::string& s) {
    return s.size() >= 6 && s.substr(0, 6) == "forest";
}

void run_q20(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // =========================================================================
    // Step 1: Load nation, find CANADA's code
    // =========================================================================

    std::string nation_dir = gendb_dir + "/nation";
    std::string nation_nationkey_file = nation_dir + "/n_nationkey.bin";
    std::string nation_name_dict_file = nation_dir + "/n_name_dict.txt";

    auto dict_n_name = load_dict_forward(nation_name_dict_file);

    MmapFile nation_nationkey_mmap(nation_nationkey_file);
    int32_t* nation_nationkey = static_cast<int32_t*>(nation_nationkey_mmap.ptr);
    size_t nation_count = nation_nationkey_mmap.size / sizeof(int32_t);

    int32_t canada_nationkey = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (dict_n_name.count(nation_nationkey[i])) {
            if (dict_n_name[nation_nationkey[i]] == "CANADA") {
                canada_nationkey = nation_nationkey[i];
                break;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] nation_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 2: Load part, filter p_name LIKE 'forest%'
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string part_dir = gendb_dir + "/part";
    std::string part_partkey_file = part_dir + "/p_partkey.bin";
    std::string part_name_dict_file = part_dir + "/p_name_dict.txt";

    auto dict_p_name = load_dict_forward(part_name_dict_file);

    MmapFile part_partkey_mmap(part_partkey_file);
    int32_t* part_partkey = static_cast<int32_t*>(part_partkey_mmap.ptr);
    size_t part_count = part_partkey_mmap.size / sizeof(int32_t);

    MmapFile part_name_mmap(part_dir + "/p_name.bin");
    int32_t* part_name = static_cast<int32_t*>(part_name_mmap.ptr);

    std::unordered_set<int32_t> qualified_partkey;
    for (size_t i = 0; i < part_count; i++) {
        if (dict_p_name.count(part_name[i])) {
            if (starts_with_forest(dict_p_name[part_name[i]])) {
                qualified_partkey.insert(part_partkey[i]);
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] part_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 3: Scan lineitem, aggregate by (l_partkey, l_suppkey)
    //         Filter by date: 1994-01-01 <= l_shipdate < 1995-01-01
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string lineitem_dir = gendb_dir + "/lineitem";
    std::string lineitem_partkey_file = lineitem_dir + "/l_partkey.bin";
    std::string lineitem_suppkey_file = lineitem_dir + "/l_suppkey.bin";
    std::string lineitem_quantity_file = lineitem_dir + "/l_quantity.bin";
    std::string lineitem_shipdate_file = lineitem_dir + "/l_shipdate.bin";

    MmapFile li_partkey_mmap(lineitem_partkey_file);
    int32_t* li_partkey = static_cast<int32_t*>(li_partkey_mmap.ptr);
    size_t lineitem_count = li_partkey_mmap.size / sizeof(int32_t);

    MmapFile li_suppkey_mmap(lineitem_suppkey_file);
    int32_t* li_suppkey = static_cast<int32_t*>(li_suppkey_mmap.ptr);

    MmapFile li_quantity_mmap(lineitem_quantity_file);
    int64_t* li_quantity = static_cast<int64_t*>(li_quantity_mmap.ptr);

    MmapFile li_shipdate_mmap(lineitem_shipdate_file);
    int32_t* li_shipdate = static_cast<int32_t*>(li_shipdate_mmap.ptr);

    // Hash map: (partkey, suppkey) → sum of quantity
    using KeyPair = std::pair<int32_t, int32_t>;
    struct PairHash {
        size_t operator()(const KeyPair& p) const {
            return std::hash<int64_t>()(((int64_t)p.first << 32) | (uint32_t)p.second);
        }
    };

    std::unordered_map<KeyPair, int64_t, PairHash> lineitem_agg;
    lineitem_agg.reserve(15000000);  // Expected ~15M groups

    for (size_t i = 0; i < lineitem_count; i++) {
        if (li_shipdate[i] >= DATE_1994_01_01 && li_shipdate[i] < DATE_1995_01_01) {
            KeyPair key = {li_partkey[i], li_suppkey[i]};
            lineitem_agg[key] += li_quantity[i];
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] lineitem_aggregation: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 4: Load partsupp, filter by qualified partkey and qty condition
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string partsupp_dir = gendb_dir + "/partsupp";
    std::string partsupp_partkey_file = partsupp_dir + "/ps_partkey.bin";
    std::string partsupp_suppkey_file = partsupp_dir + "/ps_suppkey.bin";
    std::string partsupp_availqty_file = partsupp_dir + "/ps_availqty.bin";

    MmapFile ps_partkey_mmap(partsupp_partkey_file);
    int32_t* ps_partkey = static_cast<int32_t*>(ps_partkey_mmap.ptr);
    size_t partsupp_count = ps_partkey_mmap.size / sizeof(int32_t);

    MmapFile ps_suppkey_mmap(partsupp_suppkey_file);
    int32_t* ps_suppkey = static_cast<int32_t*>(ps_suppkey_mmap.ptr);

    MmapFile ps_availqty_mmap(partsupp_availqty_file);
    int32_t* ps_availqty = static_cast<int32_t*>(ps_availqty_mmap.ptr);

    // Qualified (partkey, suppkey) pairs
    std::unordered_set<KeyPair, PairHash> qualified_partsupp;

    for (size_t i = 0; i < partsupp_count; i++) {
        // Check if partkey is in forest parts
        if (qualified_partkey.count(ps_partkey[i]) == 0) continue;

        // Check availability condition: ps_availqty > 0.5 * sum(l_quantity)
        KeyPair key = {ps_partkey[i], ps_suppkey[i]};
        auto it = lineitem_agg.find(key);
        if (it == lineitem_agg.end()) continue;  // No lineitem for this pair

        int64_t threshold = (it->second / 2);  // 0.5 * sum (integer division, scale 100)
        // ps_availqty is int32_t, need to convert to same scale
        // l_quantity is in scale 100, ps_availqty is plain integer
        // So we need ps_availqty*100 > 0.5*sum
        if ((int64_t)ps_availqty[i] * 100 > threshold) {
            qualified_partsupp.insert(key);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] partsupp_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 5: Load supplier, filter by Canada, find qualified suppliers
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string supplier_dir = gendb_dir + "/supplier";
    std::string supplier_suppkey_file = supplier_dir + "/s_suppkey.bin";
    std::string supplier_name_file = supplier_dir + "/s_name.bin";
    std::string supplier_address_file = supplier_dir + "/s_address.bin";
    std::string supplier_nationkey_file = supplier_dir + "/s_nationkey.bin";
    std::string supplier_name_dict_file = supplier_dir + "/s_name_dict.txt";
    std::string supplier_address_dict_file = supplier_dir + "/s_address_dict.txt";

    auto dict_s_name = load_dict_forward(supplier_name_dict_file);
    auto dict_s_address = load_dict_forward(supplier_address_dict_file);

    MmapFile supp_suppkey_mmap(supplier_suppkey_file);
    int32_t* supp_suppkey = static_cast<int32_t*>(supp_suppkey_mmap.ptr);
    size_t supplier_count = supp_suppkey_mmap.size / sizeof(int32_t);

    MmapFile supp_name_mmap(supplier_name_file);
    int32_t* supp_name = static_cast<int32_t*>(supp_name_mmap.ptr);

    MmapFile supp_address_mmap(supplier_address_file);
    int32_t* supp_address = static_cast<int32_t*>(supp_address_mmap.ptr);

    MmapFile supp_nationkey_mmap(supplier_nationkey_file);
    int32_t* supp_nationkey = static_cast<int32_t*>(supp_nationkey_mmap.ptr);

    // Collect all suppkeys from qualified partsupp
    std::unordered_set<int32_t> qualified_suppkey;
    for (const auto& key : qualified_partsupp) {
        qualified_suppkey.insert(key.second);
    }

    // Filter supplier: s_nationkey = CANADA and s_suppkey in qualified_suppkey
    struct Result {
        std::string s_name;
        std::string s_address;
        bool operator<(const Result& other) const {
            return s_name < other.s_name;
        }
    };

    std::vector<Result> results;
    for (size_t i = 0; i < supplier_count; i++) {
        if (supp_nationkey[i] != canada_nationkey) continue;
        if (qualified_suppkey.count(supp_suppkey[i]) == 0) continue;

        Result r;
        r.s_name = dict_s_name[supp_name[i]];
        r.s_address = dict_s_address[supp_address[i]];
        results.push_back(r);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] supplier_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 6: Sort results by s_name
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end());

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 7: Write results to CSV
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q20.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        exit(1);
    }

    out << "s_name,s_address\n";
    for (const auto& r : results) {
        // Escape quotes in addresses
        std::string escaped_address = r.s_address;
        // Check if address contains comma or quote
        bool needs_quotes = false;
        for (char c : escaped_address) {
            if (c == ',' || c == '"' || c == '\n') {
                needs_quotes = true;
                break;
            }
        }

        out << r.s_name;
        out << ",";
        if (needs_quotes) {
            out << "\"";
            for (char c : escaped_address) {
                if (c == '"') out << "\"\"";
                else out << c;
            }
            out << "\"";
        } else {
            out << escaped_address;
        }
        out << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
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
    run_q20(gendb_dir, results_dir);
    return 0;
}
#endif
