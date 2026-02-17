/*
 * Q20: Potential Part Promotion (Iteration 0)
 *
 * LOGICAL PLAN:
 * 1. Filter nation: n_name = 'CANADA' → 1 row
 * 2. Filter part: p_name LIKE 'forest%' → ~40K rows (2% of 2M)
 * 3. Build part_set: hash set of qualifying p_partkey
 * 4. Filter partsupp: ps_partkey IN part_set → ~160K candidates
 * 5. Decorrelate correlated subquery:
 *    - Pre-compute: GROUP BY (l_partkey, l_suppkey)
 *                   SUM(l_quantity) WHERE l_shipdate ∈ [1994-01-01, 1995-01-01)
 *    - Build hash map: (partkey, suppkey) → sum_quantity
 * 6. Filter partsupp: ps_availqty > 0.5 * threshold
 * 7. Build suppkey_set from qualifying partsupp
 * 8. Join supplier ⋈ nation on n_nationkey, filter by suppkey_set
 * 9. Sort by s_name
 *
 * PHYSICAL PLAN:
 * - Nation: Direct scan (25 rows), find 'CANADA'
 * - Part: Parallel scan with prefix filter → hash set
 * - Lineitem: Parallel scan + aggregation → hash map (partkey, suppkey) → sum_qty
 * - Partsupp: Scan filtered by part_set, check threshold, build suppkey_set
 * - Supplier: Scan filtered by nation + suppkey_set
 * - Sort: std::sort on s_name
 *
 * CRITICAL: Subquery decorrelation is key — single lineitem scan instead of per-partsupp evaluation
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <thread>
#include <mutex>

// Date conversion: '1994-01-01' and '1995-01-01' to epoch days
// 1994-01-01: (1994-1970)*365 + 6 leap years (1972,1976,1980,1984,1988,1992) = 8766
// 1995-01-01: 8766 + 365 = 9131
constexpr int32_t DATE_1994_01_01 = 8766;
constexpr int32_t DATE_1995_01_01 = 9131;

// Composite key hash for (partkey, suppkey)
struct PairHash {
    size_t operator()(const std::pair<int32_t, int32_t>& p) const {
        return ((size_t)p.first * 0x9E3779B97F4A7C15ULL) ^ ((size_t)p.second * 0x517CC1B727220A95ULL);
    }
};

// Memory-mapped file helper
template<typename T>
T* mmap_column(const std::string& path, size_t& out_count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Failed to open %s\n", path.c_str());
        return nullptr;
    }
    struct stat sb;
    fstat(fd, &sb);
    out_count = sb.st_size / sizeof(T);
    T* data = (T*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return data;
}

// Load string column (binary: [uint32_t len][char data...] per row)
std::vector<std::string> load_strings(const std::string& path, size_t num_rows) {
    std::vector<std::string> result;
    result.reserve(num_rows);
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return result;
    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    size_t offset = 0;
    for (size_t i = 0; i < num_rows && offset + 4 <= (size_t)sb.st_size; i++) {
        uint32_t len = *(uint32_t*)(data + offset);
        offset += 4;
        if (offset + len <= (size_t)sb.st_size) {
            result.emplace_back(data + offset, len);
            offset += len;
        }
    }
    munmap(data, sb.st_size);
    return result;
}

void run_q20(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Step 1: Find CANADA nationkey
#ifdef GENDB_PROFILE
    auto t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t nation_count;
    int32_t* n_nationkey = mmap_column<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    auto n_name = load_strings(gendb_dir + "/nation/n_name.bin", nation_count);

    int32_t canada_nationkey = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (n_name[i] == "CANADA") {
            canada_nationkey = n_nationkey[i];
            break;
        }
    }
#ifdef GENDB_PROFILE
    auto t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] nation_scan: %.2f ms\n", std::chrono::duration<double, std::milli>(t2 - t1).count());
#endif

    // Step 2: Filter part for 'forest%' prefix
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t part_count;
    int32_t* p_partkey = mmap_column<int32_t>(gendb_dir + "/part/p_partkey.bin", part_count);
    auto p_name = load_strings(gendb_dir + "/part/p_name.bin", part_count);

    std::unordered_set<int32_t> forest_parts;
    forest_parts.reserve(100000);

    for (size_t i = 0; i < part_count; i++) {
        if (p_name[i].size() >= 6 && p_name[i].substr(0, 6) == "forest") {
            forest_parts.insert(p_partkey[i]);
        }
    }
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] part_filter: %.2f ms (forest parts: %zu)\n",
           std::chrono::duration<double, std::milli>(t2 - t1).count(), forest_parts.size());
#endif

    // Step 3: Decorrelate subquery — scan lineitem once, aggregate by (partkey, suppkey)
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t lineitem_count;
    int32_t* l_partkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", lineitem_count);
    int32_t* l_suppkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_count);
    int64_t* l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", lineitem_count);
    int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);

    // Parallel aggregation with thread-local hash maps
    int num_threads = std::thread::hardware_concurrency();
    std::vector<std::unordered_map<std::pair<int32_t, int32_t>, int64_t, PairHash>> thread_maps(num_threads);

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t chunk = (lineitem_count + num_threads - 1) / num_threads;
            size_t start = t * chunk;
            size_t end = std::min(start + chunk, lineitem_count);

            auto& local_map = thread_maps[t];
            local_map.reserve(50000);

            for (size_t i = start; i < end; i++) {
                if (l_shipdate[i] >= DATE_1994_01_01 && l_shipdate[i] < DATE_1995_01_01) {
                    auto key = std::make_pair(l_partkey[i], l_suppkey[i]);
                    local_map[key] += l_quantity[i];
                }
            }
        });
    }
    for (auto& th : threads) th.join();

    // Merge thread-local maps
    std::unordered_map<std::pair<int32_t, int32_t>, int64_t, PairHash> lineitem_agg;
    lineitem_agg.reserve(200000);
    for (auto& tmap : thread_maps) {
        for (auto& kv : tmap) {
            lineitem_agg[kv.first] += kv.second;
        }
    }
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] lineitem_agg: %.2f ms (groups: %zu)\n",
           std::chrono::duration<double, std::milli>(t2 - t1).count(), lineitem_agg.size());
#endif

    // Step 4: Filter partsupp
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t partsupp_count;
    int32_t* ps_partkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", partsupp_count);
    int32_t* ps_suppkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", partsupp_count);
    int32_t* ps_availqty = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_availqty.bin", partsupp_count);

    std::unordered_set<int32_t> qualifying_suppkeys;
    qualifying_suppkeys.reserve(10000);

    for (size_t i = 0; i < partsupp_count; i++) {
        // Check if part is in forest_parts
        if (forest_parts.count(ps_partkey[i]) == 0) continue;

        // Check availability threshold
        auto key = std::make_pair(ps_partkey[i], ps_suppkey[i]);
        auto it = lineitem_agg.find(key);

        // If no lineitem data for this (partkey, suppkey) pair, the subquery returns NULL
        // In SQL, ps_availqty > NULL evaluates to NULL (not true), so exclude this row
        if (it == lineitem_agg.end()) continue;

        // l_quantity is scaled by 100, so sum is also scaled by 100
        // 0.5 * sum means sum / 2
        int64_t threshold = it->second / 2;

        // ps_availqty is int32_t, threshold is int64_t (scaled by 100)
        // Convert ps_availqty to same scale: ps_availqty * 100
        if ((int64_t)ps_availqty[i] * 100 > threshold) {
            qualifying_suppkeys.insert(ps_suppkey[i]);
        }
    }
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] partsupp_filter: %.2f ms (qualifying suppkeys: %zu)\n",
           std::chrono::duration<double, std::milli>(t2 - t1).count(), qualifying_suppkeys.size());
#endif

    // Step 5: Filter supplier by nationkey and suppkey
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t supplier_count;
    int32_t* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);
    auto s_name = load_strings(gendb_dir + "/supplier/s_name.bin", supplier_count);
    auto s_address = load_strings(gendb_dir + "/supplier/s_address.bin", supplier_count);

    struct Result {
        std::string s_name;
        std::string s_address;
    };
    std::vector<Result> results;
    results.reserve(1000);

    for (size_t i = 0; i < supplier_count; i++) {
        if (s_nationkey[i] == canada_nationkey && qualifying_suppkeys.count(s_suppkey[i])) {
            results.push_back({s_name[i], s_address[i]});
        }
    }
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] supplier_join: %.2f ms\n", std::chrono::duration<double, std::milli>(t2 - t1).count());
#endif

    // Step 6: Sort by s_name
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.s_name < b.s_name;
    });
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] sort: %.2f ms\n", std::chrono::duration<double, std::milli>(t2 - t1).count());
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] total: %.2f ms\n", std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count());
#endif

    // Step 7: Write output
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    std::ofstream outfile(results_dir + "/Q20.csv");
    outfile << "s_name,s_address\n";
    for (const auto& r : results) {
        // Quote fields if they contain commas
        auto quote_if_needed = [](const std::string& s) {
            if (s.find(',') != std::string::npos || s.find('"') != std::string::npos) {
                std::string quoted = "\"";
                for (char c : s) {
                    if (c == '"') quoted += "\"\"";
                    else quoted += c;
                }
                quoted += "\"";
                return quoted;
            }
            return s;
        };
        outfile << quote_if_needed(r.s_name) << "," << quote_if_needed(r.s_address) << "\n";
    }
    outfile.close();
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] output: %.2f ms\n", std::chrono::duration<double, std::milli>(t2 - t1).count());
#endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q20(gendb_dir, results_dir);
    return 0;
}
#endif
