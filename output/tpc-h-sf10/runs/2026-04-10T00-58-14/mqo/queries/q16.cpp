// Q16 tail — Parts/Supplier Relationship (standalone, no shared components)
// Pipeline: supplier anti-join set → part filtered hash map → partsupp scan+probe+agg → sort → output

#include "mqo_profile.hpp"
#include "../shared/mqo_io.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <omp.h>

namespace mqo::tails {

void run_Q16(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q16_tail");

    const std::string storage = ctx.gendb_dir;
    const std::string supp_dir = storage + "/supplier";
    const std::string part_dir = storage + "/part";
    const std::string ps_dir   = storage + "/partsupp";

    // --- Step 1: Build anti-join set from supplier ---
    std::unordered_set<int32_t> bad_suppkeys;
    {
        MQO_TIME_PHASE("Q16_supplier_filter");
        size_t n_supp = mqo::io::read_row_count(supp_dir + "/meta.txt");
        size_t dummy;
        const int32_t* s_suppkey = mqo::io::mmap_column_unchecked<int32_t>(supp_dir + "/s_suppkey.bin", dummy);
        auto s_comment = mqo::io::mmap_varlen(supp_dir, "s_comment");

        bad_suppkeys.reserve(256);
        for (size_t i = 0; i < n_supp; ++i) {
            auto sv = s_comment.get(i);
            // Match '%Customer%Complaints%' — find "Customer" then "Complaints" after it
            auto pos1 = sv.find("Customer");
            if (pos1 == std::string_view::npos) continue;
            auto pos2 = sv.find("Complaints", pos1 + 8);
            if (pos2 == std::string_view::npos) continue;
            bad_suppkeys.insert(s_suppkey[i]);
        }
    }

    // --- Step 2: Load part dictionaries and build filtered part hash map ---
    // part: p_brand (uint8 dict), p_type (uint8 dict), p_size (int32)
    size_t n_part = mqo::io::read_row_count(part_dir + "/meta.txt");
    auto brand_dict = mqo::io::read_dictionary(part_dir + "/p_brand_dict.bin");
    auto type_dict  = mqo::io::read_dictionary(part_dir + "/p_type_dict.bin");

    // Precompute filter masks on dictionary codes
    // p_brand <> 'Brand#45'
    uint8_t brand45_code = 255;
    for (size_t i = 0; i < brand_dict.size(); ++i) {
        if (brand_dict[i] == "Brand#45") { brand45_code = (uint8_t)i; break; }
    }

    // p_type NOT LIKE 'MEDIUM POLISHED%'
    std::vector<bool> type_ok(type_dict.size(), true);
    for (size_t i = 0; i < type_dict.size(); ++i) {
        if (type_dict[i].size() >= 15 &&
            type_dict[i].compare(0, 15, "MEDIUM POLISHED") == 0) {
            type_ok[i] = false;
        }
    }

    // p_size IN (49, 14, 23, 45, 19, 3, 36, 9) — bitset for sizes 0..49
    uint64_t size_bitset = 0;
    static const int valid_sizes[] = {49, 14, 23, 45, 19, 3, 36, 9};
    for (int s : valid_sizes) size_bitset |= (1ULL << s);

    // Part hash map: partkey -> {brand_code, type_code, size}
    struct PartEntry {
        uint8_t brand_code;
        uint8_t type_code;
        int32_t size;
    };

    std::unordered_map<int32_t, PartEntry> part_map;
    {
        MQO_TIME_PHASE("Q16_part_filter");
        size_t dummy;
        const int32_t* p_partkey = mqo::io::mmap_column_unchecked<int32_t>(part_dir + "/p_partkey.bin", dummy);
        const uint8_t* p_brand   = mqo::io::mmap_column_unchecked<uint8_t>(part_dir + "/p_brand.bin", dummy);
        const uint8_t* p_type    = mqo::io::mmap_column_unchecked<uint8_t>(part_dir + "/p_type.bin", dummy);
        const int32_t* p_size    = mqo::io::mmap_column_unchecked<int32_t>(part_dir + "/p_size.bin", dummy);

        part_map.reserve(400000);
        for (size_t i = 0; i < n_part; ++i) {
            uint8_t bc = p_brand[i];
            if (bc == brand45_code) continue;
            uint8_t tc = p_type[i];
            if (!type_ok[tc]) continue;
            int32_t sz = p_size[i];
            if (sz < 0 || sz > 49 || !((size_bitset >> sz) & 1)) continue;
            part_map.insert({p_partkey[i], {bc, tc, sz}});
        }
    }

    // --- Step 3: Scan partsupp, probe part map, anti-join, COUNT(DISTINCT) ---
    // Group key: (brand_code:8, type_code:8, size:16) packed into uint32_t
    // Dedup key: (group_key:32, suppkey:32) packed into uint64_t

    size_t n_ps = mqo::io::read_row_count(ps_dir + "/meta.txt");
    size_t dummy;
    const int32_t* ps_partkey = mqo::io::mmap_column_unchecked<int32_t>(ps_dir + "/ps_partkey.bin", dummy);
    const int32_t* ps_suppkey = mqo::io::mmap_column_unchecked<int32_t>(ps_dir + "/ps_suppkey.bin", dummy);

    // Use per-thread dedup sets for parallel scan
    int max_threads = omp_get_max_threads();
    // Bound intra-tail parallelism to avoid oversubscription
    int n_threads = std::min(max_threads, 16);

    std::vector<std::unordered_set<uint64_t>> thread_dedup(n_threads);
    for (auto& s : thread_dedup) s.reserve(1 << 17); // ~130K slots per thread

    {
        MQO_TIME_PHASE("Q16_partsupp_scan");

        #pragma omp parallel num_threads(n_threads)
        {
            int tid = omp_get_thread_num();
            auto& my_dedup = thread_dedup[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_ps; ++i) {
                int32_t pk = ps_partkey[i];
                auto it = part_map.find(pk);
                if (it == part_map.end()) continue;

                int32_t sk = ps_suppkey[i];
                if (bad_suppkeys.count(sk)) continue;

                const auto& pe = it->second;
                uint32_t gk = ((uint32_t)pe.brand_code << 24) |
                              ((uint32_t)pe.type_code << 16) |
                              ((uint32_t)(pe.size & 0xFFFF));
                uint64_t dk = ((uint64_t)gk << 32) | (uint32_t)sk;
                my_dedup.insert(dk);
            }
        }
    }

    // --- Step 4: Merge per-thread dedup sets and count per group ---
    struct ResultRow {
        uint8_t brand_code;
        uint8_t type_code;
        int32_t size;
        int64_t supplier_cnt;
    };

    std::vector<ResultRow> results;
    {
        MQO_TIME_PHASE("Q16_aggregate");

        // Merge all thread-local sets into one global set
        std::unordered_set<uint64_t> global_dedup;
        size_t total_est = 0;
        for (auto& s : thread_dedup) total_est += s.size();
        global_dedup.reserve(total_est);
        for (auto& s : thread_dedup) {
            for (uint64_t v : s) global_dedup.insert(v);
            s.clear();
        }

        // Count per group
        std::unordered_map<uint32_t, int64_t> group_counts;
        group_counts.reserve(32768);
        for (uint64_t v : global_dedup) {
            uint32_t gk = (uint32_t)(v >> 32);
            group_counts[gk]++;
        }

        results.reserve(group_counts.size());
        for (auto& [gk, cnt] : group_counts) {
            uint8_t bc = (uint8_t)(gk >> 24);
            uint8_t tc = (uint8_t)((gk >> 16) & 0xFF);
            int32_t sz = (int32_t)(gk & 0xFFFF);
            results.push_back({bc, tc, sz, cnt});
        }
    }

    // --- Step 5: Sort by supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC ---
    {
        MQO_TIME_PHASE("Q16_sort");
        std::sort(results.begin(), results.end(),
            [&](const ResultRow& a, const ResultRow& b) {
                if (a.supplier_cnt != b.supplier_cnt) return a.supplier_cnt > b.supplier_cnt;
                // Compare brand strings
                int cb = brand_dict[a.brand_code].compare(brand_dict[b.brand_code]);
                if (cb != 0) return cb < 0;
                // Compare type strings
                int ct = type_dict[a.type_code].compare(type_dict[b.type_code]);
                if (ct != 0) return ct < 0;
                return a.size < b.size;
            });
    }

    // --- Step 6: Output CSV ---
    {
        MQO_TIME_PHASE("Q16_output");
        std::string outpath = ctx.output_dir + "/q16.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[Q16] Cannot open output: %s\n", outpath.c_str());
            return;
        }
        std::fprintf(fp, "p_brand,p_type,p_size,supplier_cnt\n");
        for (const auto& r : results) {
            std::fprintf(fp, "%s,%s,%d,%ld\n",
                brand_dict[r.brand_code].c_str(),
                type_dict[r.type_code].c_str(),
                r.size,
                (long)r.supplier_cnt);
        }
        std::fclose(fp);
    }
}

}  // namespace mqo::tails
