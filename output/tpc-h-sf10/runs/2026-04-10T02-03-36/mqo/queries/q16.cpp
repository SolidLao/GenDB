// Q16 tail — Parts/Supplier Relationship
// Shared inputs: hash_part_by_partkey, scan_partsupp_full, hash_supplier_by_suppkey
// Operators: anti-join (supplier complaints), part filter, scan+agg partsupp, sort, output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/hash_part_by_partkey.hpp"
#include "shared/scan_partsupp_full.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <bitset>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace mqo { namespace tails {

void run_Q16(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q16_tail");

    const auto& part = mqo::shared::hash_part_by_partkey::get();
    const auto& ps   = mqo::shared::scan_partsupp_full::get();
    const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();

    // Load dictionaries for p_brand and p_type
    mqo::io::Dictionary brand_dict, type_dict;
    {
        MQO_TIME_PHASE("Q16_load_dicts");
        brand_dict.load(ctx.gendb_dir + "/part/p_brand_dict.bin");
        type_dict.load(ctx.gendb_dir + "/part/p_type_dict.bin");
    }

    // ----------------------------------------------------------------
    // Step 1: Build anti-join bitset — suppliers with '%Customer%Complaints%'
    // ----------------------------------------------------------------
    // Use a vector<bool> sized to max_suppkey+1
    static constexpr int32_t MAX_SUPPKEY = 100001;
    std::vector<bool> complaint_supp(MAX_SUPPKEY, false);
    {
        MQO_TIME_PHASE("Q16_anti_join_build");
        for (size_t i = 0; i < supp.n_rows; ++i) {
            auto comment = supp.s_comment.get(i);
            // Match '%Customer%Complaints%' — find "Customer" then "Complaints" after it
            auto pos1 = comment.find("Customer");
            if (pos1 != std::string_view::npos) {
                auto pos2 = comment.find("Complaints", pos1 + 8);
                if (pos2 != std::string_view::npos) {
                    int32_t sk = supp.s_suppkey[i];
                    if (sk >= 0 && sk < MAX_SUPPKEY) {
                        complaint_supp[sk] = true;
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------
    // Step 2: Build part filter — resolve dict codes, build partkey bitset + group map
    // ----------------------------------------------------------------
    // Resolve 'Brand#45' dict code
    uint8_t brand45_code = 255;
    for (uint32_t i = 0; i < brand_dict.count; ++i) {
        if (brand_dict.entries[i] == "Brand#45") {
            brand45_code = static_cast<uint8_t>(i);
            break;
        }
    }

    // Precompute excluded type codes (those starting with "MEDIUM POLISHED")
    std::vector<bool> excluded_type(256, false);
    for (uint32_t i = 0; i < type_dict.count; ++i) {
        if (type_dict.entries[i].compare(0, 15, "MEDIUM POLISHED") == 0) {
            excluded_type[i] = true;
        }
    }

    // Allowed sizes
    bool allowed_size[51] = {};
    {
        int sizes[] = {49, 14, 23, 45, 19, 3, 36, 9};
        for (int s : sizes) allowed_size[s] = true;
    }

    // partkey_to_group: 0 = not valid, >0 = group_id+1
    // group_id packing: brand_code * 16384 + type_code * 64 + size
    // Max: 255*16384 + 255*64 + 50 ~= 4.2M — sparse but fine
    static constexpr int32_t MAX_PARTKEY = 2000001;
    std::vector<uint32_t> partkey_to_group(MAX_PARTKEY, 0);

    // Also collect unique group_ids and their decoded strings
    struct GroupInfo {
        std::string brand;
        std::string type;
        int32_t size;
    };

    // We'll use a map from group_id -> index for output
    // But first, build the partkey_to_group map
    // group_id = brand_code * 16384 + type_code * 64 + size (1-indexed: +1)
    {
        MQO_TIME_PHASE("Q16_part_filter");
        for (size_t i = 0; i < part.n_rows; ++i) {
            uint8_t bc = part.p_brand[i];
            if (bc == brand45_code) continue;

            uint8_t tc = part.p_type[i];
            if (excluded_type[tc]) continue;

            int32_t sz = part.p_size[i];
            if (sz < 1 || sz > 50 || !allowed_size[sz]) continue;

            int32_t pk = part.p_partkey[i];
            if (pk >= 0 && pk < MAX_PARTKEY) {
                uint32_t gid = static_cast<uint32_t>(bc) * 16384u +
                               static_cast<uint32_t>(tc) * 64u +
                               static_cast<uint32_t>(sz);
                partkey_to_group[pk] = gid + 1; // +1 so 0 means invalid
            }
        }
    }

    // ----------------------------------------------------------------
    // Step 3: Scan partsupp, apply anti-join + semi-join, collect (group_id, suppkey)
    // ----------------------------------------------------------------
    struct Pair {
        uint32_t group_id;
        int32_t  suppkey;
    };

    std::vector<Pair> pairs;
    {
        MQO_TIME_PHASE("Q16_main_scan");
        // Estimate ~1.2M surviving rows
        pairs.reserve(1500000);

        const int32_t* ps_pk = ps.ps_partkey;
        const int32_t* ps_sk = ps.ps_suppkey;
        const size_t n = ps.n_rows;

        for (size_t i = 0; i < n; ++i) {
            int32_t sk = ps_sk[i];
            // Anti-join: skip if complaint supplier
            if (sk >= 0 && sk < MAX_SUPPKEY && complaint_supp[sk]) continue;

            int32_t pk = ps_pk[i];
            // Semi-join: skip if partkey not in valid set
            if (pk < 0 || pk >= MAX_PARTKEY) continue;
            uint32_t g = partkey_to_group[pk];
            if (g == 0) continue;

            pairs.push_back({g - 1, sk});
        }
    }

    // ----------------------------------------------------------------
    // Step 4: Sort pairs by (group_id, suppkey), then count distinct per group
    // ----------------------------------------------------------------
    struct AggResult {
        uint32_t group_id;
        int64_t  supplier_cnt;
    };
    std::vector<AggResult> agg_results;
    {
        MQO_TIME_PHASE("Q16_aggregation");
        std::sort(pairs.begin(), pairs.end(), [](const Pair& a, const Pair& b) {
            if (a.group_id != b.group_id) return a.group_id < b.group_id;
            return a.suppkey < b.suppkey;
        });

        // Count distinct suppkeys per group
        agg_results.reserve(20000);
        size_t i = 0;
        while (i < pairs.size()) {
            uint32_t gid = pairs[i].group_id;
            int64_t cnt = 1;
            size_t j = i + 1;
            while (j < pairs.size() && pairs[j].group_id == gid) {
                if (pairs[j].suppkey != pairs[j-1].suppkey) {
                    ++cnt;
                }
                ++j;
            }
            agg_results.push_back({gid, cnt});
            i = j;
        }
    }

    // ----------------------------------------------------------------
    // Step 5: Decode group_id and sort results
    // ----------------------------------------------------------------
    struct OutputRow {
        std::string brand;
        std::string type;
        int32_t size;
        int64_t supplier_cnt;
    };
    std::vector<OutputRow> results;
    {
        MQO_TIME_PHASE("Q16_sort");
        results.resize(agg_results.size());
        for (size_t i = 0; i < agg_results.size(); ++i) {
            uint32_t gid = agg_results[i].group_id;
            uint8_t bc = static_cast<uint8_t>(gid / 16384u);
            uint8_t tc = static_cast<uint8_t>((gid % 16384u) / 64u);
            int32_t sz = static_cast<int32_t>(gid % 64u);

            results[i].brand = brand_dict.get(bc);
            results[i].type  = type_dict.get(tc);
            results[i].size  = sz;
            results[i].supplier_cnt = agg_results[i].supplier_cnt;
        }

        std::sort(results.begin(), results.end(), [](const OutputRow& a, const OutputRow& b) {
            if (a.supplier_cnt != b.supplier_cnt) return a.supplier_cnt > b.supplier_cnt; // DESC
            if (a.brand != b.brand) return a.brand < b.brand;
            if (a.type != b.type) return a.type < b.type;
            return a.size < b.size;
        });
    }

    // ----------------------------------------------------------------
    // Step 6: Output CSV
    // ----------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q16_output");
        std::string path = ctx.output_dir + "/q16.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", path.c_str());
            return;
        }
        std::fprintf(f, "p_brand,p_type,p_size,supplier_cnt\n");
        for (const auto& r : results) {
            std::fprintf(f, "%s,%s,%d,%ld\n",
                         r.brand.c_str(), r.type.c_str(), r.size, r.supplier_cnt);
        }
        std::fclose(f);
    }
}

}} // namespace mqo::tails
