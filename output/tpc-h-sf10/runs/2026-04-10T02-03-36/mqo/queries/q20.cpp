// Q20 tail: Potential Part Promotion
// Shared inputs: hash_part_by_partkey, scan_partsupp_full, hash_supplier_by_suppkey
// Private: nation scan, lineitem index-driven aggregate

#include "mqo_profile.hpp"
#include "../shared/hash_part_by_partkey.hpp"
#include "../shared/scan_partsupp_full.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mqo { namespace tails {

void run_Q20(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q20_tail");

    const auto& part = mqo::shared::hash_part_by_partkey::get();
    const auto& ps   = mqo::shared::scan_partsupp_full::get();
    const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();

    // ================================================================
    // Step 1: Build forest partkeys bitset from shared part hash
    // ================================================================
    std::vector<bool> forest_bitset(static_cast<size_t>(part.max_key) + 1, false);
    {
        MQO_TIME_PHASE("Q20_filter_part");
        for (size_t i = 0; i < part.n_rows; ++i) {
            auto name = part.p_name.get(i);
            if (name.size() >= 6 &&
                name[0] == 'f' && name[1] == 'o' && name[2] == 'r' &&
                name[3] == 'e' && name[4] == 's' && name[5] == 't') {
                int32_t pk = part.p_partkey[i];
                forest_bitset[static_cast<size_t>(pk)] = true;
            }
        }
    }

    // ================================================================
    // Step 2: Scan nation for CANADA nationkey
    // ================================================================
    int32_t canada_nationkey = -1;
    {
        MQO_TIME_PHASE("Q20_filter_nation");
        std::string nation_dir = ctx.gendb_dir + "/nation";
        size_t n_nation = mqo::io::read_row_count(nation_dir);
        // n_name is dict-encoded
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(
            nation_dir + "/n_name.bin", n_nation);
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(
            nation_dir + "/n_nationkey.bin", n_nation);

        mqo::io::Dictionary n_name_dict;
        n_name_dict.load(nation_dir + "/n_name_dict.bin");

        // Find CANADA code
        uint8_t canada_code = 255;
        for (uint32_t d = 0; d < n_name_dict.count; ++d) {
            if (n_name_dict.entries[d] == "CANADA") {
                canada_code = static_cast<uint8_t>(d);
                break;
            }
        }
        for (size_t i = 0; i < n_nation; ++i) {
            if (n_name_codes[i] == canada_code) {
                canada_nationkey = n_nationkey[i];
                break;
            }
        }
    }

    // ================================================================
    // Step 3: Filter suppliers by canada_nationkey, build suppkey bitset + map
    // ================================================================
    std::vector<bool> canada_supp_bitset(static_cast<size_t>(supp.max_key) + 1, false);
    // Store row_ids of canadian suppliers for later name/address lookup
    std::vector<int32_t> canada_supp_rowids;
    canada_supp_rowids.reserve(4100);
    {
        MQO_TIME_PHASE("Q20_filter_supplier");
        for (size_t i = 0; i < supp.n_rows; ++i) {
            if (supp.s_nationkey[i] == canada_nationkey) {
                int32_t sk = supp.s_suppkey[i];
                canada_supp_bitset[static_cast<size_t>(sk)] = true;
                canada_supp_rowids.push_back(static_cast<int32_t>(i));
            }
        }
    }

    // ================================================================
    // Step 4: Filter partsupp by dual bitsets, collect qualifying triples
    // ================================================================
    struct PsPair {
        int32_t partkey;
        int32_t suppkey;
        int32_t availqty;
    };
    std::vector<PsPair> qual_ps;
    qual_ps.reserve(4000);
    std::unordered_set<int32_t> distinct_partkeys;
    {
        MQO_TIME_PHASE("Q20_filter_partsupp");
        size_t forest_max = forest_bitset.size();
        size_t supp_max = canada_supp_bitset.size();
        for (size_t i = 0; i < ps.n_rows; ++i) {
            int32_t pk = ps.ps_partkey[i];
            if (static_cast<size_t>(pk) >= forest_max || !forest_bitset[static_cast<size_t>(pk)])
                continue;
            int32_t sk = ps.ps_suppkey[i];
            if (static_cast<size_t>(sk) >= supp_max || !canada_supp_bitset[static_cast<size_t>(sk)])
                continue;
            qual_ps.push_back({pk, sk, ps.ps_availqty[i]});
            distinct_partkeys.insert(pk);
        }
    }

    // ================================================================
    // Step 5: Build (partkey,suppkey)->availqty map + sorted distinct partkeys
    // ================================================================
    // Use a hash map keyed on (partkey<<20)|suppkey won't work safely.
    // Use a proper pair hash.
    struct PairHash {
        size_t operator()(std::pair<int32_t,int32_t> p) const {
            return static_cast<size_t>(p.first) * 100003ULL + static_cast<size_t>(p.second);
        }
    };
    std::unordered_map<std::pair<int32_t,int32_t>, int32_t, PairHash> ps_avail_map;
    ps_avail_map.reserve(qual_ps.size() * 2);
    for (auto& q : qual_ps) {
        ps_avail_map[{q.partkey, q.suppkey}] = q.availqty;
    }

    std::vector<int32_t> sorted_partkeys(distinct_partkeys.begin(), distinct_partkeys.end());
    std::sort(sorted_partkeys.begin(), sorted_partkeys.end());

    // ================================================================
    // Step 6: Index-driven lineitem aggregation
    // ================================================================
    // Aggregate SUM(l_quantity) GROUP BY (l_partkey, l_suppkey) for qualifying pairs
    // using lineitem_partkey_grouped index
    std::unordered_map<std::pair<int32_t,int32_t>, double, PairHash> qty_agg;
    {
        MQO_TIME_PHASE("Q20_lineitem_agg");

        std::string idx_dir = ctx.gendb_dir + "/indexes";
        std::string li_dir  = ctx.gendb_dir + "/lineitem";

        // Index format: offsets[max_key+2] uint32_t, row_ids[num_rows] uint32_t
        size_t idx_off_sz = 0;
        const uint32_t* idx_offsets = reinterpret_cast<const uint32_t*>(
            mqo::io::mmap_file_raw(idx_dir + "/lineitem_partkey_grouped_offsets.bin", idx_off_sz));
        size_t idx_row_sz = 0;
        const uint32_t* idx_rows = reinterpret_cast<const uint32_t*>(
            mqo::io::mmap_file_raw(idx_dir + "/lineitem_partkey_grouped_rows.bin", idx_row_sz));

        size_t li_n = 59986052; // known from meta
        const int32_t* l_partkey  = mqo::io::mmap_column<int32_t>(li_dir + "/l_partkey.bin", li_n);
        const int32_t* l_suppkey  = mqo::io::mmap_column<int32_t>(li_dir + "/l_suppkey.bin", li_n);
        const int32_t* l_shipdate = mqo::io::mmap_column<int32_t>(li_dir + "/l_shipdate.bin", li_n);
        const double*  l_quantity = mqo::io::mmap_column<double> (li_dir + "/l_quantity.bin", li_n);

        // Date constants: 1994-01-01 = 8766 days from epoch, 1995-01-01 = 9131
        constexpr int32_t DATE_LO = 8766;
        constexpr int32_t DATE_HI = 9131;

        qty_agg.reserve(qual_ps.size() * 2);

        // Process each distinct partkey via index
        for (int32_t pk : sorted_partkeys) {
            uint32_t start = idx_offsets[pk];
            uint32_t end   = idx_offsets[pk + 1];
            for (uint32_t j = start; j < end; ++j) {
                uint32_t rid = idx_rows[j];
                int32_t sd = l_shipdate[rid];
                if (sd < DATE_LO || sd >= DATE_HI) continue;
                int32_t sk = l_suppkey[rid];
                // Check if this (pk, sk) pair is in our qualifying set
                auto it = ps_avail_map.find({pk, sk});
                if (it == ps_avail_map.end()) continue;
                qty_agg[{pk, sk}] += l_quantity[rid];
            }
        }
    }

    // ================================================================
    // Step 7: Correlated comparison — ps_availqty > 0.5 * SUM(l_quantity)
    // ================================================================
    std::unordered_set<int32_t> qualifying_suppkeys;
    {
        MQO_TIME_PHASE("Q20_comparison");
        for (auto& [pair, availqty] : ps_avail_map) {
            double sum_qty = 0.0;
            auto it = qty_agg.find(pair);
            if (it != qty_agg.end()) {
                sum_qty = it->second;
            }
            if (static_cast<double>(availqty) > 0.5 * sum_qty) {
                qualifying_suppkeys.insert(pair.second);
            }
        }
    }

    // ================================================================
    // Step 8: Semi-join with canada suppliers to get output rows
    // ================================================================
    struct ResultRow {
        std::string_view s_name;
        std::string_view s_address;
    };
    std::vector<ResultRow> results;
    results.reserve(qualifying_suppkeys.size());
    {
        MQO_TIME_PHASE("Q20_semi_join");
        for (int32_t rid : canada_supp_rowids) {
            int32_t sk = supp.s_suppkey[rid];
            if (qualifying_suppkeys.count(sk)) {
                results.push_back({supp.s_name.get(static_cast<size_t>(rid)),
                                   supp.s_address.get(static_cast<size_t>(rid))});
            }
        }
    }

    // ================================================================
    // Step 9: Sort by s_name ASC
    // ================================================================
    {
        MQO_TIME_PHASE("Q20_sort");
        std::sort(results.begin(), results.end(),
                  [](const ResultRow& a, const ResultRow& b) {
                      return a.s_name < b.s_name;
                  });
    }

    // ================================================================
    // Output
    // ================================================================
    {
        MQO_TIME_PHASE("Q20_output");
        std::string out_path = ctx.output_dir + "/q20.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", out_path.c_str());
            return;
        }
        std::fprintf(f, "s_name,s_address\n");
        for (auto& r : results) {
            std::fwrite(r.s_name.data(), 1, r.s_name.size(), f);
            std::fputc(',', f);
            std::fwrite(r.s_address.data(), 1, r.s_address.size(), f);
            std::fputc('\n', f);
        }
        std::fclose(f);
    }
}

}} // namespace mqo::tails
