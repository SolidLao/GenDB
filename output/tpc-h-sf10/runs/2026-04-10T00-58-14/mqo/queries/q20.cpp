// Q20 tail — Potential Part Promotion
// Shared inputs: scan_lineitem_shipdate_1994, hash_supplier_by_suppkey
// Operators: nation filter, part prefix filter, lineitem agg, partsupp indexed check, supplier probe, sort

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_shipdate_1994.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mqo::tails {

void run_Q20(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q20_tail");

    // -----------------------------------------------------------------------
    // Step 1: Scan nation for CANADA → single n_nationkey
    // -----------------------------------------------------------------------
    int32_t canada_nationkey = -1;
    {
        MQO_TIME_PHASE("Q20_nation_filter");
        const std::string ndir = ctx.gendb_dir + "/nation/";
        const size_t n_nation = mqo::io::read_row_count(ndir + "meta.txt");
        auto dict = mqo::io::read_dictionary(ndir + "n_name_dict.bin");
        const auto* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "n_name.bin", n_nation);
        const auto* n_nationkey  = mqo::io::mmap_column<int32_t>(ndir + "n_nationkey.bin", n_nation);

        // Find CANADA dict code
        int canada_code = -1;
        for (int i = 0; i < (int)dict.size(); ++i) {
            if (dict[i] == "CANADA") { canada_code = i; break; }
        }
        for (size_t i = 0; i < n_nation; ++i) {
            if (n_name_codes[i] == (uint8_t)canada_code) {
                canada_nationkey = n_nationkey[i];
                break;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 2: Scan part for p_name LIKE 'forest%' → bitset
    // -----------------------------------------------------------------------
    static constexpr size_t MAX_PARTKEY = 2000001;
    std::vector<bool> forest_parts(MAX_PARTKEY, false);
    {
        MQO_TIME_PHASE("Q20_part_filter");
        const std::string pdir = ctx.gendb_dir + "/part/";
        const size_t n_part = mqo::io::read_row_count(pdir + "meta.txt");
        const auto* p_partkey = mqo::io::mmap_column<int32_t>(pdir + "p_partkey.bin", n_part);
        auto va_pname = mqo::io::mmap_varlen(pdir, "p_name");

        for (size_t i = 0; i < n_part; ++i) {
            auto name = va_pname.get(i);
            if (name.size() >= 6 &&
                name[0] == 'f' && name[1] == 'o' && name[2] == 'r' &&
                name[3] == 'e' && name[4] == 's' && name[5] == 't') {
                forest_parts[p_partkey[i]] = true;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 3: Aggregate lineitem 1994 scan: SUM(l_quantity) GROUP BY (l_partkey, l_suppkey)
    //         Only for rows where l_partkey in forest_parts
    // -----------------------------------------------------------------------
    // Key: pack (partkey, suppkey) into uint64_t
    std::unordered_map<uint64_t, double> qty_agg;
    {
        MQO_TIME_PHASE("Q20_lineitem_agg");
        const auto& li = mqo::shared::scan_lineitem_shipdate_1994::get();
        const size_t n = li.n_rows;

        qty_agg.reserve(65536);
        for (size_t i = 0; i < n; ++i) {
            const int32_t pk = li.l_partkey[i];
            if ((size_t)pk < MAX_PARTKEY && forest_parts[pk]) {
                uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(pk)) << 32) |
                               static_cast<uint64_t>(static_cast<uint32_t>(li.l_suppkey[i]));
                qty_agg[key] += li.l_quantity[i];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 4: Indexed scan of partsupp for forest partkeys
    //         Check ps_availqty > 0.5 * SUM(l_quantity)
    //         Collect qualifying ps_suppkey into bitset
    // -----------------------------------------------------------------------
    static constexpr size_t MAX_SUPPKEY = 100001;
    std::vector<bool> qualifying_supp(MAX_SUPPKEY, false);
    {
        MQO_TIME_PHASE("Q20_partsupp_check");
        const std::string psdir = ctx.gendb_dir + "/partsupp/";
        const size_t n_ps = mqo::io::read_row_count(psdir + "meta.txt");
        const auto* ps_partkey  = mqo::io::mmap_column<int32_t>(psdir + "ps_partkey.bin", n_ps);
        const auto* ps_suppkey  = mqo::io::mmap_column<int32_t>(psdir + "ps_suppkey.bin", n_ps);
        const auto* ps_availqty = mqo::io::mmap_column<int32_t>(psdir + "ps_availqty.bin", n_ps);

        // Load dense_range index: array of (start_u32, count_u32) indexed by partkey
        const std::string idx_path = ctx.gendb_dir + "/indexes/partsupp_partkey_idx.bin";
        size_t idx_sz = 0;
        const auto* idx = static_cast<const uint32_t*>(mqo::io::mmap_file(idx_path, idx_sz));
        // Each entry is 2 uint32_t: [start, count], indexed by partkey (1..2000000)
        // Entry for partkey k is at idx[2*k] and idx[2*k+1]

        for (size_t pk = 1; pk < MAX_PARTKEY; ++pk) {
            if (!forest_parts[pk]) continue;
            uint32_t start = idx[2 * pk];
            uint32_t count = idx[2 * pk + 1];
            for (uint32_t r = start; r < start + count; ++r) {
                int32_t sk = ps_suppkey[r];
                int32_t avail = ps_availqty[r];
                // Lookup lineitem aggregate
                uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(pk)) << 32) |
                               static_cast<uint64_t>(static_cast<uint32_t>(sk));
                auto it = qty_agg.find(key);
                double threshold = 0.0;
                if (it != qty_agg.end()) {
                    threshold = 0.5 * it->second;
                }
                if (static_cast<double>(avail) > threshold) {
                    if (sk >= 0 && (size_t)sk < MAX_SUPPKEY) {
                        qualifying_supp[sk] = true;
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 5: Probe shared supplier hash, filter nationkey = CANADA
    // -----------------------------------------------------------------------
    struct Result {
        std::string name;
        std::string address;
    };
    std::vector<Result> results;
    {
        MQO_TIME_PHASE("Q20_supplier_probe");
        const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();
        results.reserve(400);

        for (size_t sk = 1; sk < MAX_SUPPKEY; ++sk) {
            if (!qualifying_supp[sk]) continue;
            const auto* e = supp.probe(static_cast<int32_t>(sk));
            if (!e) continue;
            if (e->s_nationkey != canada_nationkey) continue;
            auto sname = supp.get_name(e->row_id);
            auto saddr = supp.get_address(e->row_id);
            results.push_back({std::string(sname), std::string(saddr)});
        }
    }

    // -----------------------------------------------------------------------
    // Step 6: Sort by s_name ASC
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q20_sort");
        std::sort(results.begin(), results.end(),
                  [](const Result& a, const Result& b) { return a.name < b.name; });
    }

    // -----------------------------------------------------------------------
    // Output CSV
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q20_output");
        std::string out_path = ctx.output_dir + "/q20.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[MQO] Cannot open output: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(fp, "s_name,s_address\n");
        for (const auto& r : results) {
            std::fprintf(fp, "%s,%s\n", r.name.c_str(), r.address.c_str());
        }
        std::fclose(fp);
    }
}

}  // namespace mqo::tails
