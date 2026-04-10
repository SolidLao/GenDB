// Q2 tail: Minimum Cost Supplier
// Shared inputs: hash_part_by_partkey, scan_partsupp_full, hash_supplier_by_suppkey
// Operators: scan_filter(region), scan_filter(nation), build_european_suppliers,
//            residual_filter(part), indexed_subquery_resolve(partsupp), sort_limit, output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/hash_part_by_partkey.hpp"
#include "shared/scan_partsupp_full.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstring>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace mqo { namespace tails {

void run_Q2(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q2_tail");

    const auto& part = mqo::shared::hash_part_by_partkey::get();
    const auto& ps   = mqo::shared::scan_partsupp_full::get();
    const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();

    // ---------------------------------------------------------------
    // Step 1: Find EUROPE regionkey from region table
    // ---------------------------------------------------------------
    int32_t europe_rkey = -1;
    {
        MQO_TIME_PHASE("Q2_region_filter");
        std::string rdir = ctx.gendb_dir + "/region";
        size_t rn = mqo::io::read_row_count(rdir);
        const int32_t* r_regionkey = mqo::io::mmap_column<int32_t>(rdir + "/r_regionkey.bin", rn);
        const uint8_t* r_name_codes = mqo::io::mmap_column<uint8_t>(rdir + "/r_name.bin", rn);
        mqo::io::Dictionary r_name_dict;
        r_name_dict.load(rdir + "/r_name_dict.bin");
        for (size_t i = 0; i < rn; ++i) {
            if (r_name_dict.get(r_name_codes[i]) == "EUROPE") {
                europe_rkey = r_regionkey[i];
                break;
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 2: Find European nations (nationkey -> n_name)
    // ---------------------------------------------------------------
    struct NationInfo { int32_t nkey; std::string name; };
    std::vector<NationInfo> eu_nations;
    // Also build a nationkey -> name lookup (dense, max 25)
    std::vector<std::string> nkey_to_name(26);
    {
        MQO_TIME_PHASE("Q2_nation_filter");
        std::string ndir = ctx.gendb_dir + "/nation";
        size_t nn = mqo::io::read_row_count(ndir);
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(ndir + "/n_nationkey.bin", nn);
        const int32_t* n_regionkey = mqo::io::mmap_column<int32_t>(ndir + "/n_regionkey.bin", nn);
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "/n_name.bin", nn);
        mqo::io::Dictionary n_name_dict;
        n_name_dict.load(ndir + "/n_name_dict.bin");
        for (size_t i = 0; i < nn; ++i) {
            if (n_regionkey[i] == europe_rkey) {
                int32_t nk = n_nationkey[i];
                std::string nm = n_name_dict.get(n_name_codes[i]);
                eu_nations.push_back({nk, nm});
                if (nk >= 0 && nk < (int32_t)nkey_to_name.size())
                    nkey_to_name[nk] = nm;
            }
        }
    }

    // Build a fast lookup set for European nationkeys
    std::unordered_set<int32_t> eu_nkey_set;
    for (auto& n : eu_nations) eu_nkey_set.insert(n.nkey);

    // ---------------------------------------------------------------
    // Step 3: Build set of European suppliers
    // ---------------------------------------------------------------
    std::unordered_set<int32_t> eu_supp_set;
    {
        MQO_TIME_PHASE("Q2_build_eu_suppliers");
        eu_supp_set.reserve(25000);
        for (size_t i = 0; i < supp.n_rows; ++i) {
            if (eu_nkey_set.count(supp.s_nationkey[i])) {
                eu_supp_set.insert(supp.s_suppkey[i]);
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 4: Filter part: p_size = 15 AND p_type LIKE '%BRASS'
    // ---------------------------------------------------------------
    // Resolve matching type codes from part dictionary
    mqo::io::Dictionary p_type_dict;
    p_type_dict.load(ctx.gendb_dir + "/part/p_type_dict.bin");
    std::vector<bool> brass_type_codes(256, false);
    for (uint32_t c = 0; c < p_type_dict.count; ++c) {
        const auto& s = p_type_dict.entries[c];
        if (s.size() >= 5 && s.compare(s.size() - 5, 5, "BRASS") == 0) {
            brass_type_codes[c] = true;
        }
    }

    std::vector<int32_t> qual_partkeys;
    {
        MQO_TIME_PHASE("Q2_part_filter");
        qual_partkeys.reserve(2000);
        for (size_t i = 0; i < part.n_rows; ++i) {
            if (part.p_size[i] == 15 && brass_type_codes[part.p_type[i]]) {
                qual_partkeys.push_back(part.p_partkey[i]);
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 5: For each qualifying partkey, index-lookup partsupp rows,
    //         filter by European supplier, find MIN(ps_supplycost)
    // ---------------------------------------------------------------
    struct ResultTuple {
        int32_t ps_partkey;
        int32_t ps_suppkey;
        double  ps_supplycost;
    };
    std::vector<ResultTuple> results;
    {
        MQO_TIME_PHASE("Q2_subquery_resolve");
        // Load partsupp_partkey_idx: dense_range, max_key=2000000, entry = (start_u32, count_u32)
        struct IdxEntry { uint32_t start; uint32_t count; };
        std::string idx_path = ctx.gendb_dir + "/indexes/partsupp_partkey_idx.bin";
        size_t idx_max = 2000001; // max_key + 1
        const IdxEntry* ps_idx = reinterpret_cast<const IdxEntry*>(
            mqo::io::mmap_column<uint64_t>(idx_path, idx_max));

        results.reserve(qual_partkeys.size());
        for (int32_t pk : qual_partkeys) {
            if (pk < 0 || pk >= (int32_t)idx_max) continue;
            const auto& entry = ps_idx[pk];
            uint32_t start = entry.start;
            uint32_t cnt   = entry.count;

            double min_cost = 1e30;
            int32_t best_suppkey = -1;
            for (uint32_t j = 0; j < cnt; ++j) {
                size_t row = start + j;
                int32_t sk = ps.ps_suppkey[row];
                if (eu_supp_set.count(sk)) {
                    double cost = ps.ps_supplycost[row];
                    if (cost < min_cost) {
                        min_cost = cost;
                        best_suppkey = sk;
                    }
                }
            }
            if (best_suppkey >= 0) {
                results.push_back({pk, best_suppkey, min_cost});
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 6 & 7: Materialize output attributes + sort + limit 100
    // ---------------------------------------------------------------
    struct OutputRow {
        double      s_acctbal;
        std::string s_name;
        std::string n_name;
        int32_t     p_partkey;
        std::string p_mfgr;
        std::string s_address;
        std::string s_phone;
        std::string s_comment;
    };

    mqo::io::Dictionary p_mfgr_dict;
    p_mfgr_dict.load(ctx.gendb_dir + "/part/p_mfgr_dict.bin");

    std::vector<OutputRow> out_rows;
    {
        MQO_TIME_PHASE("Q2_materialize");
        out_rows.reserve(results.size());
        for (auto& r : results) {
            OutputRow o;
            o.p_partkey = r.ps_partkey;

            // Part lookup
            int32_t p_row = part.pk_index[r.ps_partkey];
            o.p_mfgr = p_mfgr_dict.get(part.p_mfgr[p_row]);

            // Supplier lookup
            int32_t s_row = supp.pk_index[r.ps_suppkey];
            o.s_acctbal = supp.s_acctbal[s_row];
            o.s_name    = std::string(supp.s_name.get(s_row));
            o.s_address = std::string(supp.s_address.get(s_row));
            o.s_phone   = std::string(supp.s_phone.get(s_row));
            o.s_comment = std::string(supp.s_comment.get(s_row));

            // Nation name lookup
            int32_t nk = supp.s_nationkey[s_row];
            o.n_name = (nk >= 0 && nk < (int32_t)nkey_to_name.size()) ? nkey_to_name[nk] : "";

            out_rows.push_back(std::move(o));
        }
    }

    // Sort: s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC
    {
        MQO_TIME_PHASE("Q2_sort");
        std::sort(out_rows.begin(), out_rows.end(), [](const OutputRow& a, const OutputRow& b) {
            if (a.s_acctbal != b.s_acctbal) return a.s_acctbal > b.s_acctbal;
            if (a.n_name != b.n_name) return a.n_name < b.n_name;
            if (a.s_name != b.s_name) return a.s_name < b.s_name;
            return a.p_partkey < b.p_partkey;
        });
    }

    size_t limit = std::min<size_t>(100, out_rows.size());

    // ---------------------------------------------------------------
    // Step 8: Write CSV output
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q2_output");
        std::string outpath = ctx.output_dir + "/q2.csv";
        FILE* f = std::fopen(outpath.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(f, "s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment\n");
        for (size_t i = 0; i < limit; ++i) {
            const auto& o = out_rows[i];
            std::fprintf(f, "%.2f,%s,%s,%d,%s,%s,%s,%s\n",
                         o.s_acctbal,
                         o.s_name.c_str(),
                         o.n_name.c_str(),
                         o.p_partkey,
                         o.p_mfgr.c_str(),
                         o.s_address.c_str(),
                         o.s_phone.c_str(),
                         o.s_comment.c_str());
        }
        std::fclose(f);
    }
}

}} // namespace mqo::tails
