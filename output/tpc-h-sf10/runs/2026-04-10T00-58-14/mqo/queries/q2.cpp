// Q2 tail: Minimum Cost Supplier
// Shared input: hash_supplier_by_suppkey
// Own work: region/nation filter, part scan+filter, partsupp index lookup,
//           join with European suppliers, MIN aggregation, sort+limit

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mqo::tails {

void run_Q2(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q2_tail");

    const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();
    const std::string sd = ctx.gendb_dir + "/";

    // -----------------------------------------------------------------------
    // Step 1: Region scan — find EUROPE regionkey
    // -----------------------------------------------------------------------
    int32_t europe_rk = -1;
    {
        MQO_TIME_PHASE("Q2_region_scan");
        const size_t rn = mqo::io::read_row_count(sd + "region/meta.txt");
        const int32_t* r_rk = mqo::io::mmap_column<int32_t>(sd + "region/r_regionkey.bin", rn);
        // r_name is dict-encoded uint8
        const uint8_t* r_name_codes = mqo::io::mmap_column<uint8_t>(sd + "region/r_name.bin", rn);
        auto r_dict = mqo::io::read_dictionary(sd + "region/r_name_dict.bin");

        // Find the dict code for EUROPE
        uint8_t europe_code = 255;
        for (uint8_t i = 0; i < (uint8_t)r_dict.size(); ++i) {
            if (r_dict[i] == "EUROPE") { europe_code = i; break; }
        }
        for (size_t i = 0; i < rn; ++i) {
            if (r_name_codes[i] == europe_code) { europe_rk = r_rk[i]; break; }
        }
    }

    // -----------------------------------------------------------------------
    // Step 2: Nation scan — find European nations
    // -----------------------------------------------------------------------
    // Direct array: nationkey → n_name string (for the 25 nations)
    std::string nation_names[25];
    bool is_european[25] = {};
    {
        MQO_TIME_PHASE("Q2_nation_scan");
        const size_t nn = mqo::io::read_row_count(sd + "nation/meta.txt");
        const int32_t* n_nk = mqo::io::mmap_column<int32_t>(sd + "nation/n_nationkey.bin", nn);
        const int32_t* n_rk = mqo::io::mmap_column<int32_t>(sd + "nation/n_regionkey.bin", nn);
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(sd + "nation/n_name.bin", nn);
        auto n_dict = mqo::io::read_dictionary(sd + "nation/n_name_dict.bin");

        for (size_t i = 0; i < nn; ++i) {
            int32_t nk = n_nk[i];
            nation_names[nk] = n_dict[n_name_codes[i]];
            if (n_rk[i] == europe_rk) {
                is_european[nk] = true;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 3: Filter shared supplier hash → European suppliers
    // Build suppkey-keyed map with all payload columns
    // -----------------------------------------------------------------------
    struct SupplierInfo {
        int32_t s_suppkey;
        int32_t s_nationkey;
        int32_t row_id;
        double  s_acctbal;
    };

    // Use a direct array indexed by suppkey (100K entries, ~2MB)
    static constexpr int32_t MAX_SUPPKEY = 100000;
    auto eu_supp = std::make_unique<uint8_t[]>(MAX_SUPPKEY + 1);  // boolean: is European?
    std::memset(eu_supp.get(), 0, MAX_SUPPKEY + 1);

    {
        MQO_TIME_PHASE("Q2_filter_european_suppliers");
        for (int32_t k = 1; k <= MAX_SUPPKEY; ++k) {
            const auto* e = supp.probe(k);
            if (e && is_european[e->s_nationkey]) {
                eu_supp[k] = 1;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 4: Part scan — filter p_size=15 AND p_type LIKE '%BRASS'
    // -----------------------------------------------------------------------
    struct PartInfo {
        int32_t p_partkey;
        uint8_t p_mfgr_code;
    };
    std::vector<PartInfo> qualifying_parts;

    {
        MQO_TIME_PHASE("Q2_part_scan");
        const size_t pn = mqo::io::read_row_count(sd + "part/meta.txt");
        const int32_t* p_pk = mqo::io::mmap_column<int32_t>(sd + "part/p_partkey.bin", pn);
        const int32_t* p_sz = mqo::io::mmap_column<int32_t>(sd + "part/p_size.bin", pn);
        const uint8_t* p_type_codes = mqo::io::mmap_column<uint8_t>(sd + "part/p_type.bin", pn);
        const uint8_t* p_mfgr_codes = mqo::io::mmap_column<uint8_t>(sd + "part/p_mfgr.bin", pn);

        // Read p_type dictionary and find codes ending with "BRASS"
        auto p_type_dict = mqo::io::read_dictionary(sd + "part/p_type_dict.bin");
        std::vector<bool> brass_type(p_type_dict.size(), false);
        for (size_t i = 0; i < p_type_dict.size(); ++i) {
            const auto& s = p_type_dict[i];
            if (s.size() >= 5 && s.compare(s.size() - 5, 5, "BRASS") == 0) {
                brass_type[i] = true;
            }
        }

        qualifying_parts.reserve(2000);
        for (size_t i = 0; i < pn; ++i) {
            if (p_sz[i] == 15 && brass_type[p_type_codes[i]]) {
                qualifying_parts.push_back({p_pk[i], p_mfgr_codes[i]});
            }
        }
    }

    // Read p_mfgr dictionary for later output
    auto p_mfgr_dict = mqo::io::read_dictionary(sd + "part/p_mfgr_dict.bin");

    // -----------------------------------------------------------------------
    // Step 5+6+7: Index lookup into partsupp, join with European suppliers,
    //             compute MIN(ps_supplycost) per partkey
    // -----------------------------------------------------------------------
    struct JoinedRow {
        int32_t p_partkey;
        uint8_t p_mfgr_code;
        int32_t ps_suppkey;
        double  ps_supplycost;
    };
    std::vector<JoinedRow> joined_rows;

    // min_cost per partkey — use unordered_map since only ~1333 keys
    std::unordered_map<int32_t, double> min_cost;

    {
        MQO_TIME_PHASE("Q2_partsupp_join");

        // mmap partsupp columns
        const size_t psn = mqo::io::read_row_count(sd + "partsupp/meta.txt");
        const int32_t* ps_pk = mqo::io::mmap_column<int32_t>(sd + "partsupp/ps_partkey.bin", psn);
        const int32_t* ps_sk = mqo::io::mmap_column<int32_t>(sd + "partsupp/ps_suppkey.bin", psn);
        const double*  ps_sc = mqo::io::mmap_column<double>(sd + "partsupp/ps_supplycost.bin", psn);

        // mmap partsupp_partkey_idx: array of {uint32_t start, uint32_t count}
        // indexed by partkey, max_key = 2000000
        struct IdxEntry { uint32_t start; uint32_t count; };
        size_t idx_sz = 0;
        const IdxEntry* ps_idx = static_cast<const IdxEntry*>(
            mqo::io::mmap_file(sd + "indexes/partsupp_partkey_idx.bin", idx_sz));

        joined_rows.reserve(6000);
        min_cost.reserve(2048);

        for (const auto& part : qualifying_parts) {
            const IdxEntry& ie = ps_idx[part.p_partkey];
            uint32_t row_start = ie.start;
            uint32_t row_count = ie.count;

            for (uint32_t r = row_start; r < row_start + row_count; ++r) {
                int32_t suppkey = ps_sk[r];
                // Check if this supplier is European
                if (suppkey >= 1 && suppkey <= MAX_SUPPKEY && eu_supp[suppkey]) {
                    double cost = ps_sc[r];
                    joined_rows.push_back({part.p_partkey, part.p_mfgr_code, suppkey, cost});

                    auto it = min_cost.find(part.p_partkey);
                    if (it == min_cost.end()) {
                        min_cost.emplace(part.p_partkey, cost);
                    } else if (cost < it->second) {
                        it->second = cost;
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 8: Filter by min cost
    // -----------------------------------------------------------------------
    struct ResultRow {
        double      s_acctbal;
        int32_t     s_suppkey;
        int32_t     s_nationkey;
        int32_t     s_row_id;
        int32_t     p_partkey;
        uint8_t     p_mfgr_code;
    };
    std::vector<ResultRow> results;
    {
        MQO_TIME_PHASE("Q2_filter_min");
        results.reserve(2000);
        for (const auto& jr : joined_rows) {
            if (jr.ps_supplycost == min_cost[jr.p_partkey]) {
                const auto* se = supp.probe(jr.ps_suppkey);
                results.push_back({
                    se->s_acctbal,
                    se->s_suppkey,
                    se->s_nationkey,
                    se->row_id,
                    jr.p_partkey,
                    jr.p_mfgr_code
                });
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 10: Sort by s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC
    //          then LIMIT 100
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q2_sort");
        std::sort(results.begin(), results.end(),
            [&](const ResultRow& a, const ResultRow& b) {
                if (a.s_acctbal != b.s_acctbal) return a.s_acctbal > b.s_acctbal;
                int ncmp = nation_names[a.s_nationkey].compare(nation_names[b.s_nationkey]);
                if (ncmp != 0) return ncmp < 0;
                int scmp = supp.get_name(a.s_row_id).compare(supp.get_name(b.s_row_id));
                if (scmp != 0) return scmp < 0;
                return a.p_partkey < b.p_partkey;
            });
        if (results.size() > 100) results.resize(100);
    }

    // -----------------------------------------------------------------------
    // Step 11: Output CSV
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q2_output");
        std::string out_path = ctx.output_dir + "/q2.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[Q2] Cannot open output: %s\n", out_path.c_str());
            return;
        }

        std::fprintf(fp, "s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment\n");

        for (const auto& r : results) {
            auto s_name    = supp.get_name(r.s_row_id);
            auto s_address = supp.get_address(r.s_row_id);
            auto s_phone   = supp.get_phone(r.s_row_id);
            auto s_comment = supp.get_comment(r.s_row_id);
            const auto& n_name = nation_names[r.s_nationkey];
            const auto& p_mfgr = p_mfgr_dict[r.p_mfgr_code];

            std::fprintf(fp, "%.2f,%.*s,%s,%d,%s,%.*s,%.*s,%.*s\n",
                r.s_acctbal,
                (int)s_name.size(), s_name.data(),
                n_name.c_str(),
                r.p_partkey,
                p_mfgr.c_str(),
                (int)s_address.size(), s_address.data(),
                (int)s_phone.size(), s_phone.data(),
                (int)s_comment.size(), s_comment.data());
        }

        std::fclose(fp);
    }
}

}  // namespace mqo::tails
