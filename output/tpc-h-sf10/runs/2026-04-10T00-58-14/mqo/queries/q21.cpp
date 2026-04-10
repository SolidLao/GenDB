// Q21 tail: Suppliers Who Kept Orders Waiting
// Consumes: scan_lineitem_full, hash_orders_by_orderkey, hash_supplier_by_suppkey

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/hash_orders_by_orderkey.hpp"
#include "shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <bitset>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace mqo::tails {

void run_Q21(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q21_tail");

    // ---------------------------------------------------------------
    // Step 0: Find SAUDI ARABIA nationkey from nation table
    // ---------------------------------------------------------------
    int32_t saudi_nationkey = -1;
    {
        MQO_TIME_PHASE("Q21_nation_filter");
        const std::string nb = ctx.gendb_dir + "/nation/";
        const size_t n_nations = mqo::io::read_row_count(nb + "meta.txt");
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(nb + "n_nationkey.bin", n_nations);
        // n_name is dict-encoded as uint8_t codes
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(nb + "n_name.bin", n_nations);
        auto n_name_dict = mqo::io::read_dictionary(nb + "n_name_dict.bin");

        for (size_t i = 0; i < n_nations; ++i) {
            if (n_name_dict[n_name_codes[i]] == "SAUDI ARABIA") {
                saudi_nationkey = n_nationkey[i];
                break;
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 1: Build Saudi supplier bitset from shared supplier hash
    // ---------------------------------------------------------------
    static constexpr int32_t MAX_SUPPKEY = 100000;
    // Use a vector<bool> as compact bitset
    std::vector<bool> saudi_supp(MAX_SUPPKEY + 1, false);
    {
        MQO_TIME_PHASE("Q21_build_supplier_bitset");
        const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();
        for (int32_t sk = 1; sk <= MAX_SUPPKEY; ++sk) {
            const auto* e = supp.probe(sk);
            if (e && e->s_nationkey == saudi_nationkey) {
                saudi_supp[sk] = true;
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 2: Stream over lineitem sorted by l_orderkey, group-process
    // ---------------------------------------------------------------
    // Thread-local aggregation maps: s_name -> count
    const int max_threads = std::min(16, omp_get_max_threads());

    struct NameCount {
        std::unordered_map<std::string_view, int64_t> map;
    };
    std::vector<NameCount> tl_maps(max_threads);

    {
        MQO_TIME_PHASE("Q21_main_scan");

        const auto& li = mqo::shared::scan_lineitem_full::get_columns(ctx);
        const auto& orders = mqo::shared::hash_orders_by_orderkey::get();
        const auto& supp = mqo::shared::hash_supplier_by_suppkey::get();

        const size_t n = li.n_rows;
        const int32_t* l_orderkey   = li.l_orderkey;
        const int32_t* l_suppkey    = li.l_suppkey;
        const int32_t* l_commitdate = li.l_commitdate;
        const int32_t* l_receiptdate = li.l_receiptdate;

        // Find group boundaries (orderkey changes) for morsel partitioning
        // Since lineitem is sorted by l_orderkey, we can partition by range
        // Each thread processes a contiguous range of rows ensuring complete groups

        #pragma omp parallel num_threads(max_threads)
        {
            const int tid = omp_get_thread_num();
            const int nthreads = omp_get_num_threads();
            auto& my_map = tl_maps[tid].map;
            my_map.reserve(4096);

            // Divide rows evenly, adjust to group boundaries
            size_t chunk = n / nthreads;
            size_t start = tid * chunk;
            size_t end = (tid == nthreads - 1) ? n : (tid + 1) * chunk;

            // Adjust start to group boundary (skip to next group if mid-group)
            if (tid > 0 && start < n) {
                int32_t prev_key = l_orderkey[start - 1];
                while (start < n && l_orderkey[start] == prev_key) ++start;
            }
            // Adjust end to group boundary
            if (tid < nthreads - 1 && end < n) {
                int32_t boundary_key = l_orderkey[end - 1];
                while (end < n && l_orderkey[end] == boundary_key) ++end;
            }

            // Small group buffer: (suppkey, is_late)
            struct GroupRow {
                int32_t suppkey;
                bool is_late;
            };
            std::vector<GroupRow> group_buf;
            group_buf.reserve(8);

            size_t i = start;
            while (i < end) {
                const int32_t ok = l_orderkey[i];

                // Collect all rows in this orderkey group
                group_buf.clear();
                size_t j = i;
                while (j < end && l_orderkey[j] == ok) {
                    group_buf.push_back({l_suppkey[j], l_receiptdate[j] > l_commitdate[j]});
                    ++j;
                }
                i = j;

                // Step 2b: Probe orders — skip if not status 'F'
                const auto* oe = orders.probe(ok);
                if (!oe || oe->o_orderstatus != 'F') continue;

                // Step 2c: For each late row from a Saudi supplier, check EXISTS/NOT EXISTS
                const size_t gsz = group_buf.size();
                for (size_t g1 = 0; g1 < gsz; ++g1) {
                    if (!group_buf[g1].is_late) continue;
                    const int32_t sk1 = group_buf[g1].suppkey;
                    if (!saudi_supp[sk1]) continue;

                    // EXISTS: another row with different suppkey
                    bool exists_diff = false;
                    for (size_t g2 = 0; g2 < gsz; ++g2) {
                        if (group_buf[g2].suppkey != sk1) {
                            exists_diff = true;
                            break;
                        }
                    }
                    if (!exists_diff) continue;

                    // NOT EXISTS: no other late row with different suppkey
                    bool exists_late_diff = false;
                    for (size_t g2 = 0; g2 < gsz; ++g2) {
                        if (group_buf[g2].suppkey != sk1 && group_buf[g2].is_late) {
                            exists_late_diff = true;
                            break;
                        }
                    }
                    if (exists_late_diff) continue;

                    // Step 2d: Resolve supplier name
                    const auto* se = supp.probe(sk1);
                    if (!se) continue;
                    std::string_view sname = supp.get_name(se->row_id);
                    my_map[sname]++;
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 3: Merge thread-local maps
    // ---------------------------------------------------------------
    std::unordered_map<std::string_view, int64_t> merged;
    {
        MQO_TIME_PHASE("Q21_aggregation");
        merged.reserve(4096);
        for (auto& tm : tl_maps) {
            for (auto& [name, cnt] : tm.map) {
                merged[name] += cnt;
            }
        }
    }

    // ---------------------------------------------------------------
    // Step 4: Sort by numwait DESC, s_name ASC, limit 100
    // ---------------------------------------------------------------
    struct Result {
        std::string_view s_name;
        int64_t numwait;
    };
    std::vector<Result> results;
    {
        MQO_TIME_PHASE("Q21_sort");
        results.reserve(merged.size());
        for (auto& [name, cnt] : merged) {
            results.push_back({name, cnt});
        }
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            if (a.numwait != b.numwait) return a.numwait > b.numwait;
            return a.s_name < b.s_name;
        });
        if (results.size() > 100) results.resize(100);
    }

    // ---------------------------------------------------------------
    // Step 5: Output
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q21_output");
        std::string path = ctx.output_dir + "/q21.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "[Q21] Cannot open output: %s\n", path.c_str());
            return;
        }
        std::fprintf(f, "s_name,numwait\n");
        for (const auto& r : results) {
            std::fprintf(f, "%.*s,%ld\n",
                         static_cast<int>(r.s_name.size()), r.s_name.data(),
                         r.numwait);
        }
        std::fclose(f);
    }
}

}  // namespace mqo::tails
