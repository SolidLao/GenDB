// Q21 tail: Suppliers Who Kept Orders Waiting
// Shared inputs: scan_orders_full, hash_supplier_by_suppkey
// Private: nation scan, lineitem sequential scan with orderkey-group processing

#include "mqo_profile.hpp"
#include "../shared/scan_orders_full.hpp"
#include "../shared/hash_supplier_by_suppkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <omp.h>

namespace mqo { namespace tails {

void run_Q21(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q21_tail");

    // ================================================================
    // Step 1: Scan nation, find SAUDI ARABIA nationkey
    // ================================================================
    int32_t sa_nationkey = -1;
    {
        MQO_TIME_PHASE("Q21_nation_filter");
        std::string ndir = ctx.gendb_dir + "/nation";
        size_t n_nation = mqo::io::read_row_count(ndir);

        // n_name is dict-encoded: n_name.bin (uint8 codes), n_name_dict.bin
        mqo::io::Dictionary n_name_dict;
        n_name_dict.load(ndir + "/n_name_dict.bin");
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "/n_name.bin", n_nation);
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(ndir + "/n_nationkey.bin", n_nation);

        // Find the dict code for SAUDI ARABIA
        uint8_t sa_code = 255;
        for (uint32_t i = 0; i < n_name_dict.count; ++i) {
            if (n_name_dict.entries[i] == "SAUDI ARABIA") {
                sa_code = static_cast<uint8_t>(i);
                break;
            }
        }
        for (size_t i = 0; i < n_nation; ++i) {
            if (n_name_codes[i] == sa_code) {
                sa_nationkey = n_nationkey[i];
                break;
            }
        }
    }

    // ================================================================
    // Step 2: Build SA supplier map from shared supplier hash
    // ================================================================
    // Map: suppkey -> index into sa_names vector
    // sa_names stores the actual s_name strings
    const auto& sup = mqo::shared::hash_supplier_by_suppkey::get();

    std::vector<std::string> sa_names;
    // sa_suppkey_to_idx: for suppkeys 0..100000, stores index into sa_names or -1
    std::vector<int32_t> sa_suppkey_to_idx(static_cast<size_t>(sup.max_key) + 1, -1);
    {
        MQO_TIME_PHASE("Q21_build_sa_supplier");
        sa_names.reserve(4096);
        for (size_t i = 0; i < sup.n_rows; ++i) {
            if (sup.s_nationkey[i] == sa_nationkey) {
                int32_t sk = sup.s_suppkey[i];
                int32_t idx = static_cast<int32_t>(sa_names.size());
                sa_names.emplace_back(sup.s_name.get(i));
                sa_suppkey_to_idx[sk] = idx;
            }
        }
    }
    const size_t num_sa_suppliers = sa_names.size();

    // ================================================================
    // Step 3: Build F-orders bitset from shared orders scan
    // ================================================================
    const auto& ord = mqo::shared::scan_orders_full::get();
    static constexpr size_t MAX_ORDERKEY = 60000001;
    // Use a vector<bool> — compact bitset, ~7.5MB
    std::vector<bool> f_orders(MAX_ORDERKEY, false);
    {
        MQO_TIME_PHASE("Q21_filter_orders");
        const int8_t status_F = static_cast<int8_t>('F');
        for (size_t i = 0; i < ord.n_rows; ++i) {
            if (ord.o_orderstatus[i] == status_F) {
                int32_t ok = ord.o_orderkey[i];
                if (ok >= 0 && static_cast<size_t>(ok) < MAX_ORDERKEY)
                    f_orders[ok] = true;
            }
        }
    }

    // ================================================================
    // Step 4: Sequential scan over lineitem with orderkey-group processing
    // ================================================================
    // mmap lineitem columns directly
    std::string ldir = ctx.gendb_dir + "/lineitem";
    size_t n_li = mqo::io::read_row_count(ldir);
    const int32_t* l_orderkey   = mqo::io::mmap_column<int32_t>(ldir + "/l_orderkey.bin", n_li);
    const int32_t* l_suppkey    = mqo::io::mmap_column<int32_t>(ldir + "/l_suppkey.bin", n_li);
    const int32_t* l_receiptdate = mqo::io::mmap_column<int32_t>(ldir + "/l_receiptdate.bin", n_li);
    const int32_t* l_commitdate = mqo::io::mmap_column<int32_t>(ldir + "/l_commitdate.bin", n_li);

    // Load orderkey index for partitioning
    std::string idx_dir = ctx.gendb_dir + "/indexes";
    struct OkIdx { uint32_t start; uint32_t count; };
    static constexpr size_t MAX_OK = 60000001;
    const OkIdx* ok_idx = reinterpret_cast<const OkIdx*>(
        mqo::io::mmap_column<uint32_t>(idx_dir + "/lineitem_orderkey_idx.bin", MAX_OK * 2));

    // Determine thread count — bounded since we may run alongside other tails
    int num_threads = std::min(omp_get_max_threads(), 16);

    // Thread-local aggregation: each thread has a vector of counts indexed by sa_supplier index
    std::vector<std::vector<int64_t>> tl_counts(num_threads);
    for (auto& v : tl_counts) v.assign(num_sa_suppliers, 0);

    {
        MQO_TIME_PHASE("Q21_lineitem_scan");

        // Partition orderkey range across threads
        // Orderkeys range from 1 to 60000000
        #pragma omp parallel num_threads(num_threads)
        {
            int tid = omp_get_thread_num();
            int nt = omp_get_num_threads();
            auto& my_counts = tl_counts[tid];

            // Each thread handles a range of orderkeys
            int64_t ok_range = 60000000;
            int64_t ok_start = (ok_range * tid / nt) + 1;
            int64_t ok_end = (ok_range * (tid + 1) / nt) + 1; // exclusive

            // Small buffer for group processing
            struct LineInfo { int32_t suppkey; bool is_late; bool is_sa; };
            std::vector<LineInfo> group_buf;
            group_buf.reserve(8);

            for (int64_t okey = ok_start; okey < ok_end; ++okey) {
                // Quick check: is this an F-order?
                if (!f_orders[okey]) continue;

                // Check if this orderkey has lineitem rows
                uint32_t row_start = ok_idx[okey].start;
                uint32_t row_count = ok_idx[okey].count;
                if (row_count == 0) continue;

                // Collect group info
                group_buf.clear();
                bool has_sa_late = false;
                int total_suppliers = 0; // count of distinct suppkeys doesn't matter; we just need >1 row with different suppkey
                int late_count = 0;

                for (uint32_t r = row_start; r < row_start + row_count; ++r) {
                    int32_t sk = l_suppkey[r];
                    bool is_late = l_receiptdate[r] > l_commitdate[r];
                    bool is_sa = (sk <= sup.max_key && sa_suppkey_to_idx[sk] >= 0);
                    group_buf.push_back({sk, is_late, is_sa});
                    if (is_sa && is_late) has_sa_late = true;
                    if (is_late) late_count++;
                }

                // Quick skip: no SA late suppliers in this group
                if (!has_sa_late) continue;

                // For each l1 candidate (SA supplier with late receipt):
                for (size_t i = 0; i < group_buf.size(); ++i) {
                    const auto& l1 = group_buf[i];
                    if (!l1.is_sa || !l1.is_late) continue;

                    // EXISTS: at least one row with different suppkey
                    bool exists = false;
                    for (size_t j = 0; j < group_buf.size(); ++j) {
                        if (group_buf[j].suppkey != l1.suppkey) {
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) continue;

                    // NOT EXISTS: no other late row with different suppkey
                    bool not_exists = true;
                    for (size_t j = 0; j < group_buf.size(); ++j) {
                        if (group_buf[j].suppkey != l1.suppkey && group_buf[j].is_late) {
                            not_exists = false;
                            break;
                        }
                    }
                    if (!not_exists) continue;

                    // This l1 row qualifies — increment count for this supplier
                    my_counts[sa_suppkey_to_idx[l1.suppkey]]++;
                }
            }
        }
    }

    // ================================================================
    // Step 5: Merge thread-local counts
    // ================================================================
    std::vector<int64_t> final_counts(num_sa_suppliers, 0);
    {
        MQO_TIME_PHASE("Q21_merge_agg");
        for (int t = 0; t < num_threads; ++t) {
            for (size_t i = 0; i < num_sa_suppliers; ++i) {
                final_counts[i] += tl_counts[t][i];
            }
        }
    }

    // ================================================================
    // Step 6: Sort by numwait DESC, s_name ASC; LIMIT 100
    // ================================================================
    struct Result { std::string_view name; int64_t numwait; };
    std::vector<Result> results;
    {
        MQO_TIME_PHASE("Q21_sort");
        results.reserve(num_sa_suppliers);
        for (size_t i = 0; i < num_sa_suppliers; ++i) {
            if (final_counts[i] > 0) {
                results.push_back({sa_names[i], final_counts[i]});
            }
        }
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            if (a.numwait != b.numwait) return a.numwait > b.numwait;
            return a.name < b.name;
        });
        if (results.size() > 100) results.resize(100);
    }

    // ================================================================
    // Output
    // ================================================================
    {
        MQO_TIME_PHASE("Q21_output");
        std::string outpath = ctx.output_dir + "/q21.csv";
        FILE* f = std::fopen(outpath.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(f, "s_name,numwait\n");
        for (const auto& r : results) {
            std::fprintf(f, "%.*s,%ld\n",
                         static_cast<int>(r.name.size()), r.name.data(),
                         r.numwait);
        }
        std::fclose(f);
    }
}

}} // namespace mqo::tails
