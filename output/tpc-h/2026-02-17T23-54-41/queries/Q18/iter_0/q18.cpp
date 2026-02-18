/*
 * Q18: Large Volume Customer — Iteration 0
 *
 * SQL:
 *   SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS sum_qty
 *   FROM customer, orders, lineitem
 *   WHERE o_orderkey IN (
 *       SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
 *   )
 *   AND c_custkey = o_custkey AND o_orderkey = l_orderkey
 *   GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
 *   ORDER BY o_totalprice DESC, o_orderdate ASC
 *   LIMIT 100;
 *
 * ── Logical Plan ────────────────────────────────────────────────────────────
 *  Step 1 (subquery decorrelation):
 *    Scan lineitem (59.9M rows), accumulate SUM(l_quantity) per l_orderkey.
 *    HAVING SUM > 300 (scaled: > 30000 since l_quantity has scale_factor=100).
 *    Result: CompactHashSet<int32_t> qualified_orderkeys   (~600 keys)
 *
 *  Step 2 (filter orders):
 *    Scan orders (15M rows), probe qualified_orderkeys semi-join.
 *    Result: compact array of ~600 OrderInfo rows.
 *    Build CompactHashMap<int32_t, OrderInfo> orders_map  (orderkey → info).
 *
 *  Step 3 (customer lookup):
 *    c_custkey is sorted 1..1.5M — load as direct array (custkey → row_idx).
 *    Load c_name dictionary for late materialization.
 *
 *  Step 4 (main lineitem scan + aggregation):
 *    Scan lineitem again (59.9M rows), probe orders_map.
 *    Aggregate SUM(l_quantity) per (c_custkey, o_orderkey, o_orderdate, o_totalprice).
 *    ~600 groups → flat array indexed by position in orders_map fits in cache.
 *
 *  Step 5 (top-K):
 *    TopKHeap<ResultRow>(100) — ORDER BY o_totalprice DESC, o_orderdate ASC.
 *
 * ── Physical Plan ────────────────────────────────────────────────────────────
 *  - Phase subquery_precompute:
 *      Parallel lineitem scan (64 threads, morsel=200K).
 *      Thread-local CompactHashMap<int32_t, int64_t> → merge → filter HAVING.
 *      CompactHashSet<int32_t> qualified_orderkeys.
 *
 *  - Phase dim_filter (orders):
 *      Sequential scan of orders (15M) probing CompactHashSet.
 *      Build CompactHashMap<int32_t, OrderInfo> orders_map (~600 entries).
 *
 *  - Phase build_joins (customer):
 *      Load c_custkey → row_idx as direct array (custkey is dense 1..1.5M).
 *      Load name_dict.txt lines into vector<string> indexed by dict code.
 *
 *  - Phase main_scan (second lineitem pass):
 *      Parallel lineitem scan (64 threads, morsel=200K).
 *      Thread-local agg: unordered_map<int32_t,int64_t> (orderkey → sum_qty).
 *      Merge thread-local → global agg.
 *
 *  - Phase sort_topk:
 *      TopKHeap(100) over ~600 rows.
 *
 *  - Phase output: Write Q18.csv.
 *
 * ── Key Design Choices ───────────────────────────────────────────────────────
 *  1. l_quantity scale_factor=100 → threshold = 300*100 = 30000.
 *  2. c_name is dictionary-encoded: load name_dict.txt, index by c_name[i] code.
 *  3. Customer custkey is dense; direct array avoids hash lookup overhead.
 *  4. orders_map has ~600 entries — tiny, fits L1 cache for fast probe.
 *  5. Two lineitem passes instead of one (to avoid huge intermediate materialization).
 *  6. CompactHashMap (Robin Hood) instead of std::unordered_map for both passes.
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <thread>
#include <atomic>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ─── Result row ────────────────────────────────────────────────────────────────
struct ResultRow {
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;    // epoch days
    int64_t o_totalprice;   // scaled ×100
    int64_t sum_qty;        // scaled ×100
    int32_t name_code;      // dict code for c_name
};

// ─── Order info (compact, cache-friendly) ──────────────────────────────────────
struct OrderInfo {
    int32_t custkey;
    int32_t orderdate;
    int64_t totalprice;
};

// ─── Aggregation key (without string) ─────────────────────────────────────────
// The GROUP BY is (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice).
// Since c_name is functionally determined by c_custkey, we key on o_orderkey
// (which uniquely determines the order, and thus custkey/date/price too).
// So aggregation key = o_orderkey (int32_t) → SUM(l_quantity).

// ─── Thread-local aggregation using compact hash map ───────────────────────────
// Each thread accumulates orderkey → sum_qty locally.

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int64_t LINEITEM_ROWS = 59986052;
    const int64_t ORDERS_ROWS   = 15000000;
    const int64_t CUSTOMER_ROWS = 1500000;
    const int64_t QTY_THRESHOLD = 30000;  // 300 * scale_factor(100)
    const size_t  MORSEL        = 200000;

    const unsigned NTHREADS = std::min((unsigned)64u,
                                       std::thread::hardware_concurrency());

    // ─── Phase 1: Subquery — parallel lineitem scan, accumulate qty per orderkey ──
    {
        GENDB_PHASE("subquery_precompute");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_quantity (gendb_dir + "/lineitem/l_quantity.bin");

        // Thread-local maps: orderkey → sum_qty
        // ~15M distinct orderkeys / 64 threads ≈ 235K per thread
        std::vector<gendb::CompactHashMap<int32_t,int64_t>> local_maps(NTHREADS);
        for (auto& m : local_maps) m.reserve(300000);

        std::atomic<int64_t> cursor{0};

        auto worker = [&](unsigned tid) {
            auto& lm = local_maps[tid];
            while (true) {
                int64_t base = cursor.fetch_add((int64_t)MORSEL,
                                               std::memory_order_relaxed);
                if (base >= LINEITEM_ROWS) break;
                int64_t end = std::min(base + (int64_t)MORSEL, LINEITEM_ROWS);
                for (int64_t i = base; i < end; ++i) {
                    int32_t ok = l_orderkey[i];
                    int64_t* p = lm.find(ok);
                    if (p) {
                        *p += l_quantity[i];
                    } else {
                        lm.insert(ok, l_quantity[i]);
                    }
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (unsigned t = 0; t < NTHREADS; ++t)
            threads.emplace_back(worker, t);
        for (auto& th : threads) th.join();

        // Merge and build qualified set
        // Expect ~600 qualified orderkeys
        gendb::CompactHashMap<int32_t,int64_t> global_qty(300000);
        for (unsigned t = 0; t < NTHREADS; ++t) {
            for (auto [k, v] : local_maps[t]) {
                int64_t* gp = global_qty.find(k);
                if (gp) *gp += v;
                else     global_qty.insert(k, v);
            }
            local_maps[t].table.clear(); // release memory
        }

        // Build qualified set (HAVING SUM > 300 scaled)
        gendb::CompactHashSet<int32_t> qualified(2048);
        for (auto [k, v] : global_qty) {
            if (v > QTY_THRESHOLD) qualified.insert(k);
        }

        // ─── Phase 2: Filter orders ──────────────────────────────────────────────
        {
            GENDB_PHASE("dim_filter");

            gendb::MmapColumn<int32_t> o_orderkey  (gendb_dir + "/orders/o_orderkey.bin");
            gendb::MmapColumn<int32_t> o_custkey   (gendb_dir + "/orders/o_custkey.bin");
            gendb::MmapColumn<int32_t> o_orderdate (gendb_dir + "/orders/o_orderdate.bin");
            gendb::MmapColumn<int64_t> o_totalprice(gendb_dir + "/orders/o_totalprice.bin");

            // Build orders_map: orderkey → OrderInfo (~600 entries)
            gendb::CompactHashMap<int32_t,OrderInfo> orders_map(2048);

            for (int64_t i = 0; i < ORDERS_ROWS; ++i) {
                int32_t ok = o_orderkey[i];
                if (qualified.contains(ok)) {
                    orders_map.insert(ok, {o_custkey[i], o_orderdate[i], o_totalprice[i]});
                }
            }

            // ─── Phase 3: Build customer lookup ─────────────────────────────────
            {
                GENDB_PHASE("build_joins");

                // Load c_custkey → dict code for c_name.
                // c_name is dict-encoded: c_name.bin contains int32_t dict codes.
                // The actual dict is name_dict.txt.
                gendb::MmapColumn<int32_t> c_custkey_col(gendb_dir + "/customer/c_custkey.bin");
                gendb::MmapColumn<int32_t> c_name_col   (gendb_dir + "/customer/c_name.bin");

                // Direct array: custkey → name_code (custkeys are 1-based, 1..1.5M)
                std::vector<int32_t> cust_name_code(CUSTOMER_ROWS + 1, -1);
                for (int64_t i = 0; i < CUSTOMER_ROWS; ++i) {
                    int32_t ck = c_custkey_col[i];
                    if (ck >= 0 && ck <= CUSTOMER_ROWS)
                        cust_name_code[ck] = c_name_col[i];
                }

                // Load name dictionary
                std::vector<std::string> name_dict;
                name_dict.reserve(CUSTOMER_ROWS);
                {
                    std::ifstream df(gendb_dir + "/customer/name_dict.txt");
                    std::string line;
                    while (std::getline(df, line))
                        name_dict.push_back(line);
                }

                // ─── Phase 4: Main lineitem scan + aggregation ───────────────────
                {
                    GENDB_PHASE("main_scan");

                    gendb::MmapColumn<int32_t> l_ok2(gendb_dir + "/lineitem/l_orderkey.bin");
                    gendb::MmapColumn<int64_t> l_q2 (gendb_dir + "/lineitem/l_quantity.bin");

                    // Thread-local: orderkey → sum_qty (~600 groups, tiny)
                    std::vector<gendb::CompactHashMap<int32_t,int64_t>> agg_local(NTHREADS);
                    for (auto& m : agg_local) m.reserve(2048);

                    std::atomic<int64_t> cursor2{0};

                    auto worker2 = [&](unsigned tid) {
                        auto& lm = agg_local[tid];
                        while (true) {
                            int64_t base = cursor2.fetch_add((int64_t)MORSEL,
                                                            std::memory_order_relaxed);
                            if (base >= LINEITEM_ROWS) break;
                            int64_t end = std::min(base + (int64_t)MORSEL, LINEITEM_ROWS);
                            for (int64_t i = base; i < end; ++i) {
                                int32_t ok = l_ok2[i];
                                if (orders_map.contains(ok)) {
                                    int64_t* p = lm.find(ok);
                                    if (p) *p += l_q2[i];
                                    else   lm.insert(ok, l_q2[i]);
                                }
                            }
                        }
                    };

                    std::vector<std::thread> threads2;
                    threads2.reserve(NTHREADS);
                    for (unsigned t = 0; t < NTHREADS; ++t)
                        threads2.emplace_back(worker2, t);
                    for (auto& th : threads2) th.join();

                    // Merge thread-local agg maps
                    gendb::CompactHashMap<int32_t,int64_t> global_agg(2048);
                    for (unsigned t = 0; t < NTHREADS; ++t) {
                        for (auto [k, v] : agg_local[t]) {
                            int64_t* gp = global_agg.find(k);
                            if (gp) *gp += v;
                            else     global_agg.insert(k, v);
                        }
                    }

                    // ─── Phase 5: Top-K sort ──────────────────────────────────────
                    {
                        GENDB_PHASE("sort_topk");

                        // Comparator: ORDER BY o_totalprice DESC, o_orderdate ASC
                        // TopKHeap uses a max-heap of the "worst" element.
                        // "better" = higher price, or same price and earlier date.
                        auto cmp = [](const ResultRow& a, const ResultRow& b) {
                            // returns true if a is WORSE than b (a should be evicted)
                            if (a.o_totalprice != b.o_totalprice)
                                return a.o_totalprice < b.o_totalprice;
                            return a.o_orderdate > b.o_orderdate;
                        };

                        gendb::TopKHeap<ResultRow, decltype(cmp)> heap(100, cmp);

                        for (auto [ok, sum_qty] : global_agg) {
                            const OrderInfo* oi = orders_map.find(ok);
                            if (!oi) continue;
                            int32_t ck = oi->custkey;
                            int32_t name_code = (ck >= 0 && ck <= (int32_t)CUSTOMER_ROWS)
                                                ? cust_name_code[ck] : -1;
                            ResultRow row{ck, ok, oi->orderdate, oi->totalprice,
                                          sum_qty, name_code};
                            heap.push(row);
                        }

                        auto top = heap.sorted();

                        // ─── Phase output ──────────────────────────────────────────
                        {
                            GENDB_PHASE("output");

                            std::string out_path = results_dir + "/Q18.csv";
                            FILE* fp = std::fopen(out_path.c_str(), "w");
                            if (!fp) {
                                std::fprintf(stderr, "Cannot open output: %s\n",
                                             out_path.c_str());
                                return;
                            }
                            std::fprintf(fp,
                                "c_name,c_custkey,o_orderkey,o_orderdate,"
                                "o_totalprice,sum_qty\n");

                            char date_buf[11];
                            for (const auto& row : top) {
                                const char* name_str = "Unknown";
                                std::string name_owned;
                                if (row.name_code >= 0 &&
                                    row.name_code < (int32_t)name_dict.size()) {
                                    name_str = name_dict[row.name_code].c_str();
                                }

                                gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);

                                // totalprice and sum_qty both scale ×100
                                long long tp_int  = row.o_totalprice / 100;
                                long long tp_frac = row.o_totalprice % 100;
                                if (tp_frac < 0) { tp_int--; tp_frac = -tp_frac; }

                                long long sq_int  = row.sum_qty / 100;
                                long long sq_frac = row.sum_qty % 100;
                                if (sq_frac < 0) { sq_int--; sq_frac = -sq_frac; }

                                std::fprintf(fp, "%s,%d,%d,%s,%lld.%02lld,%lld.%02lld\n",
                                    name_str,
                                    row.c_custkey,
                                    row.o_orderkey,
                                    date_buf,
                                    tp_int, tp_frac,
                                    sq_int, sq_frac);
                            }
                            std::fclose(fp);
                        } // output
                    } // sort_topk
                } // main_scan
            } // build_joins
        } // dim_filter
    } // subquery_precompute (includes all nested phases)
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
