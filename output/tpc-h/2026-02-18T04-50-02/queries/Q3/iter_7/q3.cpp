/*
 * Q3: Shipping Priority — TPC-H  [ITERATION 7 — FULL ARCHITECTURE REWRITE]
 *
 * SQL:
 *   SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *          o_orderdate, o_shippriority
 *   FROM customer, orders, lineitem
 *   WHERE c_mktsegment = 'BUILDING'
 *     AND c_custkey = o_custkey
 *     AND l_orderkey = o_orderkey
 *     AND o_orderdate < DATE '1995-03-15'
 *     AND l_shipdate  > DATE '1995-03-15'
 *   GROUP BY l_orderkey, o_orderdate, o_shippriority
 *   ORDER BY revenue DESC, o_orderdate
 *   LIMIT 10;
 *
 * ============================================================
 * BOTTLENECK DIAGNOSIS (previous iterations)
 * ============================================================
 * 1. build_joins=272ms: Single-threaded 15M orders scan building a 3M-entry
 *    CompactHashMap. Fix: PARALLELIZE orders scan + use flat array (O(1) lookup).
 * 2. aggregation_merge=113ms: 64 thread-local CompactHashMaps each ~1M entries,
 *    merged serially. Fix: ELIMINATE thread-local maps; use shared flat int64_t
 *    array indexed directly by orderkey (15M entries, 120MB, fits in 376GB RAM).
 *    Atomic int64 adds are lock-free and efficient for low-contention workloads.
 * 3. main_scan=77ms: lineitem zone-map pruning already good. Fix: since lineitem
 *    is sorted ASC by l_shipdate, qualifying rows (l_shipdate > threshold) are
 *    contiguous at the END. Find the first qualifying block via zone map binary
 *    search; scan ONLY from that point (eliminates all skipped-block overhead).
 *
 * ============================================================
 * NEW PHYSICAL PLAN
 * ============================================================
 * A. FLAT ARRAY STRATEGY (eliminates ALL hash tables in hot path):
 *    - order_valid[]: uint8_t[15000001] — 1 if orderkey qualifies
 *    - order_date[]:  int32_t[15000001] — o_orderdate per orderkey
 *    - order_prio[]:  int32_t[15000001] — o_shippriority per orderkey
 *    - rev_agg[]:     int64_t[15000001] — accumulated revenue per orderkey
 *    Orderkeys are 1..15000000 (contiguous). Direct array indexing = zero hash overhead.
 *
 * B. PARALLEL ORDERS BUILD (Phase 2):
 *    Divide 15M orders into 64 chunks. Each thread writes to shared flat arrays
 *    independently (no key conflicts since each thread writes to distinct orderkeys
 *    scattered across [1..15M]). Use a per-orderkey write since orderkeys are unique —
 *    no race conditions on writes (each orderkey appears exactly once in orders).
 *    Actually orders: orderkey is PK (unique). So multiple threads can write to
 *    order_valid/order_date/order_prio arrays without conflict (different keys,
 *    different array slots). Cache line conflicts are unlikely given 64-byte lines
 *    covering 16 int32_t values vs 15M entries spread across memory.
 *
 * C. LINEITEM SUFFIX SCAN (Phase 3):
 *    lineitem is sorted by l_shipdate ASC. Zone map gives block min/max.
 *    Binary search for first block where max_val > threshold → all later blocks
 *    also qualify for the zone-map level (since data is sorted). Start sequential
 *    scan from the first qualifying block's row_start.
 *    Within partial blocks: per-row l_shipdate check.
 *    For full qualifying blocks (all l_shipdate > threshold): skip per-row check.
 *
 * D. PARALLEL LINEITEM AGGREGATION (Phase 3):
 *    Divide lineitem qualifying row range into 64 equal chunks.
 *    Each thread: for its rows, if l_shipdate > threshold && order_valid[l_orderkey]:
 *      __atomic_fetch_add(&rev_agg[l_orderkey], revenue, __ATOMIC_RELAXED)
 *    Atomics are lock-free int64 additions. Contention low (~2-3M matching rows
 *    spread across ~1-2M distinct orderkeys → average 1.5 hits per key, rarely same
 *    key accessed simultaneously by 2+ threads).
 *
 * E. RESULT COLLECTION (Phase 4):
 *    Walk rev_agg[1..15000000]: if order_valid[ok] && rev_agg[ok] > 0: emit row.
 *    Expected ~2-3M qualifying orders with revenue > 0.
 *
 * Key constants:
 *   BUILDING dict code = 0
 *   DATE '1995-03-15' = epoch day 9205
 *   MAX_ORDERKEY = 15000000 (exact, from Storage Guide)
 *   l_extendedprice scale = 100, l_discount scale = 100
 *   revenue_scaled = extendedprice * (100 - discount)  [scale=10000]
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <atomic>
#include <omp.h>

// POSIX for mmap
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"
#include <iostream>

// ============================================================
// Zone map entry layout (verified by previous iterations):
// 24 bytes: [int32_t min, int32_t max, uint64_t row_start, uint32_t row_count, uint32_t pad]
// ============================================================
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
    uint32_t _pad;
};
static_assert(sizeof(ZoneMapEntry) == 24, "ZoneMapEntry must be 24 bytes");

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled; // divide by 10000 for output
    int32_t o_orderdate;
    int32_t o_shippriority;
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = gendb::date_str_to_epoch_days("1995-03-15");
    const int32_t MAX_ORDERKEY   = 15000000;

    // Load c_mktsegment dictionary to find runtime code for 'BUILDING'
    int32_t BUILDING_CODE = -1;
    {
        std::ifstream dict_file(gendb_dir + "/customer/c_mktsegment_dict.txt");
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open dictionary: " + gendb_dir + "/customer/c_mktsegment_dict.txt");
        std::string line;
        int32_t code = 0;
        while (std::getline(dict_file, line)) {
            if (line == "BUILDING") {
                BUILDING_CODE = code;
                break;
            }
            code++;
        }
        if (BUILDING_CODE < 0)
            throw std::runtime_error("'BUILDING' not found in c_mktsegment dictionary");
    }

    // ============================================================
    // Phase 1: Customer filter → cust_building bitset
    // ============================================================
    // Use a compact uint8_t array for cache efficiency (1B per entry vs 1 bit for std::vector<bool>)
    // 1500001 bytes = 1.5MB — fits comfortably in L3
    std::vector<uint8_t> cust_building;
    {
        GENDB_PHASE("dim_filter");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

        size_t n = c_custkey.size();
        // Max custkey = 1500000 (from Storage Guide)
        cust_building.assign(1500001, 0);

        for (size_t i = 0; i < n; i++) {
            if (c_mktsegment[i] == BUILDING_CODE) {
                cust_building[c_custkey[i]] = 1;
            }
        }
    }

    // ============================================================
    // Phase 2: Orders build — flat arrays indexed by orderkey
    // PARALLEL: each thread handles a chunk of 15M orders.
    // No hash tables — direct flat array writes.
    // orderkey is the PK of orders → each slot written by exactly one row → no races.
    // ============================================================

    // Flat arrays: 15000001 entries each
    // order_valid: 15MB (uint8_t), order_date: 60MB (int32_t), order_prio: 60MB (int32_t)
    // Total: ~135MB — well within 376GB
    static uint8_t  order_valid[15000001];  // 1 if qualifies
    static int32_t  order_date [15000001];  // o_orderdate
    static int32_t  order_prio [15000001];  // o_shippriority
    static int64_t  rev_agg   [15000001];  // revenue accumulator (atomic-written)

    memset(order_valid, 0, sizeof(order_valid));
    memset(rev_agg,     0, sizeof(rev_agg));

    {
        GENDB_PHASE("build_joins");

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        size_t n = o_orderkey.size(); // 15000000

        // Advise sequential access for all orders columns
        o_orderkey.advise_sequential();
        o_custkey.advise_sequential();
        o_orderdate.advise_sequential();
        o_shippriority.advise_sequential();

        const size_t max_ckey = cust_building.size() - 1;

        // Parallel scan: each thread scans its chunk of orders
        // Writes to order_valid/order_date/order_prio are race-free because
        // o_orderkey is PK (unique) → each array index written at most once.
        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t i = 0; i < n; i++) {
            int32_t od = o_orderdate[i];
            if (od >= DATE_THRESHOLD) continue;  // o_orderdate < threshold

            int32_t ck = o_custkey[i];
            if ((size_t)ck > max_ckey || !cust_building[ck]) continue;

            int32_t ok = o_orderkey[i];
            // ok is unique in orders → no write race
            order_valid[ok] = 1;
            order_date [ok] = od;
            order_prio [ok] = o_shippriority[i];
        }
    }

    // ============================================================
    // Phase 3: Lineitem scan — zone-map suffix skip + parallel aggregation
    //
    // lineitem is sorted ASC by l_shipdate. We need l_shipdate > threshold.
    // → Find the first block where max_val > threshold using zone map.
    //   All rows from that block's row_start onward are candidates.
    // → Parallel scan of candidate rows with atomic int64 aggregation.
    // ============================================================
    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

        // Load zone map
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone map: " + zm_path);
        struct stat zm_st;
        fstat(zm_fd, &zm_st);
        const uint8_t* zm_raw = (const uint8_t*)mmap(nullptr, zm_st.st_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_raw == MAP_FAILED) { ::close(zm_fd); throw std::runtime_error("Cannot mmap zone map"); }
        ::close(zm_fd);

        uint32_t num_blocks = *(const uint32_t*)zm_raw;
        const ZoneMapEntry* zones = (const ZoneMapEntry*)(zm_raw + sizeof(uint32_t));

        // Since lineitem is sorted ASC by l_shipdate:
        // Find the FIRST block where max_val > threshold (binary search).
        // All blocks from that point onward have at least some qualifying rows.
        // Blocks before that point have max_val <= threshold → entirely skip.
        uint64_t scan_start_row = l_shipdate.size(); // default: empty (no qualifying rows)

        // Binary search for first block with max_val > threshold
        {
            uint32_t lo = 0, hi = num_blocks;
            while (lo < hi) {
                uint32_t mid = (lo + hi) / 2;
                if (zones[mid].max_val <= DATE_THRESHOLD) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if (lo < num_blocks) {
                scan_start_row = zones[lo].row_start;
            }
        }

        munmap((void*)zm_raw, zm_st.st_size);

        size_t total_rows = l_shipdate.size();
        if (scan_start_row >= total_rows) {
            // No qualifying rows
            goto done_scan;
        }

        // Advise sequential for the suffix we'll scan
        l_shipdate.advise_sequential();
        l_orderkey.advise_sequential();
        l_extendedprice.advise_sequential();
        l_discount.advise_sequential();

        {
            size_t scan_end = total_rows;
            // Parallel scan of the qualifying suffix
            // Each thread handles a chunk; atomic int64 add for revenue accumulation.
            // Contention is low: ~1-2M distinct qualifying orderkeys spread across 15M slots,
            // 64 threads → average ~23K rows/thread/orderkey → extremely rare same-slot conflict.
            #pragma omp parallel for schedule(static) num_threads(64)
            for (size_t i = scan_start_row; i < scan_end; i++) {
                if (l_shipdate[i] <= DATE_THRESHOLD) continue;  // partial first block check

                int32_t ok = l_orderkey[i];
                if (!order_valid[ok]) continue;  // not a qualifying order

                int64_t rev = l_extendedprice[i] * (100LL - l_discount[i]);
                __atomic_fetch_add(&rev_agg[ok], rev, __ATOMIC_RELAXED);
            }
        }

        done_scan:;
    }

    // ============================================================
    // Phase 4: Collect results + top-10
    // Walk flat arrays: find all qualifying orderkeys with revenue > 0.
    // ============================================================
    std::vector<ResultRow> top10;
    {
        GENDB_PHASE("aggregation_merge");

        // Collect all qualifying rows
        std::vector<ResultRow> candidates;
        candidates.reserve(3000000); // ~2-3M expected

        for (int32_t ok = 1; ok <= MAX_ORDERKEY; ok++) {
            if (!order_valid[ok]) continue;
            int64_t rev = rev_agg[ok];
            if (rev <= 0) continue;
            candidates.push_back({ok, rev, order_date[ok], order_prio[ok]});
        }

        // Top-10: revenue DESC, o_orderdate ASC
        auto cmp = [](const ResultRow& a, const ResultRow& b) -> bool {
            if (a.revenue_scaled != b.revenue_scaled)
                return a.revenue_scaled > b.revenue_scaled;
            return a.o_orderdate < b.o_orderdate;
        };

        int k = (int)std::min((size_t)10, candidates.size());
        if (k > 0) {
            std::partial_sort(candidates.begin(), candidates.begin() + k, candidates.end(), cmp);
            top10.assign(candidates.begin(), candidates.begin() + k);
        }
    }

    // ============================================================
    // Phase 5: Output CSV
    // ============================================================
    {
        GENDB_PHASE("sort_topk");  // keep phase name for timing compatibility

        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

        char date_buf[11];
        for (auto& row : top10) {
            int64_t whole = row.revenue_scaled / 10000;
            int64_t frac  = row.revenue_scaled % 10000;
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            fprintf(f, "%d,%lld.%04lld,%s,%d\n",
                row.l_orderkey,
                (long long)whole,
                (long long)frac,
                date_buf,
                row.o_shippriority);
        }

        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
