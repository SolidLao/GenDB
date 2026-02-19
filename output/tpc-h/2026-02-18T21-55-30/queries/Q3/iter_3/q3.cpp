/*
 * Q3: Shipping Priority
 *
 * SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 *        o_orderdate, o_shippriority
 * FROM customer, orders, lineitem
 * WHERE c_mktsegment = 'BUILDING'
 *   AND c_custkey = o_custkey
 *   AND l_orderkey = o_orderkey
 *   AND o_orderdate < DATE '1995-03-15'
 *   AND l_shipdate > DATE '1995-03-15'
 * GROUP BY l_orderkey, o_orderdate, o_shippriority
 * ORDER BY revenue DESC, o_orderdate
 * LIMIT 10;
 *
 * Strategy (Iter 3):
 *   KEY INSIGHT: o_orderdate and o_shippriority are functionally determined by o_orderkey,
 *   so GROUP BY (l_orderkey, o_orderdate, o_shippriority) == GROUP BY l_orderkey.
 *   We embed revenue directly into OrderEntry using atomic int64 accumulation.
 *   This eliminates separate AggHashMaps and the expensive merge phase entirely.
 *
 *   1. Scan customer -> build dense bitset on c_custkey for BUILDING segment (~1.5M bits = 187KB)
 *   2. Scan orders (zone-map pruned for o_orderdate < cutoff), filter by bitset + date,
 *      build compact open-addressing hash map: o_orderkey -> {o_orderdate, o_shippriority, revenue=0}
 *   3. Binary-search lineitem (sorted by l_shipdate) for l_shipdate > cutoff.
 *      Parallel scan: probe orders hash map, atomically accumulate revenue into the slot.
 *   4. Collect slots with revenue > 0, partial_sort top-10, output CSV.
 *
 *   Timing expected:
 *   - dim_filter: ~3ms (unchanged)
 *   - build_joins: ~85ms (unchanged)
 *   - main_scan: ~60ms (unchanged, but no per-thread AggHashMap init/use)
 *   - output: ~5ms (no merge, just iterate ~3.75M slots, partial_sort top-10)
 */

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <iostream>
#include <atomic>
#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Orders hash map with embedded atomic revenue accumulation
// Open-addressing, linear probing, power-of-2 size
//
// KEY INSIGHT: o_orderdate and o_shippriority are 1:1 with o_orderkey,
// so we accumulate revenue directly in OrderEntry via atomic int64 add.
// This eliminates separate aggregation maps and the merge phase.
// ---------------------------------------------------------------------------
struct OrderEntry {
    int32_t orderkey;       // 0 = empty slot
    int32_t orderdate;
    int32_t shippriority;
    int32_t _pad;           // pad to 16 bytes for alignment
    int64_t revenue;        // accumulated atomically (scale x10000)
};
static_assert(sizeof(OrderEntry) == 24, "OrderEntry size mismatch");

struct OrdersHashMap {
    OrderEntry* slots;
    uint32_t mask;
    uint32_t sz;

    void init(uint32_t capacity) {
        // Next power of 2 >= capacity * 2 (50% load factor)
        uint32_t s = 1;
        while (s < capacity * 2) s <<= 1;
        sz = s;
        mask = s - 1;
        // Use posix_memalign for cache-line alignment
        void* ptr = nullptr;
        if (posix_memalign(&ptr, 64, (size_t)s * sizeof(OrderEntry)) != 0)
            throw std::bad_alloc();
        slots = static_cast<OrderEntry*>(ptr);
        memset(slots, 0, (size_t)s * sizeof(OrderEntry));
    }

    void destroy() {
        if (slots) { free(slots); slots = nullptr; }
    }

    inline void insert(int32_t key, int32_t orderdate, int32_t shippriority) {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (slots[h].orderkey != 0) {
            if (slots[h].orderkey == key) return; // duplicate
            h = (h + 1) & mask;
        }
        slots[h].orderkey = key;
        slots[h].orderdate = orderdate;
        slots[h].shippriority = shippriority;
        slots[h].revenue = 0;
        // No atomic needed here — build phase is single-threaded
    }

    // Returns pointer to slot for atomic revenue accumulation (probe phase)
    inline OrderEntry* find(int32_t key) const {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (true) {
            OrderEntry* e = &slots[h];
            if (e->orderkey == 0) return nullptr;
            if (e->orderkey == key) return e;
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    int32_t orderkey;
    int64_t revenue;  // scale x10000
    int32_t orderdate;
    int32_t shippriority;
};

void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t CUTOFF = gendb::date_str_to_epoch_days("1995-03-15"); // 9204

    // -------------------------------------------------------------------------
    // Phase 1: Load customer, build bitset on c_custkey for BUILDING segment
    // -------------------------------------------------------------------------
    static const int CUST_MAX = 1500001;
    // Bitset: 187KB, fits comfortably in L3 cache
    std::vector<uint64_t> cust_bitset((CUST_MAX + 63) / 64, 0);
    int building_code = -1;

    {
        GENDB_PHASE("dim_filter");

        // Load c_mktsegment dictionary
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
        std::ifstream dict_file(dict_path);
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open " + dict_path);
        std::string word;
        int code = 0;
        while (std::getline(dict_file, word)) {
            if (word == "BUILDING") { building_code = code; break; }
            ++code;
        }
        if (building_code < 0)
            throw std::runtime_error("BUILDING not found in dictionary");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");
        const uint64_t n_cust = c_custkey.size();

        // Parallel scan with atomic OR per 64-bit word (bits are set-only, so safe)
        #pragma omp parallel for schedule(static) num_threads(16)
        for (uint64_t i = 0; i < n_cust; ++i) {
            if (c_mktsegment[i] == building_code) {
                int32_t ck = c_custkey[i];
                if (ck > 0 && ck < CUST_MAX) {
                    uint32_t word_idx = (uint32_t)ck >> 6;
                    uint64_t bit = 1ULL << ((uint32_t)ck & 63);
                    __atomic_fetch_or(&cust_bitset[word_idx], bit, __ATOMIC_RELAXED);
                }
            }
        }
    }

    // Helper: test bitset
    auto in_bitset = [&](int32_t ck) -> bool {
        if (ck <= 0 || ck >= CUST_MAX) return false;
        return (cust_bitset[(uint32_t)ck >> 6] >> ((uint32_t)ck & 63)) & 1;
    };

    // -------------------------------------------------------------------------
    // Phase 2: Scan orders (zone-map pruned), build hash map with embedded revenue
    // -------------------------------------------------------------------------
    OrdersHashMap orders_map;
    orders_map.slots = nullptr;

    {
        GENDB_PHASE("build_joins");

        // Load zone map index for o_orderdate
        // Layout: [uint32_t num_blocks] then [int32_t min, int32_t max, uint32_t count] x num_blocks
        std::string zm_path = gendb_dir + "/indexes/zone_map_o_orderdate.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        uint32_t num_blocks = 0;
        struct ZMEntry { int32_t zm_min, zm_max; uint32_t count; };
        std::vector<ZMEntry> zm_entries;
        if (zm_fd >= 0) {
            (void)read(zm_fd, &num_blocks, 4);
            zm_entries.resize(num_blocks);
            (void)read(zm_fd, zm_entries.data(), num_blocks * sizeof(ZMEntry));
            close(zm_fd);
        }

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        const uint64_t n_orders = o_orderkey.size();

        // Zone-map pruning: orders sorted by o_orderdate, find last qualifying block
        uint32_t end_block = num_blocks;
        for (uint32_t b = 0; b < num_blocks; ++b) {
            if (zm_entries[b].zm_min >= CUTOFF) { end_block = b; break; }
        }
        const uint32_t BLOCK_SIZE = 100000;
        uint64_t end_row = (uint64_t)end_block * BLOCK_SIZE;
        if (end_row > n_orders) end_row = n_orders;

        // Collect qualifying orders into thread-local vectors, then bulk insert
        const int N_THREADS_BUILD = 32;
        std::vector<std::vector<std::tuple<int32_t,int32_t,int32_t>>> thread_results(N_THREADS_BUILD);

        #pragma omp parallel num_threads(N_THREADS_BUILD)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_results[tid];
            local.reserve(200000);

            #pragma omp for schedule(static)
            for (uint64_t i = 0; i < end_row; ++i) {
                int32_t od = o_orderdate[i];
                if (od >= CUTOFF) continue;
                int32_t ck = o_custkey[i];
                if (!in_bitset(ck)) continue;
                local.emplace_back(o_orderkey[i], od, o_shippriority[i]);
            }
        }

        // Count total qualifying orders
        uint64_t total_qual = 0;
        for (auto& v : thread_results) total_qual += v.size();

        // Init hash map (revenue field initialized to 0 in init())
        orders_map.init((uint32_t)total_qual + 1);

        // Single-threaded insert (hash map not concurrent-safe for inserts)
        for (auto& v : thread_results) {
            for (auto& [ok, od, sp] : v) {
                orders_map.insert(ok, od, sp);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3: Scan lineitem from shipdate > CUTOFF, atomically accumulate revenue
    // No per-thread AggHashMaps — revenue accumulates directly into orders_map slots
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

        const uint64_t n_lineitem = l_shipdate.size();

        // Binary search for first row where l_shipdate > CUTOFF (lineitem sorted by l_shipdate)
        uint64_t lo = 0, hi = n_lineitem;
        while (lo < hi) {
            uint64_t mid = (lo + hi) / 2;
            if (l_shipdate[mid] <= CUTOFF) lo = mid + 1;
            else hi = mid;
        }
        const uint64_t start_row = lo;

        // Parallel scan: probe orders_map (read-only find), then atomic add revenue
        // orders_map.find() is safe concurrently (read-only hash probe, no mutation of structure)
        // Only revenue field is written via __atomic_fetch_add
        #pragma omp parallel for schedule(static) num_threads(64)
        for (uint64_t i = start_row; i < n_lineitem; ++i) {
            int32_t lok = l_orderkey[i];
            OrderEntry* oe = orders_map.find(lok);
            if (!oe) continue;

            // revenue = l_extendedprice * (100 - l_discount) — scale x10000
            int64_t ep   = l_extendedprice[i]; // scale x100
            int64_t disc = l_discount[i];       // scale x100
            int64_t rev  = ep * (100LL - disc) / 100; // scale x100

            // Atomically accumulate into the slot's revenue field
            __atomic_fetch_add(&oe->revenue, rev, __ATOMIC_RELAXED);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Collect results, top-10 sort, output CSV
    // No merge step — revenue is already fully aggregated in orders_map slots
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Collect all qualifying slots (revenue > 0 means at least one lineitem matched)
        std::vector<ResultRow> all_rows;
        all_rows.reserve(4000000);

        for (uint32_t i = 0; i < orders_map.sz; ++i) {
            const OrderEntry& e = orders_map.slots[i];
            if (e.orderkey == 0) continue;
            if (e.revenue == 0) continue; // no matching lineitems (shipdate filter excluded all)
            all_rows.push_back({e.orderkey, e.revenue, e.orderdate, e.shippriority});
        }

        // Top-10: ORDER BY revenue DESC, o_orderdate ASC
        size_t k = std::min((size_t)10, all_rows.size());
        std::partial_sort(all_rows.begin(), all_rows.begin() + k, all_rows.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });
        all_rows.resize(k);

        // Write output CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + out_path);

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[12];
        for (const auto& row : all_rows) {
            // revenue is scale x10000; output with 2 decimal places
            // actual value = rev / 10000; display = integer_part . frac_2dec
            int64_t integer_part = row.revenue / 10000;
            int64_t remainder    = row.revenue % 10000;
            int64_t frac_part    = remainder / 100;
            int64_t third_dec    = remainder % 100;
            if (third_dec >= 50) frac_part++;
            if (frac_part >= 100) { frac_part = 0; integer_part++; }

            gendb::epoch_days_to_date_str(row.orderdate, date_buf);
            fprintf(f, "%d,%lld.%02lld,%s,%d\n",
                    row.orderkey,
                    (long long)integer_part,
                    (long long)frac_part,
                    date_buf,
                    row.shippriority);
        }
        fclose(f);

        orders_map.destroy();
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
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
