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
 * Strategy:
 *   1. Scan customer, build bitset on c_custkey for BUILDING segment
 *   2. Scan orders (zone-map pruned for o_orderdate < cutoff), filter by bitset + date,
 *      build compact hash map: o_orderkey -> {o_orderdate, o_shippriority}
 *   3. Scan lineitem from shipdate > cutoff (binary search start), probe orders hash map,
 *      accumulate revenue per (l_orderkey, o_orderdate, o_shippriority)
 *   4. Top-10 by revenue DESC, o_orderdate ASC
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
#include <omp.h>
#include <atomic>

#include "date_utils.h"
#include "mmap_utils.h"
#include "hash_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Orders hash map: o_orderkey -> {o_orderdate, o_shippriority}
// Open-addressing, linear probing, power-of-2 size
// ---------------------------------------------------------------------------
struct OrderEntry {
    int32_t orderkey;   // 0 = empty slot
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdersHashMap {
    std::vector<OrderEntry> slots;
    uint32_t mask;
    uint32_t count;

    void init(uint32_t capacity) {
        // Next power of 2 >= capacity * 2 (50% load factor)
        uint32_t sz = 1;
        while (sz < capacity * 2) sz <<= 1;
        mask = sz - 1;
        slots.assign(sz, {0, 0, 0});
        count = 0;
    }

    inline void insert(int32_t key, int32_t orderdate, int32_t shippriority) {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (slots[h].orderkey != 0) {
            if (slots[h].orderkey == key) return; // duplicate
            h = (h + 1) & mask;
        }
        slots[h] = {key, orderdate, shippriority};
        ++count;
    }

    inline const OrderEntry* find(int32_t key) const {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (true) {
            const OrderEntry& e = slots[h];
            if (e.orderkey == 0) return nullptr;
            if (e.orderkey == key) return &e;
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Aggregation key: (l_orderkey, o_orderdate, o_shippriority)
// ---------------------------------------------------------------------------
struct AggKey {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;

    bool operator==(const AggKey& o) const {
        return orderkey == o.orderkey && orderdate == o.orderdate && shippriority == o.shippriority;
    }
};

struct AggEntry {
    AggKey key;
    int64_t revenue; // scaled x100 (scale_factor=4 intermediate)
    bool used;
};

// Simple open-addressing hash map for aggregation
struct AggHashMap {
    std::vector<AggEntry> slots;
    uint32_t mask;

    void init(uint32_t capacity) {
        uint32_t sz = 1;
        while (sz < capacity * 2) sz <<= 1;
        mask = sz - 1;
        slots.resize(sz);
        for (auto& e : slots) e.used = false;
    }

    inline void accumulate(int32_t orderkey, int32_t orderdate, int32_t shippriority, int64_t rev) {
        uint64_t h = (uint64_t)(uint32_t)orderkey * 0x9E3779B97F4A7C15ULL;
        h ^= (uint64_t)(uint32_t)orderdate * 0x517CC1B727220A95ULL;
        h ^= (uint64_t)(uint32_t)shippriority * 0xBF58476D1CE4E5B9ULL;
        uint32_t idx = (uint32_t)(h >> 32) & mask;
        while (true) {
            AggEntry& e = slots[idx];
            if (!e.used) {
                e.key = {orderkey, orderdate, shippriority};
                e.revenue = rev;
                e.used = true;
                return;
            }
            if (e.key.orderkey == orderkey && e.key.orderdate == orderdate &&
                e.key.shippriority == shippriority) {
                e.revenue += rev;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    int32_t orderkey;
    int64_t revenue; // intermediate scale (x10000)
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
    // Bitset for custkey [1..1500000], 187KB
    static const int CUST_MAX = 1500001;
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
            if (word == "BUILDING") {
                building_code = code;
                break;
            }
            ++code;
        }
        if (building_code < 0)
            throw std::runtime_error("BUILDING not found in dictionary");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");
        const uint64_t n_cust = c_custkey.size();

        // Parallel scan with thread-local bitsets merged via atomic OR
        // Since bits are set monotonically, atomic OR per 64-bit word is safe
        #pragma omp parallel for schedule(static) num_threads(16)
        for (uint64_t i = 0; i < n_cust; ++i) {
            if (c_mktsegment[i] == building_code) {
                int32_t ck = c_custkey[i];
                if (ck > 0 && ck < CUST_MAX) {
                    uint32_t word_idx = (uint32_t)ck >> 6;
                    uint64_t bit = 1ULL << ((uint32_t)ck & 63);
                    // Atomic OR
                    __atomic_fetch_or(&cust_bitset[word_idx], bit, __ATOMIC_RELAXED);
                }
            }
        }
    }

    // Helper to test bitset
    auto in_bitset = [&](int32_t ck) -> bool {
        if (ck <= 0 || ck >= CUST_MAX) return false;
        return (cust_bitset[(uint32_t)ck >> 6] >> ((uint32_t)ck & 63)) & 1;
    };

    // -------------------------------------------------------------------------
    // Phase 2: Scan orders (zone-map pruned), build hash map
    // -------------------------------------------------------------------------
    // Estimate: ~3.75M qualifying orders
    OrdersHashMap orders_map;

    {
        GENDB_PHASE("build_joins");

        // Load zone map index for o_orderdate
        std::string zm_path = gendb_dir + "/indexes/zone_map_o_orderdate.bin";
        int zm_fd = open(zm_path.c_str(), O_RDONLY);
        uint32_t num_blocks = 0;
        if (zm_fd >= 0) {
            (void)read(zm_fd, &num_blocks, 4);
        }

        struct ZMEntry { int32_t zm_min, zm_max; uint32_t count; };
        std::vector<ZMEntry> zm_entries(num_blocks);
        if (zm_fd >= 0) {
            (void)read(zm_fd, zm_entries.data(), num_blocks * sizeof(ZMEntry));
            close(zm_fd);
        }

        gendb::MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");

        const uint64_t n_orders = o_orderkey.size();

        // Collect qualifying orders into thread-local vectors, then merge into hash map
        // Determine which blocks to scan based on zone map
        // orders sorted by o_orderdate: find last block where zm_min < CUTOFF
        uint32_t end_block = num_blocks; // exclusive
        if (!zm_entries.empty()) {
            // Since sorted by o_orderdate, find first block where zm_min >= CUTOFF
            // All blocks before that are candidates
            for (uint32_t b = 0; b < num_blocks; ++b) {
                if (zm_entries[b].zm_min >= CUTOFF) {
                    end_block = b;
                    break;
                }
            }
        }

        const uint32_t BLOCK_SIZE = 100000;
        uint64_t end_row = (uint64_t)end_block * BLOCK_SIZE;
        if (end_row > n_orders) end_row = n_orders;

        // Collect all qualifying orders first (thread-local), then bulk insert
        const int N_THREADS = 32;
        std::vector<std::vector<std::tuple<int32_t,int32_t,int32_t>>> thread_results(N_THREADS);

        #pragma omp parallel num_threads(N_THREADS)
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

        // Init hash map
        orders_map.init((uint32_t)total_qual + 1);

        // Single-threaded insert (hash map not concurrent-safe)
        for (auto& v : thread_results) {
            for (auto& [ok, od, sp] : v) {
                orders_map.insert(ok, od, sp);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3: Scan lineitem from shipdate > CUTOFF, aggregate
    // -------------------------------------------------------------------------
    // Thread-local aggregation maps, then merge
    const int N_THREADS = 64;
    std::vector<AggHashMap> agg_maps(N_THREADS);

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

        const uint64_t n_lineitem = l_shipdate.size();

        // Binary search for first row where l_shipdate > CUTOFF
        // lineitem sorted by l_shipdate
        uint64_t lo = 0, hi = n_lineitem;
        while (lo < hi) {
            uint64_t mid = (lo + hi) / 2;
            if (l_shipdate[mid] <= CUTOFF) lo = mid + 1;
            else hi = mid;
        }
        const uint64_t start_row = lo;

        // Init per-thread agg maps (estimated ~100K groups per thread)
        for (int t = 0; t < N_THREADS; ++t) {
            agg_maps[t].init(131072); // 128K slots
        }

        #pragma omp parallel num_threads(N_THREADS)
        {
            int tid = omp_get_thread_num();
            AggHashMap& local_agg = agg_maps[tid];

            #pragma omp for schedule(static)
            for (uint64_t i = start_row; i < n_lineitem; ++i) {
                int32_t lok = l_orderkey[i];
                const OrderEntry* oe = orders_map.find(lok);
                if (!oe) continue;

                // revenue = l_extendedprice * (100 - l_discount) in scale x10000
                int64_t ep = l_extendedprice[i];   // scale x100
                int64_t disc = l_discount[i];       // scale x100
                int64_t rev = ep * (100LL - disc);  // scale x10000

                local_agg.accumulate(lok, oe->orderdate, oe->shippriority, rev);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Merge aggregation results, top-10
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Merge all thread-local agg maps into global result vector
        std::vector<ResultRow> all_rows;
        all_rows.reserve(4000000);

        // Use a single merge map
        // Estimate ~3.75M groups total
        AggHashMap global_agg;
        global_agg.init(8388608); // 8M slots (power of 2 >= 3.75M * 2)

        for (int t = 0; t < N_THREADS; ++t) {
            for (const auto& e : agg_maps[t].slots) {
                if (!e.used) continue;
                global_agg.accumulate(e.key.orderkey, e.key.orderdate, e.key.shippriority, e.revenue);
            }
        }

        // Collect all results
        for (const auto& e : global_agg.slots) {
            if (!e.used) continue;
            all_rows.push_back({e.key.orderkey, e.revenue, e.key.orderdate, e.key.shippriority});
        }

        // Top-10: ORDER BY revenue DESC, o_orderdate ASC
        // Use partial_sort for top 10
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
            // revenue: scale x10000, output with 2 decimal places => divide by 100 (show x100 scale)
            // Wait: ep is scale x100, disc is scale x100
            // rev = ep * (100 - disc) => scale x10000
            // To get 2 decimal places: revenue_display = rev / 100 with 2 decimal places
            // i.e., print rev/100 with format %.2f equivalent
            // rev is in units of 0.0001, so actual value = rev / 10000
            // display with 2 decimals: actual_cents = rev / 100 (integer part scaled by 100 for display)
            int64_t integer_part = row.revenue / 10000;
            int64_t frac_part = (row.revenue % 10000) / 100; // 2 decimal places
            // Handle rounding of the third decimal
            int64_t third_dec = (row.revenue % 100);
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
