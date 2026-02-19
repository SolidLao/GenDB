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
 * Strategy (iter_2):
 *   1. Parse dict file (format: "code|name"), find BUILDING code.
 *      Scan customer in parallel, build bitset on c_custkey for BUILDING segment.
 *   2. Scan orders with zone-map pruning (o_orderdate < cutoff).
 *      Filter by bitset + date; thread-local collection, single-threaded hash map build.
 *      Build Bloom filter (8M bits, ~1MB, L2-resident) over qualifying o_orderkeys.
 *   3. Scan lineitem from l_shipdate > CUTOFF (binary-search start).
 *      Per row: cheap Bloom filter check → orders hash map probe → accumulate revenue.
 *      Thread-local AggHashMaps (keyed only by orderkey, which uniquely determines
 *      orderdate+shippriority in this query).
 *   4. Partition-parallel merge: 64 threads each own 1 partition. Thread i iterates all
 *      64 thread-local agg maps and merges entries whose orderkey hashes to partition i.
 *      No contention, fully parallel.
 *   5. Collect results, partial_sort for top-10.
 *
 * Bug fixes vs iter_0/iter_1:
 *   - Dictionary file format is "code|name" (e.g., "0|BUILDING"), NOT just "name".
 *     Previous code did getline + compare to "BUILDING" which always failed.
 *   - AggHashMap: use int32_t orderkey sentinel (0 = empty) instead of bool flag,
 *     reducing struct size from 32B to 24B for better cache density.
 *   - OrdersHashMap: removed duplicate-check in insert (orderkeys are unique per order).
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
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <atomic>

#include "date_utils.h"
#include "mmap_utils.h"
#include "hash_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Orders hash map: o_orderkey -> {o_orderdate, o_shippriority}
// Open-addressing, linear probing, power-of-2 size.
// Sentinel: orderkey == 0 (TPC-H orderkeys start at 1).
// 12 bytes per slot: dense and cache-friendly.
// ---------------------------------------------------------------------------
struct OrderEntry {
    int32_t orderkey;    // 0 = empty
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdersHashMap {
    OrderEntry* slots;
    uint32_t mask;

    void init(uint32_t sz, OrderEntry* mem) {
        // sz must be power of 2
        mask = sz - 1;
        slots = mem;
        memset(slots, 0, sz * sizeof(OrderEntry));
    }

    // Insert without duplicate check (o_orderkey is primary key — guaranteed unique)
    inline void insert(int32_t key, int32_t orderdate, int32_t shippriority) {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (slots[h].orderkey != 0) {
            h = (h + 1) & mask;
        }
        slots[h] = {key, orderdate, shippriority};
    }

    inline const OrderEntry* find(int32_t key) const {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (true) {
            const OrderEntry& e = slots[h];
            if (__builtin_expect(e.orderkey == 0, 0)) return nullptr;
            if (e.orderkey == key) return &e;
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Bloom filter: 8M bits (1MB), L2-resident, 3 hash probes.
// At ~3.75M insertions: FPR ≈ 0.7%. Eliminates ~99.3% of non-matching probes
// in the lineitem scan before touching the orders hash map.
// ---------------------------------------------------------------------------
struct BloomFilter {
    static constexpr uint32_t BIT_COUNT = (1u << 23); // 8M bits = 1MB
    static constexpr uint32_t BIT_MASK  = BIT_COUNT - 1;

    std::vector<uint64_t> bits;

    void init() {
        bits.assign(BIT_COUNT / 64, 0ULL);
    }

    inline void insert(uint32_t key) {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h >> 41) & BIT_MASK;
        uint32_t h2 = (uint32_t)(h >> 17) & BIT_MASK;
        uint32_t h3 = (uint32_t)(h      ) & BIT_MASK;
        bits[h1 >> 6] |= (1ULL << (h1 & 63));
        bits[h2 >> 6] |= (1ULL << (h2 & 63));
        bits[h3 >> 6] |= (1ULL << (h3 & 63));
    }

    inline bool maybe_contains(uint32_t key) const {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h >> 41) & BIT_MASK;
        uint32_t h2 = (uint32_t)(h >> 17) & BIT_MASK;
        uint32_t h3 = (uint32_t)(h      ) & BIT_MASK;
        return ((bits[h1 >> 6] >> (h1 & 63)) &
                (bits[h2 >> 6] >> (h2 & 63)) &
                (bits[h3 >> 6] >> (h3 & 63)) & 1);
    }
};

// ---------------------------------------------------------------------------
// Thread-local aggregation hash map.
// Key: orderkey only (orderkey → (orderdate, shippriority) is 1:1 in this query).
// 24 bytes per slot: 3× int32_t + 1× int64_t, no padding bool.
// Sentinel: orderkey == 0.
// ---------------------------------------------------------------------------
struct AggEntry {
    int32_t orderkey;    // 0 = empty
    int32_t orderdate;
    int32_t shippriority;
    int64_t revenue;     // scale x10000
};
static_assert(sizeof(AggEntry) == 24, "AggEntry must be 24B");

struct AggHashMap {
    AggEntry* slots;
    uint32_t mask;

    void init(uint32_t sz, AggEntry* mem) {
        mask = sz - 1;
        slots = mem;
        memset(slots, 0, sz * sizeof(AggEntry));
    }

    // Key by orderkey only (1:1 mapping to orderdate+shippriority for this query)
    inline void accumulate(int32_t ok, int32_t od, int32_t sp, int64_t rev) {
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)ok * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (true) {
            AggEntry& e = slots[h];
            if (e.orderkey == 0) {
                e.orderkey = ok;
                e.orderdate = od;
                e.shippriority = sp;
                e.revenue = rev;
                return;
            }
            if (e.orderkey == ok) {
                e.revenue += rev;
                return;
            }
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    int32_t orderkey;
    int64_t revenue;   // scale x10000
    int32_t orderdate;
    int32_t shippriority;
};

void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Correctness anchor: 1995-03-15 = 9204 days since epoch
    const int32_t CUTOFF = 9204;

    // -------------------------------------------------------------------------
    // Phase 1: Parse dictionary (format: "code|name"), find BUILDING code.
    //          Scan customer in parallel, build bitset for BUILDING custkeys.
    // -------------------------------------------------------------------------
    static const int CUST_MAX = 1500001;
    std::vector<uint64_t> cust_bitset((CUST_MAX + 63) / 64, 0);

    int building_code = -1;

    {
        GENDB_PHASE("dim_filter");

        // Dictionary file format: "0|BUILDING\n1|AUTOMOBILE\n..."
        // Parse each line: split on '|', extract code from left, name from right.
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
        std::ifstream dict_file(dict_path);
        if (!dict_file.is_open())
            throw std::runtime_error("Cannot open " + dict_path);

        std::string line;
        while (std::getline(dict_file, line)) {
            // Find '|' separator
            size_t sep = line.find('|');
            if (sep == std::string::npos) continue;
            int code = std::stoi(line.substr(0, sep));
            std::string name = line.substr(sep + 1);
            // Trim trailing \r if present (Windows line endings)
            if (!name.empty() && name.back() == '\r') name.pop_back();
            if (name == "BUILDING") {
                building_code = code;
                break;
            }
        }
        if (building_code < 0)
            throw std::runtime_error("BUILDING not found in dictionary");

        gendb::MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");
        gendb::MmapColumn<int32_t> c_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");
        const uint64_t n_cust = c_custkey.size();
        const int32_t bcode = building_code; // capture for lambda

        // Parallel scan; atomic OR per 64-bit word is race-free (bits only set)
        #pragma omp parallel for schedule(static) num_threads(16)
        for (uint64_t i = 0; i < n_cust; ++i) {
            if (c_mktsegment[i] == bcode) {
                int32_t ck = c_custkey[i];
                if (ck > 0 && ck < CUST_MAX) {
                    uint32_t widx = (uint32_t)ck >> 6;
                    uint64_t bit  = 1ULL << ((uint32_t)ck & 63);
                    __atomic_fetch_or(&cust_bitset[widx], bit, __ATOMIC_RELAXED);
                }
            }
        }
    }

    auto in_bitset = [&](int32_t ck) __attribute__((always_inline)) -> bool {
        if ((uint32_t)ck >= (uint32_t)CUST_MAX) return false;
        return (cust_bitset[(uint32_t)ck >> 6] >> ((uint32_t)ck & 63)) & 1;
    };

    // -------------------------------------------------------------------------
    // Phase 2: Scan orders (zone-map pruned for o_orderdate < CUTOFF),
    //          filter by bitset + date, build orders hash map + Bloom filter.
    //
    //   Two-pass:
    //   Pass A (parallel): each thread collects qualifying rows into thread-local buffer.
    //   Pass B (sequential): insert into fixed-size orders hash map.
    //   Simultaneously build Bloom filter over qualifying o_orderkeys.
    //
    //   Hash map: 8M slots (power-of-2 ≥ 3.75M * 2), 12B/slot = 96MB total.
    //   Load factor: ~3.75M / 8M ≈ 46% — excellent for linear probing.
    // -------------------------------------------------------------------------
    static const uint32_t ORDERS_MAP_SZ = 8388608u; // 8M power-of-2 slots
    std::vector<OrderEntry> orders_mem(ORDERS_MAP_SZ);
    OrdersHashMap orders_map;

    BloomFilter bloom;
    bloom.init();

    {
        GENDB_PHASE("build_joins");

        // Load zone map index
        struct ZMEntry { int32_t zm_min, zm_max; uint32_t count; };
        std::vector<ZMEntry> zm_entries;
        {
            std::string zm_path = gendb_dir + "/indexes/zone_map_o_orderdate.bin";
            int zm_fd = open(zm_path.c_str(), O_RDONLY);
            if (zm_fd >= 0) {
                uint32_t nb = 0;
                if (read(zm_fd, &nb, 4) == 4) {
                    zm_entries.resize(nb);
                    ssize_t r = read(zm_fd, zm_entries.data(), nb * sizeof(ZMEntry));
                    (void)r;
                }
                close(zm_fd);
            }
        }

        gendb::MmapColumn<int32_t> o_orderkey  (gendb_dir + "/orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_custkey   (gendb_dir + "/orders/o_custkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate (gendb_dir + "/orders/o_orderdate.bin");
        gendb::MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders/o_shippriority.bin");
        const uint64_t n_orders = o_orderkey.size();

        // Zone-map pruning: orders sorted by o_orderdate.
        // Skip block b if zm_entries[b].zm_min >= CUTOFF (entire block is >= cutoff).
        uint32_t num_blocks = (uint32_t)zm_entries.size();
        uint32_t end_block  = num_blocks;
        for (uint32_t b = 0; b < num_blocks; ++b) {
            if (zm_entries[b].zm_min >= CUTOFF) {
                end_block = b;
                break;
            }
        }
        const uint32_t BLOCK_SIZE = 100000;
        uint64_t end_row = std::min((uint64_t)end_block * BLOCK_SIZE, n_orders);

        // Parallel collect into thread-local buffers
        const int N_BUILD = 32;
        std::vector<std::vector<OrderEntry>> thread_results(N_BUILD);

        #pragma omp parallel num_threads(N_BUILD)
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
                local.push_back({o_orderkey[i], od, o_shippriority[i]});
            }
        }

        // Single-threaded sequential hash map build + Bloom filter
        // (sequential insert: avoids false-sharing, ensures correctness)
        orders_map.init(ORDERS_MAP_SZ, orders_mem.data());

        for (auto& v : thread_results) {
            for (const OrderEntry& e : v) {
                orders_map.insert(e.orderkey, e.orderdate, e.shippriority);
                bloom.insert((uint32_t)e.orderkey);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3: Scan lineitem from l_shipdate > CUTOFF (binary-search start).
    //          Probe Bloom filter first (L2-resident), then orders hash map.
    //          Thread-local aggregation (24B/entry AggHashMap, keyed by orderkey).
    // -------------------------------------------------------------------------
    const int N_THREADS = 64;
    static const uint32_t AGG_SLOTS = 131072u; // 128K slots per thread (power-of-2)
    std::vector<AggEntry> agg_mem((size_t)N_THREADS * AGG_SLOTS);
    std::vector<AggHashMap> agg_maps(N_THREADS);
    for (int t = 0; t < N_THREADS; ++t) {
        agg_maps[t].init(AGG_SLOTS, &agg_mem[(size_t)t * AGG_SLOTS]);
    }

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<int32_t> l_orderkey     (gendb_dir + "/lineitem/l_orderkey.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount      (gendb_dir + "/lineitem/l_discount.bin");
        gendb::MmapColumn<int32_t> l_shipdate      (gendb_dir + "/lineitem/l_shipdate.bin");

        const uint64_t n_lineitem = l_shipdate.size();

        // Binary search for first row where l_shipdate > CUTOFF
        uint64_t lo = 0, hi = n_lineitem;
        while (lo < hi) {
            uint64_t mid = (lo + hi) / 2;
            if (l_shipdate[mid] <= CUTOFF) lo = mid + 1;
            else hi = mid;
        }
        const uint64_t start_row = lo;

        #pragma omp parallel num_threads(N_THREADS)
        {
            int tid = omp_get_thread_num();
            AggHashMap& local_agg = agg_maps[tid];

            #pragma omp for schedule(static)
            for (uint64_t i = start_row; i < n_lineitem; ++i) {
                int32_t lok = l_orderkey[i];

                // Bloom filter: cheap L2-resident check eliminates ~99% of non-matches
                if (!bloom.maybe_contains((uint32_t)lok)) continue;

                // Full orders hash map probe (only for Bloom-positive)
                const OrderEntry* oe = orders_map.find(lok);
                if (!oe) continue;

                // revenue = extendedprice * (1 - discount)
                // Both at scale 100: result is scale 10000
                int64_t ep   = l_extendedprice[i];
                int64_t disc = l_discount[i];
                int64_t rev  = ep * (100LL - disc) / 100;

                local_agg.accumulate(lok, oe->orderdate, oe->shippriority, rev);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Partition-parallel merge of thread-local aggregation maps.
    //          64 partitions, 64 threads: thread p collects all entries
    //          from all 64 agg_maps whose (orderkey hash & 63) == p.
    //          No contention — fully parallel, each thread owns its partition.
    //          Then collect results, partial_sort for top-10.
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        const uint32_t N_PARTS = (uint32_t)N_THREADS;
        const uint32_t PART_MASK = N_PARTS - 1;

        // Per-partition result vectors (written exclusively by one thread)
        std::vector<std::vector<ResultRow>> part_results(N_PARTS);

        // Per-partition merge hash maps (128K slots each)
        static const uint32_t MERGE_SZ = 131072u;
        std::vector<std::vector<AggEntry>> merge_mems(N_PARTS,
            std::vector<AggEntry>(MERGE_SZ));

        #pragma omp parallel num_threads(N_PARTS)
        {
            uint32_t part = (uint32_t)omp_get_thread_num();

            // Initialize this partition's merge map
            AggHashMap mmap;
            mmap.init(MERGE_SZ, merge_mems[part].data());

            // Iterate over all thread-local agg maps, pick entries belonging to this partition
            for (int t = 0; t < N_THREADS; ++t) {
                const AggHashMap& src = agg_maps[t];
                uint32_t src_sz = src.mask + 1;
                const AggEntry* src_slots = src.slots;
                for (uint32_t s = 0; s < src_sz; ++s) {
                    const AggEntry& e = src_slots[s];
                    if (e.orderkey == 0) continue;
                    // Partition assignment: use same hash as insert to ensure consistent mapping
                    uint32_t phash = (uint32_t)(((uint64_t)(uint32_t)e.orderkey
                                                  * 0x9E3779B97F4A7C15ULL) >> 32);
                    if ((phash & PART_MASK) != part) continue;
                    mmap.accumulate(e.orderkey, e.orderdate, e.shippriority, e.revenue);
                }
            }

            // Collect this partition's results
            auto& local_rows = part_results[part];
            uint32_t msz = mmap.mask + 1;
            const AggEntry* mslots = mmap.slots;
            for (uint32_t s = 0; s < msz; ++s) {
                const AggEntry& e = mslots[s];
                if (e.orderkey == 0) continue;
                local_rows.push_back({e.orderkey, e.revenue, e.orderdate, e.shippriority});
            }
        }

        // Combine all partition results (sequential, ~3.75M rows total)
        std::vector<ResultRow> all_rows;
        all_rows.reserve(4000000);
        for (uint32_t p = 0; p < N_PARTS; ++p) {
            for (auto& r : part_results[p])
                all_rows.push_back(r);
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
            // revenue: scale x10000 → display with 2 decimal places
            // Actual value = row.revenue / 10000
            // Display with 2dp: integer = rev/10000, frac = (rev%10000)/100
            // Round at 3rd decimal: if (rev%100) >= 50 then frac++
            int64_t integer_part = row.revenue / 10000;
            int64_t frac_part    = (row.revenue % 10000) / 100;
            int64_t third_dec    = (row.revenue % 100);
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
