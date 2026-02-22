// Q3: Shipping Priority — GenDB iteration 0
// Strategy: custkey bitset → zone-map orders scan → bloom + hash lineitem scan → thread-local agg → top-10

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <climits>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <atomic>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <cassert>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ─── Constants ────────────────────────────────────────────────────────────────
static constexpr int32_t DATE_THRESH    = 9204;      // epoch days for 1995-03-15
static constexpr int32_t MKTSEG_BUILDING = 1;
static constexpr uint32_t MAX_CUSTKEY   = 1500001;   // keys in [1, 1500000]
static constexpr int32_t  EMPTY_KEY     = INT32_MIN;
static constexpr int       NTHREADS     = 64;
static constexpr uint32_t  BLOCK_SIZE   = 100000;

// Orders hash map: o_orderkey → {o_orderdate, o_shippriority}
static constexpr uint32_t ORD_CAP  = 2097152; // 2^21 > 1461923/0.7
static constexpr uint32_t ORD_MASK = ORD_CAP - 1;

// Aggregation hash map: l_orderkey → {revenue, o_orderdate, o_shippriority}
// AGG_CAP = 2^22 ≥ 2 × 1,470,000 estimated qualifying orders → load factor ≤ 50%
// Per-thread maps sized for FULL group cardinality (dynamic block scheduling can
// route all groups to one thread). global_agg uses same cap.
static constexpr uint32_t AGG_CAP  = 4194304; // 2^22
static constexpr uint32_t AGG_MASK = AGG_CAP - 1;

// Bloom filter over qualifying order keys
static constexpr uint64_t BLOOM_BITS  = 150994944ULL; // ~18 MB
static constexpr uint64_t BLOOM_WORDS = BLOOM_BITS / 64;
static constexpr uint64_t BLOOM_MASK_B = BLOOM_BITS - 1;

// ─── Structs ──────────────────────────────────────────────────────────────────
struct OrderEntry {
    int32_t key;           // o_orderkey, EMPTY_KEY if vacant
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct AggEntry {
    int32_t orderkey;      // EMPTY_KEY if vacant
    int32_t o_orderdate;
    int32_t o_shippriority;
    double  revenue;
};

// ─── Hash helpers ─────────────────────────────────────────────────────────────
static inline uint32_t ord_hash(int32_t k)  { return ((uint32_t)k * 2654435761U) & ORD_MASK; }
static inline uint32_t agg_hash(int32_t k)  { return ((uint32_t)k * 2654435761U) & AGG_MASK; }

// ─── Bloom helpers ────────────────────────────────────────────────────────────
static inline void bloom_insert(uint64_t* bloom, int32_t key) {
    uint64_t k = (uint64_t)(uint32_t)key;
    uint64_t h1 = (k * 2654435761ULL) & BLOOM_MASK_B;
    uint64_t h2 = (k * 2246822519ULL) & BLOOM_MASK_B;
    uint64_t h3 = (k * 3266489917ULL) & BLOOM_MASK_B;
    bloom[h1 >> 6] |= (1ULL << (h1 & 63));
    bloom[h2 >> 6] |= (1ULL << (h2 & 63));
    bloom[h3 >> 6] |= (1ULL << (h3 & 63));
}
static inline bool bloom_check(const uint64_t* bloom, int32_t key) {
    uint64_t k = (uint64_t)(uint32_t)key;
    uint64_t h1 = (k * 2654435761ULL) & BLOOM_MASK_B;
    uint64_t h2 = (k * 2246822519ULL) & BLOOM_MASK_B;
    uint64_t h3 = (k * 3266489917ULL) & BLOOM_MASK_B;
    return ((bloom[h1 >> 6] >> (h1 & 63)) & 1) &&
           ((bloom[h2 >> 6] >> (h2 & 63)) & 1) &&
           ((bloom[h3 >> 6] >> (h3 & 63)) & 1);
}

// ─── mmap helper ─────────────────────────────────────────────────────────────
static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, out_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ─── Main query function ──────────────────────────────────────────────────────
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // ── File paths ──
    const std::string d = gendb_dir + "/";
    // zone maps
    const std::string zm_ord_path = d + "indexes/orders_orderdate_zonemap.bin";
    const std::string zm_li_path  = d + "indexes/lineitem_shipdate_zonemap.bin";
    // customer
    const std::string c_seg_path  = d + "customer/c_mktsegment.bin";
    const std::string c_key_path  = d + "customer/c_custkey.bin";
    // orders
    const std::string o_odate_path = d + "orders/o_orderdate.bin";
    const std::string o_ckey_path  = d + "orders/o_custkey.bin";
    const std::string o_okey_path  = d + "orders/o_orderkey.bin";
    const std::string o_sprio_path = d + "orders/o_shippriority.bin";
    // lineitem
    const std::string l_ship_path  = d + "lineitem/l_shipdate.bin";
    const std::string l_okey_path  = d + "lineitem/l_orderkey.bin";
    const std::string l_epx_path   = d + "lineitem/l_extendedprice.bin";
    const std::string l_disc_path  = d + "lineitem/l_discount.bin";

    // ── Phase 0: Data loading ──────────────────────────────────────────────────
    // Zone maps first (tiny), then columns
    const int32_t *zm_ord = nullptr, *zm_li = nullptr;
    uint32_t zm_ord_nblocks = 0, zm_li_nblocks = 0;

    const uint8_t* c_seg  = nullptr;
    const int32_t* c_key  = nullptr;
    const int32_t* o_odate = nullptr, *o_ckey = nullptr;
    const int32_t* o_okey = nullptr, *o_sprio = nullptr;
    const int32_t* l_ship = nullptr, *l_okey = nullptr;
    const double*  l_epx  = nullptr, *l_disc = nullptr;
    size_t n_orders = 0, n_lineitems = 0, n_customers = 0;

    {
        GENDB_PHASE("data_loading");
        // Zone maps
        {
            size_t sz;
            const uint32_t* p = (const uint32_t*)mmap_file(zm_ord_path, sz);
            zm_ord_nblocks = p[0];
            zm_ord = (const int32_t*)(p + 1); // [min, max] × nblocks
        }
        {
            size_t sz;
            const uint32_t* p = (const uint32_t*)mmap_file(zm_li_path, sz);
            zm_li_nblocks = p[0];
            zm_li = (const int32_t*)(p + 1);
        }
        // Customer columns
        {
            size_t sz;
            c_seg = (const uint8_t*)mmap_file(c_seg_path, sz);
            n_customers = sz / sizeof(uint8_t);
        }
        {
            size_t sz;
            c_key = (const int32_t*)mmap_file(c_key_path, sz);
        }
        // Orders columns
        {
            size_t sz;
            o_odate = (const int32_t*)mmap_file(o_odate_path, sz);
            n_orders = sz / sizeof(int32_t);
        }
        {
            size_t sz;
            o_ckey = (const int32_t*)mmap_file(o_ckey_path, sz);
        }
        {
            size_t sz;
            o_okey = (const int32_t*)mmap_file(o_okey_path, sz);
        }
        {
            size_t sz;
            o_sprio = (const int32_t*)mmap_file(o_sprio_path, sz);
        }
        // Lineitem columns
        {
            size_t sz;
            l_ship = (const int32_t*)mmap_file(l_ship_path, sz);
            n_lineitems = sz / sizeof(int32_t);
        }
        {
            size_t sz;
            l_okey = (const int32_t*)mmap_file(l_okey_path, sz);
        }
        {
            size_t sz;
            l_epx = (const double*)mmap_file(l_epx_path, sz);
        }
        {
            size_t sz;
            l_disc = (const double*)mmap_file(l_disc_path, sz);
        }
    }

    // ── Phase 1: Customer scan → custkey bitset ────────────────────────────────
    // Bitset: 1500001 bits = 23438 uint64_t words
    static constexpr uint32_t CUST_WORDS = (MAX_CUSTKEY + 63) / 64;
    uint64_t* cust_bitset = new uint64_t[CUST_WORDS]();  // zero-initialized

    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < n_customers; i++) {
            if (c_seg[i] == MKTSEG_BUILDING) {
                int32_t ck = c_key[i];
                cust_bitset[(uint32_t)ck >> 6] |= (1ULL << (ck & 63));
            }
        }
    }

    // ── Phase 2: Parallel orders scan → hash map + bloom filter ───────────────
    OrderEntry* ord_map = new OrderEntry[ORD_CAP];
    // Initialize keys to EMPTY_KEY
    for (uint32_t i = 0; i < ORD_CAP; i++) ord_map[i].key = EMPTY_KEY;

    uint64_t* bloom = new uint64_t[BLOOM_WORDS]();  // zero-initialized

    {
        GENDB_PHASE("build_joins");

        // Identify qualifying order blocks from zone map
        // Skip block if block_min >= DATE_THRESH (all dates >= threshold)
        std::vector<uint32_t> ord_blocks;
        ord_blocks.reserve(zm_ord_nblocks);
        for (uint32_t b = 0; b < zm_ord_nblocks; b++) {
            int32_t bmin = zm_ord[b * 2];
            // int32_t bmax = zm_ord[b * 2 + 1];
            if (bmin < DATE_THRESH) {  // block may have qualifying rows
                ord_blocks.push_back(b);
            }
        }

        // Thread-local qualifying order lists
        // Each entry: (orderkey, orderdate, shippriority)
        struct OrdTuple { int32_t okey, odate, sprio; };
        std::vector<std::vector<OrdTuple>> tl_orders(NTHREADS);
        for (auto& v : tl_orders) v.reserve(32768);

        std::atomic<uint32_t> block_cursor{0};

        #pragma omp parallel num_threads(NTHREADS)
        {
            int tid = omp_get_thread_num();
            auto& local = tl_orders[tid];

            while (true) {
                uint32_t bi = block_cursor.fetch_add(1, std::memory_order_relaxed);
                if (bi >= (uint32_t)ord_blocks.size()) break;
                uint32_t block_id = ord_blocks[bi];
                uint32_t row_start = block_id * BLOCK_SIZE;
                uint32_t row_end   = std::min(row_start + BLOCK_SIZE, (uint32_t)n_orders);

                for (uint32_t r = row_start; r < row_end; r++) {
                    int32_t odate = o_odate[r];
                    if (odate >= DATE_THRESH) continue;       // filter o_orderdate < 9204
                    int32_t ck = o_ckey[r];
                    // check custkey bitset
                    if (!((cust_bitset[(uint32_t)ck >> 6] >> (ck & 63)) & 1)) continue;
                    local.push_back({o_okey[r], odate, o_sprio[r]});
                }
            }
        }

        // Sequential merge: insert into orders hash map and bloom filter
        for (int tid = 0; tid < NTHREADS; tid++) {
            for (auto& t : tl_orders[tid]) {
                // Insert into orders hash map (open-addressing, linear probe)
                uint32_t h = ord_hash(t.okey);
                for (uint32_t probe = 0; probe < ORD_CAP; probe++) {
                    uint32_t slot = (h + probe) & ORD_MASK;
                    if (ord_map[slot].key == EMPTY_KEY) {
                        ord_map[slot].key          = t.okey;
                        ord_map[slot].o_orderdate  = t.odate;
                        ord_map[slot].o_shippriority = t.sprio;
                        bloom_insert(bloom, t.okey);
                        break;
                    }
                    if (ord_map[slot].key == t.okey) break; // duplicate (shouldn't happen for PK)
                }
            }
        }
    }

    // ── Phase 3: Parallel lineitem scan + thread-local aggregation ────────────
    // Allocate thread-local agg maps
    AggEntry* agg_maps = new AggEntry[(size_t)NTHREADS * AGG_CAP];
    // Initialize
    for (size_t i = 0; i < (size_t)NTHREADS * AGG_CAP; i++) {
        agg_maps[i].orderkey = EMPTY_KEY;
    }

    {
        GENDB_PHASE("main_scan");

        // Identify qualifying lineitem blocks: skip if block_max <= DATE_THRESH
        std::vector<uint32_t> li_blocks;
        li_blocks.reserve(zm_li_nblocks);
        for (uint32_t b = 0; b < zm_li_nblocks; b++) {
            int32_t bmax = zm_li[b * 2 + 1];
            if (bmax > DATE_THRESH) {
                li_blocks.push_back(b);
            }
        }

        std::atomic<uint32_t> block_cursor{0};

        #pragma omp parallel num_threads(NTHREADS)
        {
            int tid = omp_get_thread_num();
            AggEntry* local_agg = agg_maps + (size_t)tid * AGG_CAP;

            while (true) {
                uint32_t bi = block_cursor.fetch_add(1, std::memory_order_relaxed);
                if (bi >= (uint32_t)li_blocks.size()) break;
                uint32_t block_id = li_blocks[bi];
                uint32_t row_start = block_id * BLOCK_SIZE;
                uint32_t row_end   = std::min(row_start + BLOCK_SIZE, (uint32_t)n_lineitems);

                for (uint32_t r = row_start; r < row_end; r++) {
                    if (l_ship[r] <= DATE_THRESH) continue;   // l_shipdate > 9204
                    int32_t okey = l_okey[r];
                    if (!bloom_check(bloom, okey)) continue;  // bloom filter

                    // Probe orders hash map
                    uint32_t h = ord_hash(okey);
                    int32_t odate = EMPTY_KEY, sprio = 0;
                    for (uint32_t probe = 0; probe < ORD_CAP; probe++) {
                        uint32_t slot = (h + probe) & ORD_MASK;
                        if (ord_map[slot].key == EMPTY_KEY) break;
                        if (ord_map[slot].key == okey) {
                            odate = ord_map[slot].o_orderdate;
                            sprio = ord_map[slot].o_shippriority;
                            break;
                        }
                    }
                    if (odate == EMPTY_KEY) continue; // not in qualifying orders

                    // Compute revenue
                    double rev = l_epx[r] * (1.0 - l_disc[r]);

                    // Insert/update into thread-local agg map
                    uint32_t ah = agg_hash(okey);
                    for (uint32_t probe = 0; probe < AGG_CAP; probe++) {
                        uint32_t slot = (ah + probe) & AGG_MASK;
                        if (local_agg[slot].orderkey == EMPTY_KEY) {
                            local_agg[slot].orderkey      = okey;
                            local_agg[slot].o_orderdate   = odate;
                            local_agg[slot].o_shippriority = sprio;
                            local_agg[slot].revenue       = rev;
                            break;
                        }
                        if (local_agg[slot].orderkey == okey) {
                            local_agg[slot].revenue += rev;
                            break;
                        }
                    }
                }
            }
        }
    }

    // ── Phase 4: Merge thread-local agg maps + top-10 ─────────────────────────
    // Global agg map for merging (same size as per-thread)
    AggEntry* global_agg = new AggEntry[AGG_CAP];
    for (uint32_t i = 0; i < AGG_CAP; i++) global_agg[i].orderkey = EMPTY_KEY;

    {
        GENDB_PHASE("aggregation_merge");

        for (int tid = 0; tid < NTHREADS; tid++) {
            AggEntry* local_agg = agg_maps + (size_t)tid * AGG_CAP;
            for (uint32_t i = 0; i < AGG_CAP; i++) {
                if (local_agg[i].orderkey == EMPTY_KEY) continue;
                int32_t okey = local_agg[i].orderkey;
                double  rev  = local_agg[i].revenue;
                int32_t odate = local_agg[i].o_orderdate;
                int32_t sprio = local_agg[i].o_shippriority;

                uint32_t h = agg_hash(okey);
                for (uint32_t probe = 0; probe < AGG_CAP; probe++) {
                    uint32_t slot = (h + probe) & AGG_MASK;
                    if (global_agg[slot].orderkey == EMPTY_KEY) {
                        global_agg[slot].orderkey      = okey;
                        global_agg[slot].o_orderdate   = odate;
                        global_agg[slot].o_shippriority = sprio;
                        global_agg[slot].revenue       = rev;
                        break;
                    }
                    if (global_agg[slot].orderkey == okey) {
                        global_agg[slot].revenue += rev;
                        break;
                    }
                }
            }
        }
    }

    // Top-10: revenue DESC, o_orderdate ASC
    // Use min-heap of size 10; comparator: larger revenue = higher priority (so heap keeps smallest)
    // Comparator for min-heap: element with lower priority (revenue or later date) is at top to be evicted
    struct Result {
        int32_t orderkey, o_orderdate, o_shippriority;
        double  revenue;
    };
    // Min-heap: element to evict = smallest revenue (or latest date on tie)
    auto cmp = [](const Result& a, const Result& b) {
        // Returns true if a has higher priority (should be kept over b)
        // For min-heap used as top-10 descending, we want the "worst" element at top
        if (a.revenue != b.revenue) return a.revenue > b.revenue; // larger revenue = better, stays
        return a.o_orderdate < b.o_orderdate; // earlier date = better, stays
    };
    // Actually we need: the min-heap should evict elements with the lowest priority
    // So the comparator for std::priority_queue (max-heap by default) to act as min-heap:
    // We want the element with lowest priority at top → use: a < b means a has higher priority
    // Let's just collect all results and sort
    std::vector<Result> results;
    results.reserve(400000);

    {
        GENDB_PHASE("sort_topk");
        for (uint32_t i = 0; i < AGG_CAP; i++) {
            if (global_agg[i].orderkey == EMPTY_KEY) continue;
            results.push_back({global_agg[i].orderkey,
                               global_agg[i].o_orderdate,
                               global_agg[i].o_shippriority,
                               global_agg[i].revenue});
        }

        // Partial sort: top 10 by revenue DESC, o_orderdate ASC
        auto sort_cmp = [](const Result& a, const Result& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.o_orderdate < b.o_orderdate;
        };
        if (results.size() > 10) {
            std::partial_sort(results.begin(), results.begin() + 10, results.end(), sort_cmp);
            results.resize(10);
        } else {
            std::sort(results.begin(), results.end(), sort_cmp);
        }
    }

    // ── Output ────────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q3.csv";
        std::ofstream out(out_path);
        if (!out) { std::cerr << "Cannot open output: " << out_path << "\n"; exit(1); }
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed;
        out.precision(2);
        char date_buf[16];
        for (auto& r : results) {
            gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
            out << r.orderkey << ","
                << r.revenue << ","
                << date_buf << ","
                << r.o_shippriority << "\n";
        }
    }

    // Cleanup
    delete[] cust_bitset;
    delete[] ord_map;
    delete[] bloom;
    delete[] agg_maps;
    delete[] global_agg;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
