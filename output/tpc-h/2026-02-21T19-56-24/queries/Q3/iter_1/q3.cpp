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

// Dense-index approach: qualifying orders get index 0..N-1 at build time.
// Thread-local revenue = flat double array indexed by dense_idx (no hash maps needed).

// Bloom filter over qualifying order keys
static constexpr uint64_t BLOOM_BITS  = 150994944ULL; // ~18 MB
static constexpr uint64_t BLOOM_WORDS = BLOOM_BITS / 64;
static constexpr uint64_t BLOOM_MASK_B = BLOOM_BITS - 1;

// ─── Structs ──────────────────────────────────────────────────────────────────
struct OrderEntry {
    int32_t key;           // o_orderkey, EMPTY_KEY if vacant
    int32_t o_orderdate;
    int32_t o_shippriority;
    int32_t dense_idx;     // dense 0-based index assigned after build phase
};

// Compact representation for sorted output
struct QualOrder {
    int32_t okey, odate, sprio;
};

// ─── Hash helpers ─────────────────────────────────────────────────────────────
static inline uint32_t ord_hash(int32_t k)  { return ((uint32_t)k * 2654435761U) & ORD_MASK; }

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
    // ord_list populated after build; maps dense_idx → (okey, odate, sprio)
    std::vector<QualOrder> ord_list;
    ord_list.reserve(1500000);

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

        // Assign dense_idx to each qualifying order; populate ord_list
        for (uint32_t i = 0; i < ORD_CAP; i++) {
            if (ord_map[i].key != EMPTY_KEY) {
                ord_map[i].dense_idx = (int32_t)ord_list.size();
                ord_list.push_back({ord_map[i].key, ord_map[i].o_orderdate, ord_map[i].o_shippriority});
            }
        }
    }

    // ── Phase 3: Parallel lineitem scan + thread-local aggregation ────────────
    // Flat per-thread revenue arrays indexed by dense_idx.
    // 64 threads × ~1.47M orders × 8B ≈ 750 MB (vs 6.4 GB for per-thread hash maps).
    const uint32_t n_qual_orders = (uint32_t)ord_list.size();
    // calloc gives OS-zeroed pages; page faults deferred to first access (cheap).
    double* tl_revenue = (double*)calloc((size_t)NTHREADS * n_qual_orders, sizeof(double));

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
            double* local_rev = tl_revenue + (size_t)tid * n_qual_orders;

            while (true) {
                uint32_t bi = block_cursor.fetch_add(1, std::memory_order_relaxed);
                if (bi >= (uint32_t)li_blocks.size()) break;
                uint32_t block_id = li_blocks[bi];
                uint32_t row_start = block_id * BLOCK_SIZE;
                uint32_t row_end   = std::min(row_start + BLOCK_SIZE, (uint32_t)n_lineitems);

                for (uint32_t r = row_start; r < row_end; r++) {
                    if (l_ship[r] <= DATE_THRESH) continue;   // l_shipdate > 9204
                    int32_t okey = l_okey[r];
                    if (!bloom_check(bloom, okey)) continue;  // bloom pre-filter

                    // Probe orders hash map → get dense_idx
                    uint32_t h = ord_hash(okey);
                    int32_t didx = -1;
                    for (uint32_t probe = 0; probe <= ORD_MASK; probe++) {
                        uint32_t slot = (h + probe) & ORD_MASK;
                        if (ord_map[slot].key == EMPTY_KEY) break;
                        if (ord_map[slot].key == okey) { didx = ord_map[slot].dense_idx; break; }
                    }
                    if (didx < 0) continue; // not in qualifying orders

                    // Accumulate revenue into flat array (no hash collision overhead)
                    local_rev[didx] += l_epx[r] * (1.0 - l_disc[r]);
                }
            }
        }
    }

    // ── Phase 4: Parallel reduce thread-local revenue arrays → global_revenue ──
    double* global_revenue = (double*)calloc(n_qual_orders, sizeof(double));

    {
        GENDB_PHASE("aggregation_merge");

        // Each thread sums its stripe; column-major reduction avoids false sharing.
        #pragma omp parallel for num_threads(NTHREADS) schedule(static)
        for (uint32_t i = 0; i < n_qual_orders; i++) {
            double sum = 0.0;
            for (int t = 0; t < NTHREADS; t++) {
                sum += tl_revenue[(size_t)t * n_qual_orders + i];
            }
            global_revenue[i] = sum;
        }
    }

    // Top-10: revenue DESC, o_orderdate ASC
    struct Result {
        int32_t orderkey, o_orderdate, o_shippriority;
        double  revenue;
    };
    std::vector<Result> results;
    results.reserve(n_qual_orders);

    {
        GENDB_PHASE("sort_topk");
        // ord_list is dense; only include orders that had at least one qualifying lineitem
        for (uint32_t i = 0; i < n_qual_orders; i++) {
            double rev = global_revenue[i];
            if (rev == 0.0) continue; // no qualifying lineitems joined to this order
            results.push_back({ord_list[i].okey, ord_list[i].odate, ord_list[i].sprio, rev});
        }

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
    free(tl_revenue);
    free(global_revenue);
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
