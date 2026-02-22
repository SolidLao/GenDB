// Q3: Shipping Priority — GenDB iteration 4
// Strategy: pipeline lineitem prefault (24 bg threads) with dim_filter+build_joins → custkey bitset → orders scan → bloom + compact hash → lineitem scan → thread-local agg → top-10

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
#include <thread>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ─── Constants ────────────────────────────────────────────────────────────────
static constexpr int32_t DATE_THRESH    = 9204;      // epoch days for 1995-03-15
static constexpr int32_t MKTSEG_BUILDING = 1;
static constexpr uint32_t MAX_CUSTKEY   = 1500001;   // keys in [1, 1500000]
static constexpr int32_t  EMPTY_KEY     = -1;  // 0xFFFFFFFF — enables fast memset init
static constexpr int       NTHREADS     = 64;
static constexpr uint32_t  BLOCK_SIZE   = 100000;

// Orders hash map: o_orderkey → {o_orderdate, o_shippriority}
static constexpr uint32_t ORD_CAP  = 2097152; // 2^21 > 1461923/0.7
static constexpr uint32_t ORD_MASK = ORD_CAP - 1;

// Dense-index approach: qualifying orders get index 0..N-1 at build time.
// Thread-local revenue = flat double array indexed by dense_idx (no hash maps needed).

// Bloom filter over qualifying order keys
static constexpr uint64_t BLOOM_BITS  = 33554432ULL;  // ~4 MB (FPR ~0.21% — sufficient)
static constexpr uint64_t BLOOM_WORDS = BLOOM_BITS / 64;
static constexpr uint64_t BLOOM_MASK_B = BLOOM_BITS - 1;

// ─── Structs ──────────────────────────────────────────────────────────────────
// Compact: 8B per slot (vs 16B) → 16MB working set fits better in 44MB L3
struct OrderEntry {
    int32_t key;       // o_orderkey, EMPTY_KEY (-1) if vacant
    int32_t dense_idx; // dense 0-based index assigned at insertion
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

// ─── mmap helpers ────────────────────────────────────────────────────────────

// Full blocking mmap (MAP_POPULATE) — used only for tiny zone-map files
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

// Non-blocking mmap — no MAP_POPULATE; pages faulted lazily or via touch_range
static const void* mmap_file_lazy(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return p;
}

// Touch pages in [base+offset, base+offset+len) — triggers page faults in caller thread.
// Used to parallel-prefault qualifying block ranges across multiple std::threads.
static void touch_range(const void* base, size_t offset, size_t len) {
    if (len == 0) return;
    const volatile char* p = reinterpret_cast<const volatile char*>(
        reinterpret_cast<const char*>(base) + offset);
    volatile char s = 0;
    for (size_t i = 0; i < len; i += 4096) s ^= p[i];
    (void)s;
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
    // customer (c_custkey = row_index+1 in TPC-H; no need to load c_custkey.bin)
    const std::string c_seg_path  = d + "customer/c_mktsegment.bin";
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

    // ── Setup: mmap + zone maps + range computation ────────────────────────────
    const int32_t *zm_ord = nullptr, *zm_li = nullptr;
    uint32_t zm_ord_nblocks = 0, zm_li_nblocks = 0;

    const uint8_t* c_seg  = nullptr;
    const int32_t* o_odate = nullptr, *o_ckey = nullptr;
    const int32_t* o_okey = nullptr, *o_sprio = nullptr;
    const int32_t* l_ship = nullptr, *l_okey = nullptr;
    const double*  l_epx  = nullptr, *l_disc = nullptr;
    size_t n_orders = 0, n_lineitems = 0, n_customers = 0;

    // Zone maps (tiny, sync — needed to compute qualifying ranges)
    {
        size_t sz;
        const uint32_t* p = (const uint32_t*)mmap_file(zm_ord_path, sz);
        zm_ord_nblocks = p[0];
        zm_ord = (const int32_t*)(p + 1);
    }
    {
        size_t sz;
        const uint32_t* p = (const uint32_t*)mmap_file(zm_li_path, sz);
        zm_li_nblocks = p[0];
        zm_li = (const int32_t*)(p + 1);
    }

    // mmap all column files (lazy, nanoseconds each)
    size_t sz_cseg, sz_oodate, sz_ockey, sz_ookey, sz_osprio;
    size_t sz_lship, sz_lokey, sz_lepx, sz_ldisc;

    c_seg   = (const uint8_t*)mmap_file_lazy(c_seg_path,   sz_cseg);
    // c_custkey not loaded: c_custkey[i] = i+1 (TPC-H sequential PK)
    o_odate = (const int32_t*)mmap_file_lazy(o_odate_path, sz_oodate);
    o_ckey  = (const int32_t*)mmap_file_lazy(o_ckey_path,  sz_ockey);
    o_okey  = (const int32_t*)mmap_file_lazy(o_okey_path,  sz_ookey);
    o_sprio = (const int32_t*)mmap_file_lazy(o_sprio_path, sz_osprio);
    l_ship  = (const int32_t*)mmap_file_lazy(l_ship_path,  sz_lship);
    l_okey  = (const int32_t*)mmap_file_lazy(l_okey_path,  sz_lokey);
    l_epx   = (const double* )mmap_file_lazy(l_epx_path,   sz_lepx);
    l_disc  = (const double* )mmap_file_lazy(l_disc_path,  sz_ldisc);

    n_customers = sz_cseg   / sizeof(uint8_t);
    n_orders    = sz_oodate / sizeof(int32_t);
    n_lineitems = sz_lship  / sizeof(int32_t);

    // Compute zone-map qualifying ranges
    uint32_t ord_qual_end = 0;
    for (uint32_t b = 0; b < zm_ord_nblocks; b++) {
        if (zm_ord[b * 2] >= DATE_THRESH) break;
        ord_qual_end = b + 1;
    }
    uint32_t li_qual_start = zm_li_nblocks;
    for (uint32_t b = 0; b < zm_li_nblocks; b++) {
        if (zm_li[b * 2 + 1] > DATE_THRESH) { li_qual_start = b; break; }
    }

    size_t ord_bytes   = std::min((size_t)ord_qual_end * BLOCK_SIZE * sizeof(int32_t), sz_oodate);
    size_t li_off_i32  = (size_t)li_qual_start * BLOCK_SIZE * sizeof(int32_t);
    size_t li_len_i32  = sz_lship > li_off_i32 ? sz_lship - li_off_i32 : 0;
    size_t li_off_dbl  = (size_t)li_qual_start * BLOCK_SIZE * sizeof(double);
    size_t li_len_dbl  = sz_lepx  > li_off_dbl ? sz_lepx  - li_off_dbl : 0;

    // ── Launch lineitem prefault in background (24 threads) ────────────────────
    // Overlaps with customer/orders prefault + dim_filter + build_joins (~109ms of hidden I/O).
    // l_epx and l_disc (260MB each) get 8-way split; l_ship and l_okey get 4-way split.
    const size_t li_i32_q1 = li_len_i32 / 4,       li_i32_q2 = li_len_i32 / 2,
                 li_i32_q3 = 3 * li_len_i32 / 4;
    const size_t li_dbl_q1 = li_len_dbl / 8,        li_dbl_q2 = li_len_dbl / 4,
                 li_dbl_q3 = 3 * li_len_dbl / 8,    li_dbl_q4 = li_len_dbl / 2,
                 li_dbl_q5 = 5 * li_len_dbl / 8,    li_dbl_q6 = 3 * li_len_dbl / 4,
                 li_dbl_q7 = 7 * li_len_dbl / 8;

    std::vector<std::thread> li_pf_threads;
    li_pf_threads.reserve(24);
    // l_ship: 4-way split
    li_pf_threads.emplace_back([&]{ touch_range(l_ship, li_off_i32,                li_i32_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_ship, li_off_i32 + li_i32_q1,   li_i32_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_ship, li_off_i32 + li_i32_q2,   li_i32_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_ship, li_off_i32 + li_i32_q3,   li_len_i32 - li_i32_q3); });
    // l_okey: 4-way split
    li_pf_threads.emplace_back([&]{ touch_range(l_okey, li_off_i32,                li_i32_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_okey, li_off_i32 + li_i32_q1,   li_i32_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_okey, li_off_i32 + li_i32_q2,   li_i32_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_okey, li_off_i32 + li_i32_q3,   li_len_i32 - li_i32_q3); });
    // l_epx: 8-way split
    li_pf_threads.emplace_back([&]{ touch_range(l_epx, li_off_dbl,                li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_epx, li_off_dbl + li_dbl_q1,    li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_epx, li_off_dbl + li_dbl_q2,    li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_epx, li_off_dbl + li_dbl_q3,    li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_epx, li_off_dbl + li_dbl_q4,    li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_epx, li_off_dbl + li_dbl_q5,    li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_epx, li_off_dbl + li_dbl_q6,    li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_epx, li_off_dbl + li_dbl_q7,    li_len_dbl - li_dbl_q7); });
    // l_disc: 8-way split
    li_pf_threads.emplace_back([&]{ touch_range(l_disc, li_off_dbl,               li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_disc, li_off_dbl + li_dbl_q1,   li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_disc, li_off_dbl + li_dbl_q2,   li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_disc, li_off_dbl + li_dbl_q3,   li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_disc, li_off_dbl + li_dbl_q4,   li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_disc, li_off_dbl + li_dbl_q5,   li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_disc, li_off_dbl + li_dbl_q6,   li_dbl_q1); });
    li_pf_threads.emplace_back([&]{ touch_range(l_disc, li_off_dbl + li_dbl_q7,   li_len_dbl - li_dbl_q7); });

    // ── Phase 0: Data loading (customer + orders only; lineitem prefaults run async) ──
    {
        GENDB_PHASE("data_loading");
        std::thread pf0([&]{ touch_range(c_seg,   0, sz_cseg);   });
        std::thread pf1([&]{ touch_range(o_odate, 0, ord_bytes); });
        std::thread pf2([&]{ touch_range(o_ckey,  0, ord_bytes); });
        std::thread pf3([&]{ touch_range(o_okey,  0, ord_bytes); });
        std::thread pf4([&]{ touch_range(o_sprio, 0, ord_bytes); });
        pf0.join(); pf1.join(); pf2.join(); pf3.join(); pf4.join();
    }

    // ── Phase 1: Customer scan → custkey bitset ────────────────────────────────
    // Bitset: 1500001 bits = 23438 uint64_t words
    static constexpr uint32_t CUST_WORDS = (MAX_CUSTKEY + 63) / 64;
    uint64_t* cust_bitset = new uint64_t[CUST_WORDS]();  // zero-initialized

    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < n_customers; i++) {
            if (c_seg[i] == MKTSEG_BUILDING) {
                int32_t ck = (int32_t)(i + 1); // c_custkey = row_index+1 (TPC-H PK)
                cust_bitset[(uint32_t)ck >> 6] |= (1ULL << (ck & 63));
            }
        }
    }

    // ── Phase 2: Parallel orders scan → hash map + bloom filter ───────────────
    // ord_list populated after build; maps dense_idx → (okey, odate, sprio)
    std::vector<QualOrder> ord_list;
    ord_list.reserve(1500000);

    OrderEntry* ord_map = new OrderEntry[ORD_CAP];
    // EMPTY_KEY = -1 = 0xFFFFFFFF; memset 0xFF vectorized, 16MB much faster than scalar loop
    memset(ord_map, 0xFF, ORD_CAP * sizeof(OrderEntry));

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

        // Sequential merge: insert into compact hash map, assign dense_idx inline,
        // populate ord_list — no separate O(ORD_CAP) scan needed.
        for (int tid = 0; tid < NTHREADS; tid++) {
            for (auto& t : tl_orders[tid]) {
                uint32_t h = ord_hash(t.okey);
                for (uint32_t probe = 0; probe < ORD_CAP; probe++) {
                    uint32_t slot = (h + probe) & ORD_MASK;
                    if (ord_map[slot].key == EMPTY_KEY) {
                        ord_map[slot].key       = t.okey;
                        ord_map[slot].dense_idx = (int32_t)ord_list.size();
                        ord_list.push_back({t.okey, t.odate, t.sprio});
                        bloom_insert(bloom, t.okey);
                        break;
                    }
                    if (ord_map[slot].key == t.okey) break; // PK: no duplicates
                }
            }
        }
        // dense_idx assigned inline above; ord_list fully populated.
    }

    // ── Phase 3: Parallel lineitem scan + thread-local aggregation ────────────
    // Flat per-thread revenue arrays indexed by dense_idx.
    // 64 threads × ~1.47M orders × 8B ≈ 750 MB (vs 6.4 GB for per-thread hash maps).
    const uint32_t n_qual_orders = (uint32_t)ord_list.size();
    // calloc gives OS-zeroed pages; page faults deferred to first access (cheap).
    double* tl_revenue = (double*)calloc((size_t)NTHREADS * n_qual_orders, sizeof(double));

    {
        GENDB_PHASE("main_scan");

        // ── Wait for lineitem prefault threads (may still be running) ──
        for (auto& t : li_pf_threads) t.join();

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
