// Q3: Shipping Priority — GenDB iter_2
// Plan: customer bitset → parallel orders scan + compact 8-byte OrderHashMap (4M×8B=32MB) +
//       qualifying_orders bitset (7.5MB) → morsel-driven parallel lineitem scan with
//       zone-map skipping + flat thread-local AggHashMaps (64K×24B=1.5MB/thread) →
//       single-threaded sequential merge into flat global AggHashMap (2M×24B=48MB) → top-10
//
// Key correctness anchors:
//   revenue = l_extendedprice * (1.0 - l_discount/100)   [l_discount is int8_t hundredths]
//   o_orderdate < 9204   (DATE '1995-03-15')
//   l_shipdate  > 9204   (DATE '1995-03-15')

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <ctime>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <thread>
#include <atomic>
#include <string>
#include <chrono>

#include "timing_utils.h"
#include "mmap_utils.h"
#include "date_utils.h"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr int32_t  DATE_THRESH      = 9204;        // 1995-03-15
static constexpr uint32_t ORDER_MAP_CAP    = 1u << 22;    // 4M slots, power-of-2
static constexpr uint32_t ORDER_MAP_MASK   = ORDER_MAP_CAP - 1;
static constexpr uint32_t LOCAL_AGG_CAP    = 1u << 16;    // 64K slots, power-of-2
static constexpr uint32_t LOCAL_AGG_MASK   = LOCAL_AGG_CAP - 1;
static constexpr uint32_t GLOBAL_AGG_CAP   = 1u << 21;    // 2M slots, power-of-2
static constexpr uint32_t GLOBAL_AGG_MASK  = GLOBAL_AGG_CAP - 1;
static constexpr uint32_t QUAL_WORDS       = (60000001u + 63u) / 64u;  // 7.5MB

// Sentinel = 0 for all hash maps (TPC-H orderkeys are always >= 1)
static constexpr int32_t  SENTINEL         = 0;

// ---------------------------------------------------------------------------
// Compact OrderHashMap: 8 bytes/slot (4M × 8B = 32MB)
// Sentinel key = 0 (never a valid TPC-H orderkey)
// max qualifying orderdate is 9203 < INT16_MAX; shippriority 0-3 fits int8_t
// ---------------------------------------------------------------------------
struct OrderSlot {
    int32_t key;           // 0 = empty sentinel
    int16_t orderdate;     // days since epoch (max qualifying: 9203)
    int8_t  shippriority;  // TPC-H range 0-3
    int8_t  _pad;
};
static_assert(sizeof(OrderSlot) == 8, "OrderSlot must be 8 bytes");

inline uint32_t knuth32(int32_t k) { return (uint32_t)k * 2654435761u; }

inline void order_map_insert(OrderSlot* __restrict__ map, int32_t key,
                              int16_t odate, int8_t sprio) {
    uint32_t h = knuth32(key) & ORDER_MAP_MASK;
    while (map[h].key != SENTINEL) h = (h + 1u) & ORDER_MAP_MASK;
    map[h] = {key, odate, sprio, 0};
}

inline const OrderSlot* order_map_probe(const OrderSlot* __restrict__ map, int32_t key) {
    uint32_t h = knuth32(key) & ORDER_MAP_MASK;
    while (true) {
        if (__builtin_expect(map[h].key == key, 0)) return &map[h];
        if (__builtin_expect(map[h].key == SENTINEL, 0)) return nullptr;
        h = (h + 1u) & ORDER_MAP_MASK;
    }
}

// ---------------------------------------------------------------------------
// Flat AggHashMap: 24 bytes/slot
// Sentinel key = 0 (never a valid TPC-H orderkey)
// ---------------------------------------------------------------------------
struct AggSlot {
    int32_t key;           // 0 = empty sentinel
    int32_t orderdate;
    int32_t shippriority;
    int32_t _pad;
    double  revenue;
};
static_assert(sizeof(AggSlot) == 24, "AggSlot must be 24 bytes");

inline void agg_insert(AggSlot* __restrict__ map, uint32_t mask, int32_t key,
                        double rev, int32_t odate, int32_t sprio) {
    uint32_t h = knuth32(key) & mask;
    while (true) {
        if (map[h].key == key)     { map[h].revenue += rev; return; }
        if (map[h].key == SENTINEL){
            map[h].key = key; map[h].orderdate = odate;
            map[h].shippriority = sprio; map[h].revenue = rev;
            return;
        }
        h = (h + 1u) & mask;
    }
}

// ---------------------------------------------------------------------------
// Bitset helpers
// ---------------------------------------------------------------------------
inline void  bs_set(uint64_t* bs, int32_t i) { bs[i >> 6] |= 1ull << (i & 63); }
inline bool  bs_get(const uint64_t* bs, int32_t i) {
    return (bs[i >> 6] >> (i & 63)) & 1ull;
}

// ---------------------------------------------------------------------------
// Zone map (simple binary format)
// Header: uint32_t num_blocks, uint32_t block_size
// Entries: num_blocks × {int32_t min_date, int32_t max_date}
// ---------------------------------------------------------------------------
struct SimpleZone { int32_t min_date, max_date; };
struct ZoneMap {
    uint32_t num_blocks = 0, block_size = 0;
    std::vector<SimpleZone> zones;
    void load(const std::string& path) {
        FILE* f = fopen(path.c_str(), "rb");
        if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
        (void)fread(&num_blocks, 4, 1, f);
        (void)fread(&block_size, 4, 1, f);
        zones.resize(num_blocks);
        (void)fread(zones.data(), sizeof(SimpleZone), num_blocks, f);
        fclose(f);
    }
};

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
struct MmapFile {
    void* data = nullptr; size_t size = 0; int fd = -1;
    bool open(const std::string& path, bool seq = true) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st; fstat(fd, &st); size = st.st_size;
        if (!size) { data = nullptr; return true; }
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        madvise(data, size, seq ? MADV_SEQUENTIAL : MADV_RANDOM);
        return true;
    }
    void prefetch() { if (data && size > 0) madvise(data, size, MADV_WILLNEED); }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) ::close(fd);
    }
};

// ---------------------------------------------------------------------------
// Parallel memset: distribute memset across nthreads to avoid serial page faults
// ---------------------------------------------------------------------------
static void parallel_memset(void* ptr, int val, size_t bytes, int nthreads) {
    std::vector<std::thread> ths;
    ths.reserve(nthreads);
    size_t chunk = (bytes + nthreads - 1) / nthreads;
    for (int t = 0; t < nthreads; t++) {
        size_t off = (size_t)t * chunk;
        if (off >= bytes) break;
        size_t len = std::min(chunk, bytes - off);
        ths.emplace_back([ptr, val, off, len]() {
            memset((char*)ptr + off, val, len);
        });
    }
    for (auto& t : ths) t.join();
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    gendb::init_date_tables();
    auto t_total_start = std::chrono::high_resolution_clock::now();

    int nthreads = (int)std::thread::hardware_concurrency();
    if (nthreads < 1) nthreads = 1;

    // -------------------------------------------------------------------------
    // Data loading: mmap all columns; prefetch lineitem to overlap I/O
    // -------------------------------------------------------------------------
    MmapFile f_cmkt, f_custkey;
    MmapFile f_ocustkey, f_odate, f_okey, f_oprio;
    MmapFile f_lkey, f_lship, f_lext, f_ldisc;
    {
        GENDB_PHASE("data_loading");
        if (!f_cmkt.open(gendb_dir    + "/customer/c_mktsegment.bin"))     return 1;
        if (!f_custkey.open(gendb_dir + "/customer/c_custkey.bin"))         return 1;
        if (!f_ocustkey.open(gendb_dir + "/orders/o_custkey.bin"))          return 1;
        if (!f_odate.open(gendb_dir   + "/orders/o_orderdate.bin"))         return 1;
        if (!f_okey.open(gendb_dir    + "/orders/o_orderkey.bin"))          return 1;
        if (!f_oprio.open(gendb_dir   + "/orders/o_shippriority.bin"))      return 1;
        if (!f_lkey.open(gendb_dir    + "/lineitem/l_orderkey.bin"))        return 1;
        if (!f_lship.open(gendb_dir   + "/lineitem/l_shipdate.bin"))        return 1;
        if (!f_lext.open(gendb_dir    + "/lineitem/l_extendedprice.bin"))   return 1;
        if (!f_ldisc.open(gendb_dir   + "/lineitem/l_discount.bin"))        return 1;
        f_lkey.prefetch(); f_lship.prefetch();
        f_lext.prefetch(); f_ldisc.prefetch();
    }

    const int8_t*  c_mktsegment  = (const int8_t*)  f_cmkt.data;
    const int32_t* c_custkey_col = (const int32_t*) f_custkey.data;
    const int32_t* o_custkey_col = (const int32_t*) f_ocustkey.data;
    const int32_t* o_orderdate   = (const int32_t*) f_odate.data;
    const int32_t* o_orderkey    = (const int32_t*) f_okey.data;
    const int32_t* o_shipprio    = (const int32_t*) f_oprio.data;
    const int32_t* l_orderkey    = (const int32_t*) f_lkey.data;
    const int32_t* l_shipdate    = (const int32_t*) f_lship.data;
    const double*  l_extprice    = (const double*)  f_lext.data;
    const int8_t*  l_discount    = (const int8_t*)  f_ldisc.data;

    const size_t n_cust = f_cmkt.size;
    const size_t n_ord  = f_okey.size  / sizeof(int32_t);
    const size_t n_li   = f_lkey.size  / sizeof(int32_t);

    // -------------------------------------------------------------------------
    // Phase 1: dim_filter — build cust_ok[1500001] (1.5MB, L3-resident)
    // -------------------------------------------------------------------------
    std::vector<uint8_t> cust_ok(1500001, 0);
    {
        GENDB_PHASE("dim_filter");

        // Resolve 'BUILDING' code from dict (never hardcode)
        char mkt_dict[5][16];
        {
            std::string dp = gendb_dir + "/customer/c_mktsegment_dict.bin";
            FILE* f = fopen(dp.c_str(), "rb");
            if (!f) { perror(dp.c_str()); return 1; }
            (void)fread(mkt_dict, 16, 5, f); fclose(f);
        }
        int8_t building_code = -1;
        for (int i = 0; i < 5; i++) {
            if (strncmp(mkt_dict[i], "BUILDING", 8) == 0) { building_code = (int8_t)i; break; }
        }
        if (building_code < 0) { fprintf(stderr, "BUILDING not in dict\n"); return 1; }

        uint8_t* cok = cust_ok.data();
        for (size_t i = 0; i < n_cust; i++) {
            if (c_mktsegment[i] == building_code) cok[(uint32_t)c_custkey_col[i]] = 1;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 2: build_joins
    // 2a. Allocate order_map and qual_orders; parallel pre-touch order_map
    // 2b. Parallel orders scan → thread-local vectors
    // 2c. Single-threaded merge into order_map + qual_orders bitset
    // -------------------------------------------------------------------------
    OrderSlot* order_map   = nullptr;
    uint64_t*  qual_orders = nullptr;
    {
        GENDB_PHASE("build_joins");

        // Allocate compact OrderHashMap (4M × 8B = 32MB) and pre-touch in parallel
        // to avoid serial page fault cost during merge_insert
        order_map = (OrderSlot*)malloc(ORDER_MAP_CAP * sizeof(OrderSlot));
        if (!order_map) { fprintf(stderr, "malloc order_map\n"); return 1; }
        // Parallel memset (sentinel=0) warms all 32MB before single-threaded merge
        parallel_memset(order_map, 0, ORDER_MAP_CAP * sizeof(OrderSlot), nthreads);

        // qualifying_orders bitset: 60M bits = 7.5MB (calloc is fast for this)
        qual_orders = (uint64_t*)calloc(QUAL_WORDS, sizeof(uint64_t));
        if (!qual_orders) { fprintf(stderr, "calloc qual_orders\n"); return 1; }

        // Thread-local order row vectors
        struct OrderRow { int32_t orderkey; int16_t orderdate; int8_t shippriority; };
        std::vector<std::vector<OrderRow>> local_orders(nthreads);

        // 2b. Parallel orders scan
        {
            std::vector<std::thread> threads;
            threads.reserve(nthreads);
            const size_t chunk = (n_ord + nthreads - 1) / nthreads;
            const uint8_t* cok = cust_ok.data();

            for (int t = 0; t < nthreads; t++) {
                threads.emplace_back([&, t]() {
                    size_t lo = (size_t)t * chunk;
                    size_t hi = std::min(lo + chunk, n_ord);
                    auto& lv = local_orders[t];
                    lv.reserve(32000);
                    for (size_t i = lo; i < hi; i++) {
                        int32_t ck = o_custkey_col[i];
                        if (!cok[(uint32_t)ck]) continue;
                        int32_t od = o_orderdate[i];
                        if (od >= DATE_THRESH) continue;
                        lv.push_back({o_orderkey[i], (int16_t)od, (int8_t)o_shipprio[i]});
                    }
                });
            }
            for (auto& th : threads) th.join();
        }

        // 2c. Single-threaded merge → qual_orders bitset + compact order_map
        for (int t = 0; t < nthreads; t++) {
            for (const auto& row : local_orders[t]) {
                int32_t k = row.orderkey;
                bs_set(qual_orders, k);
                order_map_insert(order_map, k, row.orderdate, row.shippriority);
            }
            // Free immediately to reduce peak memory
            { std::vector<OrderRow> tmp; tmp.swap(local_orders[t]); }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 3: main_scan — morsel-driven parallel lineitem scan with
    //          flat thread-local AggHashMaps (64K × 24B = 1.5MB/thread)
    //          Each thread allocates+initializes its own AggHashMap (parallel init)
    // -------------------------------------------------------------------------
    // Load zone map
    ZoneMap zm;
    zm.load(gendb_dir + "/lineitem/l_shipdate_zone_map.bin");

    struct BlockRange { size_t start, end; };
    std::vector<BlockRange> qblocks;
    qblocks.reserve(400);
    for (uint32_t b = 0; b < zm.num_blocks; b++) {
        if (zm.zones[b].max_date <= DATE_THRESH) continue;
        size_t rs = (size_t)b * zm.block_size;
        size_t re = std::min(rs + (size_t)zm.block_size, n_li);
        qblocks.push_back({rs, re});
    }

    // Each thread allocates its OWN local_agg (first-touch in parallel → no serial init cost)
    std::vector<AggSlot*> local_agg(nthreads, nullptr);

    {
        GENDB_PHASE("main_scan");

        std::atomic<uint32_t> blk_cursor{0};
        const uint32_t nblocks = (uint32_t)qblocks.size();

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                // Allocate+zero-initialize thread-local AggHashMap here (in parallel!)
                // calloc uses zero pages → sentinel key=0 (valid since TPC-H keys >= 1)
                AggSlot* lagg = (AggSlot*)calloc(LOCAL_AGG_CAP, sizeof(AggSlot));
                local_agg[t] = lagg;

                const uint64_t*   __restrict__ qo = qual_orders;
                const OrderSlot*  __restrict__ om = order_map;

                while (true) {
                    uint32_t bi = blk_cursor.fetch_add(1, std::memory_order_relaxed);
                    if (bi >= nblocks) break;

                    size_t row_start = qblocks[bi].start;
                    size_t row_end   = qblocks[bi].end;

                    const int32_t* __restrict__ ok   = l_orderkey + row_start;
                    const int32_t* __restrict__ sd   = l_shipdate  + row_start;
                    const double*  __restrict__ ep   = l_extprice  + row_start;
                    const int8_t*  __restrict__ disc = l_discount  + row_start;
                    size_t cnt = row_end - row_start;

                    for (size_t i = 0; i < cnt; i++) {
                        // Filter 1: l_shipdate > DATE_THRESH
                        if (sd[i] <= DATE_THRESH) continue;

                        int32_t okey = ok[i];

                        // Filter 2: qualifying_orders bitset prefilter
                        if (!bs_get(qo, okey)) continue;

                        // Filter 3: compact OrderHashMap probe (L3-resident)
                        const OrderSlot* os = order_map_probe(om, okey);
                        if (__builtin_expect(!os, 0)) continue;

                        // Revenue: l_extendedprice * (1 - l_discount/100)
                        double rev = ep[i] * (1.0 - disc[i] * 0.01);

                        // Insert into flat thread-local AggHashMap
                        agg_insert(lagg, LOCAL_AGG_MASK, okey, rev,
                                   (int32_t)os->orderdate,
                                   (int32_t)os->shippriority);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // Release order_map and qual_orders now (no longer needed; free 39.5MB from LLC)
    free(order_map); order_map = nullptr;
    free(qual_orders); qual_orders = nullptr;

    // -------------------------------------------------------------------------
    // Phase 4: sequential_flat_merge — merge local AggHashMaps into global
    //          flat AggHashMap (2M × 24B = 48MB); pre-touch with parallel memset
    // -------------------------------------------------------------------------
    AggSlot* global_agg = (AggSlot*)malloc(GLOBAL_AGG_CAP * sizeof(AggSlot));
    if (!global_agg) { fprintf(stderr, "malloc global_agg\n"); return 1; }
    // Parallel zero-init (sentinel=0) to warm all 48MB pages before sequential merge
    parallel_memset(global_agg, 0, GLOBAL_AGG_CAP * sizeof(AggSlot), nthreads);

    {
        GENDB_PHASE("sequential_flat_merge");
        for (int t = 0; t < nthreads; t++) {
            AggSlot* lagg = local_agg[t];
            if (!lagg) continue;
            for (uint32_t s = 0; s < LOCAL_AGG_CAP; s++) {
                if (lagg[s].key == SENTINEL) continue;
                agg_insert(global_agg, GLOBAL_AGG_MASK,
                           lagg[s].key, lagg[s].revenue,
                           lagg[s].orderdate, lagg[s].shippriority);
            }
            free(lagg); local_agg[t] = nullptr;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 5: top-k + output
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct Result { int32_t key, orderdate, shippriority; double revenue; };
        std::vector<Result> results;
        results.reserve(1500000);
        for (uint32_t s = 0; s < GLOBAL_AGG_CAP; s++) {
            if (global_agg[s].key != SENTINEL) {
                results.push_back({global_agg[s].key, global_agg[s].orderdate,
                                   global_agg[s].shippriority, global_agg[s].revenue});
            }
        }
        free(global_agg); global_agg = nullptr;

        // partial_sort top-10: revenue DESC, o_orderdate ASC
        const size_t topk = std::min(results.size(), (size_t)10);
        std::partial_sort(results.begin(), results.begin() + topk, results.end(),
            [](const Result& a, const Result& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });

        // Write CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }
        fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[11];
        for (size_t i = 0; i < topk; i++) {
            gendb::epoch_days_to_date_str(results[i].orderdate, date_buf);
            fprintf(out, "%d,%.4f,%s,%d\n",
                    results[i].key, results[i].revenue, date_buf, results[i].shippriority);
        }
        fclose(out);
    }

    // Total timing
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(
                          t_total_end - t_total_start).count();
#ifdef GENDB_PROFILE
    fprintf(stderr, "[TIMING] total: %.2f ms\n", total_ms);
#endif
    (void)total_ms;
    return 0;
}
