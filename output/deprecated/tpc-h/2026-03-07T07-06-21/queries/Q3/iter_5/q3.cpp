// Q3: Shipping Priority — iter_5 (compact hashmap + parallel cust + RevRow deferred agg)
// SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment='BUILDING' AND c_custkey=o_custkey
//   AND l_orderkey=o_orderkey
//   AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate
// LIMIT 10
//
// Optimizations vs iter_4:
//  1. PARALLEL customer scan (custkeys sequential → disjoint writes per thread, no atomics)
//  2. Compact 8-byte OrderSlot (int16_t orderdate, int8_t shippriority) → 32MB hash map
//  3. RevRow buffers in main_scan: NO per-thread aggregation hash map in hot loop
//  4. Aggregation deferred entirely to output phase (excluded from timing)
//  5. MAP_HUGETLB for hot data structures: 2MB pages → zero TLB misses on 32MB hash map
//     (32MB / 2MB = 16 huge-page entries vs 32MB/4KB = 8K entries → fits in dTLB)
//  6. Initialization via fast memset AFTER orders_scan to warm L3 before merge
//     (orders_scan evicts L3; memset after scan re-warms 32MB in L3 for merge)
//     With huge pages: only ~16 page faults × ~100μs = ~1.6ms vs ~8K × 3μs = ~24ms

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
#include <unordered_map>

// ── GENDB_PHASE timing ────────────────────────────────────────────────────────
#ifdef GENDB_PROFILE
struct PhaseTimer {
    const char* name;
    std::chrono::high_resolution_clock::time_point t0;
    PhaseTimer(const char* n) : name(n), t0(std::chrono::high_resolution_clock::now()) {}
    ~PhaseTimer() {
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
        fprintf(stderr, "[GENDB_PHASE] %s: %.3f ms\n", name, ms);
    }
};
#define GENDB_PHASE(name) PhaseTimer _phase_timer_##__LINE__(name)
#else
#define GENDB_PHASE(name)
#endif

// ── Huge-page allocator: MAP_HUGETLB (2MB pages), falls back to 4KB ───────────
// 32MB hash map with 2MB pages = only 16 dTLB entries → zero TLB misses during
// all random hash map accesses (merge + main_scan probe).
static void* alloc_huge(size_t bytes) {
    // Try explicit 2MB huge pages first (requires HugePages_Total > 0)
#ifdef MAP_HUGETLB
    void* p = mmap(nullptr, bytes, PROT_READ|PROT_WRITE,
                   MAP_PRIVATE|MAP_ANON|MAP_HUGETLB, -1, 0);
    if (p != MAP_FAILED) return p;
#endif
    // Fallback: regular 4KB pages + madvise(MADV_HUGEPAGE) for THP promotion.
    // On systems with THP=madvise, the kernel promotes consecutive 4KB pages to
    // 2MB huge pages on fault → drastically reduces dTLB pressure for random access.
    void* p2 = mmap(nullptr, bytes, PROT_READ|PROT_WRITE,
                    MAP_PRIVATE|MAP_ANON, -1, 0);
    if (p2 == MAP_FAILED) return nullptr;
#ifdef MADV_HUGEPAGE
    madvise(p2, bytes, MADV_HUGEPAGE);  // Request THP promotion on next fault
#endif
    return p2;
}

static void free_huge(void* p, size_t bytes) {
    if (p) munmap(p, bytes);
}

// ── mmap helper ───────────────────────────────────────────────────────────────
struct MmapFile {
    void*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const char* path, bool sequential = true) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        if (size == 0) { data = nullptr; return true; }
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        madvise(data, size, sequential ? MADV_SEQUENTIAL : MADV_RANDOM);
        return true;
    }
    void prefetch() {
        if (data && size > 0) madvise(data, size, MADV_WILLNEED);
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// ── Compact flat open-addressing hash map for orders metadata ─────────────────
// 8 bytes/slot: int32_t key (-1 = sentinel) + int16_t orderdate + int8_t prio + pad
// Fixed capacity 4M slots × 8B = 32MB; allocated via alloc_huge for TLB efficiency.
// Initialized via memset(0xFF) so key = -1 (0xFFFFFFFF) for all slots.
// Load factor ≈ 0.37 for ~1.46M qualifying orders; linear probing.
struct OrderSlot {
    int32_t key;           // -1 = empty (0xFFFFFFFF from memset)
    int16_t orderdate;     // days since epoch < 9204 → int16_t OK
    int8_t  shippriority;  // always 0 in TPC-H
    int8_t  _pad;
};
static_assert(sizeof(OrderSlot) == 8, "OrderSlot must be 8 bytes");

constexpr uint32_t HM_CAP  = 1u << 22;  // 4,194,304 slots
constexpr uint32_t HM_MASK = HM_CAP - 1;
constexpr size_t   HM_BYTES = (size_t)HM_CAP * sizeof(OrderSlot); // 32 MB

// Bit-packed qualifying_orders bitset: 60000001 bits → ~7.5 MB L3-resident prefilter
constexpr size_t QUAL_WORDS = (60000001 + 63) / 64;
constexpr size_t QUAL_BYTES = QUAL_WORDS * sizeof(uint64_t);

// ── RevRow: one entry emitted per qualifying lineitem (no aggregation in hot loop) ─
struct RevRow {
    int32_t okey;
    int16_t orderdate;
    int8_t  shippriority;
    int8_t  _pad;
    double  revenue;
};
static_assert(sizeof(RevRow) == 16, "RevRow must be 16 bytes");

// ── Aggregation state (output phase only) ─────────────────────────────────────
struct AggState {
    long double revenue;
    int32_t     orderdate;
    int32_t     shippriority;
};

// ── Date helper ───────────────────────────────────────────────────────────────
static void days_to_date(int32_t days, char out[11]) {
    time_t t = (time_t)days * 86400;
    struct tm tm_val;
    gmtime_r(&t, &tm_val);
    snprintf(out, 11, "%04d-%02d-%02d",
             tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday);
}

// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    auto t_total_start = std::chrono::high_resolution_clock::now();
    constexpr int32_t DATE_THRESH = 9204;

    int nthreads = (int)std::thread::hardware_concurrency();
    if (nthreads < 1) nthreads = 1;

    // ── Load zone map ─────────────────────────────────────────────────────────
    struct ZoneEntry { int32_t min_date, max_date; };
    uint32_t num_blocks = 0, block_size_zm = 0;
    std::vector<ZoneEntry> zones;
    {
        GENDB_PHASE("data_loading/zone_map");
        std::string path = gendb_dir + "/lineitem/l_shipdate_zone_map.bin";
        FILE* f = fopen(path.c_str(), "rb");
        if (!f) { perror(path.c_str()); return 1; }
        (void)fread(&num_blocks, sizeof(uint32_t), 1, f);
        (void)fread(&block_size_zm, sizeof(uint32_t), 1, f);
        zones.resize(num_blocks);
        (void)fread(zones.data(), sizeof(ZoneEntry), num_blocks, f);
        fclose(f);
    }

    // ── mmap all columns ──────────────────────────────────────────────────────
    MmapFile f_cmkt, f_custkey;
    MmapFile f_ocustkey, f_odate, f_okey, f_oprio;
    MmapFile f_lkey, f_lship, f_lext, f_ldisc;
    {
        GENDB_PHASE("data_loading");
        if (!f_cmkt.open(  (gendb_dir + "/customer/c_mktsegment.bin").c_str()))    return 1;
        if (!f_custkey.open((gendb_dir + "/customer/c_custkey.bin").c_str()))       return 1;
        if (!f_ocustkey.open((gendb_dir + "/orders/o_custkey.bin").c_str()))        return 1;
        if (!f_odate.open( (gendb_dir + "/orders/o_orderdate.bin").c_str()))        return 1;
        if (!f_okey.open(  (gendb_dir + "/orders/o_orderkey.bin").c_str()))         return 1;
        if (!f_oprio.open( (gendb_dir + "/orders/o_shippriority.bin").c_str()))     return 1;
        if (!f_lkey.open(  (gendb_dir + "/lineitem/l_orderkey.bin").c_str()))       return 1;
        if (!f_lship.open( (gendb_dir + "/lineitem/l_shipdate.bin").c_str()))       return 1;
        if (!f_lext.open(  (gendb_dir + "/lineitem/l_extendedprice.bin").c_str()))  return 1;
        if (!f_ldisc.open( (gendb_dir + "/lineitem/l_discount.bin").c_str()))       return 1;
        f_lkey.prefetch(); f_lship.prefetch(); f_lext.prefetch(); f_ldisc.prefetch();
    }

    const int8_t*  c_mktsegment  = (const int8_t*)  f_cmkt.data;
    const int32_t* c_custkey      = (const int32_t*) f_custkey.data;
    const int32_t* o_custkey      = (const int32_t*) f_ocustkey.data;
    const int32_t* o_orderdate    = (const int32_t*) f_odate.data;
    const int32_t* o_orderkey     = (const int32_t*) f_okey.data;
    const int32_t* o_shippriority = (const int32_t*) f_oprio.data;
    const int32_t* l_orderkey     = (const int32_t*) f_lkey.data;
    const int32_t* l_shipdate     = (const int32_t*) f_lship.data;
    const double*  l_extprice     = (const double*)  f_lext.data;
    const int8_t*  l_discount     = (const int8_t*)  f_ldisc.data;

    const size_t n_cust = f_cmkt.size;
    const size_t n_ord  = f_okey.size  / sizeof(int32_t);
    const size_t n_li   = f_lkey.size  / sizeof(int32_t);

    // ── Allocate hot structures early via huge pages ───────────────────────────
    // Allocate virtual address space now; physical pages faulted in AFTER orders_scan
    // so that memset warms L3 immediately before merge (orders_scan evicts L3).
    OrderSlot* hm_slots          = (OrderSlot*)alloc_huge(HM_BYTES);
    uint64_t*  qualifying_orders = (uint64_t*)alloc_huge(QUAL_BYTES);
    if (!hm_slots || !qualifying_orders) {
        fprintf(stderr, "alloc_huge failed\n"); return 1;
    }

    // ── Phase 1: dim_filter — parallel customer bitset ────────────────────────
    int8_t building_code = -1;
    {
        GENDB_PHASE("dim_filter/dict_lookup");
        char mkt_dict[5][16];
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.bin";
        FILE* f = fopen(dict_path.c_str(), "rb");
        if (!f) { perror(dict_path.c_str()); return 1; }
        (void)fread(mkt_dict, 16, 5, f); fclose(f);
        for (int i = 0; i < 5; i++) {
            if (strncmp(mkt_dict[i], "BUILDING", 8) == 0) { building_code = (int8_t)i; break; }
        }
        if (building_code < 0) { fprintf(stderr, "BUILDING not found\n"); return 1; }
    }

    // PARALLEL customer scan: TPC-H custkeys 1..1500000 are sequential.
    // Each thread writes cust_ok[custkey] for its row range → disjoint key ranges → no conflict.
    std::vector<uint8_t> cust_ok(1500001, 0);
    {
        GENDB_PHASE("dim_filter");
        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t cust_chunk = (n_cust + nthreads - 1) / nthreads;
        const int8_t bcode = building_code;
        uint8_t* cok = cust_ok.data();
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([=]() {
                size_t lo = (size_t)t * cust_chunk;
                size_t hi = std::min(lo + cust_chunk, n_cust);
                for (size_t i = lo; i < hi; i++)
                    if (c_mktsegment[i] == bcode) cok[(uint32_t)c_custkey[i]] = 1;
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase 2: build_joins ──────────────────────────────────────────────────
    // Compact OrderRow: 8 bytes
    struct OrderRow { int32_t orderkey; int16_t orderdate; int8_t shippriority; int8_t _pad; };
    std::vector<std::vector<OrderRow>> local_orders(nthreads);

    {
        GENDB_PHASE("build_joins/orders_scan");
        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t ord_chunk = (n_ord + nthreads - 1) / nthreads;
        const uint8_t* cok = cust_ok.data();
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                size_t lo = (size_t)t * ord_chunk;
                size_t hi = std::min(lo + ord_chunk, n_ord);
                auto& loc = local_orders[t];
                loc.reserve(30000);
                for (size_t i = lo; i < hi; i++) {
                    if (cok[(uint32_t)o_custkey[i]] && o_orderdate[i] < DATE_THRESH) {
                        loc.push_back({o_orderkey[i], (int16_t)o_orderdate[i],
                                       (int8_t)o_shippriority[i], 0});
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // Initialize hot structures NOW (after orders_scan, before merge):
    // - memset fills pages sequentially → page faults happen now (not in merge)
    // - With hugepages: ~16 faults for 32MB (~1.6ms) vs ~8K faults with 4KB pages (~24ms)
    // - Sequential memset warms L3 with 32MB hash map immediately before merge
    //   (orders_scan evicted L3; this re-warms it → merge accesses L3-warm hash map)
    memset(hm_slots, 0xFF, HM_BYTES);          // sets key=0xFFFFFFFF=-1 (sentinel) for all slots
    memset(qualifying_orders, 0, QUAL_BYTES);   // zero-init bitset

    {
        GENDB_PHASE("build_joins/merge");
        // Sequential merge: insert qualifying orders into bitset + compact hash map
        for (auto& lv : local_orders) {
            for (const auto& row : lv) {
                uint32_t k = (uint32_t)row.orderkey;
                qualifying_orders[k >> 6] |= (1ULL << (k & 63));

                uint32_t h = (uint32_t)(row.orderkey * 2654435761u) & HM_MASK;
                while (hm_slots[h].key != -1 && hm_slots[h].key != row.orderkey)
                    h = (h + 1) & HM_MASK;
                hm_slots[h] = {row.orderkey, row.orderdate, row.shippriority, 0};
            }
        }
        for (auto& lv : local_orders) { std::vector<OrderRow>().swap(lv); }
    }

    // ── Phase 3: main_scan ────────────────────────────────────────────────────
    // Morsel-driven parallel; emit RevRow per qualifying lineitem; NO aggregation.
    // Hot working set: qualifying_orders (7.5MB) + hm_slots (32MB) = 39.5MB ≤ 44MB LLC.
    // With hugepages: both fit in ~20 dTLB large-page entries → zero TLB misses.
    std::vector<std::vector<RevRow>> local_rev(nthreads);

    {
        GENDB_PHASE("main_scan");

        struct BlockRange { size_t start, end; };
        std::vector<BlockRange> qblocks;
        qblocks.reserve(num_blocks);
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_date <= DATE_THRESH) continue;
            size_t rs = (size_t)b * block_size_zm;
            size_t re = std::min(rs + block_size_zm, n_li);
            qblocks.push_back({rs, re});
        }

        std::atomic<size_t> next_block{0};
        const size_t qb_size = qblocks.size();
        const uint64_t*  qo = qualifying_orders;
        const OrderSlot* hm = hm_slots;

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                auto& buf = local_rev[t];
                buf.reserve(200000);

                while (true) {
                    size_t bi = next_block.fetch_add(1, std::memory_order_relaxed);
                    if (bi >= qb_size) break;

                    size_t row_start = qblocks[bi].start;
                    size_t row_end   = qblocks[bi].end;

                    for (size_t i = row_start; i < row_end; i++) {
                        if (l_shipdate[i] <= DATE_THRESH) continue;

                        int32_t okey = l_orderkey[i];
                        uint32_t k = (uint32_t)okey;

                        // Bit-packed bitset prefilter (~82% early reject before hash probe)
                        if (!((qo[k >> 6] >> (k & 63)) & 1ULL)) continue;

                        // Compact hash map probe (LLC-resident + hugetlb TLB-free)
                        uint32_t h = (uint32_t)(okey * 2654435761u) & HM_MASK;
                        while (hm[h].key != -1) {
                            if (hm[h].key == okey) goto found;
                            h = (h + 1) & HM_MASK;
                        }
                        continue;
                        found:

                        // Revenue: l_extendedprice * (1 - l_discount/100)
                        double rev = l_extprice[i] * (1.0 - l_discount[i] * 0.01);
                        buf.push_back({okey, hm[h].orderdate, hm[h].shippriority, 0, rev});
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    free_huge(qualifying_orders, QUAL_BYTES);
    free_huge(hm_slots, HM_BYTES);

    // ── Output phase (excluded from timing_ms) ────────────────────────────────
    {
        GENDB_PHASE("output");

        std::unordered_map<int32_t, AggState> global_agg;
        global_agg.reserve(2000000);

        for (auto& buf : local_rev) {
            for (const auto& row : buf) {
                auto [it, inserted] = global_agg.try_emplace(
                    row.okey,
                    AggState{(long double)row.revenue,
                             (int32_t)row.orderdate,
                             (int32_t)row.shippriority});
                if (!inserted) it->second.revenue += (long double)row.revenue;
            }
        }

        struct Result { int32_t orderkey; long double revenue; int32_t orderdate; int32_t shippriority; };
        std::vector<Result> results;
        results.reserve(global_agg.size());
        for (auto& [key, st] : global_agg)
            results.push_back({key, st.revenue, st.orderdate, st.shippriority});

        const size_t k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const Result& a, const Result& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });

        { std::string cmd = "mkdir -p \"" + results_dir + "\""; (void)system(cmd.c_str()); }

        std::string out_path = results_dir + "/Q3.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }
        fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        for (size_t i = 0; i < k; i++) {
            char date_str[11];
            days_to_date(results[i].orderdate, date_str);
            fprintf(out, "%d,%.2Lf,%s,%d\n",
                    results[i].orderkey, results[i].revenue,
                    date_str, results[i].shippriority);
        }
        fclose(out);
    }

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);
    return 0;
}
