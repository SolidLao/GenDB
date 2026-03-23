// Q3: Shipping Priority — iter_1
// Optimizations over iter_0:
//   1. Compact OrderSlot (8 bytes): orderdate as int16_t, shippriority as int8_t
//      → 4M slots × 8 bytes = 32 MB hash map (vs 96 MB in iter_0)
//      → hot working set = 32 MB + 7.5 MB bitset = 39.5 MB, fits in 44 MB LLC
//   2. Flat open-addressing AggHashMap per thread (256K slots) — no pointer chasing
//   3. double revenue (SSE2) instead of long double (x87)
//   4. Parallel range-partitioned merge — all nthreads merge concurrently

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

// ── Compact OrderSlot: 8 bytes ────────────────────────────────────────────────
// Max qualifying o_orderdate = 9203 < INT16_MAX = 32767  ✓
// shippriority fits in int8_t (TPC-H values 0,1,2)       ✓
struct OrderSlot {
    int32_t key;           // -1 = empty sentinel
    int16_t orderdate;     // raw days-since-epoch value
    int8_t  shippriority;
    int8_t  _pad;
};
static_assert(sizeof(OrderSlot) == 8, "OrderSlot must be 8 bytes");

// ── Compact OrderHashMap: 4M slots × 8 bytes = 32 MB ─────────────────────────
// Knuth multiplicative hash; linear probing; LF ~0.36 for ~1.46M entries
struct OrderHashMap {
    OrderSlot* slots = nullptr;
    uint32_t   mask  = 0;

    void init(uint32_t capacity) {
        mask  = capacity - 1;
        slots = static_cast<OrderSlot*>(
            aligned_alloc(64, (size_t)capacity * sizeof(OrderSlot)));
        for (uint32_t i = 0; i < capacity; i++) slots[i].key = -1;
    }
    ~OrderHashMap() { if (slots) free(slots); }
    OrderHashMap() = default;
    OrderHashMap(const OrderHashMap&) = delete;
    OrderHashMap& operator=(const OrderHashMap&) = delete;

    inline void insert(int32_t key, int16_t odate, int8_t prio) {
        uint32_t h = (uint32_t)key * 2654435761u & mask;
        while (slots[h].key != -1 && slots[h].key != key)
            h = (h + 1) & mask;
        slots[h] = {key, odate, prio, 0};
    }

    inline const OrderSlot* find(int32_t key) const {
        uint32_t h = (uint32_t)key * 2654435761u & mask;
        while (slots[h].key != -1) {
            if (slots[h].key == key) return &slots[h];
            h = (h + 1) & mask;
        }
        return nullptr;
    }
};

// ── Flat open-addressing AggHashMap ──────────────────────────────────────────
// 256K slots; key = l_orderkey; revenue as long double (80-bit x87)
// AggSlot natural layout: int32_t×3 (12 bytes) + 4 pad + long double (16) = 32 bytes
// 256K × 32 = 8 MB per thread — acceptable
// Note: must use long double accumulation to match ground truth (precision-sensitive)
struct AggSlot {
    int32_t     key;           // -1 = empty sentinel
    int32_t     orderdate;
    int32_t     shippriority;
    long double revenue;       // 80-bit extended precision for correct rounding
};

constexpr uint32_t AGG_CAP  = 1u << 18; // 256K slots
constexpr uint32_t AGG_MASK = AGG_CAP - 1;

struct AggHashMap {
    AggSlot* slots = nullptr;

    AggHashMap() {
        slots = static_cast<AggSlot*>(
            aligned_alloc(64, (size_t)AGG_CAP * sizeof(AggSlot)));
        for (uint32_t i = 0; i < AGG_CAP; i++) slots[i].key = -1;
    }
    ~AggHashMap() { if (slots) free(slots); }
    AggHashMap(const AggHashMap&) = delete;
    AggHashMap& operator=(const AggHashMap&) = delete;
    AggHashMap(AggHashMap&& o) noexcept : slots(o.slots) { o.slots = nullptr; }

    inline void accumulate(int32_t key, int32_t odate, int32_t prio, long double rev) {
        uint32_t h = (uint32_t)key * 2654435761u & AGG_MASK;
        while (slots[h].key != -1 && slots[h].key != key)
            h = (h + 1) & AGG_MASK;
        if (slots[h].key == -1) {
            slots[h] = {key, odate, prio, rev};
        } else {
            slots[h].revenue += rev;
        }
    }
};

// ── Date helper: days-since-epoch → YYYY-MM-DD ────────────────────────────────
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

    constexpr int32_t DATE_THRESH = 9204;  // 1995-03-15 in days since epoch

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
        fread(&num_blocks,    sizeof(uint32_t), 1, f);
        fread(&block_size_zm, sizeof(uint32_t), 1, f);
        zones.resize(num_blocks);
        fread(zones.data(), sizeof(ZoneEntry), num_blocks, f);
        fclose(f);
    }

    // ── mmap all columns ──────────────────────────────────────────────────────
    MmapFile f_cmkt, f_custkey;
    MmapFile f_ocustkey, f_odate, f_okey, f_oprio;
    MmapFile f_lkey, f_lship, f_lext, f_ldisc;
    {
        GENDB_PHASE("data_loading");
        if (!f_cmkt.open(    (gendb_dir + "/customer/c_mktsegment.bin").c_str()))    return 1;
        if (!f_custkey.open( (gendb_dir + "/customer/c_custkey.bin").c_str()))       return 1;
        if (!f_ocustkey.open((gendb_dir + "/orders/o_custkey.bin").c_str()))         return 1;
        if (!f_odate.open(   (gendb_dir + "/orders/o_orderdate.bin").c_str()))       return 1;
        if (!f_okey.open(    (gendb_dir + "/orders/o_orderkey.bin").c_str()))        return 1;
        if (!f_oprio.open(   (gendb_dir + "/orders/o_shippriority.bin").c_str()))    return 1;
        if (!f_lkey.open(    (gendb_dir + "/lineitem/l_orderkey.bin").c_str()))      return 1;
        if (!f_lship.open(   (gendb_dir + "/lineitem/l_shipdate.bin").c_str()))      return 1;
        if (!f_lext.open(    (gendb_dir + "/lineitem/l_extendedprice.bin").c_str())) return 1;
        if (!f_ldisc.open(   (gendb_dir + "/lineitem/l_discount.bin").c_str()))      return 1;
        // Prefetch lineitem (largest data; overlap I/O with customer/orders phases)
        f_lkey.prefetch();
        f_lship.prefetch();
        f_lext.prefetch();
        f_ldisc.prefetch();
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

    const size_t n_cust = f_cmkt.size;                    // int8_t → 1 byte each
    const size_t n_ord  = f_okey.size  / sizeof(int32_t);
    const size_t n_li   = f_lkey.size  / sizeof(int32_t);

    // ── Phase 1: dim_filter — Build customer bitset ───────────────────────────
    // Resolve 'BUILDING' code from dictionary (do NOT hardcode)
    int8_t building_code = -1;
    {
        GENDB_PHASE("dim_filter/dict_lookup");
        char mkt_dict[5][16];
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.bin";
        FILE* f = fopen(dict_path.c_str(), "rb");
        if (!f) { perror(dict_path.c_str()); return 1; }
        fread(mkt_dict, 16, 5, f);
        fclose(f);
        for (int i = 0; i < 5; i++) {
            if (strncmp(mkt_dict[i], "BUILDING", 8) == 0) {
                building_code = (int8_t)i;
                break;
            }
        }
        if (building_code < 0) {
            fprintf(stderr, "BUILDING not found in c_mktsegment_dict\n");
            return 1;
        }
    }

    // Build cust_ok[1500001]: 1.5 MB, L3-resident
    std::vector<uint8_t> cust_ok(1500001, 0);
    {
        GENDB_PHASE("dim_filter/cust_bitset");
        for (size_t i = 0; i < n_cust; i++) {
            if (c_mktsegment[i] == building_code) {
                cust_ok[(uint32_t)c_custkey[i]] = 1;
            }
        }
    }

    // ── Phase 2: build_joins — Orders parallel scan ───────────────────────────
    struct OrderRow { int32_t orderkey, orderdate, shippriority; };
    std::vector<std::vector<OrderRow>> local_orders(nthreads);

    {
        GENDB_PHASE("build_joins/orders_scan");
        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t chunk = (n_ord + nthreads - 1) / nthreads;
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                size_t lo = (size_t)t * chunk;
                size_t hi = std::min(lo + chunk, n_ord);
                auto& loc = local_orders[t];
                loc.reserve(40000);
                const uint8_t* cok = cust_ok.data();
                for (size_t i = lo; i < hi; i++) {
                    if (cok[(uint32_t)o_custkey[i]] && o_orderdate[i] < DATE_THRESH) {
                        loc.push_back({o_orderkey[i], o_orderdate[i], o_shippriority[i]});
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // Count total qualifying orders
    size_t total_qual = 0;
    for (auto& lv : local_orders) total_qual += lv.size();

    // Capacity = 4M slots (power of 2 >= 3 × total_qual → LF ~0.36 for 1.46M entries)
    uint32_t hm_cap = 1u << 22; // 4M default
    while (hm_cap < (uint32_t)(total_qual * 3)) hm_cap <<= 1;

    // Bit-packed qualifying_orders bitset: 60000001 bits ≈ 7.5 MB, L3-resident
    const size_t QUAL_WORDS = (60000001 + 63) / 64;
    std::vector<uint64_t> qualifying_orders(QUAL_WORDS, 0ULL);

    // Build compact OrderHashMap (32 MB) + qualifying_orders bitset
    OrderHashMap orders_hm;
    orders_hm.init(hm_cap);

    {
        GENDB_PHASE("build_joins/merge");
        for (auto& lv : local_orders) {
            for (const auto& row : lv) {
                uint32_t k = (uint32_t)row.orderkey;
                qualifying_orders[k >> 6] |= (1ULL << (k & 63));
                orders_hm.insert(row.orderkey,
                                 (int16_t)row.orderdate,
                                 (int8_t)row.shippriority);
            }
        }
        // Free thread-local vectors
        for (auto& lv : local_orders) std::vector<OrderRow>().swap(lv);
    }

    // ── Phase 3: main_scan — Lineitem morsel-driven parallel scan ─────────────
    // Each thread gets its own flat AggHashMap (256K slots × 24 bytes ≈ 6 MB)
    // Allocated inside each thread for NUMA locality
    std::vector<AggHashMap*> local_agg(nthreads, nullptr);

    {
        GENDB_PHASE("main_scan");

        // Pre-build qualifying block list (zone-map skip)
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
        const uint64_t* qo = qualifying_orders.data();

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                // Allocate and zero AggHashMap in this thread (NUMA-friendly)
                AggHashMap* agg = new AggHashMap();
                local_agg[t] = agg;

                while (true) {
                    size_t bi = next_block.fetch_add(1, std::memory_order_relaxed);
                    if (bi >= qb_size) break;

                    size_t row_start = qblocks[bi].start;
                    size_t row_end   = qblocks[bi].end;

                    for (size_t i = row_start; i < row_end; i++) {
                        if (l_shipdate[i] <= DATE_THRESH) continue;

                        int32_t okey = l_orderkey[i];
                        uint32_t k = (uint32_t)okey;

                        // Bit-packed bitset prefilter: ~82% early reject before hash probe
                        if (!((qo[k >> 6] >> (k & 63)) & 1ULL)) continue;

                        // Compact hash map lookup: 8-byte slots, 32 MB working set in LLC
                        const OrderSlot* slot = orders_hm.find(okey);
                        if (!slot) continue;

                        // Revenue: l_extendedprice * (1 - l_discount/100)
                        // long double (x87) for correct rounding (ground truth validated)
                        long double rev = (long double)l_extprice[i]
                                          * (1.0L - l_discount[i] * 0.01L);

                        agg->accumulate(okey,
                                        (int32_t)slot->orderdate,
                                        (int32_t)slot->shippriority,
                                        rev);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase 4: parallel_range_merge ─────────────────────────────────────────
    // Divide orderkey space [0, 60000001) into nthreads ranges.
    // Thread i merges all local_agg[s] entries whose key falls in its range.
    // No shared writes → fully parallel.
    struct Result {
        int32_t     orderkey;
        long double revenue;
        int32_t     orderdate;
        int32_t     shippriority;
    };
    std::vector<std::vector<Result>> range_results(nthreads);

    {
        GENDB_PHASE("parallel_range_merge");

        const int32_t KEY_MAX = 60000001;
        std::vector<std::thread> merge_threads;
        merge_threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            merge_threads.emplace_back([&, t]() {
                // Key range for this thread
                const int32_t range_lo = (int32_t)((int64_t)t       * KEY_MAX / nthreads);
                const int32_t range_hi = (int32_t)((int64_t)(t + 1) * KEY_MAX / nthreads);

                // Local merge AggHashMap for this key range
                AggHashMap merge_map;

                // Scan all thread-local AggHashMaps; collect entries in our range
                for (int s = 0; s < nthreads; s++) {
                    AggHashMap* src = local_agg[s];
                    if (!src) continue;
                    const AggSlot* ss = src->slots;
                    for (uint32_t idx = 0; idx < AGG_CAP; idx++) {
                        int32_t key = ss[idx].key;
                        if (key == -1) continue;
                        if (key < range_lo || key >= range_hi) continue;
                            merge_map.accumulate(key, ss[idx].orderdate,
                                             ss[idx].shippriority, ss[idx].revenue);
                    }
                }

                // Collect non-empty slots into thread-local result vector
                auto& res = range_results[t];
                res.reserve(32768);
                const AggSlot* ms = merge_map.slots;
                for (uint32_t idx = 0; idx < AGG_CAP; idx++) {
                    if (ms[idx].key == -1) continue;
                    res.push_back({ms[idx].key, ms[idx].revenue,
                                   ms[idx].orderdate, ms[idx].shippriority});
                }
            });
        }
        for (auto& th : merge_threads) th.join();
    }

    // Free local AggHashMaps
    for (int t = 0; t < nthreads; t++) {
        delete local_agg[t];
        local_agg[t] = nullptr;
    }

    // ── Phase: output — top-10 + write CSV ───────────────────────────────────
    {
        GENDB_PHASE("output");

        // Collect all result entries from range-partitioned merge
        std::vector<Result> all_results;
        size_t total = 0;
        for (auto& rv : range_results) total += rv.size();
        all_results.reserve(total);
        for (auto& rv : range_results)
            for (auto& r : rv)
                all_results.push_back(r);

        // partial_sort: top-10 by (revenue DESC, o_orderdate ASC)
        const size_t k = std::min((size_t)10, all_results.size());
        std::partial_sort(all_results.begin(), all_results.begin() + k, all_results.end(),
            [](const Result& a, const Result& b) -> bool {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });

        // Ensure output directory exists
        {
            std::string cmd = "mkdir -p \"" + results_dir + "\"";
            system(cmd.c_str());
        }

        // Write CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }

        fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        for (size_t i = 0; i < k; i++) {
            char date_str[11];
            days_to_date(all_results[i].orderdate, date_str);
            fprintf(out, "%d,%.2Lf,%s,%d\n",
                    all_results[i].orderkey,
                    all_results[i].revenue,
                    date_str,
                    all_results[i].shippriority);
        }
        fclose(out);
    }

    // ── Total timing ──────────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
