// Q3: Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment='BUILDING' AND c_custkey=o_custkey
//   AND l_orderkey=o_orderkey
//   AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate
// LIMIT 10

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

// ── Flat open-addressing hash map for orders metadata ─────────────────────────
// Keys = o_orderkey (int32_t, 1..60M); sentinel = -1 (empty)
// Capacity must be power of 2; load factor kept <= 0.5
struct OrderSlot {
    int32_t key;           // -1 = empty
    int32_t orderdate;
    int32_t shippriority;
};

struct OrderHashMap {
    std::vector<OrderSlot> slots;
    uint32_t mask;

    void init(uint32_t capacity) {
        // capacity must be a power of 2 and > 2 * expected_entries
        slots.assign(capacity, {-1, 0, 0});
        mask = capacity - 1;
    }

    inline void insert(int32_t key, int32_t odate, int32_t prio) {
        uint32_t h = (uint32_t)(key * 2654435761u) & mask;
        while (slots[h].key != -1 && slots[h].key != key)
            h = (h + 1) & mask;
        slots[h] = {key, odate, prio};
    }

    inline const OrderSlot* find(int32_t key) const {
        uint32_t h = (uint32_t)(key * 2654435761u) & mask;
        while (slots[h].key != -1) {
            if (slots[h].key == key) return &slots[h];
            h = (h + 1) & mask;
        }
        return nullptr;
    }
};

// ── Aggregation state ─────────────────────────────────────────────────────────
struct AggState {
    long double revenue;    // 80-bit extended precision avoids SUM rounding errors
    int32_t     orderdate;
    int32_t     shippriority;
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

    constexpr int32_t DATE_THRESH = 9204;  // 1995-03-15

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
        if (!f_cmkt.open(   (gendb_dir + "/customer/c_mktsegment.bin").c_str()))     return 1;
        if (!f_custkey.open((gendb_dir + "/customer/c_custkey.bin").c_str()))         return 1;
        if (!f_ocustkey.open((gendb_dir + "/orders/o_custkey.bin").c_str()))          return 1;
        if (!f_odate.open(  (gendb_dir + "/orders/o_orderdate.bin").c_str()))         return 1;
        if (!f_okey.open(   (gendb_dir + "/orders/o_orderkey.bin").c_str()))          return 1;
        if (!f_oprio.open(  (gendb_dir + "/orders/o_shippriority.bin").c_str()))      return 1;
        if (!f_lkey.open(   (gendb_dir + "/lineitem/l_orderkey.bin").c_str()))        return 1;
        if (!f_lship.open(  (gendb_dir + "/lineitem/l_shipdate.bin").c_str()))        return 1;
        if (!f_lext.open(   (gendb_dir + "/lineitem/l_extendedprice.bin").c_str()))   return 1;
        if (!f_ldisc.open(  (gendb_dir + "/lineitem/l_discount.bin").c_str()))        return 1;
        // Prefetch lineitem (large; overlap I/O with customer/orders phases)
        f_lkey.prefetch();
        f_lship.prefetch();
        f_lext.prefetch();
        f_ldisc.prefetch();
    }

    const int8_t*  c_mktsegment   = (const int8_t*)  f_cmkt.data;
    const int32_t* c_custkey       = (const int32_t*) f_custkey.data;
    const int32_t* o_custkey       = (const int32_t*) f_ocustkey.data;
    const int32_t* o_orderdate     = (const int32_t*) f_odate.data;
    const int32_t* o_orderkey      = (const int32_t*) f_okey.data;
    const int32_t* o_shippriority  = (const int32_t*) f_oprio.data;
    const int32_t* l_orderkey      = (const int32_t*) f_lkey.data;
    const int32_t* l_shipdate      = (const int32_t*) f_lship.data;
    const double*  l_extprice      = (const double*)  f_lext.data;
    const int8_t*  l_discount      = (const int8_t*)  f_ldisc.data;

    const size_t n_cust = f_cmkt.size;                    // int8_t, 1 byte each
    const size_t n_ord  = f_okey.size  / sizeof(int32_t);
    const size_t n_li   = f_lkey.size  / sizeof(int32_t);

    int nthreads = (int)std::thread::hardware_concurrency();
    if (nthreads < 1) nthreads = 1;

    // ── Phase 1: dim_filter — Customer bitset ─────────────────────────────────
    // Resolve 'BUILDING' code from dictionary
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

    // Build cust_ok[1500001] — 1.5 MB, L3-resident
    std::vector<uint8_t> cust_ok(1500001, 0);
    {
        GENDB_PHASE("dim_filter/cust_bitset");
        for (size_t i = 0; i < n_cust; i++) {
            if (c_mktsegment[i] == building_code) {
                cust_ok[(uint32_t)c_custkey[i]] = 1;
            }
        }
    }

    // ── Phase 2: build_joins — Orders scan (parallel) ─────────────────────────
    // Thread-local result vectors → merge into qualifying_orders bitset + hash map
    struct OrderRow { int32_t orderkey, orderdate, shippriority; };
    std::vector<std::vector<OrderRow>> local_orders(nthreads);

    {
        GENDB_PHASE("build_joins/orders_scan");
        std::vector<std::thread> threads;
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

    // Determine hash map capacity: power of 2 >= 3 * total_qual (LF <= 0.33)
    uint32_t hm_cap = 1u << 22; // 4M default
    while (hm_cap < (uint32_t)(3 * total_qual)) hm_cap <<= 1;

    // Bit-packed qualifying_orders bitset: 60000001 bits → ~7.5 MB, L3-resident
    const size_t QUAL_WORDS = (60000001 + 63) / 64;
    std::vector<uint64_t> qualifying_orders(QUAL_WORDS, 0ULL);

    // Build hash map
    OrderHashMap orders_hm;
    orders_hm.init(hm_cap);

    {
        GENDB_PHASE("build_joins/merge");
        for (auto& lv : local_orders) {
            for (const auto& row : lv) {
                uint32_t k = (uint32_t)row.orderkey;
                qualifying_orders[k >> 6] |= (1ULL << (k & 63));
                orders_hm.insert(row.orderkey, row.orderdate, row.shippriority);
            }
        }
        // Release local vectors
        for (auto& lv : local_orders) { std::vector<OrderRow>().swap(lv); }
    }

    // ── Phase 3: main_scan — Lineitem scan (parallel, zone map) ───────────────
    // Thread-local aggregation maps keyed by l_orderkey
    std::vector<std::unordered_map<int32_t, AggState>> local_agg(nthreads);

    {
        GENDB_PHASE("main_scan");

        // Build qualifying block list (zone-map skip)
        struct BlockRange { size_t start, end; };
        std::vector<BlockRange> qblocks;
        qblocks.reserve(num_blocks);
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].max_date <= DATE_THRESH) continue; // all rows fail l_shipdate > THRESH
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
                auto& agg = local_agg[t];
                agg.reserve(100000);

                while (true) {
                    size_t bi = next_block.fetch_add(1, std::memory_order_relaxed);
                    if (bi >= qb_size) break;

                    size_t row_start = qblocks[bi].start;
                    size_t row_end   = qblocks[bi].end;

                    for (size_t i = row_start; i < row_end; i++) {
                        if (l_shipdate[i] <= DATE_THRESH) continue;

                        int32_t okey = l_orderkey[i];
                        uint32_t k = (uint32_t)okey;

                        // Bit-packed bitset prefilter (~82% early reject)
                        if (!((qo[k >> 6] >> (k & 63)) & 1ULL)) continue;

                        // Hash map lookup for order metadata
                        const OrderSlot* slot = orders_hm.find(okey);
                        if (!slot) continue;

                        // Accumulate revenue: l_extendedprice * (1 - l_discount/100)
                        long double rev = (long double)l_extprice[i]
                                          * (1.0L - l_discount[i] * 0.01L);

                        auto [it, inserted] = agg.try_emplace(
                            okey, AggState{rev, slot->orderdate, slot->shippriority});
                        if (!inserted) it->second.revenue += rev;
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: output — merge agg maps + top-10 + write CSV ──────────────────
    {
        GENDB_PHASE("output");

        // Merge thread-local maps into global
        std::unordered_map<int32_t, AggState> global_agg;
        global_agg.reserve(2000000);

        for (auto& lmap : local_agg) {
            for (auto& [key, st] : lmap) {
                auto [it, inserted] = global_agg.try_emplace(key, st);
                if (!inserted) it->second.revenue += st.revenue;
            }
        }

        // Build result array for partial sort
        struct Result {
            int32_t     orderkey;
            long double revenue;
            int32_t     orderdate;
            int32_t     shippriority;
        };
        std::vector<Result> results;
        results.reserve(global_agg.size());
        for (auto& [key, st] : global_agg) {
            results.push_back({key, st.revenue, st.orderdate, st.shippriority});
        }

        // partial_sort: top-10 by (revenue DESC, orderdate ASC)
        const size_t k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const Result& a, const Result& b) {
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
            days_to_date(results[i].orderdate, date_str);
            fprintf(out, "%d,%.2Lf,%s,%d\n",
                    results[i].orderkey,
                    results[i].revenue,
                    date_str,
                    results[i].shippriority);
        }
        fclose(out);
    }

    // ── Total timing ──────────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
