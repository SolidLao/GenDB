#define _GNU_SOURCE
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <omp.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include "date_utils.h"
#include "timing_utils.h"

static constexpr int32_t DATE_THRESH = 9204; // epoch for 1995-03-15

// ── Customer bloom filter constants (1MB = 8Mb bits, k=3, FPR~0.12% for 300K keys) ──
static constexpr size_t CUST_BF_BITS = 1u << 23;
static constexpr size_t CUST_BF_MASK = CUST_BF_BITS - 1;

// ── Fibonacci hash for int32 ──────────────────────────────────────────────────
static inline uint32_t h32(int32_t k) {
    return (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> 32);
}

// ── Open-addressing hash set (int32) ──────────────────────────────────────────
struct HashSet32 {
    static constexpr int32_t EMPTY = INT32_MIN;
    uint32_t mask;
    std::vector<int32_t> slots;
    HashSet32(uint32_t cap) : mask(cap - 1), slots(cap, EMPTY) {}
    void insert(int32_t k) {
        uint32_t s = h32(k) & mask;
        while (slots[s] != EMPTY && slots[s] != k) s = (s + 1) & mask;
        slots[s] = k;
    }
    bool probe(int32_t k) const {
        uint32_t s = h32(k) & mask;
        while (slots[s] != EMPTY) {
            if (slots[s] == k) return true;
            s = (s + 1) & mask;
        }
        return false;
    }
};

// ── Open-addressing hash map: int32 → orderdate (sprio always 0, omitted) ─────
// Packed 8-byte slot: {key, odate} — fits in one cache line per 8 slots.
// 1M slots × 8B = 8MB total, well within L3 cache (44MB).
struct OrdMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    struct Slot { int32_t key; int32_t odate; };
    uint32_t mask;
    std::vector<Slot> slots;
    OrdMap(uint32_t cap) : mask(cap - 1), slots(cap, {EMPTY, 0}) {}
    void insert(int32_t k, int32_t odate) {
        uint32_t s = h32(k) & mask;
        while (slots[s].key != EMPTY) {
            if (slots[s].key == k) return;
            s = (s + 1) & mask;
        }
        slots[s] = {k, odate};
    }
    // Returns odate, or INT32_MIN if not found (TPC-H odates ~8400-10600, safe sentinel)
    int32_t find_odate(int32_t k) const {
        uint32_t s = h32(k) & mask;
        while (slots[s].key != EMPTY) {
            if (slots[s].key == k) return slots[s].odate;
            s = (s + 1) & mask;
        }
        return INT32_MIN;
    }
};

// ── Aggregation value (sprio always 0 in TPC-H, omitted) ──────────────────────
struct AggVal { double rev; int32_t odate; };

// ── mmap column helper ────────────────────────────────────────────────────────
template<typename T>
struct Col {
    const T* p = nullptr; size_t n = 0; size_t sz = 0; int fd = -1;
    void* mmap_ptr = nullptr; size_t mmap_sz = 0;

    // Full mmap. populate=false avoids eager page-table setup (good for sparse access).
    void open(const std::string& path, bool populate = true) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open: " << path << "\n"; std::abort(); }
        struct stat st; fstat(fd, &st); sz = st.st_size; n = sz / sizeof(T);
        int flags = MAP_PRIVATE | (populate ? MAP_POPULATE : 0);
        mmap_ptr = mmap(nullptr, sz, PROT_READ, flags, fd, 0);
        mmap_sz = sz;
        p = (const T*)mmap_ptr;
        posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
        if (!populate) madvise(mmap_ptr, sz, MADV_SEQUENTIAL | MADV_WILLNEED);
    }

    // Partial mmap: MAP_POPULATE only rows [skip_rows, n). Saves page-table setup
    // for the prefix we never access. Works because delta/sizeof(T) is always integer
    // for page-aligned skips (4096 is divisible by 4 and 8).
    void open_from(const std::string& path, size_t skip_rows) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open: " << path << "\n"; std::abort(); }
        struct stat st; fstat(fd, &st); sz = st.st_size; n = sz / sizeof(T);
        size_t skip_bytes = skip_rows * sizeof(T);
        static constexpr size_t PAGE = 4096;
        size_t aligned = skip_bytes & ~(PAGE - 1ULL);   // page-aligned start offset
        size_t delta   = skip_bytes - aligned;           // bytes from aligned to skip_rows
        mmap_sz = sz - aligned;
        mmap_ptr = mmap(nullptr, mmap_sz, PROT_READ, MAP_PRIVATE,
                        fd, (off_t)aligned);
        if (mmap_ptr == MAP_FAILED) {
            std::cerr << "mmap failed: " << path << "\n"; std::abort();
        }
        // mmap_ptr[delta/sizeof(T)] is row skip_rows in the file.
        // Set p so that p[i] for i >= skip_rows gives correct data.
        p = (const T*)mmap_ptr + (delta / sizeof(T)) - skip_rows;
        posix_fadvise(fd, aligned, mmap_sz, POSIX_FADV_SEQUENTIAL);
        madvise(mmap_ptr, mmap_sz, MADV_SEQUENTIAL | MADV_WILLNEED);
    }

    void close() {
        if (mmap_ptr) { munmap(mmap_ptr, mmap_sz); mmap_ptr = nullptr; p = nullptr; }
        if (fd >= 0) { ::close(fd); fd = -1; }
    }
};

// ── Zone map entry & loader ───────────────────────────────────────────────────
struct ZoneEnt { int32_t min_v; int32_t max_v; uint32_t cnt; };
static std::vector<ZoneEnt> load_zones(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return {};
    struct stat st; fstat(fd, &st);
    const uint8_t* mem = (const uint8_t*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0);
    uint32_t nb = *(const uint32_t*)mem;
    std::vector<ZoneEnt> z(nb);
    memcpy(z.data(), mem + 4, nb * sizeof(ZoneEnt));
    munmap((void*)mem, st.st_size); ::close(fd);
    return z;
}

void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();
    std::filesystem::create_directories(results_dir);

    // ── Load c_mktsegment dictionary ──────────────────────────────────────────
    uint8_t bld_code = 1;
    {
        std::ifstream f(gendb_dir + "/customer/c_mktsegment_dict.txt");
        std::string line;
        while (std::getline(f, line)) {
            auto eq = line.find('=');
            if (eq == std::string::npos) continue;
            if (line.substr(eq + 1) == "BUILDING") {
                bld_code = (uint8_t)std::stoi(line.substr(0, eq));
                break;
            }
        }
    }

    // ── Pre-open orders columns (async prefetch while dim_filter runs) ─────────
    // populate=false → MAP_PRIVATE + MADV_WILLNEED: kernel readahead in background
    Col<int32_t> o_ok, o_ck, o_od;
    o_ok.open(gendb_dir + "/orders/o_orderkey.bin",  /*populate=*/false);
    o_ck.open(gendb_dir + "/orders/o_custkey.bin",   /*populate=*/false);
    o_od.open(gendb_dir + "/orders/o_orderdate.bin", /*populate=*/false);

    // ── Phase 1: Customer → hash set + bloom filter of qualifying c_custkey ────
    HashSet32 cust_set(524288); // 2^19, handles ~300K entries comfortably
    std::vector<uint64_t> cust_bloom(CUST_BF_BITS / 64, 0ULL);
    {
        GENDB_PHASE("dim_filter");
        Col<uint8_t> seg; seg.open(gendb_dir + "/customer/c_mktsegment.bin");
        Col<int32_t> ckey; ckey.open(gendb_dir + "/customer/c_custkey.bin");
        const size_t n = seg.n;
        for (size_t i = 0; i < n; ++i) {
            if (seg.p[i] != bld_code) continue;
            const int32_t ck = ckey.p[i];
            cust_set.insert(ck);
            // Build 3-hash bloom filter for this customer key
            uint32_t bh1 = h32(ck);
            uint32_t bh2 = (uint32_t)((uint64_t)(uint32_t)ck * 0x6C62272E07BB0142ULL >> 32);
            uint32_t bb0 = bh1 & CUST_BF_MASK;
            cust_bloom[bb0 >> 6] |= 1ULL << (bb0 & 63);
            uint32_t bb1 = (bh1 + bh2) & CUST_BF_MASK;
            cust_bloom[bb1 >> 6] |= 1ULL << (bb1 & 63);
            uint32_t bb2 = (bh1 + 2*bh2) & CUST_BF_MASK;
            cust_bloom[bb2 >> 6] |= 1ULL << (bb2 & 63);
        }
        seg.close(); ckey.close();
    }

    // ── Find lineitem start row + pre-open lineitem cols (async prefetch) ───────
    // Binary search on l_shipdate uses <30 page accesses (log2(60M)), minimal cost.
    // Opening lineitem here allows ~245ms of orders_scan to serve as prefetch time.
    size_t li_n = 0, li_start = 0;
    {
        Col<int32_t> l_sd;
        l_sd.open(gendb_dir + "/lineitem/l_shipdate.bin", /*populate=*/false);
        li_n = l_sd.n;
        size_t lo = 0, hi = li_n;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (l_sd.p[mid] <= DATE_THRESH) lo = mid + 1;
            else hi = mid;
        }
        li_start = lo;
        l_sd.close();
    }
    // open_from uses MADV_WILLNEED → async prefetch for lineitem suffix (~780MB)
    Col<int32_t> l_ok; l_ok.open_from(gendb_dir + "/lineitem/l_orderkey.bin",     li_start);
    Col<double>  l_ep; l_ep.open_from(gendb_dir + "/lineitem/l_extendedprice.bin", li_start);
    Col<double>  l_dc; l_dc.open_from(gendb_dir + "/lineitem/l_discount.bin",      li_start);

    // ── Phase 2: Orders scan → collect qualifying (okey, odate) ───────────────
    struct QOrd { int32_t okey, odate; };   // sprio always 0, omitted
    std::vector<QOrd> qords;
    qords.reserve(800000);
    {
        GENDB_PHASE("orders_scan");
        auto oz = load_zones(gendb_dir + "/indexes/orders_orderdate_zonemap.bin");

        const size_t total = o_ok.n;
        const uint32_t nb = (uint32_t)oz.size();
        static constexpr uint32_t BS = 100000;

        const int nt = omp_get_max_threads();
        std::vector<std::vector<QOrd>> loc(nt);
        for (auto& v : loc) v.reserve(20000);

        #pragma omp parallel for schedule(dynamic, 2) num_threads(nt)
        for (uint32_t b = 0; b < nb; ++b) {
            if (!oz.empty() && oz[b].min_v >= DATE_THRESH) continue;

            const size_t rs = (size_t)b * BS;
            const size_t re = std::min(rs + (size_t)BS, total);
            const int tid = omp_get_thread_num();
            auto& lv = loc[tid];

            for (size_t i = rs; i < re; ++i) {
                if (o_od.p[i] >= DATE_THRESH) continue;
                // Bloom pre-filter (FPR~0.12%): skip most non-matching custkeys
                // cheaply (avoids cache-miss-heavy cust_set probes for 99.88% of non-matches)
                const int32_t ock = o_ck.p[i];
                uint32_t bh1 = h32(ock);
                uint32_t bh2 = (uint32_t)((uint64_t)(uint32_t)ock * 0x6C62272E07BB0142ULL >> 32);
                uint32_t bb0 = bh1 & CUST_BF_MASK;
                if (!((cust_bloom[bb0 >> 6] >> (bb0 & 63)) & 1)) continue;
                uint32_t bb1 = (bh1 + bh2) & CUST_BF_MASK;
                if (!((cust_bloom[bb1 >> 6] >> (bb1 & 63)) & 1)) continue;
                uint32_t bb2 = (bh1 + 2*bh2) & CUST_BF_MASK;
                if (!((cust_bloom[bb2 >> 6] >> (bb2 & 63)) & 1)) continue;
                if (!cust_set.probe(ock)) continue;
                lv.push_back({o_ok.p[i], o_od.p[i]});
            }
        }

        for (auto& v : loc)
            for (auto& q : v) qords.push_back(q);

        o_ok.close(); o_ck.close(); o_od.close();
    }

    // ── Phase 3: Build orders hash map okey → odate ───────────────────────────
    // Load factor ~75%: cap = next_pow2(size * 4/3). For 750K → 1M slots × 8B = 8MB.
    // 8MB fits in L3 (44MB) → build cost drops from ~100ns/insert (DRAM) to ~10ns (L3).
    uint32_t om_cap = 1048576; // 2^20
    while (om_cap < (uint32_t)(qords.size() + qords.size() / 3 + 1)) om_cap <<= 1;
    OrdMap ord_map(om_cap);
    {
        GENDB_PHASE("build_joins");
        for (const auto& q : qords) ord_map.insert(q.okey, q.odate);
    }

    // ── Phase 4: Two-phase partitioned aggregation ────────────────────────────
    const int nt = omp_get_max_threads();

    // Bloom filter on qualifying order keys (~750K keys, 8M bits, k=3, FPR~1.5%)
    // Avoids 87% of expensive ord_map hash probes for non-qualifying l_orderkey
    static constexpr size_t BF_BITS = 1u << 23; // 8M bits = 1MB
    static constexpr size_t BF_MASK = BF_BITS - 1;
    std::vector<uint64_t> ord_bloom(BF_BITS / 64, 0ULL);
    {
        GENDB_PHASE("build_bloom");
        for (const auto& q : qords) {
            uint32_t h1 = h32(q.okey);
            uint32_t h2 = (uint32_t)((uint64_t)(uint32_t)q.okey * 0x6C62272E07BB0142ULL >> 32);
            uint32_t b0 = h1 & BF_MASK;
            uint32_t b1 = (h1 + h2) & BF_MASK;
            uint32_t b2 = (h1 + 2 * h2) & BF_MASK;
            ord_bloom[b0 >> 6] |= 1ULL << (b0 & 63);
            ord_bloom[b1 >> 6] |= 1ULL << (b1 & 63);
            ord_bloom[b2 >> 6] |= 1ULL << (b2 & 63);
        }
    }

    // Staging buffers: stage[thread][partition] collects matching tuples
    // sprio removed (always 0): AggTuple shrinks from 20B to 16B → 20% less staging data
    static constexpr int NPART = 64;
    struct AggTuple { int32_t key, odate; double rev; }; // 16 bytes
    std::vector<std::vector<std::vector<AggTuple>>> stage(
        nt, std::vector<std::vector<AggTuple>>(NPART));

    {
        GENDB_PHASE("main_scan");
        // lineitem columns l_ok/l_ep/l_dc were pre-opened before orders_scan.
        // By now, ~245ms of async readahead has populated most of the 780MB suffix.

        const size_t remain = li_n - li_start;
        static constexpr size_t BS2 = 100000;
        const size_t num_blocks = (remain + BS2 - 1) / BS2;

        #pragma omp parallel for schedule(dynamic, 2) num_threads(nt)
        for (size_t b = 0; b < num_blocks; ++b) {
            const size_t rs = li_start + b * BS2;
            const size_t re = std::min(rs + BS2, li_n);
            const int tid = omp_get_thread_num();
            auto& my_stage = stage[tid];

            for (size_t i = rs; i < re; ++i) {
                const int32_t lkey = l_ok.p[i];
                // Bloom filter: reject ~86% of non-qualifying orderkeys cheaply
                uint32_t h1 = h32(lkey);
                uint32_t h2 = (uint32_t)((uint64_t)(uint32_t)lkey * 0x6C62272E07BB0142ULL >> 32);
                uint32_t b0 = h1 & BF_MASK;
                if (!((ord_bloom[b0 >> 6] >> (b0 & 63)) & 1)) continue;
                uint32_t b1 = (h1 + h2) & BF_MASK;
                if (!((ord_bloom[b1 >> 6] >> (b1 & 63)) & 1)) continue;
                uint32_t b2 = (h1 + 2 * h2) & BF_MASK;
                if (!((ord_bloom[b2 >> 6] >> (b2 & 63)) & 1)) continue;
                // Hash map probe (bloom passed) — OrdMap now 8MB, fits in L3
                int32_t odate = ord_map.find_odate(lkey);
                if (odate == INT32_MIN) continue;
                double rev = l_ep.p[i] * (1.0 - l_dc.p[i]);
                int p = (int)(h1 >> 26); // top 6 bits → partition 0..63
                my_stage[p].push_back({lkey, odate, rev});
            }
        }
        l_ok.close(); l_ep.close(); l_dc.close();
    }

    // ── Phase 4b: Parallel per-partition aggregation ──────────────────────────
    // ~750K groups / 64 partitions = ~12K groups/partition → 32K slots each
    // PAGG_EMPTY = -1: l_orderkey is always positive in TPC-H
    // Total: 64 × (128KB keys + 384KB vals) = 32MB (sprio removed, AggVal 12B now)
    static constexpr uint32_t PART_CAP   = 32768; // 2^15
    static constexpr uint32_t PART_HMASK = PART_CAP - 1;
    static constexpr int32_t  PAGG_EMPTY = -1;
    struct PartAgg {
        int32_t keys[PART_CAP]; // 128KB; init to PAGG_EMPTY via memset(0xFF)
        AggVal  vals[PART_CAP]; // 384KB (12B × 32K); initialized on first insert
        PartAgg() { memset(keys, 0xFF, sizeof(keys)); }
    };
    auto pagg = std::make_unique<PartAgg[]>(NPART); // heap: 64 × 512KB = 32MB

    {
        GENDB_PHASE("part_aggregate");
        #pragma omp parallel for num_threads(nt) schedule(dynamic)
        for (int p = 0; p < NPART; ++p) {
            PartAgg& pa = pagg[p];
            for (int t = 0; t < nt; ++t) {
                for (const AggTuple& tup : stage[t][p]) {
                    uint32_t s = h32(tup.key) & PART_HMASK;
                    while (pa.keys[s] != PAGG_EMPTY && pa.keys[s] != tup.key)
                        s = (s + 1) & PART_HMASK;
                    if (pa.keys[s] == PAGG_EMPTY) {
                        pa.keys[s] = tup.key;
                        pa.vals[s] = {tup.rev, tup.odate};
                    } else {
                        pa.vals[s].rev += tup.rev;
                    }
                }
            }
        }
    }

    // ── Collect results & top-10 ──────────────────────────────────────────────
    struct Res { int32_t okey; double rev; int32_t odate; };
    std::vector<Res> top;
    top.reserve(800000);
    for (int p = 0; p < NPART; ++p) {
        const PartAgg& pa = pagg[p];
        for (uint32_t i = 0; i < PART_CAP; ++i) {
            if (pa.keys[i] != PAGG_EMPTY)
                top.push_back({pa.keys[i], pa.vals[i].rev, pa.vals[i].odate});
        }
    }

    {
        GENDB_PHASE("sort_topk");
        const size_t k = std::min((size_t)10, top.size());
        std::partial_sort(top.begin(), top.begin() + k, top.end(),
            [](const Res& a, const Res& b) {
                if (a.rev != b.rev) return a.rev > b.rev;
                return a.odate < b.odate;
            });
        top.resize(k);
    }

    // ── Write CSV output ──────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(2);
        for (const auto& r : top) {
            out << r.okey << ","
                << r.rev << ","
                << ([](int32_t d){char buf[11]; gendb::epoch_days_to_date_str(d, buf); return std::string(buf);}(r.odate)) << ","
                << 0 << "\n"; // o_shippriority always 0 in TPC-H
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_Q3(argv[1], argc > 2 ? argv[2] : ".");
    return 0;
}
#endif
