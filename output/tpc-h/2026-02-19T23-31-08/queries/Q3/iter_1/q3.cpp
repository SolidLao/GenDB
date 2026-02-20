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

// ── Open-addressing hash map: int32 -> {orderdate, shippriority} ──────────────
struct OrdVal { int32_t odate; int32_t sprio; };
struct OrdMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    uint32_t mask;
    std::vector<int32_t> keys;
    std::vector<OrdVal> vals;
    OrdMap(uint32_t cap) : mask(cap - 1), keys(cap, EMPTY), vals(cap) {}
    void insert(int32_t k, int32_t odate, int32_t sprio) {
        uint32_t s = h32(k) & mask;
        while (keys[s] != EMPTY) {
            if (keys[s] == k) return;
            s = (s + 1) & mask;
        }
        keys[s] = k; vals[s] = {odate, sprio};
    }
    const OrdVal* find(int32_t k) const {
        uint32_t s = h32(k) & mask;
        while (keys[s] != EMPTY) {
            if (keys[s] == k) return &vals[s];
            s = (s + 1) & mask;
        }
        return nullptr;
    }
};

// ── Aggregation value ─────────────────────────────────────────────────────────
struct AggVal { double rev; int32_t odate; int32_t sprio; };

// ── mmap column helper ────────────────────────────────────────────────────────
template<typename T>
struct Col {
    const T* p = nullptr; size_t n = 0; size_t sz = 0; int fd = -1;
    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open: " << path << "\n"; std::abort(); }
        struct stat st; fstat(fd, &st); sz = st.st_size; n = sz / sizeof(T);
        p = (const T*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0);
        posix_fadvise(fd, 0, sz, POSIX_FADV_SEQUENTIAL);
    }
    void close() {
        if (p) { munmap((void*)p, sz); p = nullptr; }
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

    // ── Phase 1: Customer → hash set of qualifying c_custkey ─────────────────
    HashSet32 cust_set(524288); // 2^19, handles ~300K entries comfortably
    {
        GENDB_PHASE("dim_filter");
        Col<uint8_t> seg; seg.open(gendb_dir + "/customer/c_mktsegment.bin");
        Col<int32_t> ckey; ckey.open(gendb_dir + "/customer/c_custkey.bin");
        const size_t n = seg.n;
        for (size_t i = 0; i < n; ++i) {
            if (seg.p[i] == bld_code) cust_set.insert(ckey.p[i]);
        }
        seg.close(); ckey.close();
    }

    // ── Phase 2: Orders scan → collect qualifying (okey, odate, sprio) ────────
    struct QOrd { int32_t okey, odate, sprio; };
    std::vector<QOrd> qords;
    qords.reserve(800000);
    {
        GENDB_PHASE("orders_scan");
        auto oz = load_zones(gendb_dir + "/indexes/orders_orderdate_zonemap.bin");

        Col<int32_t> o_ok; o_ok.open(gendb_dir + "/orders/o_orderkey.bin");
        Col<int32_t> o_ck; o_ck.open(gendb_dir + "/orders/o_custkey.bin");
        Col<int32_t> o_od; o_od.open(gendb_dir + "/orders/o_orderdate.bin");
        Col<int32_t> o_sp; o_sp.open(gendb_dir + "/orders/o_shippriority.bin");

        const size_t total = o_ok.n;
        const uint32_t nb = (uint32_t)oz.size();
        static constexpr uint32_t BS = 100000;

        const int nt = omp_get_max_threads();
        std::vector<std::vector<QOrd>> loc(nt);
        for (auto& v : loc) v.reserve(20000);

        #pragma omp parallel for schedule(dynamic, 2) num_threads(nt)
        for (uint32_t b = 0; b < nb; ++b) {
            // Skip block if all dates >= threshold (cannot pass o_orderdate < THRESH)
            if (!oz.empty() && oz[b].min_v >= DATE_THRESH) continue;

            const size_t rs = (size_t)b * BS;
            const size_t re = std::min(rs + (size_t)BS, total);
            const int tid = omp_get_thread_num();
            auto& lv = loc[tid];

            for (size_t i = rs; i < re; ++i) {
                if (o_od.p[i] >= DATE_THRESH) continue;
                if (!cust_set.probe(o_ck.p[i])) continue;
                lv.push_back({o_ok.p[i], o_od.p[i], o_sp.p[i]});
            }
        }

        for (auto& v : loc)
            for (auto& q : v) qords.push_back(q);

        o_ok.close(); o_ck.close(); o_od.close(); o_sp.close();
    }

    // ── Phase 3: Build orders hash map okey → {odate, sprio} ─────────────────
    uint32_t om_cap = 1048576; // 2^20
    while (om_cap < (uint32_t)(qords.size() * 2)) om_cap <<= 1;
    OrdMap ord_map(om_cap);
    {
        GENDB_PHASE("build_joins");
        for (const auto& q : qords) ord_map.insert(q.okey, q.odate, q.sprio);
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
    // Replaces 64 × 2M-slot AggMaps (2.5 GB) with small per-partition staging
    static constexpr int NPART = 64;
    struct AggTuple { int32_t key, odate, sprio; double rev; }; // 20 bytes
    std::vector<std::vector<std::vector<AggTuple>>> stage(
        nt, std::vector<std::vector<AggTuple>>(NPART));

    {
        GENDB_PHASE("main_scan");
        Col<int32_t> l_ok; l_ok.open(gendb_dir + "/lineitem/l_orderkey.bin");
        Col<int32_t> l_sd; l_sd.open(gendb_dir + "/lineitem/l_shipdate.bin");
        Col<double>  l_ep; l_ep.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        Col<double>  l_dc; l_dc.open(gendb_dir + "/lineitem/l_discount.bin");

        // lineitem is sorted by l_shipdate ascending → binary search for first row > THRESH
        const size_t li_n = l_sd.n;
        size_t li_start = 0;
        {
            size_t lo = 0, hi = li_n;
            while (lo < hi) {
                size_t mid = lo + (hi - lo) / 2;
                if (l_sd.p[mid] <= DATE_THRESH) lo = mid + 1;
                else hi = mid;
            }
            li_start = lo;
        }

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
                // Hash map probe (bloom passed)
                const OrdVal* ov = ord_map.find(lkey);
                if (!ov) continue;
                double rev = l_ep.p[i] * (1.0 - l_dc.p[i]);
                int p = (int)(h1 >> 26); // top 6 bits → partition 0..63
                my_stage[p].push_back({lkey, ov->odate, ov->sprio, rev});
            }
        }
        l_ok.close(); l_sd.close(); l_ep.close(); l_dc.close();
    }

    // ── Phase 4b: Parallel per-partition aggregation ──────────────────────────
    // ~750K groups / 64 partitions = ~12K groups/partition → 32K slots each
    // PAGG_EMPTY = -1: l_orderkey is always positive in TPC-H
    // Total: 64 × (128KB keys + 512KB vals) = 40MB (vs 2.5GB before)
    static constexpr uint32_t PART_CAP   = 32768; // 2^15
    static constexpr uint32_t PART_HMASK = PART_CAP - 1;
    static constexpr int32_t  PAGG_EMPTY = -1;
    struct PartAgg {
        int32_t keys[PART_CAP]; // 128KB; init to PAGG_EMPTY via memset(0xFF)
        AggVal  vals[PART_CAP]; // 512KB; initialized on first insert
        PartAgg() { memset(keys, 0xFF, sizeof(keys)); }
    };
    auto pagg = std::make_unique<PartAgg[]>(NPART); // heap: 64 × 640KB = 40MB

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
                        pa.vals[s] = {tup.rev, tup.odate, tup.sprio};
                    } else {
                        pa.vals[s].rev += tup.rev;
                    }
                }
            }
        }
    }

    // ── Collect results & top-10 ──────────────────────────────────────────────
    struct Res { int32_t okey; double rev; int32_t odate; int32_t sprio; };
    std::vector<Res> top;
    top.reserve(800000);
    for (int p = 0; p < NPART; ++p) {
        const PartAgg& pa = pagg[p];
        for (uint32_t i = 0; i < PART_CAP; ++i) {
            if (pa.keys[i] != PAGG_EMPTY)
                top.push_back({pa.keys[i], pa.vals[i].rev,
                               pa.vals[i].odate, pa.vals[i].sprio});
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
                << r.sprio << "\n";
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
