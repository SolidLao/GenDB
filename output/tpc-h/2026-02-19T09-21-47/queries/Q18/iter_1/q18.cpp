// Q18: Large Volume Customer
//
// === ROOT CAUSE (iter_0 bottleneck: dim_filter 1055ms = 99%) ===
// The key-partitioned approach scanned the ENTIRE 720MB lineitem dataset on all 64 threads.
// Effective memory traffic: 64 × 720MB = ~46GB → saturated DRAM bandwidth (~920ms).
//
// === FIX: Morsel-Driven Parallel Aggregation + Parallel Partitioned Merge ===
//
//   Phase 1a (parallel, morsel-driven):
//     Thread t processes rows [t*M, (t+1)*M) — no overlap, no redundant reads.
//     Data read ONCE total (720MB) vs 46GB before → 64× less DRAM traffic.
//     Each thread builds its own local AggMap (~234K unique keys, cap=1M, 16MB).
//     Maps initialized inside threads (NUMA first-touch + parallel calloc lazy-zero).
//     EMPTY=0 (TPC-H orderkeys always > 0) + calloc → OS COW zero pages, free init.
//
//   Phase 1b (parallel partitioned merge):
//     64 merge threads, merge thread m owns keys where HIGH32(key×C) & mask == m.
//     Each merge thread scans all 64 local maps SEQUENTIALLY (hardware-prefetchable).
//     Since all merge threads read the SAME local map pages concurrently, L3 cache
//     sharing means DRAM fetches ≈ 1024MB total (not 64×1024MB).
//     Each merge thread builds its own small merge map (cap=1M, 23% load) → L3-partial.
//     After merge: scan own map for qualifying entries (sum > 300).
//
//   Phase 2 (sequential): Index point lookups for qualifying orderkeys.
//   Phase 3: partial_sort for top-100 by o_totalprice DESC, o_orderdate ASC.

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>

#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
static void* do_mmap(const std::string& path, size_t& out_size, bool populate = false) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    int flags = MAP_PRIVATE | (populate ? MAP_POPULATE : 0);
    void* p = mmap(nullptr, out_size, PROT_READ, flags, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    return p;
}

// ---------------------------------------------------------------------------
// Open-addressing hash map: int32_t key -> double sum
//
// EMPTY = 0: TPC-H orderkeys are always positive (>0), so 0 is a safe sentinel.
//   Benefit: calloc() zero-initializes slots — OS provides COW zero pages with
//   no upfront memset cost. First-touch happens lazily inside threads (parallel).
//
// Probe hash: LOW 32 bits of (key × C).
// Partition hash (for merge): HIGH 32 bits of (key × C). Statistically independent
//   of probe hash — prevents slot-alignment clustering when using same hash.
// ---------------------------------------------------------------------------
struct AggMap {
    static constexpr int32_t  EMPTY  = 0;
    static constexpr uint64_t HASH_C = 0x9E3779B97F4A7C15ULL;

    struct Slot2 { int32_t key; int32_t _pad; double sum; }; // 16 bytes, naturally aligned

    Slot2*   slots = nullptr;
    uint32_t cap   = 0;
    uint32_t mask  = 0;

    AggMap() = default;
    ~AggMap() { if (slots) { free(slots); slots = nullptr; } }
    AggMap(const AggMap&)            = delete;
    AggMap& operator=(const AggMap&) = delete;

    void init(uint32_t pow2_cap) {
        cap  = pow2_cap;
        mask = cap - 1;
        // calloc: OS provides zero-initialized (COW) pages; no explicit memset.
        // EMPTY=0 means freshly calloc'd slots ARE correctly empty.
        slots = static_cast<Slot2*>(calloc(static_cast<size_t>(cap), sizeof(Slot2)));
        if (!slots) { perror("calloc AggMap"); exit(1); }
    }

    // Probe hash uses LOW 32 bits; independent of partition hash (HIGH 32 bits).
    inline void add(int32_t key, double val) {
        uint32_t h = static_cast<uint32_t>(
            static_cast<uint64_t>(static_cast<uint32_t>(key)) * HASH_C) & mask;
        for (;;) {
            if (__builtin_expect(slots[h].key == key, 1)) {
                slots[h].sum += val;
                return;
            }
            if (slots[h].key == EMPTY) {
                slots[h].key = key;
                slots[h].sum = val;
                return;
            }
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Hash index slot (matches on-disk layout)
// ---------------------------------------------------------------------------
struct HashSlot { int32_t key; uint32_t row_pos; };

static inline uint32_t idx_lookup(const HashSlot* slots, uint32_t cap,
                                   int32_t key, int shift) {
    uint32_t h = static_cast<uint32_t>(
        (static_cast<uint64_t>(static_cast<uint32_t>(key)) * 0x9E3779B97F4A7C15ULL) >> shift)
        & (cap - 1);
    for (;;) {
        if (slots[h].key == key)        return slots[h].row_pos;
        if (slots[h].key == INT32_MIN)  return UINT32_MAX;
        h = (h + 1) & (cap - 1);
    }
}

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    std::string c_name;
    int32_t     c_custkey;
    int32_t     o_orderkey;
    int32_t     o_orderdate;
    double      o_totalprice;
    double      sum_qty;
};

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // =========================================================================
    // Phase 1 (dim_filter): Aggregate lineitem → qualifying orderkeys (sum > 300)
    // Two-sub-phases:
    //   1a. Morsel-driven parallel scan: thread t reads rows [t*M, (t+1)*M) only.
    //       Total DRAM reads: 720MB (not 64×720MB as in key-partitioned approach).
    //   1b. Parallel partitioned merge: merge thread m collects keys for partition m
    //       from all local maps. Sequential scans share L3 cache lines across threads.
    // =========================================================================
    std::vector<std::pair<int32_t, double>> qualifying;

    {
        GENDB_PHASE("dim_filter");

        // mmap lineitem columns (MAP_POPULATE: kernel pre-faults pages into page cache)
        size_t sz_lok, sz_lqty;
        const int32_t* l_orderkey = static_cast<const int32_t*>(
            do_mmap(gendb_dir + "/lineitem/l_orderkey.bin", sz_lok, /*populate=*/true));
        const double* l_quantity  = static_cast<const double*>(
            do_mmap(gendb_dir + "/lineitem/l_quantity.bin", sz_lqty, /*populate=*/true));
        const size_t nrows = sz_lok / sizeof(int32_t);

        // Thread count: largest power of 2 ≤ hardware_concurrency, max 64.
        // Must be a power of 2 so (pmask = nthreads-1) works as a bitmask.
        int hw = static_cast<int>(std::thread::hardware_concurrency());
        if (hw <= 0) hw = 16;
        int nthreads = 1;
        while (nthreads * 2 <= hw && nthreads < 64) nthreads <<= 1;
        const uint32_t pmask = static_cast<uint32_t>(nthreads - 1);

        // LOCAL_CAP: 15M unique orderkeys / 64 threads ≈ 234K per morsel.
        // 1M slots → 23% load factor → very fast linear probing, few collisions.
        // calloc inside thread → parallel lazy page allocation (NUMA-local).
        const uint32_t LOCAL_CAP = 1u << 20; // 1M slots = 16MB per thread

        // --- Phase 1a: Morsel-driven parallel local aggregation ---
        const size_t morsel = (nrows + static_cast<size_t>(nthreads) - 1)
                              / static_cast<size_t>(nthreads);

        std::vector<AggMap> local_maps(nthreads);
        {
            std::vector<std::thread> threads;
            threads.reserve(nthreads);
            for (int t = 0; t < nthreads; t++) {
                threads.emplace_back([&, t]() {
                    // Init inside thread: parallel calloc + NUMA first-touch locality.
                    local_maps[t].init(LOCAL_CAP);
                    const size_t start = static_cast<size_t>(t) * morsel;
                    const size_t end   = std::min(start + morsel, nrows);
                    AggMap& m = local_maps[t];
                    // Inner loop: sequential read of 11.25MB per thread.
                    // Hardware prefetcher handles sequential streams automatically.
                    for (size_t i = start; i < end; i++) {
                        m.add(l_orderkey[i], l_quantity[i]);
                    }
                });
            }
            for (auto& th : threads) th.join();
        }

        // --- Phase 1b: Parallel partitioned merge ---
        // Partition function: HIGH 32 bits of (key × C), masked to [0, nthreads).
        // Independent of probe hash (LOW 32 bits) → no clustering.
        //
        // All merge threads scan the SAME local maps sequentially → hardware prefetch
        // loads each cache line ONCE into L3, shared by all merge threads.
        // Net DRAM traffic ≈ 64 × 16MB = 1024MB (not 64× repeated) ≈ 7ms at 150GB/s.
        //
        // Each merge thread's merge map: cap=1M, 23% load → mostly L3-resident per thread.
        const uint32_t MERGE_CAP = 1u << 20; // 1M slots = 16MB per merge thread

        std::vector<AggMap> merge_maps(nthreads);
        // Each merge thread collects its qualifying entries independently (no contention).
        std::vector<std::vector<std::pair<int32_t, double>>> qual_parts(nthreads);

        {
            std::vector<std::thread> threads;
            threads.reserve(nthreads);
            for (int m = 0; m < nthreads; m++) {
                threads.emplace_back([&, m]() {
                    // Init merge map inside thread (parallel lazy allocation).
                    merge_maps[m].init(MERGE_CAP);
                    AggMap& mm           = merge_maps[m];
                    const uint32_t mypart = static_cast<uint32_t>(m);

                    // Scan ALL local maps for keys belonging to partition m.
                    // Sequential per-map scan: hardware prefetcher covers 16MB/map.
                    for (int t = 0; t < nthreads; t++) {
                        const AggMap::Slot2* __restrict__ lslots = local_maps[t].slots;
                        const uint32_t lcap = local_maps[t].cap;
                        for (uint32_t i = 0; i < lcap; i++) {
                            const int32_t k = lslots[i].key;
                            // 78% of slots are empty (22% load factor) → predict empty.
                            if (__builtin_expect(k == AggMap::EMPTY, 1)) continue;
                            // Partition: HIGH 32 bits of (k × C), independent of probe hash.
                            const uint32_t p = static_cast<uint32_t>(
                                (static_cast<uint64_t>(static_cast<uint32_t>(k))
                                 * AggMap::HASH_C) >> 32) & pmask;
                            if (p == mypart) {
                                mm.add(k, lslots[i].sum);
                            }
                        }
                    }

                    // Collect qualifying entries from this partition's merge map.
                    // Expected: very few (~few hundred / nthreads) qualifying orderkeys.
                    auto& qp = qual_parts[m];
                    for (uint32_t i = 0; i < mm.cap; i++) {
                        if (mm.slots[i].key != AggMap::EMPTY && mm.slots[i].sum > 300.0) {
                            qp.emplace_back(mm.slots[i].key, mm.slots[i].sum);
                        }
                    }
                });
            }
            for (auto& th : threads) th.join();
        }

        // Collect qualifying from all partitions (trivially small — few hundred rows total).
        for (int m = 0; m < nthreads; m++) {
            for (auto& e : qual_parts[m]) {
                qualifying.emplace_back(e.first, e.second);
            }
        }
    } // dim_filter scope ends: local_maps, merge_maps freed

    // =========================================================================
    // Phase 2: For each qualifying orderkey, index point lookups:
    //   o_orderkey_hash → orders row → o_custkey, o_orderdate, o_totalprice
    //   c_custkey_hash  → customer row → c_name (varstring)
    // sum_qty reused from Phase 1 — no second lineitem scan.
    // =========================================================================
    std::vector<ResultRow> results;
    results.reserve(qualifying.size() + 4);

    {
        GENDB_PHASE("build_joins");

        // --- Orders hash index ---
        // Layout: [uint32_t cap][uint32_t num_entries][HashSlot × cap]
        // cap=2^25=33554432; hash: h=(key×C)>>39 & (cap-1); empty: key==INT32_MIN
        size_t sz_ohi;
        const uint8_t* ohi_raw = static_cast<const uint8_t*>(
            do_mmap(gendb_dir + "/orders/indexes/o_orderkey_hash.bin", sz_ohi));
        const uint32_t  ohi_cap   = *reinterpret_cast<const uint32_t*>(ohi_raw);
        const HashSlot* ohi       = reinterpret_cast<const HashSlot*>(ohi_raw + 8);
        const int       ohi_shift = 64 - 25; // cap=2^25

        // --- Customer hash index ---
        // Layout: [uint32_t cap][uint32_t num_entries][HashSlot × cap]
        // cap=2^22=4194304; hash: h=(key×C)>>42 & (cap-1); empty: key==INT32_MIN
        size_t sz_chi;
        const uint8_t* chi_raw = static_cast<const uint8_t*>(
            do_mmap(gendb_dir + "/customer/indexes/c_custkey_hash.bin", sz_chi));
        const uint32_t  chi_cap   = *reinterpret_cast<const uint32_t*>(chi_raw);
        const HashSlot* chi       = reinterpret_cast<const HashSlot*>(chi_raw + 8);
        const int       chi_shift = 64 - 22; // cap=2^22

        // --- Orders columns ---
        size_t sz_oc, sz_od, sz_op;
        const int32_t* o_custkey    = static_cast<const int32_t*>(
            do_mmap(gendb_dir + "/orders/o_custkey.bin", sz_oc));
        const int32_t* o_orderdate  = static_cast<const int32_t*>(
            do_mmap(gendb_dir + "/orders/o_orderdate.bin", sz_od));
        const double*  o_totalprice = static_cast<const double*>(
            do_mmap(gendb_dir + "/orders/o_totalprice.bin", sz_op));

        // --- Customer name (varstring) ---
        size_t sz_cno, sz_cnd;
        const uint32_t* c_name_off = static_cast<const uint32_t*>(
            do_mmap(gendb_dir + "/customer/c_name_offsets.bin", sz_cno));
        const char* c_name_dat = static_cast<const char*>(
            do_mmap(gendb_dir + "/customer/c_name_data.bin", sz_cnd));

        for (auto& [okey, sqty] : qualifying) {
            // O(1) point lookup: qualifying orderkey → orders row_pos
            const uint32_t orow = idx_lookup(ohi, ohi_cap, okey, ohi_shift);
            if (orow == UINT32_MAX) continue;

            const int32_t custkey    = o_custkey[orow];
            const int32_t orderdate  = o_orderdate[orow];
            const double  totalprice = o_totalprice[orow];

            // O(1) point lookup: o_custkey → customer row_pos → c_name
            const uint32_t crow = idx_lookup(chi, chi_cap, custkey, chi_shift);
            if (crow == UINT32_MAX) continue;

            const uint32_t ns = c_name_off[crow];
            const uint32_t ne = c_name_off[crow + 1];

            ResultRow r;
            r.c_name.assign(c_name_dat + ns, ne - ns);
            r.c_custkey    = custkey;
            r.o_orderkey   = okey;
            r.o_orderdate  = orderdate;
            r.o_totalprice = totalprice;
            r.sum_qty      = sqty;
            results.push_back(std::move(r));
        }
    }

    // =========================================================================
    // Phase 3: Sort by o_totalprice DESC, o_orderdate ASC; take top 100
    // =========================================================================
    {
        GENDB_PHASE("sort_topk");
        const int LIMIT = 100;
        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.o_totalprice != b.o_totalprice)
                return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        };
        if (static_cast<int>(results.size()) > LIMIT) {
            std::partial_sort(results.begin(), results.begin() + LIMIT,
                              results.end(), cmp);
            results.resize(LIMIT);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }
    }

    // =========================================================================
    // Phase 4: Write CSV output
    // =========================================================================
    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q18.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); exit(1); }

        fprintf(fout, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char dbuf[16];
        for (const auto& r : results) {
            gendb::epoch_days_to_date_str(r.o_orderdate, dbuf);
            fprintf(fout, "%s,%d,%d,%s,%.2f,%.2f\n",
                    r.c_name.c_str(),
                    r.c_custkey,
                    r.o_orderkey,
                    dbuf,
                    r.o_totalprice,
                    r.sum_qty);
        }
        fclose(fout);
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
