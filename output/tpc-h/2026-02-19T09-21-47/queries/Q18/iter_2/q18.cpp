// Q18: Large Volume Customer
// Strategy:
//   Phase 1 (parallel): Key-partitioned aggregation over lineitem (l_orderkey, l_quantity).
//     Thread t owns keys where (HIGH32(key*C) & part_mask) == t.
//     Each thread-local map is small (~234K entries), fits well in cache.
//     No synchronization needed.
//   Phase 2 (sequential): Index point lookups into o_orderkey_hash and c_custkey_hash
//     for qualifying orderkeys. Reuses Phase-1 sums — no second lineitem scan needed.
//   Phase 3: Sort by o_totalprice DESC, o_orderdate ASC; emit top 100.

#include <cstdint>
#include <cstring>
#include <cstdio>
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
// Uses LOW 32 bits of (key * GOLDEN_RATIO) as probe hash.
// Partition assignment uses HIGH 32 bits — these are statistically independent,
// preventing the slot-alignment clustering bug that would arise from using the
// same hash bits for both partition and probe.
// ---------------------------------------------------------------------------
struct AggMap {
    static constexpr int32_t EMPTY = INT32_MIN;

    struct alignas(16) Slot { int32_t key; float _pad; double sum; };
    // Note: use a simpler layout for cache efficiency
    struct Slot2 { int32_t key; int32_t _pad; double sum; };

    Slot2* slots = nullptr;
    uint32_t cap = 0, mask = 0;

    AggMap() = default;
    ~AggMap() { delete[] slots; }
    AggMap(const AggMap&) = delete;
    AggMap& operator=(const AggMap&) = delete;

    void init(uint32_t pow2_cap) {
        cap = pow2_cap;
        mask = cap - 1;
        slots = new Slot2[cap];
        for (uint32_t i = 0; i < cap; i++) {
            slots[i].key = EMPTY;
            slots[i].sum = 0.0;
        }
    }

    // Probe hash uses LOW 32 bits of full 64-bit product
    inline void add(int32_t key, double val) {
        uint32_t h = (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) & mask;
        for (;;) {
            if (__builtin_expect(slots[h].key == key, 1)) { slots[h].sum += val; return; }
            if (slots[h].key == EMPTY) { slots[h].key = key; slots[h].sum = val; return; }
            h = (h + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------------------
// Hash index slot (matches on-disk layout)
// ---------------------------------------------------------------------------
struct HashSlot { int32_t key; uint32_t row_pos; };

// Point lookup in a pre-built open-addressing hash index.
// Returns row_pos, or UINT32_MAX if key not found.
static inline uint32_t idx_lookup(const HashSlot* slots, uint32_t cap,
                                   int32_t key, int shift) {
    uint32_t h = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> shift) & (cap - 1);
    for (;;) {
        if (slots[h].key == key) return slots[h].row_pos;
        if (slots[h].key == INT32_MIN) return UINT32_MAX;
        h = (h + 1) & (cap - 1);
    }
}

// ---------------------------------------------------------------------------
// Result row
// ---------------------------------------------------------------------------
struct ResultRow {
    std::string c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double  o_totalprice;
    double  sum_qty;
};

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // =========================================================================
    // Phase 1: Parallel lineitem scan — build orderkey -> SUM(l_quantity)
    //   Key-partitioned: thread t owns keys whose high-word hash & part_mask == t.
    //   No mutex needed; each thread writes only to its own AggMap.
    // =========================================================================

    std::vector<std::pair<int32_t, double>> qualifying; // (orderkey, sum_qty) where sum > 300

    {
        GENDB_PHASE("dim_filter");

        // Prefault lineitem data into page cache (MAP_POPULATE) so threads start immediately
        size_t sz_lok, sz_lqty;
        const int32_t* l_orderkey = (const int32_t*)do_mmap(
            gendb_dir + "/lineitem/l_orderkey.bin", sz_lok, /*populate=*/true);
        const double* l_quantity = (const double*)do_mmap(
            gendb_dir + "/lineitem/l_quantity.bin", sz_lqty, /*populate=*/true);
        const size_t nrows = sz_lok / sizeof(int32_t);

        // Thread/partition count: power of 2, up to 64
        int hw = (int)std::thread::hardware_concurrency();
        if (hw <= 0) hw = 16;
        int npart = 1;
        while (npart * 2 <= hw && npart < 64) npart <<= 1;
        const uint32_t part_mask = (uint32_t)(npart - 1);

        // Per-partition aggregation maps.
        // Expected unique keys per partition: 15M / npart ≈ 234K (for npart=64).
        // Cap = 1M gives ~23% load factor — ample headroom for statistical variance.
        // Memory: 64 * 1M * 16B = 1GB total (fine on 376GB machine).
        // Scale capacity with npart to maintain ≤50% load factor regardless of core count.
        // Each partition may see up to 15M/npart unique orderkeys; multiply by 2× for headroom.
        const uint32_t MAP_CAP = std::max(1u << 20, (uint32_t)(30000000 / npart));

        std::vector<AggMap> maps(npart);
        for (int t = 0; t < npart; t++) maps[t].init(MAP_CAP);

        // Launch parallel threads
        std::vector<std::thread> threads;
        threads.reserve(npart);
        for (int t = 0; t < npart; t++) {
            threads.emplace_back([&, t]() {
                AggMap& m = maps[t];
                const uint32_t my_part = (uint32_t)t;
                const uint32_t pmask   = part_mask;
                // Partition key: HIGH 32 bits of (key * C).
                // Map probe: LOW 32 bits of (key * C) — independent of partition bits.
                for (size_t i = 0; i < nrows; i++) {
                    const int32_t k = l_orderkey[i];
                    const uint64_t h64 = (uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL;
                    const uint32_t p   = (uint32_t)(h64 >> 32) & pmask;
                    if (__builtin_expect(p == my_part, 0)) {
                        m.add(k, l_quantity[i]);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Collect qualifying orderkeys (SUM(l_quantity) > 300)
        for (int t = 0; t < npart; t++) {
            const AggMap& m = maps[t];
            for (uint32_t i = 0; i < m.cap; i++) {
                if (m.slots[i].key != AggMap::EMPTY && m.slots[i].sum > 300.0) {
                    qualifying.emplace_back(m.slots[i].key, m.slots[i].sum);
                }
            }
        }
    } // Phase 1 done; thread-local maps destroyed

    // =========================================================================
    // Phase 2: For each qualifying orderkey, do index point lookups:
    //   o_orderkey_hash  -> orders row_pos -> o_custkey, o_orderdate, o_totalprice
    //   c_custkey_hash   -> customer row_pos -> c_name (varstring)
    //   sum_qty reused from Phase 1 (no second lineitem scan needed).
    // =========================================================================

    std::vector<ResultRow> results;
    results.reserve(qualifying.size() + 4);

    {
        GENDB_PHASE("build_joins");

        // --- Orders hash index ---
        // Layout: [uint32_t cap][uint32_t num_entries][HashSlot × cap]
        // Hash formula: h = (key * C) >> (64-25)  & (cap-1),  cap = 2^25
        size_t sz_ohi;
        const uint8_t* ohi_raw = (const uint8_t*)do_mmap(
            gendb_dir + "/orders/indexes/o_orderkey_hash.bin", sz_ohi);
        const uint32_t ohi_cap  = *(const uint32_t*)(ohi_raw + 0);
        const HashSlot* ohi     = (const HashSlot*)(ohi_raw + 8);
        const int       ohi_shift = 64 - 25; // >> 39, cap = 2^25

        // --- Customer hash index ---
        // Layout: [uint32_t cap][uint32_t num_entries][HashSlot × cap]
        // Hash formula: h = (key * C) >> (64-22)  & (cap-1),  cap = 2^22
        size_t sz_chi;
        const uint8_t* chi_raw = (const uint8_t*)do_mmap(
            gendb_dir + "/customer/indexes/c_custkey_hash.bin", sz_chi);
        const uint32_t chi_cap  = *(const uint32_t*)(chi_raw + 0);
        const HashSlot* chi     = (const HashSlot*)(chi_raw + 8);
        const int       chi_shift = 64 - 22; // >> 42, cap = 2^22

        // --- Orders columns ---
        size_t sz_oc, sz_od, sz_op;
        const int32_t* o_custkey   = (const int32_t*)do_mmap(gendb_dir + "/orders/o_custkey.bin", sz_oc);
        const int32_t* o_orderdate = (const int32_t*)do_mmap(gendb_dir + "/orders/o_orderdate.bin", sz_od);
        const double*  o_totalprice = (const double*)do_mmap(gendb_dir + "/orders/o_totalprice.bin", sz_op);

        // --- Customer name (varstring) ---
        size_t sz_cno, sz_cnd;
        const uint32_t* c_name_off = (const uint32_t*)do_mmap(
            gendb_dir + "/customer/c_name_offsets.bin", sz_cno);
        const char* c_name_dat = (const char*)do_mmap(
            gendb_dir + "/customer/c_name_data.bin", sz_cnd);

        for (auto& [okey, sqty] : qualifying) {
            // Lookup order row
            const uint32_t orow = idx_lookup(ohi, ohi_cap, okey, ohi_shift);
            if (orow == UINT32_MAX) continue; // shouldn't happen for valid data

            const int32_t custkey    = o_custkey[orow];
            const int32_t orderdate  = o_orderdate[orow];
            const double  totalprice = o_totalprice[orow];

            // Lookup customer row
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
        if ((int)results.size() > LIMIT) {
            std::partial_sort(results.begin(), results.begin() + LIMIT, results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    if (a.o_totalprice != b.o_totalprice)
                        return a.o_totalprice > b.o_totalprice;
                    return a.o_orderdate < b.o_orderdate;
                });
            results.resize(LIMIT);
        } else {
            std::sort(results.begin(), results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    if (a.o_totalprice != b.o_totalprice)
                        return a.o_totalprice > b.o_totalprice;
                    return a.o_orderdate < b.o_orderdate;
                });
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
