// Q9: Product Type Profit Measure
// Strategy (iter_2 — three major upgrades over iter_1):
//
//  UPGRADE 1: PARALLEL dim_filter (part scan)
//    - OpenMP 32 threads scan 2M part rows (112MB p_name) for 'green'
//    - Atomic OR for concurrent bitset updates (no per-thread bitset copies)
//    - Expected: 50ms → ~8ms
//
//  UPGRADE 2: PARALLEL build_joins (partsupp scan)
//    - 32 threads scan 8M partsupp rows, each collecting qualifying (key,val) pairs
//      in per-thread vectors (cache-line aligned to prevent false sharing)
//    - Sequential insert of ~384K qualifying entries into 16MB hash map
//    - Expected: 53ms → ~8ms
//
//  UPGRADE 3: COMPACT year_arr replaces 268MB orders hash index
//    - Pre-build flat uint8_t year_arr[60000001] from o_orderkey + o_orderdate
//    - Direct array lookup: year_arr[ok] → year_offset, O(1), no hash/probe overhead
//    - 4.5× denser than pre-built hash index (1 byte/entry vs 8 bytes/slot)
//    - Fewer cache misses in main_scan loop for ~2.88M qualifying lineitem rows
//    - Built in parallel with OpenMP; replaces mmap of 268MB orders_orderkey_hash.bin
//
//  UNCHANGED:
//    4. Compact open-addressing partsupp hash map (16MB, L3-resident)
//    5. Parallel lineitem scan with direct thread-local agg[nation][year] arrays
//    6. Trivial merge + sort + CSV output

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <algorithm>
#include <string>
#include <vector>
#include <fstream>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

static constexpr int      NUM_THREADS  = 32;
static constexpr int      NUM_NATIONS  = 25;
static constexpr int      NUM_YEARS    = 7;   // 1992–1998
static constexpr uint32_t YEAR_ARR_SIZE = 60000001u; // TPC-H SF10 max o_orderkey

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
template<typename T>
static const T* mmap_col(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { fprintf(stderr, "Cannot open: %s\n", path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    void* ptr = mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed: %s\n", path.c_str()); exit(1); }
    count = (size_t)st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

// ---------------------------------------------------------------------------
// Bitset for qualifying part keys (p_partkey in [1..2000000])
// ~250KB — fits in L2 cache per thread
// ---------------------------------------------------------------------------
struct PartBitset {
    static constexpr uint32_t MAX_KEY = 2000001u;
    static constexpr uint32_t NWORDS  = (MAX_KEY + 63u) / 64u;
    uint64_t words[NWORDS];

    void clear() { memset(words, 0, sizeof(words)); }

    // Atomic OR — used during parallel part scan (concurrent writes to same word are safe)
    void set_atomic(uint32_t k) {
        __atomic_fetch_or(&words[k >> 6], (1ULL << (k & 63u)), __ATOMIC_RELAXED);
    }

    bool test(uint32_t k) const { return (words[k >> 6] >> (k & 63u)) & 1u; }
};

// ---------------------------------------------------------------------------
// Compact open-addressing hash map: uint64 key -> double value
// For partsupp: key = (ps_partkey<<32 | ps_suppkey), value = ps_supplycost
// CAP=1<<20 → 16MB (L3-resident after filtering to ~384K qualifying rows)
// ---------------------------------------------------------------------------
struct PartsuppMap {
    static constexpr uint32_t CAP  = 1u << 20; // 1,048,576 slots
    static constexpr uint32_t MASK = CAP - 1u;
    static constexpr uint64_t EMPTY_KEY = UINT64_MAX;

    struct Slot { uint64_t key; double val; };
    Slot* slots = nullptr;

    void init() {
        slots = (Slot*)malloc((size_t)CAP * sizeof(Slot));
        if (!slots) { perror("malloc PartsuppMap"); exit(1); }
        // memset to 0xFF: uint64_t → 0xFFFF...FFFF = UINT64_MAX = EMPTY_KEY
        memset(slots, 0xFF, (size_t)CAP * sizeof(Slot));
    }

    static uint32_t hash(uint64_t k) {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return (uint32_t)(k & MASK);
    }

    void insert(uint64_t key, double val) {
        uint32_t h = hash(key);
        while (slots[h].key != EMPTY_KEY) h = (h + 1u) & MASK;
        slots[h] = {key, val};
    }

    bool lookup(uint64_t key, double& out) const {
        uint32_t h = hash(key);
        while (__builtin_expect(slots[h].key != EMPTY_KEY, 1)) {
            if (slots[h].key == key) { out = slots[h].val; return true; }
            h = (h + 1u) & MASK;
        }
        return false;
    }
};

// ---------------------------------------------------------------------------
// Compact year array: year_arr[o_orderkey] = (o_year - 1992) as uint8_t
//   0xFF = invalid (not a valid orderkey)
//   Range [0..6] = years 1992-1998
// Replaces the 268MB pre-built orders hash index.
// 60MB flat array; 4.5× denser → far fewer L3 cache misses per lookup.
// ---------------------------------------------------------------------------
static uint8_t year_arr[YEAR_ARR_SIZE]; // BSS, zero-initialized; we memset to 0xFF

// ---------------------------------------------------------------------------
// Shared data for parallel lineitem scan threads
// ---------------------------------------------------------------------------
struct SharedData {
    const int32_t* l_partkey;
    const int32_t* l_suppkey;
    const int32_t* l_orderkey;
    const double*  l_extprice;
    const double*  l_discount;
    const double*  l_quantity;
    size_t         n_li;

    const uint8_t*     year_arr_ptr;  // compact orderkey→year_offset array
    const PartBitset*  part_bitset;
    const int8_t*      supp_nk;       // suppkey → nationkey flat array
    const PartsuppMap* psmap;
};

// Thread work: parallel probe — directly accumulates into thread-local agg array
struct ThreadWork {
    const SharedData* shared;
    size_t start, end;
    // Thread-local aggregation: double[nation][year-1992]
    // 25 × 8 × 8 = 1600 bytes — stays in L1 cache throughout probe
    double local_agg[NUM_NATIONS][8];
};

static void* thread_func(void* arg) {
    ThreadWork* w = (ThreadWork*)arg;
    const SharedData& s = *w->shared;
    memset(w->local_agg, 0, sizeof(w->local_agg));

    const PartBitset&  bitset  = *s.part_bitset;
    const int8_t*      snk     = s.supp_nk;
    const PartsuppMap& psmap   = *s.psmap;
    const uint8_t*     yararr  = s.year_arr_ptr;
    double (*agg)[8] = w->local_agg; // L1-resident aggregation grid

    for (size_t i = w->start; i < w->end; i++) {
        // Gate 1: part bitset — eliminates ~95.2% of rows (bitset is L2-resident)
        uint32_t pk = (uint32_t)s.l_partkey[i];
        if (!bitset.test(pk)) continue;

        int32_t sk = s.l_suppkey[i];

        // Gate 2: partsupp lookup — L3-resident (16MB map)
        uint64_t ps_key = ((uint64_t)pk << 32) | (uint32_t)sk;
        double supplycost;
        if (!psmap.lookup(ps_key, supplycost)) continue;

        // Gate 3: orders year lookup — compact flat array (60MB), ~4.5× denser
        //   than 268MB hash index → fewer L3/DRAM misses per qualifying row
        uint32_t ok = (uint32_t)s.l_orderkey[i];
        uint8_t yr = (ok < YEAR_ARR_SIZE) ? yararr[ok] : 0xFFu;
        if (__builtin_expect(yr > 6u, 0)) continue;

        // Compute profit amount and accumulate into L1-resident agg array
        double amount = s.l_extprice[i] * (1.0 - s.l_discount[i])
                      - supplycost * s.l_quantity[i];

        uint8_t nk = (uint8_t)snk[sk];
        agg[nk][yr] += amount;
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// Per-thread entry buffer for parallel partsupp scan
// alignas(64) prevents false sharing between adjacent vector headers
// ---------------------------------------------------------------------------
struct alignas(64) AlignedPSVec {
    std::vector<std::pair<uint64_t,double>> v;
};

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // Static structures to avoid stack overflow
    static int8_t      supp_nk[100001]; // 100KB, suppkey→nationkey
    static PartBitset  part_bitset;     // ~250KB
    static PartsuppMap psmap;           // 16MB heap

    std::vector<std::string> nation_names(25);

    // ---- Phase 1: Load dimensions + PARALLEL part bitset ----
    {
        GENDB_PHASE("dim_filter");

        // Load nation name dictionary (25 rows, trivial)
        {
            std::string dp = gendb_dir + "/nation/n_name_dict.txt";
            std::ifstream f(dp);
            std::string line;
            int idx = 0;
            while (std::getline(f, line) && idx < 25) nation_names[idx++] = line;
        }

        // Build suppkey → nationkey flat array (100K rows, <1ms)
        {
            size_t n;
            const int32_t* s_sk = mmap_col<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", n);
            const int32_t* s_nk = mmap_col<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", n);
            memset(supp_nk, 0, sizeof(supp_nk));
            for (size_t i = 0; i < n; i++) supp_nk[s_sk[i]] = (int8_t)s_nk[i];
        }

        // PARALLEL: Filter part on p_name LIKE '%green%' → bitset
        //   32 OpenMP threads scan 2M rows (112MB) concurrently.
        //   Atomic OR ensures no bits are lost even when two threads happen to
        //   write into the same 64-bit bitset word.
        {
            part_bitset.clear();
            size_t nc;
            const char*    p_nm = mmap_col<char>(gendb_dir + "/part/p_name.bin", nc);
            size_t nk;
            const int32_t* p_pk = mmap_col<int32_t>(gendb_dir + "/part/p_partkey.bin", nk);

            #pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
            for (size_t i = 0; i < nk; i++) {
                if (strstr(p_nm + i * 56, "green")) {
                    part_bitset.set_atomic((uint32_t)p_pk[i]);
                }
            }
        }
    }

    // ---- Phase 2: PARALLEL partsupp scan + build hash map; build year_arr ----
    // Both operations are independent after Phase 1 and run sequentially here,
    // but each is now internally parallelized.
    {
        GENDB_PHASE("build_joins");

        // --- 2a: Parallel partsupp scan → collect qualifying entries ---
        //   32 threads scan 8M rows; each thread stores its qualifying (key,cost)
        //   pairs in its own cache-line-aligned vector to avoid false sharing.
        //   After the parallel section, ~384K entries are inserted sequentially
        //   into the 16MB hash map (hash map is not thread-safe for concurrent insert).
        {
            size_t n;
            const int32_t* ps_pk   = mmap_col<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", n);
            const int32_t* ps_sk   = mmap_col<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", n);
            const double*  ps_cost = mmap_col<double>(gendb_dir + "/partsupp/ps_supplycost.bin", n);

            // Per-thread local entry buffers (cache-line aligned, ~12K entries per thread avg)
            static AlignedPSVec tl_entries[NUM_THREADS];
            for (int t = 0; t < NUM_THREADS; t++) {
                tl_entries[t].v.clear();
                tl_entries[t].v.reserve(20000);
            }

            #pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
            for (size_t i = 0; i < n; i++) {
                if (!part_bitset.test((uint32_t)ps_pk[i])) continue;
                uint64_t key = ((uint64_t)(uint32_t)ps_pk[i] << 32) | (uint32_t)ps_sk[i];
                tl_entries[omp_get_thread_num()].v.emplace_back(key, ps_cost[i]);
            }

            // Sequential insert into hash map: ~384K inserts into 16MB L3-resident map
            psmap.init();
            for (int t = 0; t < NUM_THREADS; t++) {
                for (auto& [k, v] : tl_entries[t].v) psmap.insert(k, v);
            }
        }

        // --- 2b: Build compact year_arr from orders ---
        //   Replace mmap of 268MB pre-built orders hash index with a 60MB flat
        //   uint8_t array. Direct array lookup (no hash, no probe) is O(1) and
        //   4.5× denser than the hash index (1 byte/entry vs 8 bytes/slot),
        //   substantially reducing cache pressure during the main lineitem scan.
        {
            memset(year_arr, 0xFF, sizeof(year_arr)); // 0xFF = invalid

            size_t n_o;
            const int32_t* o_okey  = mmap_col<int32_t>(gendb_dir + "/orders/o_orderkey.bin", n_o);
            const int32_t* o_odate = mmap_col<int32_t>(gendb_dir + "/orders/o_orderdate.bin", n_o);

            // Parallel: each o_orderkey is unique (PK), so no write races
            #pragma omp parallel for num_threads(NUM_THREADS) schedule(static)
            for (size_t i = 0; i < n_o; i++) {
                uint32_t ok = (uint32_t)o_okey[i];
                if (ok < YEAR_ARR_SIZE) {
                    int yr = gendb::extract_year(o_odate[i]) - 1992;
                    year_arr[ok] = (uint8_t)yr;
                }
            }
        }
    }

    // Load lineitem columns
    size_t n_li, nd;
    const int32_t* l_partkey  = mmap_col<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", n_li);
    const int32_t* l_suppkey  = mmap_col<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", n_li);
    const int32_t* l_orderkey = mmap_col<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", n_li);
    const double*  l_extprice = mmap_col<double>(gendb_dir + "/lineitem/l_extendedprice.bin", nd);
    const double*  l_discount = mmap_col<double>(gendb_dir + "/lineitem/l_discount.bin", nd);
    const double*  l_quantity = mmap_col<double>(gendb_dir + "/lineitem/l_quantity.bin", nd);

    // ---- Phase 3: Parallel lineitem scan with direct thread-local aggregation ----
    SharedData shared;
    shared.l_partkey    = l_partkey;
    shared.l_suppkey    = l_suppkey;
    shared.l_orderkey   = l_orderkey;
    shared.l_extprice   = l_extprice;
    shared.l_discount   = l_discount;
    shared.l_quantity   = l_quantity;
    shared.n_li         = n_li;
    shared.year_arr_ptr = year_arr;
    shared.part_bitset  = &part_bitset;
    shared.supp_nk      = supp_nk;
    shared.psmap        = &psmap;

    static ThreadWork workers[NUM_THREADS];
    pthread_t threads[NUM_THREADS];

    {
        GENDB_PHASE("main_scan");
        size_t chunk = (n_li + NUM_THREADS - 1) / NUM_THREADS;
        for (int t = 0; t < NUM_THREADS; t++) {
            workers[t].shared = &shared;
            workers[t].start  = (size_t)t * chunk;
            workers[t].end    = std::min(workers[t].start + chunk, n_li);
            pthread_create(&threads[t], nullptr, thread_func, &workers[t]);
        }
        for (int t = 0; t < NUM_THREADS; t++) pthread_join(threads[t], nullptr);
    }

    // ---- Phase 4: Merge thread-local aggregation arrays ----
    // Trivial: 32 × 25 × 8 = 6400 additions
    {
        GENDB_PHASE("aggregation_merge");
        double agg[NUM_NATIONS][8];
        memset(agg, 0, sizeof(agg));

        for (int t = 0; t < NUM_THREADS; t++) {
            const double (*la)[8] = workers[t].local_agg;
            for (int n = 0; n < NUM_NATIONS; n++) {
                for (int y = 0; y < 8; y++) {
                    agg[n][y] += la[n][y];
                }
            }
        }

        // ---- Phase 5: Output results ----
        {
            GENDB_PHASE("output");

            struct Row { const char* nation; int year; double sum_profit; };
            std::vector<Row> results;
            results.reserve(NUM_NATIONS * NUM_YEARS);

            for (int n = 0; n < NUM_NATIONS; n++) {
                for (int y = 0; y < NUM_YEARS; y++) {
                    if (agg[n][y] != 0.0) {
                        results.push_back({nation_names[n].c_str(), 1992 + y, agg[n][y]});
                    }
                }
            }

            // Sort: nation ASC, o_year DESC
            std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
                int cmp = strcmp(a.nation, b.nation);
                if (cmp != 0) return cmp < 0;
                return a.year > b.year;
            });

            // Write CSV
            std::string out_path = results_dir + "/Q9.csv";
            FILE* f = fopen(out_path.c_str(), "w");
            if (!f) { perror("fopen Q9.csv"); exit(1); }
            fprintf(f, "nation,o_year,sum_profit\n");
            for (const auto& r : results) {
                fprintf(f, "%s,%d,%.2f\n", r.nation, r.year, r.sum_profit);
            }
            fclose(f);
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
