// Q18: Large Volume Customer
// Two-phase: (1) partitioned hash aggregation over lineitem → qualifying orderkey bitmap
//            (2) scan orders + lineitem via bitmap → aggregate, sort, top-100 output
//
// Optimization (iter_4): 2048 partitions (was 64) + flat scatter buffer
//   → per-partition HT = 16384×8B = 131KB fits in L2 (256KB per core)
//   → 64 active HTs = 8.4MB << L3 (44MB); aggregate probes hit L2 (~5ns vs ~100ns L3 miss)
//   → flat 2-pass scatter avoids 131K separate malloc calls; contiguous partition data
//     enables hardware prefetching during aggregate reads

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <climits>
#include <cmath>
#include <cassert>
#include <vector>
#include <algorithm>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <iostream>
#include "date_utils.h"
#include "timing_utils.h"

namespace {

// ── Constants ─────────────────────────────────────────────────────────────────
static constexpr uint32_t KNUTH = 2654435761u;

// Phase 1: 2048 partitions (11 bits)
// 15M unique orderkeys / 2048 = ~7324 per partition → next_pow2(7324*2) = 16384
static constexpr int      NUM_PARTITIONS = 2048;
static constexpr int      P1_PART_BITS   = 11;
static constexpr uint32_t P1_CAP         = 16384;   // next_pow2(7324*2)
static constexpr uint32_t P1_MASK        = P1_CAP - 1;

// Phase 2 aggregation hash table
// ~624 qualifying groups → next_pow2(624*2) = 2048
static constexpr uint32_t P2_CAP  = 2048;
static constexpr uint32_t P2_MASK = P2_CAP - 1;

// Bitmap size: covers orderkey up to 60M  (60,000,000 / 8 + 1 bytes)
static constexpr int BITMAP_BYTES = 7500001;

// ── Slot structs ──────────────────────────────────────────────────────────────
// P1Slot: int32_t sum_qty is sufficient (max sum per order ≈ 7×50=350 << INT32_MAX)
//   → 8 bytes/slot × 16384 slots = 131KB per partition HT → fits in L2 (256KB/core)
struct P1Slot {
    int32_t key;       // sentinel: INT32_MIN
    int32_t sum_qty;   // accumulate as int32_t (max ~700 << INT32_MAX)
};

struct P2Slot {
    int32_t o_orderkey;  // sentinel: INT32_MIN
    double  sum_qty;
};

struct ResultRow {
    int32_t o_orderkey;
    int32_t c_custkey;
    int32_t o_orderdate;
    double  o_totalprice;
    double  sum_qty;
};

// Scatter buffer entry: (key, qty) packed as int32_t×2 = 8 bytes
struct ScatterEntry {
    int32_t key;
    int32_t qty;
};

// ── Hash functions (C36: ORTHOGONAL bit ranges) ────────────────────────────────
// p1_part uses HIGH 11 bits (bits 31..21) → partitions 0..2047
static inline uint32_t p1_part(int32_t key) {
    return ((uint32_t)key * KNUTH) >> (32 - P1_PART_BITS);  // HIGH 11 bits
}
// p1_slot uses LOW 14 bits (bits 13..0) → slot within partition HT
// Orthogonal to p1_part: bits 31..21 vs bits 13..0, gap at bits 20..14
static inline uint32_t p1_slot(int32_t key) {
    return ((uint32_t)key * KNUTH) & P1_MASK;  // LOW 14 bits
}
// p2_hash uses LOW bits for compact 2048-slot table
static inline uint32_t p2_slot(int32_t key) {
    return ((uint32_t)key * KNUTH) & P2_MASK;
}

// ── mmap helper ───────────────────────────────────────────────────────────────
static const void* mmap_file(const std::string& path, size_t& out_bytes) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_bytes = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(ptr, out_bytes, MADV_SEQUENTIAL);
    close(fd);
    return ptr;
}

// ── Main query ────────────────────────────────────────────────────────────────
} // end anonymous namespace

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();  // C11

    // ── Data loading ──────────────────────────────────────────────────────────
    const int32_t* l_orderkey  = nullptr;
    const double*  l_quantity  = nullptr;
    size_t lineitem_rows       = 0;

    const int32_t* o_orderkey  = nullptr;
    const int32_t* o_custkey   = nullptr;
    const int32_t* o_orderdate = nullptr;
    const double*  o_totalprice= nullptr;
    size_t orders_rows         = 0;

    {
        GENDB_PHASE("data_loading");

        size_t sz;
        l_orderkey   = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/lineitem/l_orderkey.bin",  sz));
        lineitem_rows = sz / sizeof(int32_t);
        l_quantity   = reinterpret_cast<const double*> (mmap_file(gendb_dir + "/lineitem/l_quantity.bin",  sz));

        o_orderkey   = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/orders/o_orderkey.bin",    sz));
        orders_rows   = sz / sizeof(int32_t);
        o_custkey    = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/orders/o_custkey.bin",     sz));
        o_orderdate  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/orders/o_orderdate.bin",   sz));
        o_totalprice = reinterpret_cast<const double*> (mmap_file(gendb_dir + "/orders/o_totalprice.bin",  sz));
    }

    // ── Phase 1: Partitioned hash aggregation → orderkey bitmap ───────────────
    // 7.5MB bitmap covering orderkeys up to ~60M
    std::vector<uint8_t> orderkey_bitmap(BITMAP_BYTES, 0);

    {
        GENDB_PHASE("subquery_precompute");

        int nthreads = omp_get_max_threads();

        // ── Flat scatter buffer (480MB, single allocation — no 131K separate mallocs)
        // Allocated without zero-init (written before read in pass 2)
        ScatterEntry* scatter_raw = static_cast<ScatterEntry*>(
            ::operator new(lineitem_rows * sizeof(ScatterEntry)));

        // ── Per-thread per-partition histograms: 8KB per thread → L1-resident ─
        // hist[tid][part] = count of entries for thread tid's morsel going to partition part
        std::vector<std::array<uint32_t, NUM_PARTITIONS>> hist(nthreads);
        for (auto& h : hist) h.fill(0);

        // ── Pass 1: Count rows per (thread, partition) ─────────────────────────
        // Sequential reads of l_orderkey (hardware prefetch works); L1-resident hist
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& h = hist[tid];
            #pragma omp for schedule(static)
            for (size_t i = 0; i < lineitem_rows; i++) {
                h[p1_part(l_orderkey[i])]++;
            }
        }

        // ── Compute per-partition start offsets (prefix sum) ───────────────────
        // part_start[p] = offset in scatter_raw where partition p's data begins
        // For partition p: thread 0 writes at part_start[p],
        //                  thread 1 writes at part_start[p] + hist[0][p], etc.
        std::vector<uint32_t> part_start(NUM_PARTITIONS + 1);
        part_start[0] = 0;
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            uint32_t cnt = 0;
            for (int t = 0; t < nthreads; t++) cnt += hist[t][p];
            part_start[p + 1] = part_start[p] + cnt;
        }

        // Per-thread write offsets: write_off[tid][part] = starting slot for thread tid
        // 8KB per thread → L1-resident during scatter pass
        std::vector<std::array<uint32_t, NUM_PARTITIONS>> write_off(nthreads);
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            uint32_t base = part_start[p];
            for (int t = 0; t < nthreads; t++) {
                write_off[t][p] = base;
                base += hist[t][p];
            }
        }

        // ── Pass 2: Scatter ────────────────────────────────────────────────────
        // Each thread writes its morsel's (key, qty) entries to the correct partition slot.
        // write_off[tid] (8KB) is L1-resident; within each partition, writes are sequential.
        // Threads write to non-overlapping slots (no atomics needed).
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& wo = write_off[tid];
            #pragma omp for schedule(static)
            for (size_t i = 0; i < lineitem_rows; i++) {
                int32_t key  = l_orderkey[i];
                uint32_t p   = p1_part(key);
                scatter_raw[wo[p]++] = ScatterEntry{key, (int32_t)l_quantity[i]};
            }
        }

        // ── Pass 3: Aggregate ──────────────────────────────────────────────────
        // schedule(static) → each thread gets consecutive partitions (chunk=32).
        // Thread t reads scatter_raw[part_start[t*32]..part_start[(t+1)*32]) — contiguous!
        // Each partition HT: 16384×8B = 131KB → fits in L2 (256KB/core).
        // 64 active HTs: 64×131KB = 8.4MB << L3 (44MB). All probes hit L2.
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int part = 0; part < NUM_PARTITIONS; part++) {
            // C20: std::fill-based init (NOT memset) — 131KB, L2 resident
            std::vector<P1Slot> ht(P1_CAP, P1Slot{INT32_MIN, 0});

            uint32_t begin = part_start[part];
            uint32_t end   = part_start[part + 1];

            // Sequential read of contiguous partition data + L2-resident HT probes
            for (uint32_t idx = begin; idx < end; idx++) {
                int32_t key = scatter_raw[idx].key;
                int32_t qty = scatter_raw[idx].qty;
                uint32_t h  = p1_slot(key);
                // C24: bounded probing
                for (uint32_t pr = 0; pr < P1_CAP; pr++) {
                    uint32_t s = (h + pr) & P1_MASK;
                    if (ht[s].key == INT32_MIN) {
                        ht[s].key     = key;
                        ht[s].sum_qty = qty;
                        break;
                    }
                    if (ht[s].key == key) {
                        ht[s].sum_qty += qty;
                        break;
                    }
                }
            }

            // HAVING SUM(l_quantity) > 300 → set bit in shared bitmap (atomic OR)
            for (uint32_t i = 0; i < P1_CAP; i++) {
                if (ht[i].key != INT32_MIN && ht[i].sum_qty > 300) {
                    int32_t ok        = ht[i].key;
                    uint32_t byte_idx = (uint32_t)ok >> 3;
                    uint8_t  bit_mask = (uint8_t)(1u << (ok & 7));
                    __atomic_fetch_or(&orderkey_bitmap[byte_idx], bit_mask, __ATOMIC_RELAXED);
                }
            }
        }

        ::operator delete(scatter_raw);
    }

    // ── Phase 2a: Scan orders, collect qualifying rows ─────────────────────────
    std::vector<ResultRow> results;
    results.reserve(700);

    {
        GENDB_PHASE("build_joins");

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<ResultRow>> tl_rows(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            tl_rows[tid].reserve(16);

            #pragma omp for schedule(static)
            for (size_t i = 0; i < orders_rows; i++) {
                int32_t ok       = o_orderkey[i];
                uint32_t byte_idx = (uint32_t)ok >> 3;
                uint8_t  bit_mask = (uint8_t)(1u << (ok & 7));
                if (orderkey_bitmap[byte_idx] & bit_mask) {
                    tl_rows[tid].push_back({ok, o_custkey[i], o_orderdate[i],
                                            o_totalprice[i], 0.0});
                }
            }
        }

        // Merge thread-local order rows
        for (auto& tr : tl_rows) {
            results.insert(results.end(), tr.begin(), tr.end());
        }
    }

    // ── Phase 2b: Scan lineitem, accumulate SUM(l_quantity) per qualifying order
    {
        GENDB_PHASE("main_scan");

        int nthreads = omp_get_max_threads();

        // Thread-local 2048-slot aggregation maps (24KB each → L1 resident)
        std::vector<std::vector<P2Slot>> tl_maps(nthreads,
            std::vector<P2Slot>(P2_CAP, P2Slot{INT32_MIN, 0.0}));  // C20

        // Parallel lineitem scan with bitmap filter → accumulate into thread-local map
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& my_map = tl_maps[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < lineitem_rows; i++) {
                int32_t lk        = l_orderkey[i];
                uint32_t byte_idx = (uint32_t)lk >> 3;
                uint8_t  bit_mask = (uint8_t)(1u << (lk & 7));
                if (!(orderkey_bitmap[byte_idx] & bit_mask)) continue;

                double qty   = l_quantity[i];
                uint32_t h   = p2_slot(lk);
                // C24: bounded probing
                for (uint32_t pr = 0; pr < P2_CAP; pr++) {
                    uint32_t idx = (h + pr) & P2_MASK;
                    if (my_map[idx].o_orderkey == INT32_MIN) {
                        my_map[idx].o_orderkey = lk;
                        my_map[idx].sum_qty    = qty;
                        break;
                    }
                    if (my_map[idx].o_orderkey == lk) {
                        my_map[idx].sum_qty += qty;
                        break;
                    }
                }
            }
        }

        // Merge thread-local maps into a single global aggregation map
        std::vector<P2Slot> agg_map(P2_CAP, P2Slot{INT32_MIN, 0.0});  // C20

        for (int tid = 0; tid < nthreads; tid++) {
            for (uint32_t i = 0; i < P2_CAP; i++) {
                if (tl_maps[tid][i].o_orderkey == INT32_MIN) continue;
                int32_t key  = tl_maps[tid][i].o_orderkey;
                double  qty  = tl_maps[tid][i].sum_qty;
                uint32_t h   = p2_slot(key);
                for (uint32_t pr = 0; pr < P2_CAP; pr++) {
                    uint32_t idx = (h + pr) & P2_MASK;
                    if (agg_map[idx].o_orderkey == INT32_MIN) {
                        agg_map[idx].o_orderkey = key;
                        agg_map[idx].sum_qty    = qty;
                        break;
                    }
                    if (agg_map[idx].o_orderkey == key) {
                        agg_map[idx].sum_qty += qty;
                        break;
                    }
                }
            }
        }

        // Fill sum_qty back into results array
        for (auto& row : results) {
            uint32_t h = p2_slot(row.o_orderkey);
            for (uint32_t pr = 0; pr < P2_CAP; pr++) {
                uint32_t idx = (h + pr) & P2_MASK;
                if (agg_map[idx].o_orderkey == INT32_MIN) break;
                if (agg_map[idx].o_orderkey == row.o_orderkey) {
                    row.sum_qty = agg_map[idx].sum_qty;
                    break;
                }
            }
        }
    }

    // ── Output: sort, top-100, CSV ─────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        // C33: ORDER BY o_totalprice DESC, o_orderdate ASC, o_orderkey ASC (stable tiebreaker)
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
            if (a.o_orderdate  != b.o_orderdate)  return a.o_orderdate  < b.o_orderdate;
            return a.o_orderkey < b.o_orderkey;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q18.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }

        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        int limit = (int)std::min((size_t)100, results.size());
        for (int i = 0; i < limit; i++) {
            const auto& row = results[i];
            // c_name reconstruction: C31 quote the string
            char cname[32];
            snprintf(cname, sizeof(cname), "Customer#%09d", row.c_custkey);
            char date_buf[11];
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            // C31: always double-quote c_name
            fprintf(f, "\"%s\",%d,%d,%s,%.2f,%.2f\n",
                    cname,
                    row.c_custkey,
                    row.o_orderkey,
                    date_buf,
                    row.o_totalprice,
                    row.sum_qty);
        }

        fclose(f);
        fprintf(stderr, "[Q18] %zu qualifying orders, wrote %d rows\n",
                results.size(), limit);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir    = argv[1];
    std::string results_dir  = argc > 2 ? argv[2] : ".";
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
