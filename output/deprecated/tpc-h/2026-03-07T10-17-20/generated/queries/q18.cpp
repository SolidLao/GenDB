// Q18: Large Volume Customer — iter_3
//
// Key improvements over iter_2:
//   1. sorted_l_qty_uint8.bin (uint8_t, 60MB) replaces sorted_l_quantity.bin (double, 480MB)
//      → Pass 1 data reduced from 720MB to 300MB (sorted_l_orderkey 240MB + qty_uint8 60MB)
//   2. int32_t accumulator instead of double (integer comparison cur_sum > 300)
//   3. Inline prefault of o_orderkey_hash during Pass 1 scan (when interior qualifying key found)
//   4. Parallel prefault sweep of o_orderkey_hash AFTER boundary merge (before serial probes)
//   5. Parallel prefault of orders columns + c_custkey_hash AFTER serial hash probes
//   Together: eliminates ~12ms of serial page-fault latency from build_joins

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <climits>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <filesystem>

#include "timing_utils.h"
#include "mmap_utils.h"

using namespace gendb;

// ── Hash function (verbatim from build_indexes.cpp) ───────────────────────────
static inline uint32_t hash32(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = (x >> 16) ^ x;
    return x;
}

static inline int32_t probe_index(const int32_t* __restrict__ keys,
                                   const int32_t* __restrict__ vals,
                                   uint64_t mask, int32_t key) {
    uint64_t h = (uint64_t)hash32((uint32_t)key) & mask;
    while (keys[h] != -1) {
        if (keys[h] == key) return vals[h];
        h = (h + 1) & mask;
    }
    return -1;
}

// ── Gregorian calendar: int32_t days since 1970-01-01 → "YYYY-MM-DD" ─────────
static void decode_date(int32_t days, char out[11]) {
    int32_t z   = days + 719468;
    int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    int32_t doe = z - era * 146097;
    int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    int32_t mp  = (5*doy + 2) / 153;
    int32_t d   = doy - (153*mp + 2)/5 + 1;
    int32_t m   = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
    snprintf(out, 11, "%04d-%02d-%02d", y, m, d);
}

// ── A qualifying orderkey from Pass 1 ────────────────────────────────────────
struct QualifyingKey {
    int32_t key;
    double  sum_qty;    // stored as double for output; cast from int32_t at emit
    int64_t start_pos;
    int64_t end_pos;
};

// ── Thread boundary state ─────────────────────────────────────────────────────
// int32_t sums: max realistic sum << INT32_MAX (avg 200 per key at TPC-H SF10)
struct ThreadBoundary {
    int32_t first_key;    // key at row0 (may be continuation from prev thread)
    int32_t first_sum;    // sum for first_key within [row0, first_end)
    int64_t first_end;    // exclusive end of first_key's run within this chunk

    int32_t last_key;     // key at row1-1 (run may continue into next thread)
    int32_t last_sum;     // sum for last_key within [last_start, row1)
    int64_t last_start;   // inclusive start of last_key's run within this chunk

    bool    single_key;   // entire chunk is one key (first_key == last_key)
    int64_t row0;
    int64_t row1;
};

// ── (key, orders_row, sum_qty) triple from serial hash probes ────────────────
struct OrowTriple {
    int32_t key;
    int32_t orow;
    double  sum_qty;
};

// ── Output result row ─────────────────────────────────────────────────────────
struct Result {
    char    c_name[26];
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double  o_totalprice;
    double  sum_qty;
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    GENDB_PHASE_MS("total", total_ms);

    // ── Data loading ───────────────────────────────────────────────────────────
    // Pass 1: mmap sorted lineitem extensions (sequential access)
    // KEY CHANGE: use sorted_l_qty_uint8.bin (uint8_t, 60MB) instead of
    //             sorted_l_quantity.bin (double, 480MB) — 8x bandwidth reduction
    MmapColumn<int32_t> sorted_okey(
        gendb_dir + "/column_versions/lineitem.sorted_by_okey/sorted_l_orderkey.bin");
    MmapColumn<uint8_t> sorted_qty_u8(
        gendb_dir + "/column_versions/lineitem.sorted_by_okey/sorted_l_qty_uint8.bin");
    sorted_okey.advise_sequential();
    sorted_qty_u8.advise_sequential();

    // Pass 2: mmap orders and customer columns (random access — only ~624 rows needed)
    MmapColumn<int32_t> o_custkey_col  (gendb_dir + "/orders/o_custkey.bin");
    MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");
    MmapColumn<double>  o_totalprice_col(gendb_dir + "/orders/o_totalprice.bin");
    MmapColumn<char>    c_name_col     (gendb_dir + "/customer/c_name.bin");
    o_custkey_col.advise_random();
    o_orderdate_col.advise_random();
    o_totalprice_col.advise_random();
    c_name_col.advise_random();

    // mmap hash indexes (MADV_RANDOM — random access during probes)
    const int32_t* o_hash_keys = nullptr;
    const int32_t* o_hash_vals = nullptr;
    uint64_t       o_hash_mask = 0;
    void*          o_hash_ptr  = nullptr;
    size_t         o_hash_sz   = 0;
    {
        std::string p = gendb_dir + "/orders/o_orderkey_hash.bin";
        int fd = ::open(p.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        o_hash_sz  = st.st_size;
        o_hash_ptr = mmap(nullptr, o_hash_sz, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        madvise(o_hash_ptr, o_hash_sz, MADV_RANDOM);
        int64_t cap = ((const int64_t*)o_hash_ptr)[0];
        o_hash_mask = (uint64_t)cap - 1;
        o_hash_keys = (const int32_t*)((const char*)o_hash_ptr + 16);
        o_hash_vals = o_hash_keys + cap;
    }

    const int32_t* c_hash_keys = nullptr;
    const int32_t* c_hash_vals = nullptr;
    uint64_t       c_hash_mask = 0;
    void*          c_hash_ptr  = nullptr;
    size_t         c_hash_sz   = 0;
    {
        std::string p = gendb_dir + "/customer/c_custkey_hash.bin";
        int fd = ::open(p.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        c_hash_sz  = st.st_size;
        c_hash_ptr = mmap(nullptr, c_hash_sz, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        madvise(c_hash_ptr, c_hash_sz, MADV_RANDOM);
        int64_t cap = ((const int64_t*)c_hash_ptr)[0];
        c_hash_mask = (uint64_t)cap - 1;
        c_hash_keys = (const int32_t*)((const char*)c_hash_ptr + 16);
        c_hash_vals = c_hash_keys + cap;
    }

    {
        GENDB_PHASE("data_loading");
        // Async prefetch both sequential columns into page cache
        sorted_okey.prefetch();
        sorted_qty_u8.prefetch();
    }

    const int64_t    N  = (int64_t)sorted_okey.count;
    const int32_t* __restrict__ sk  = sorted_okey.data;
    const uint8_t* __restrict__ sq8 = sorted_qty_u8.data;  // uint8_t quantities

    const int nthreads = omp_get_max_threads();
    std::vector<ThreadBoundary>             boundaries(nthreads);
    std::vector<std::vector<QualifyingKey>> thread_qualifying(nthreads);

    // ── Pass 1: Parallel morsel-driven sequential scan ─────────────────────────
    // Reads 300MB total (sorted_l_orderkey 240MB + sorted_l_qty_uint8 60MB)
    // vs 720MB in iter_2 (sorted_l_orderkey 240MB + sorted_l_quantity 480MB)
    // Inline prefault of o_orderkey_hash pages when interior qualifying key found.
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            int nt  = omp_get_num_threads();

            int64_t chunk = (N + nt - 1) / nt;
            int64_t row0  = (int64_t)tid * chunk;
            int64_t row1  = std::min(row0 + chunk, N);

            auto& bnd     = boundaries[tid];
            auto& local_q = thread_qualifying[tid];
            bnd.row0 = row0;
            bnd.row1 = row1;
            bnd.single_key = false;

            if (row0 >= row1) {
                bnd.single_key  = true;
                bnd.first_key   = -1;  bnd.first_sum = 0; bnd.first_end  = row0;
                bnd.last_key    = -1;  bnd.last_sum  = 0; bnd.last_start = row0;
            } else {
                int32_t cur_key   = sk[row0];
                int32_t cur_sum   = 0;          // int32_t accumulator (was double in iter_2)
                int64_t cur_start = row0;
                bool    first_done = false;

                for (int64_t i = row0; i < row1; i++) {
                    int32_t k = sk[i];
                    if (__builtin_expect(k != cur_key, 0)) {
                        if (!first_done) {
                            // Record first key boundary (may be partial run)
                            bnd.first_key = cur_key;
                            bnd.first_sum = cur_sum;
                            bnd.first_end = i;
                            first_done = true;
                        } else {
                            // Interior run: complete — apply integer filter
                            if (cur_sum > 300) {
                                local_q.push_back({cur_key, (double)cur_sum, cur_start, i});
                                // Inline prefault: touch the hash page for this qualifying key
                                // Overlaps page-fault latency with the 20ms scan
                                volatile int32_t _pf = o_hash_keys[
                                    (uint64_t)hash32((uint32_t)cur_key) & o_hash_mask];
                                (void)_pf;
                            }
                        }
                        cur_key   = k;
                        cur_sum   = 0;
                        cur_start = i;
                    }
                    cur_sum += (int32_t)sq8[i];  // uint8_t → int32_t accumulation
                }

                if (!first_done) {
                    // Entire chunk is one key
                    bnd.single_key  = true;
                    bnd.first_key   = cur_key; bnd.first_sum = cur_sum; bnd.first_end  = row1;
                    bnd.last_key    = cur_key; bnd.last_sum  = cur_sum; bnd.last_start = row0;
                } else {
                    bnd.single_key  = false;
                    bnd.last_key    = cur_key;
                    bnd.last_sum    = cur_sum;
                    bnd.last_start  = cur_start;
                }
            }
        } // end parallel
    }

    // ── Pass 1 boundary merge (serial) ────────────────────────────────────────
    std::vector<QualifyingKey> global_qualifying;
    global_qualifying.reserve(1024);

    {
        GENDB_PHASE("dim_filter");

        // Collect all interior qualifying keys from thread-local vectors
        for (int t = 0; t < nthreads; t++)
            for (auto& q : thread_qualifying[t])
                global_qualifying.push_back(q);

        // Merge boundary partial sums across thread boundaries.
        // 'pending' tracks a key that started in some previous thread and may continue.
        struct Pending {
            bool    valid = false;
            int32_t key   = 0;
            int32_t sum   = 0;    // int32_t (was double in iter_2)
            int64_t start = 0;
        } pending;

        for (int t = 0; t < nthreads; t++) {
            auto& bnd    = boundaries[t];
            int64_t row0 = bnd.row0;
            int64_t row1 = bnd.row1;

            if (row0 == row1) continue;  // empty thread chunk

            if (bnd.single_key) {
                // Entire chunk is one key; may extend from prev thread or into next.
                int32_t k = bnd.first_key;
                int32_t s = bnd.first_sum;

                if (pending.valid && pending.key == k) {
                    // Continuation from previous thread(s)
                    pending.sum += s;
                } else {
                    // Different key: finalize prev pending
                    if (pending.valid) {
                        if (pending.sum > 300)
                            global_qualifying.push_back(
                                {pending.key, (double)pending.sum, pending.start, row0});
                    }
                    pending = {true, k, s, row0};
                }
                // Do NOT finalize; run may continue into next thread.

            } else {
                // Chunk has >= 2 distinct keys.
                int32_t fk = bnd.first_key;
                int32_t fs = bnd.first_sum;
                int64_t fe = bnd.first_end;

                // Resolve first key against pending
                if (pending.valid && pending.key == fk) {
                    // pending run completes at fe
                    int32_t combined = pending.sum + fs;
                    if (combined > 300)
                        global_qualifying.push_back({fk, (double)combined, pending.start, fe});
                    pending.valid = false;
                } else {
                    // Finalize old pending
                    if (pending.valid) {
                        if (pending.sum > 300)
                            global_qualifying.push_back(
                                {pending.key, (double)pending.sum, pending.start, row0});
                        pending.valid = false;
                    }
                    // First key starts fresh at row0, completes at fe
                    if (fs > 300)
                        global_qualifying.push_back({fk, (double)fs, row0, fe});
                }

                // Last key starts a new pending (may continue into next thread)
                pending = {true, bnd.last_key, bnd.last_sum, bnd.last_start};
            }
        }

        // Finalize last pending (run ends at end of sorted array)
        if (pending.valid) {
            if (pending.sum > 300)
                global_qualifying.push_back({pending.key, (double)pending.sum, pending.start, N});
        }

        fprintf(stderr, "[INFO] Qualifying orderkeys: %zu\n", global_qualifying.size());
    }

    // ── Pass 2a: Parallel prefault of o_orderkey_hash pages ───────────────────
    // After boundary merge we know all ~624 qualifying keys.
    // Touch each key's hash slot in parallel to map the 256MB hash table pages
    // into the process page table BEFORE the serial probe loop.
    // With 64 threads × ~20 pages each: ~0.1ms total (vs ~6ms serial page faults).
    {
        GENDB_PHASE("build_joins");

        const int nq = (int)global_qualifying.size();

        #pragma omp parallel for schedule(dynamic, 8) num_threads(nthreads)
        for (int i = 0; i < nq; i++) {
            uint64_t h = (uint64_t)hash32((uint32_t)global_qualifying[i].key) & o_hash_mask;
            volatile int32_t d1 = o_hash_keys[h];
            volatile int32_t d2 = o_hash_vals[h];
            (void)d1; (void)d2;
        }

        // ── Pass 2b: Serial hash probes (pages now hot in page table) ─────────
        std::vector<OrowTriple> orow_triples;
        orow_triples.reserve(nq);

        for (int i = 0; i < nq; i++) {
            const auto& qk = global_qualifying[i];
            int32_t orow = probe_index(o_hash_keys, o_hash_vals, o_hash_mask, qk.key);
            if (__builtin_expect(orow >= 0, 1))
                orow_triples.push_back({qk.key, orow, qk.sum_qty});
        }

        // ── Pass 2c: Parallel prefault of orders columns + c_custkey_hash ─────
        // Read o_custkey_col[orow] in parallel to get custkey,
        // then also touch c_custkey_hash pages for those custkeys.
        const int no = (int)orow_triples.size();
        const int32_t* oc  = o_custkey_col.data;
        const int32_t* od  = o_orderdate_col.data;
        const double*  otp = o_totalprice_col.data;

        #pragma omp parallel for schedule(dynamic, 8) num_threads(nthreads)
        for (int i = 0; i < no; i++) {
            int32_t orow = orow_triples[i].orow;
            // Prefault orders column pages
            volatile int32_t d1 = oc[orow];
            volatile int32_t d2 = od[orow];
            volatile double  d3 = otp[orow];
            (void)d1; (void)d2; (void)d3;
            // Prefault c_custkey_hash page using now-available custkey
            int32_t custkey = oc[orow];
            uint64_t h = (uint64_t)hash32((uint32_t)custkey) & c_hash_mask;
            volatile int32_t d4 = c_hash_keys[h];
            volatile int32_t d5 = c_hash_vals[h];
            (void)d4; (void)d5;
        }

        // ── Pass 2d: Serial column reads + customer lookup (all pages hot) ────
        std::vector<Result> results;
        results.reserve(no);

        const char* cn = c_name_col.data;

        for (int i = 0; i < no; i++) {
            const auto& ot = orow_triples[i];
            int32_t orow = ot.orow;

            int32_t custkey    = oc[orow];
            int32_t orderdate  = od[orow];
            double  totalprice = otp[orow];

            // Probe customer hash index: o_custkey → customer row
            int32_t crow = probe_index(c_hash_keys, c_hash_vals, c_hash_mask, custkey);
            if (__builtin_expect(crow < 0, 0)) continue;

            Result r;
            memcpy(r.c_name, cn + (int64_t)crow * 26, 26);
            r.c_name[25]   = '\0';
            r.c_custkey    = custkey;
            r.o_orderkey   = ot.key;
            r.o_orderdate  = orderdate;
            r.o_totalprice = totalprice;
            r.sum_qty      = ot.sum_qty;
            results.push_back(r);
        }

        // ── Sort and output top 100 ────────────────────────────────────────────
        {
            GENDB_PHASE("sort_topk");

            std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
                if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
                return a.o_orderdate < b.o_orderdate;
            });

            {
                GENDB_PHASE("output");

                std::string out_path = results_dir + "/Q18.csv";
                FILE* fp = fopen(out_path.c_str(), "w");
                if (!fp) {
                    fprintf(stderr, "Cannot open output: %s\n", out_path.c_str());
                    munmap(o_hash_ptr, o_hash_sz);
                    munmap(c_hash_ptr, c_hash_sz);
                    return 1;
                }

                fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

                int limit = (int)std::min(results.size(), (size_t)100);
                char date_buf[11];
                for (int i = 0; i < limit; i++) {
                    const Result& r = results[i];
                    decode_date(r.o_orderdate, date_buf);
                    fprintf(fp, "%s,%d,%d,%s,%.2f,%.2f\n",
                            r.c_name, r.c_custkey, r.o_orderkey,
                            date_buf, r.o_totalprice, r.sum_qty);
                }
                fclose(fp);
            }
        }
    } // end build_joins scope

    munmap(o_hash_ptr, o_hash_sz);
    munmap(c_hash_ptr, c_hash_sz);
    return 0;
}
