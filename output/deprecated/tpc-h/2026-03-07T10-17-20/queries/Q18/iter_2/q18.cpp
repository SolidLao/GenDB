// Q18: Large Volume Customer — iter_2
//
// Strategy:
//   Pass 1 (parallel):  Linear run-length aggregation over sorted_l_orderkey +
//                       sorted_l_quantity. No hash map. No scatter buffers.
//                       Each thread scans a contiguous [row0, row1) chunk.
//                       Interior complete runs emitted to thread-local vectors.
//                       First and last key in each chunk recorded as boundary state.
//   Boundary merge (serial): Adjacent thread boundary partial sums combined.
//   Pass 2 (serial):    ~624 qualifying keys. Probe orders + customer indexes.
//                       No second lineitem scan — positions known from Pass 1.
//   Output:             Sort ~624 results, emit top 100.

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
    double  sum_qty;
    int64_t start_pos;  // inclusive index into sorted arrays
    int64_t end_pos;    // exclusive index into sorted arrays
};

// ── Thread boundary state ─────────────────────────────────────────────────────
// Records partial information for the first and last key in each thread's chunk.
// These keys may span adjacent thread boundaries and need serial merging.
struct ThreadBoundary {
    int32_t first_key;    // key at row0 (may be continuation from prev thread)
    double  first_sum;    // sum for first_key within [row0, first_end)
    int64_t first_end;    // exclusive end of first_key's run within this chunk

    int32_t last_key;     // key at row1-1 (run may continue into next thread)
    double  last_sum;     // sum for last_key within [last_start, row1)
    int64_t last_start;   // inclusive start of last_key's run within this chunk

    bool    single_key;   // entire chunk is one key (first_key == last_key)
    int64_t row0;
    int64_t row1;
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
    // mmap sorted lineitem extension (sequential)
    MmapColumn<int32_t> sorted_okey(
        gendb_dir + "/column_versions/lineitem.sorted_by_okey/sorted_l_orderkey.bin");
    MmapColumn<double> sorted_qty(
        gendb_dir + "/column_versions/lineitem.sorted_by_okey/sorted_l_quantity.bin");
    sorted_okey.advise_sequential();
    sorted_qty.advise_sequential();

    // mmap orders and customer columns (random access — only ~624 rows needed)
    MmapColumn<int32_t> o_custkey_col  (gendb_dir + "/orders/o_custkey.bin");
    MmapColumn<int32_t> o_orderdate_col(gendb_dir + "/orders/o_orderdate.bin");
    MmapColumn<double>  o_totalprice_col(gendb_dir + "/orders/o_totalprice.bin");
    MmapColumn<char>    c_name_col     (gendb_dir + "/customer/c_name.bin");
    o_custkey_col.advise_random();
    o_orderdate_col.advise_random();
    o_totalprice_col.advise_random();
    c_name_col.advise_random();

    // mmap hash indexes
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
        // Prefetch sorted lineitem into page cache asynchronously
        sorted_okey.prefetch();
        sorted_qty.prefetch();
    }

    const int64_t   N  = (int64_t)sorted_okey.count;
    const int32_t* __restrict__ sk = sorted_okey.data;
    const double*  __restrict__ sq = sorted_qty.data;

    const int nthreads = omp_get_max_threads();
    std::vector<ThreadBoundary>             boundaries(nthreads);
    std::vector<std::vector<QualifyingKey>> thread_qualifying(nthreads);

    // ── Pass 1: Parallel morsel-driven sequential scan ─────────────────────────
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
                bnd.first_key   = -1;  bnd.first_sum = 0.0; bnd.first_end = row0;
                bnd.last_key    = -1;  bnd.last_sum  = 0.0; bnd.last_start = row0;
            } else {
                int32_t cur_key   = sk[row0];
                double  cur_sum   = 0.0;
                int64_t cur_start = row0;
                bool    first_done = false;

                for (int64_t i = row0; i < row1; i++) {
                    int32_t k = sk[i];
                    if (__builtin_expect(k != cur_key, 0)) {
                        if (!first_done) {
                            // Record first key boundary
                            bnd.first_key = cur_key;
                            bnd.first_sum = cur_sum;
                            bnd.first_end = i;
                            first_done = true;
                        } else {
                            // Interior run: complete — filter directly
                            if (cur_sum > 300.0)
                                local_q.push_back({cur_key, cur_sum, cur_start, i});
                        }
                        cur_key   = k;
                        cur_sum   = 0.0;
                        cur_start = i;
                    }
                    cur_sum += sq[i];
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

        // Merge boundary partial sums.
        // 'pending' tracks a key that started in some previous thread and may continue.
        struct Pending {
            bool    valid = false;
            int32_t key   = 0;
            double  sum   = 0.0;
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
                double  s = bnd.first_sum;

                if (pending.valid && pending.key == k) {
                    // Continuation from previous thread(s)
                    pending.sum += s;
                    // pending.start already set; do not finalize yet
                } else {
                    // Different key: finalize prev pending (ended at row0)
                    if (pending.valid) {
                        if (pending.sum > 300.0)
                            global_qualifying.push_back(
                                {pending.key, pending.sum, pending.start, row0});
                    }
                    pending = {true, k, s, row0};
                }
                // Do NOT finalize; run may continue into next thread.

            } else {
                // Chunk has >= 2 distinct keys.
                int32_t fk = bnd.first_key;
                double  fs = bnd.first_sum;
                int64_t fe = bnd.first_end;

                // Resolve first key
                if (pending.valid && pending.key == fk) {
                    // pending run completes at fe
                    double combined = pending.sum + fs;
                    if (combined > 300.0)
                        global_qualifying.push_back({fk, combined, pending.start, fe});
                    pending.valid = false;
                } else {
                    // Finalize old pending (ended at row0)
                    if (pending.valid) {
                        if (pending.sum > 300.0)
                            global_qualifying.push_back(
                                {pending.key, pending.sum, pending.start, row0});
                        pending.valid = false;
                    }
                    // First key starts fresh at row0 and completes at fe
                    if (fs > 300.0)
                        global_qualifying.push_back({fk, fs, row0, fe});
                }

                // Last key starts a new pending (may continue into next thread)
                pending = {true, bnd.last_key, bnd.last_sum, bnd.last_start};
            }
        }

        // Finalize last pending (its run ends at end of sorted array)
        if (pending.valid) {
            if (pending.sum > 300.0)
                global_qualifying.push_back({pending.key, pending.sum, pending.start, N});
        }

        fprintf(stderr, "[INFO] Qualifying orderkeys: %zu\n", global_qualifying.size());
    }

    // ── Pass 2: Serial qualifying key lookups (~624 total) ────────────────────
    std::vector<Result> results;
    results.reserve(global_qualifying.size());

    {
        GENDB_PHASE("build_joins");

        const int32_t* oc  = o_custkey_col.data;
        const int32_t* od  = o_orderdate_col.data;
        const double*  otp = o_totalprice_col.data;
        const char*    cn  = c_name_col.data;

        for (const auto& qk : global_qualifying) {
            // Probe orders hash index: l_orderkey → orders row
            int32_t orow = probe_index(o_hash_keys, o_hash_vals, o_hash_mask, qk.key);
            if (__builtin_expect(orow < 0, 0)) continue;

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
            r.o_orderkey   = qk.key;
            r.o_orderdate  = orderdate;
            r.o_totalprice = totalprice;
            r.sum_qty      = qk.sum_qty;
            results.push_back(r);
        }
    }

    // ── Sort and output top 100 ────────────────────────────────────────────────
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

    munmap(o_hash_ptr, o_hash_sz);
    munmap(c_hash_ptr, c_hash_sz);
    return 0;
}
