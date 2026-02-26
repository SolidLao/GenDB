// Q3: SEC EDGAR — Sum(value) by (name, cik) WHERE uom=USD AND fy=2022
// HAVING SUM > AVG(cik-level sums); ORDER BY total_value DESC LIMIT 100
//
// Strategy (iter_1): compact adsh index + direct per-group flat-array accumulation
//   - Pre-build int16_t adsh_compact_idx[86135] for fast fy=2022 check (replaces sub_fy random access)
//   - Pre-build compact_gid[16890] and compact_cidx[16890] for fast group/cik lookup
//   - Per-thread long double flat arrays indexed by gid and cidx (direct accumulation, no hashing)
//   - Parallel static schedule ensures deterministic block assignment
//   - Sequential reduce: matches validated accumulation order from previous iterations
//   - Zone maps for block skipping
//   - name_dict loaded upfront

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <string>
#include <vector>
#include <algorithm>
#include <limits>
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
static constexpr uint32_t BLOCK_SIZE   = 100000;
static constexpr uint32_t SUB_ROWS     = 86135;

// ---------------------------------------------------------------------------
// Zone-map block layout (packed: 1+1+4+4 = 10 bytes)
// ---------------------------------------------------------------------------
#pragma pack(push, 1)
struct ZMBlock {
    int8_t  uom_min;
    int8_t  uom_max;
    int32_t ddate_min;
    int32_t ddate_max;
};
#pragma pack(pop)

// ---------------------------------------------------------------------------
// mmap helpers
// ---------------------------------------------------------------------------
// No MAP_POPULATE for large sequential files — faults happen in parallel during scan.
// MADV_HUGEPAGE reduces TLB pressure (512MB / 2MB = 256 TLB entries vs 131072).
// Small random files use MAP_POPULATE for immediate residency.
template<typename T>
static const T* mmap_file(const std::string& path, size_t& out_bytes,
                           bool sequential = true) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); out_bytes = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    out_bytes = (size_t)st.st_size;
    int flags = MAP_PRIVATE;
    // Small random-access files: pre-fault for immediate L3 residency
    if (!sequential && out_bytes < (4u << 20))
        flags |= MAP_POPULATE;
    void* ptr = mmap(nullptr, out_bytes, PROT_READ, flags, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { perror("mmap"); out_bytes = 0; return nullptr; }
    if (sequential) {
        madvise(ptr, out_bytes, MADV_SEQUENTIAL);
#ifdef MADV_HUGEPAGE
        if (out_bytes >= (2u << 20))
            madvise(ptr, out_bytes, MADV_HUGEPAGE);  // 2MB pages: 256 TLB entries for 512MB
#endif
    } else {
        madvise(ptr, out_bytes, MADV_WILLNEED);
    }
    return reinterpret_cast<const T*>(ptr);
}

// Bulk-read a dict file: reads entire file at once (avoids 2N syscalls for N entries)
static std::vector<std::string> load_dict(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    std::vector<std::string> dict;
    if (fd < 0) { perror(path.c_str()); return dict; }
    struct stat st; fstat(fd, &st);
    std::vector<char> buf(st.st_size);
    if (read(fd, buf.data(), st.st_size) != (ssize_t)st.st_size) { close(fd); return dict; }
    close(fd);
    const char* p = buf.data();
    const char* end = p + buf.size();
    if (p + 4 > end) return dict;
    uint32_t n = *(const uint32_t*)p; p += 4;
    dict.resize(n);
    for (uint32_t i = 0; i < n && p + 2 <= end; i++) {
        uint16_t len = *(const uint16_t*)p; p += 2;
        if (p + len <= end) dict[i].assign(p, len);
        p += len;
    }
    return dict;
}

// ---------------------------------------------------------------------------
// Main query
// ---------------------------------------------------------------------------
void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // data_loading
    // -----------------------------------------------------------------------
    int8_t usd_code = -1;
    size_t sz;
    const int8_t*  num_uom   = nullptr;
    const double*  num_value = nullptr;
    const int32_t* num_adsh  = nullptr;
    const int16_t* sub_fy    = nullptr;
    const int32_t* sub_cik   = nullptr;
    const int32_t* sub_name  = nullptr;
    size_t         num_rows  = 0;
    int32_t        n_blocks_zm = 0;
    const ZMBlock* zm_blocks   = nullptr;
    std::vector<std::string> name_dict;

    {
        GENDB_PHASE("data_loading");

        // Load USD code from shared/uom.dict — never hardcode (bulk read)
        {
            auto uom_dict = load_dict(gendb_dir + "/shared/uom.dict");
            for (size_t i = 0; i < uom_dict.size(); i++) {
                if (uom_dict[i] == "USD") { usd_code = (int8_t)i; break; }
            }
        }
        if (usd_code < 0) { fprintf(stderr, "USD code not found\n"); return; }

        // Load name_dict upfront using bulk read (single syscall, avoids 2N read() overhead)
        name_dict = load_dict(gendb_dir + "/sub/name.dict");

        // Zone maps (small, cache-resident)
        {
            size_t zm_sz = 0;
            const uint8_t* raw = mmap_file<uint8_t>(
                gendb_dir + "/indexes/num_zonemaps.bin", zm_sz, false);
            if (raw) {
                n_blocks_zm = *(const int32_t*)raw;
                zm_blocks   = reinterpret_cast<const ZMBlock*>(raw + 4);
            }
        }

        // num columns — sequential mmap
        num_uom   = mmap_file<int8_t> (gendb_dir + "/num/uom.bin",   sz, true);
        num_rows  = sz / sizeof(int8_t);
        num_value = mmap_file<double> (gendb_dir + "/num/value.bin",  sz, true);
        num_adsh  = mmap_file<int32_t>(gendb_dir + "/num/adsh.bin",   sz, true);

        // sub columns — random access
        sub_fy   = mmap_file<int16_t>(gendb_dir + "/sub/fy.bin",   sz, false);
        sub_cik  = mmap_file<int32_t>(gendb_dir + "/sub/cik.bin",  sz, false);
        sub_name = mmap_file<int32_t>(gendb_dir + "/sub/name.bin", sz, false);
    }

    uint32_t n_blocks = (uint32_t)((num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE);
    int nthreads = omp_get_max_threads();

    // -----------------------------------------------------------------------
    // dim_filter + build_joins:
    // 1. Scan sub/fy.bin to collect fy=2022 rows
    // 2. Sort by (cik, name_code) to deduplicate groups
    // 3. Assign gid (name,cik group index) and cidx (cik index)
    // 4. Build adsh_compact_idx[SUB_ROWS] → int16_t compact_index (or -1)
    // 5. Build compact_gid[] and compact_cidx[] for hot loop lookups
    //
    // Compact arrays (172KB + 67KB + 67KB = 306KB) fit in L2 per core
    // -----------------------------------------------------------------------
    struct QualRow { int32_t sub_row; int32_t cik; int32_t name_code; };

    std::vector<QualRow>  qual_rows;
    qual_rows.reserve(20000);

    {
        GENDB_PHASE("dim_filter");
        const int16_t FY = (int16_t)2022;
        for (uint32_t sr = 0; sr < SUB_ROWS; sr++) {
            if (sub_fy[sr] == FY)
                qual_rows.push_back({(int32_t)sr, sub_cik[sr], sub_name[sr]});
        }
    }

    // Sort by (cik ASC, name_code ASC) for group deduplication
    std::sort(qual_rows.begin(), qual_rows.end(), [](const QualRow& a, const QualRow& b) {
        if (a.cik != b.cik) return a.cik < b.cik;
        return a.name_code < b.name_code;
    });

    // Assign gid and cidx; build group_info and cik_info
    struct GroupInfo { int32_t name_code; int32_t cik; };
    std::vector<GroupInfo> group_info;
    std::vector<int32_t>   cik_vals;
    group_info.reserve(qual_rows.size());
    cik_vals.reserve(10000);

    // sub_to_gid[sub_row] and sub_to_cidx[sub_row] — only for fy=2022 rows
    // Use compact arrays for the hot path (accessed via compact index)
    const int32_t COMPACT_MAX = (int32_t)qual_rows.size();

    // adsh_compact_idx: int16_t[SUB_ROWS], maps sub_row -> compact_index
    // compact_gid: int16_t[COMPACT_MAX] — gid for compact index
    // compact_cidx: int16_t[COMPACT_MAX] — cidx for compact index
    std::vector<int16_t> adsh_compact_idx(SUB_ROWS, (int16_t)-1);
    std::vector<int16_t> compact_gid_arr (COMPACT_MAX);
    std::vector<int16_t> compact_cidx_arr(COMPACT_MAX);

    int32_t n_groups = 0;
    int32_t n_ciks   = 0;

    {
        GENDB_PHASE("build_joins");

        // First pass: collect unique ciks and assign cidx
        // We iterate qual_rows which is already sorted by (cik, name_code)
        std::vector<std::pair<int32_t,int32_t>> cik_to_cidx_sorted; // (cik, cidx)
        cik_to_cidx_sorted.reserve(10000);

        int32_t prev_cik = INT32_MIN;
        int32_t cur_cidx = -1;
        for (const auto& q : qual_rows) {
            if (q.cik != prev_cik) {
                cur_cidx++;
                cik_vals.push_back(q.cik);
                cik_to_cidx_sorted.push_back({q.cik, cur_cidx});
                prev_cik = q.cik;
            }
        }
        n_ciks = (int32_t)cik_vals.size();

        // Build binary search lookup for cik -> cidx
        // (sorted by cik since qual_rows is sorted by cik)
        // Just use the order in cik_to_cidx_sorted directly

        // Second pass: assign gids and build compact arrays
        int32_t prev_cik2 = INT32_MIN;
        int32_t prev_nc   = INT32_MIN;
        int32_t cur_gid   = -1;
        int32_t cik_cidx  = -1;
        int32_t prev_cik_for_cidx = INT32_MIN;

        for (int32_t i = 0; i < COMPACT_MAX; i++) {
            const QualRow& q = qual_rows[i];

            // Update group (gid)
            bool new_group = (q.cik != prev_cik2 || q.name_code != prev_nc);
            if (new_group) {
                cur_gid++;
                group_info.push_back({q.name_code, q.cik});
                prev_cik2 = q.cik;
                prev_nc   = q.name_code;
            }

            // Update cidx
            if (q.cik != prev_cik_for_cidx) {
                cik_cidx++;
                prev_cik_for_cidx = q.cik;
            }

            // Assign compact index for this sub_row
            adsh_compact_idx[q.sub_row] = (int16_t)i;
            compact_gid_arr[i]  = (int16_t)cur_gid;
            compact_cidx_arr[i] = (int16_t)cik_cidx;
        }

        n_groups = (int32_t)group_info.size();
    }

    // -----------------------------------------------------------------------
    // Allocate thread-local flat arrays (long double for ground-truth precision)
    //   tl_outer[t]: long double[n_groups] — direct per-group accumulation
    //   tl_cik[t]:   long double[n_ciks]   — direct per-cik accumulation
    // First-touch init inside OMP region ensures NUMA-local page placement
    // -----------------------------------------------------------------------
    std::vector<long double*> tl_outer(nthreads, nullptr);
    std::vector<long double*> tl_cik  (nthreads, nullptr);
    for (int t = 0; t < nthreads; t++) {
        tl_outer[t] = (long double*)malloc((size_t)n_groups * sizeof(long double));
        tl_cik[t]   = (long double*)malloc((size_t)n_ciks   * sizeof(long double));
    }

    // -----------------------------------------------------------------------
    // main_scan — parallel static schedule, direct per-group accumulation
    // Hot inner loop:
    //   uom check (1 byte) + isnan + adsh lookup (4B) + compact_idx (2B) +
    //   compact_gid (2B) + compact_cidx (2B) + 2 long double accumulations
    // No hash computation, no probing — pure direct indexed writes
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        const int8_t    uc    = usd_code;
        const int16_t*  ci    = adsh_compact_idx.data();
        const int16_t*  cgid  = compact_gid_arr.data();
        const int16_t*  ccidx = compact_cidx_arr.data();

        #pragma omp parallel num_threads(nthreads)
        {
            const int tid = omp_get_thread_num();

            // First-touch init (NUMA-local)
            long double* __restrict__ my_outer = tl_outer[tid];
            long double* __restrict__ my_cik   = tl_cik[tid];
            memset(my_outer, 0, (size_t)n_groups * sizeof(long double));
            memset(my_cik,   0, (size_t)n_ciks   * sizeof(long double));

            // Static schedule for deterministic, sequential per-thread scan
            #pragma omp for schedule(static)
            for (uint32_t b = 0; b < n_blocks; b++) {
                // Zone-map skip: no USD rows in this block
                if (b < (uint32_t)n_blocks_zm) {
                    if (zm_blocks[b].uom_min > uc || zm_blocks[b].uom_max < uc) continue;
                }

                const uint64_t row_start = (uint64_t)b * BLOCK_SIZE;
                const uint64_t row_end   = std::min(row_start + BLOCK_SIZE, (uint64_t)num_rows);

                for (uint64_t i = row_start; i < row_end; i++) {
                    if (num_uom[i] != uc) continue;

                    // Check fy=2022 BEFORE loading value — avoids value load for 79% of USD rows
                    const int32_t adsh_code = num_adsh[i];
                    const int16_t cidx = ci[adsh_code];
                    if (cidx < 0) continue;  // fy != 2022

                    // Load value only for qualifying rows (fy=2022, ~18% of all rows)
                    const double val = num_value[i];
                    if (std::isnan(val)) continue;

                    // Direct accumulation — no hash, no probe
                    my_outer[cgid[cidx]]  += (long double)val;
                    my_cik  [ccidx[cidx]] += (long double)val;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // sequential_reduce — matches validated implementation order
    // O(nthreads × (n_groups + n_ciks)) long double additions
    // -----------------------------------------------------------------------
    std::vector<long double> global_outer(n_groups, 0.0L);
    std::vector<long double> global_cik  (n_ciks,   0.0L);

    {
        GENDB_PHASE("parallel_merge_compact_sum_array");

        // Parallel merge over group/cik indices, sequential over threads
        // This preserves the per-j accumulation order (thread 0 first, then 1, ...)
        // which matches the validated sequential reduce for fp precision
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int32_t j = 0; j < n_groups; j++) {
            long double s = 0.0L;
            for (int t = 0; t < nthreads; t++) s += tl_outer[t][j];
            global_outer[j] = s;
        }

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (int32_t j = 0; j < n_ciks; j++) {
            long double s = 0.0L;
            for (int t = 0; t < nthreads; t++) s += tl_cik[t][j];
            global_cik[j] = s;
        }

        for (int t = 0; t < nthreads; t++) {
            free(tl_outer[t]); tl_outer[t] = nullptr;
            free(tl_cik[t]);   tl_cik[t]   = nullptr;
        }
    }

    // -----------------------------------------------------------------------
    // compute_threshold_avg_cik_sums
    // -----------------------------------------------------------------------
    long double threshold = 0.0L;
    {
        GENDB_PHASE("compute_threshold_avg_cik_sums");
        long double total = 0.0L;
        int64_t cnt = 0;
        for (int32_t j = 0; j < n_ciks; j++) {
            if (global_cik[j] != 0.0L) {
                total += global_cik[j];
                ++cnt;
            }
        }
        // cnt may be 0 if n_ciks=0, but also include ciks with 0 sum?
        // Use n_ciks as denominator to match query semantics: only groups with rows
        if (cnt == 0) cnt = n_ciks;
        threshold = (cnt > 0) ? total / (long double)cnt : 0.0L;
    }

    // -----------------------------------------------------------------------
    // having_filter + collect survivors
    // -----------------------------------------------------------------------
    struct Survivor { int32_t name_code; int32_t cik; long double sum; };
    std::vector<Survivor> survivors;
    survivors.reserve(512);

    {
        GENDB_PHASE("having_filter_name_cik_map");
        const long double thresh = threshold;
        for (int32_t gid = 0; gid < n_groups; gid++) {
            if (global_outer[gid] > thresh) {
                survivors.push_back({group_info[gid].name_code,
                                     group_info[gid].cik,
                                     global_outer[gid]});
            }
        }
    }

    // -----------------------------------------------------------------------
    // topk_sort_limit_100
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("topk_sort_limit_100");
        if (survivors.size() > 100) {
            std::partial_sort(survivors.begin(), survivors.begin() + 100,
                              survivors.end(),
                              [](const Survivor& a, const Survivor& b) {
                                  return a.sum > b.sum;
                              });
            survivors.resize(100);
        } else {
            std::sort(survivors.begin(), survivors.end(),
                      [](const Survivor& a, const Survivor& b) {
                          return a.sum > b.sum;
                      });
        }
    }

    // -----------------------------------------------------------------------
    // output — write CSV (name_dict loaded upfront)
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");
        mkdir(results_dir.c_str(), 0755);
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }
        fprintf(f, "name,cik,total_value\n");
        for (const auto& s : survivors) {
            const std::string& nm = (s.name_code >= 0 &&
                                     (uint32_t)s.name_code < name_dict.size())
                                    ? name_dict[s.name_code] : "";
            // RFC 4180 CSV quoting
            bool needs_quote = (nm.find(',') != std::string::npos ||
                                nm.find('"') != std::string::npos);
            if (needs_quote) {
                std::string escaped;
                escaped.reserve(nm.size() + 4);
                for (char c : nm) {
                    if (c == '"') escaped += '"';
                    escaped += c;
                }
                fprintf(f, "\"%s\",%d,%.2Lf\n", escaped.c_str(), s.cik, s.sum);
            } else {
                fprintf(f, "%s,%d,%.2Lf\n", nm.c_str(), s.cik, s.sum);
            }
        }
        fclose(f);
    }
}

// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: q3 <gendb_dir> <results_dir>\n");
        return 1;
    }
    run_q3(argv[1], argv[2]);
    return 0;
}
