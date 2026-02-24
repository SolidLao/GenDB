// Q6 — GenDB iteration 0
// SELECT s.name, p.stmt, n.tag, p.plabel,
//        SUM(n.value) AS total_value, COUNT(*) AS cnt
// FROM num n JOIN sub s ON n.adsh=s.adsh
//            JOIN pre p ON n.adsh=p.adsh AND n.tag=p.tag AND n.version=p.version
// WHERE n.uom='USD' AND p.stmt='IS' AND s.fy=2023 AND n.value IS NOT NULL
// GROUP BY s.name, p.stmt, n.tag, p.plabel
// ORDER BY total_value DESC LIMIT 200;

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"

// ---- Constants ----
static constexpr int      SUB_N       = 86135;
static constexpr uint32_t PRE_CAP     = 4194304;  // 2^22, ~1.77M rows at 42% load
// Thread-local agg HT: 512K supports ~256K groups per thread (worst case)
static constexpr uint32_t AGG_CAP_L   = 524288;   // 2^19, 16MB per thread
// Per-partition merge HT: 128K slots × 32B = 4MB each, fits in L3 per thread
// With 64 partitions × 4MB = 256MB total; handles up to 64K groups per partition (≥4M total)
static constexpr uint32_t AGG_CAP_P   = 131072;   // 2^17, 4MB per partition
static constexpr int32_t  PRE_EMPTY   = -1;
static constexpr uint64_t AGG_EMPTY   = UINT64_MAX;

// ---- Pre hash table slot ----
struct PreSlot {
    int32_t adsh;   // PRE_EMPTY = empty
    int32_t tag;
    int32_t ver;
    int32_t plabel;
};
static_assert(sizeof(PreSlot) == 16, "");

// ---- Aggregation hash table slot ----
// Key: g1 = (name_code<<32)|tag_code
//      g2 = (stmt_code<<32)|plabel_code  (ALL 4 GROUP BY cols, C15)
struct AggSlot {
    uint64_t g1;       // AGG_EMPTY = empty
    uint64_t g2;
    double   sum_val;
    int64_t  cnt;
};
static_assert(sizeof(AggSlot) == 32, "");

// ---- Dictionary loading ----
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    std::ifstream f(path);
    if (!f) { fprintf(stderr, "Cannot open dict: %s\n", path.c_str()); exit(1); }
    std::string line;
    while (std::getline(f, line)) d.push_back(line);
    return d;
}

static int32_t find_code(const std::vector<std::string>& d, const std::string& val) {
    for (int32_t i = 0; i < (int32_t)d.size(); i++)
        if (d[i] == val) return i;
    return -1;
}

// ---- mmap helper ----
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    n_rows = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<const T*>(p);
}

// ---- Hash functions ----
inline uint32_t pre_hash_slot(int32_t adsh, int32_t tag, int32_t ver) {
    uint64_t k1 = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tag;
    uint64_t h  = k1 * 6364136223846793005ULL ^ (uint64_t)(uint32_t)ver * 1442695040888963407ULL;
    return (uint32_t)(h >> 32) & (PRE_CAP - 1);
}

inline uint32_t agg_hash_slot(uint64_t g1, uint64_t g2, uint32_t cap) {
    uint64_t h = g1 * 6364136223846793005ULL ^ g2 * 1442695040888963407ULL;
    return (uint32_t)(h >> 32) & (cap - 1);
}

// ---- Pre HT insert (NO dedup: allows multiple plabels per key) ----
// pre table can have multiple rows with same (adsh,tag,ver) and stmt='IS'.
// Each must produce a separate join result → store all entries without dedup.
static void pre_insert(PreSlot* ht, int32_t adsh, int32_t tag, int32_t ver, int32_t plabel) {
    uint32_t s = pre_hash_slot(adsh, tag, ver);
    for (uint32_t p = 0; p < PRE_CAP; p++) {
        uint32_t idx = (s + p) & (PRE_CAP - 1);
        if (ht[idx].adsh == PRE_EMPTY) {
            ht[idx] = {adsh, tag, ver, plabel};
            return;
        }
        // No duplicate check — allow multiple entries for same key
    }
}

// ---- Pre HT probe: call agg_fn for each matching entry ----
// Linear probing guarantees all entries for a key appear before any empty slot.
// So "break on empty" safely terminates the scan (insert-only table, no deletions).
template<typename Fn>
inline void pre_probe_all(const PreSlot* ht, int32_t adsh, int32_t tag, int32_t ver, Fn&& fn) {
    uint32_t s = pre_hash_slot(adsh, tag, ver);
    for (uint32_t p = 0; p < PRE_CAP; p++) {
        uint32_t idx = (s + p) & (PRE_CAP - 1);
        if (ht[idx].adsh == PRE_EMPTY) return;
        if (ht[idx].adsh == adsh && ht[idx].tag == tag && ht[idx].ver == ver)
            fn(ht[idx].plabel);
    }
}

// ---- Agg HT update with occupied-slot tracking ----
inline void agg_update_tracked(AggSlot* ht, uint32_t cap,
                                std::vector<uint32_t>& occupied,
                                uint64_t g1, uint64_t g2, double val) {
    uint32_t s = agg_hash_slot(g1, g2, cap);
    for (uint32_t p = 0; p < cap; p++) {
        uint32_t idx = (s + p) & (cap - 1);
        if (ht[idx].g1 == AGG_EMPTY) {
            ht[idx] = {g1, g2, val, 1LL};
            occupied.push_back(idx);
            return;
        }
        if (ht[idx].g1 == g1 && ht[idx].g2 == g2) {
            ht[idx].sum_val += val;
            ht[idx].cnt++;
            return;
        }
    }
}

// ---- Agg HT update (merge into global HT) ----
inline void agg_update(AggSlot* ht, uint32_t cap,
                       uint64_t g1, uint64_t g2, double val, int64_t cnt) {
    uint32_t s = agg_hash_slot(g1, g2, cap);
    for (uint32_t p = 0; p < cap; p++) {
        uint32_t idx = (s + p) & (cap - 1);
        if (ht[idx].g1 == AGG_EMPTY) {
            ht[idx] = {g1, g2, val, cnt};
            return;
        }
        if (ht[idx].g1 == g1 && ht[idx].g2 == g2) {
            ht[idx].sum_val += val;
            ht[idx].cnt    += cnt;
            return;
        }
    }
}

// ---- CSV field output (quoted if contains comma) ----
static void write_csv_field(FILE* fp, const char* s) {
    if (strchr(s, ',') != nullptr) {
        fputc('"', fp);
        fputs(s, fp);
        fputc('"', fp);
    } else {
        fputs(s, fp);
    }
}

// ---- Static dim arrays (BSS) ----
static bool    sub_fy2023[SUB_N];
static int32_t sub_name_arr[SUB_N];

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- Load dictionaries (C2: resolve codes at runtime) ----
    auto stmt_dict   = load_dict(gendb_dir + "/pre/stmt_dict.txt");
    auto uom_dict    = load_dict(gendb_dir + "/num/uom_dict.txt");
    auto name_dict   = load_dict(gendb_dir + "/sub/name_dict.txt");
    auto tag_dict    = load_dict(gendb_dir + "/num/tag_dict.txt");
    auto plabel_dict = load_dict(gendb_dir + "/pre/plabel_dict.txt");

    const int32_t is_code  = find_code(stmt_dict, "IS");
    const int32_t usd_code = find_code(uom_dict,  "USD");
    if (is_code  < 0) { fprintf(stderr, "ERROR: 'IS' not in stmt_dict\n");  exit(1); }
    if (usd_code < 0) { fprintf(stderr, "ERROR: 'USD' not in uom_dict\n"); exit(1); }

    // ---- Data loading ----
    size_t sub_n = 0, pre_n = 0, num_n = 0;
    const int32_t *sub_fy_col, *sub_name_col;
    const int32_t *pre_stmt_col, *pre_adsh_col, *pre_tag_col, *pre_ver_col, *pre_plabel_col;
    const int32_t *num_uom_col, *num_adsh_col, *num_tag_col, *num_ver_col;
    const double  *num_val_col;

    {
        GENDB_PHASE("data_loading");
        sub_fy_col     = mmap_col<int32_t>(gendb_dir + "/sub/fy.bin",       sub_n);
        sub_name_col   = mmap_col<int32_t>(gendb_dir + "/sub/name.bin",     sub_n);
        pre_stmt_col   = mmap_col<int32_t>(gendb_dir + "/pre/stmt.bin",     pre_n);
        pre_adsh_col   = mmap_col<int32_t>(gendb_dir + "/pre/adsh.bin",     pre_n);
        pre_tag_col    = mmap_col<int32_t>(gendb_dir + "/pre/tag.bin",      pre_n);
        pre_ver_col    = mmap_col<int32_t>(gendb_dir + "/pre/version.bin",  pre_n);
        pre_plabel_col = mmap_col<int32_t>(gendb_dir + "/pre/plabel.bin",   pre_n);
        num_uom_col    = mmap_col<int32_t>(gendb_dir + "/num/uom.bin",      num_n);
        num_val_col    = mmap_col<double>  (gendb_dir + "/num/value.bin",   num_n);
        num_adsh_col   = mmap_col<int32_t>(gendb_dir + "/num/adsh.bin",     num_n);
        num_tag_col    = mmap_col<int32_t>(gendb_dir + "/num/tag.bin",      num_n);
        num_ver_col    = mmap_col<int32_t>(gendb_dir + "/num/version.bin",  num_n);
    }

    // ---- Dim filter: flat sub arrays for O(1) lookup ----
    {
        GENDB_PHASE("dim_filter");
        for (int i = 0; i < SUB_N; i++) {
            sub_fy2023[i]   = (sub_fy_col[i] == 2023);
            sub_name_arr[i] = sub_name_col[i];
        }
    }

    // ---- Build pre join HT: (adsh,tag,ver) → plabel, filtered by stmt=IS ----
    // C9: next_power_of_2(1766547*2) = 4194304; C20: std::fill not memset
    PreSlot* pre_ht = new PreSlot[PRE_CAP];
    {
        GENDB_PHASE("build_joins");
        std::fill(pre_ht, pre_ht + PRE_CAP, PreSlot{PRE_EMPTY, 0, 0, 0});
        for (size_t i = 0; i < pre_n; i++) {
            if (pre_stmt_col[i] != is_code) continue;
            pre_insert(pre_ht, pre_adsh_col[i], pre_tag_col[i],
                       pre_ver_col[i], pre_plabel_col[i]);
        }
    }

    // ---- Parallel morsel scan of num with thread-local aggregation ----
    // Occupied-vector trick: track only populated slots → O(actual_groups) merge
    const int nthreads = omp_get_max_threads();
    std::vector<AggSlot*>               local_hts(nthreads, nullptr);
    std::vector<std::vector<uint32_t>>  local_occupied(nthreads);
    // Dense compact vectors: filled inside parallel scan block, then used for merge
    std::vector<std::vector<AggSlot>>   local_compact(nthreads);

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();

            // Allocate thread-local HT (C9: sized for full cardinality; C20: std::fill)
            AggSlot* ht = new AggSlot[AGG_CAP_L];
            std::fill(ht, ht + AGG_CAP_L, AggSlot{AGG_EMPTY, 0ULL, 0.0, 0LL});
            local_hts[tid] = ht;
            local_occupied[tid].reserve(50000);

            // C24: all inner probe loops are bounded for-loops (in pre_probe + agg_update_tracked)
            #pragma omp for schedule(static, 100000) nowait
            for (size_t i = 0; i < num_n; i++) {
                // Filter: uom = 'USD'
                if (num_uom_col[i] != usd_code) continue;
                // Filter: value IS NOT NULL
                const double val = num_val_col[i];
                if (std::isnan(val)) continue;

                // Sub join: O(1) flat array, filter fy=2023
                const int32_t adsh = num_adsh_col[i];
                if ((unsigned)adsh >= (unsigned)SUB_N) continue;
                if (!sub_fy2023[adsh]) continue;

                // Pre join: find ALL matching pre rows (1:N join, multi-value)
                const int32_t tag         = num_tag_col[i];
                const int32_t ver         = num_ver_col[i];
                const int32_t name_code   = sub_name_arr[adsh];
                const uint64_t g1_base    = ((uint64_t)(uint32_t)name_code << 32) | (uint32_t)tag;
                const uint64_t g2_hi      = (uint64_t)(uint32_t)is_code << 32;

                // Aggregate once per matching pre row (each produces a separate join result)
                pre_probe_all(pre_ht, adsh, tag, ver, [&](int32_t plabel) {
                    const uint64_t g1 = g1_base;
                    const uint64_t g2 = g2_hi | (uint32_t)plabel;
                    agg_update_tracked(ht, AGG_CAP_L, local_occupied[tid], g1, g2, val);
                });
            }

            // Compact: create dense AggSlot vector from occupied indices, free the 16MB HT
            // Done inside the parallel block so 64 threads compact in parallel.
            {
                const auto& occ = local_occupied[tid];
                auto& cmp = local_compact[tid];
                cmp.resize(occ.size());
                for (size_t j = 0; j < occ.size(); j++)
                    cmp[j] = ht[occ[j]];
            }
            delete[] ht;
            local_hts[tid] = nullptr;
        }
    }

    // ---- Parallel partitioned merge ----
    // Each of P threads owns a disjoint partition of the key space (by top bits of hash).
    // No synchronization needed: partition HT per thread (AGG_CAP_P slots = 4MB) fits in L3.
    // Compact vectors (dense, sequential) replace random 16MB HT accesses → eliminates DRAM pressure.
    const int P = nthreads;
    std::vector<AggSlot*> part_hts(P, nullptr);
    for (int p = 0; p < P; p++) {
        part_hts[p] = new AggSlot[AGG_CAP_P];
        std::fill(part_hts[p], part_hts[p] + AGG_CAP_P, AggSlot{AGG_EMPTY, 0ULL, 0.0, 0LL});
    }

    {
        GENDB_PHASE("aggregation_merge");
        #pragma omp parallel num_threads(P)
        {
            const int tid  = omp_get_thread_num();
            AggSlot* my_ht = part_hts[tid];

            // Each thread reads ALL compact vectors but inserts only entries in its partition.
            // Compact vectors are dense and sequential → excellent prefetch/cache behavior.
            for (int t = 0; t < nthreads; t++) {
                for (const AggSlot& s : local_compact[t]) {
                    // Partition: top 16 bits of hash, modulo P (correct for any nthreads)
                    uint64_t h = s.g1 * 6364136223846793005ULL ^ s.g2 * 1442695040888963407ULL;
                    int part = (int)((h >> 48) % (unsigned)P);
                    if (part != tid) continue;
                    // Slot within partition HT: bottom bits (no overlap with partition bits)
                    uint32_t slot = (uint32_t)(h & (uint64_t)(AGG_CAP_P - 1));
                    // Bounded probe (C24)
                    for (uint32_t probe = 0; probe < AGG_CAP_P; probe++) {
                        uint32_t idx = (slot + probe) & (AGG_CAP_P - 1);
                        if (my_ht[idx].g1 == AGG_EMPTY) {
                            my_ht[idx] = s;
                            break;
                        }
                        if (my_ht[idx].g1 == s.g1 && my_ht[idx].g2 == s.g2) {
                            my_ht[idx].sum_val += s.sum_val;
                            my_ht[idx].cnt     += s.cnt;
                            break;
                        }
                    }
                }
            }
        }
        // Release compact vectors to free memory
        for (int t = 0; t < nthreads; t++)
            local_compact[t].clear();
    }

    delete[] pre_ht;

    // ---- Collect, top-200 partial sort, CSV output ----
    {
        GENDB_PHASE("output");

        // Collect non-empty slots from all partition HTs
        std::vector<AggSlot> rows;
        rows.reserve(500000);
        for (int p = 0; p < P; p++) {
            for (uint32_t i = 0; i < AGG_CAP_P; i++) {
                if (part_hts[p][i].g1 != AGG_EMPTY)
                    rows.push_back(part_hts[p][i]);
            }
            delete[] part_hts[p];
            part_hts[p] = nullptr;
        }

        // Top-200 by total_value DESC (P6: O(n log k) partial_sort)
        const int k = std::min((int)rows.size(), 200);
        std::partial_sort(rows.begin(), rows.begin() + k, rows.end(),
            [](const AggSlot& a, const AggSlot& b) {
                return a.sum_val > b.sum_val;
            });
        rows.resize(k);

        // Write CSV — quote fields containing commas
        const std::string out_path = results_dir + "/Q6.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); exit(1); }

        fprintf(fp, "name,stmt,tag,plabel,total_value,cnt\n");
        for (const auto& row : rows) {
            const int32_t name_code   = (int32_t)(row.g1 >> 32);
            const int32_t tag_code    = (int32_t)(row.g1 & 0xFFFFFFFFULL);
            const int32_t stmt_code   = (int32_t)(row.g2 >> 32);
            const int32_t plabel_code = (int32_t)(row.g2 & 0xFFFFFFFFULL);

            const char* name   = (name_code   >= 0 && name_code   < (int32_t)name_dict.size())
                                 ? name_dict[name_code].c_str()    : "";
            const char* stmt   = (stmt_code   >= 0 && stmt_code   < (int32_t)stmt_dict.size())
                                 ? stmt_dict[stmt_code].c_str()    : "";
            const char* tag    = (tag_code    >= 0 && tag_code    < (int32_t)tag_dict.size())
                                 ? tag_dict[tag_code].c_str()      : "";
            const char* plabel = (plabel_code >= 0 && plabel_code < (int32_t)plabel_dict.size())
                                 ? plabel_dict[plabel_code].c_str() : "";

            write_csv_field(fp, name);   fputc(',', fp);
            write_csv_field(fp, stmt);   fputc(',', fp);
            write_csv_field(fp, tag);    fputc(',', fp);
            write_csv_field(fp, plabel); fputc(',', fp);
            fprintf(fp, "%.2f,%ld\n", row.sum_val, row.cnt);
        }
        fclose(fp);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
