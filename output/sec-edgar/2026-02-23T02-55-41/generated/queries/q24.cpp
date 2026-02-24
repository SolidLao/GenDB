#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

namespace {

// ── Pre hash set slot ──────────────────────────────────────────────────────
struct PreSlot {
    int32_t adsh, tag, ver, valid;
};

// ── Aggregation slot ───────────────────────────────────────────────────────
struct AggSlot {
    uint64_t key;
    int64_t  cnt;
    double   sum_val;
};

// ── Hash helpers ───────────────────────────────────────────────────────────
inline uint64_t pre_hash(int32_t adsh, int32_t tag, int32_t ver) {
    uint64_t k1 = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tag;
    return k1 * 6364136223846793005ULL + (uint64_t)(uint32_t)ver * 1442695040888963407ULL;
}

inline uint64_t agg_hash(int32_t tag_code, int32_t ver_code) {
    uint64_t k = ((uint64_t)(uint32_t)tag_code << 32) | (uint32_t)ver_code;
    return k * 6364136223846793005ULL;
}

// ── Zone map entry ─────────────────────────────────────────────────────────
struct ZoneEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t block_size;
};

// ── Load dictionary ────────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) dict.push_back(line);
    return dict;
}

// ── mmap helper ───────────────────────────────────────────────────────────
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); return nullptr; }
    struct stat st;
    fstat(fd, &st);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    n_out = st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(p);
}

// ── main query ─────────────────────────────────────────────────────────────
} // end anonymous namespace

void run_q24(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const std::string num_dir = gendb_dir + "/num";
    const std::string pre_dir = gendb_dir + "/pre";

    // ── Data loading ──────────────────────────────────────────────────────
    size_t pre_n = 0, num_n = 0;
    const int32_t *pre_adsh_col, *pre_tag_col, *pre_ver_col;
    const int32_t *num_adsh_col, *num_tag_col, *num_ver_col;
    const int32_t *num_ddate_col, *num_uom_col;
    const double  *num_value_col;

    uint32_t zm_num_blocks = 0;
    const ZoneEntry* zm_entries = nullptr;

    // uom/tag/version dicts
    std::vector<std::string> uom_dict, tag_dict, ver_dict;

    {
        GENDB_PHASE("data_loading");

        size_t tmp;
        pre_adsh_col = mmap_col<int32_t>(pre_dir + "/adsh.bin", pre_n);
        pre_tag_col  = mmap_col<int32_t>(pre_dir + "/tag.bin",  tmp);
        pre_ver_col  = mmap_col<int32_t>(pre_dir + "/version.bin", tmp);

        num_adsh_col  = mmap_col<int32_t>(num_dir + "/adsh.bin",  num_n);
        num_tag_col   = mmap_col<int32_t>(num_dir + "/tag.bin",   tmp);
        num_ver_col   = mmap_col<int32_t>(num_dir + "/version.bin", tmp);
        num_ddate_col = mmap_col<int32_t>(num_dir + "/ddate.bin", tmp);
        num_uom_col   = mmap_col<int32_t>(num_dir + "/uom.bin",   tmp);
        num_value_col = mmap_col<double> (num_dir + "/value.bin", tmp);

        // Zone map
        {
            std::string zm_path = num_dir + "/indexes/ddate_zone_map.bin";
            int fd = open(zm_path.c_str(), O_RDONLY);
            if (fd >= 0) {
                struct stat st; fstat(fd, &st);
                const uint8_t* raw = reinterpret_cast<const uint8_t*>(
                    mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
                close(fd);
                zm_num_blocks = *reinterpret_cast<const uint32_t*>(raw);
                zm_entries    = reinterpret_cast<const ZoneEntry*>(raw + sizeof(uint32_t));
            }
        }

        uom_dict = load_dict(num_dir + "/uom_dict.txt");
        tag_dict = load_dict(num_dir + "/tag_dict.txt");
        ver_dict = load_dict(num_dir + "/version_dict.txt");
    }

    // ── Find USD code (C2) ─────────────────────────────────────────────────
    int32_t usd_code = -1;
    for (int32_t i = 0; i < (int32_t)uom_dict.size(); ++i) {
        if (uom_dict[i] == "USD") { usd_code = i; break; }
    }

    // ── Pre hash set: mmap-based (avoids 512MB BSS page-fault storm) ───────
    // With static BSS, a fresh process triggers 131072 sequential page faults
    // through the single-threaded std::fill — costing ~49s on a 64-core NUMA box.
    // Fix: explicit mmap + parallel fill spreads faults across all 64 cores.
    static const uint64_t CAP  = 33554432ULL; // 2^25 = next_power_of_2(9600799*2), C9
    static const uint64_t MASK = CAP - 1;
    static PreSlot* pre_ht = nullptr;
    if (!pre_ht) {
        pre_ht = reinterpret_cast<PreSlot*>(
            mmap(nullptr, CAP * sizeof(PreSlot), PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    }

    // ── Build pre hash set (C9, C20, C24) ─────────────────────────────────
    {
        GENDB_PHASE("build_joins");

        // Parallel fill — 64 threads each touch 8MB, parallelising page faults
        // C20: zero sentinel {0,0,0,0} — correct with parallel assignment
        #pragma omp parallel for schedule(static)
        for (int64_t i = 0; i < (int64_t)CAP; ++i) {
            pre_ht[i] = PreSlot{0, 0, 0, 0};
        }

        // Parallel insert with CAS-based slot claiming (C24: bounded probing)
        // Build and probe are strictly separated phases (OMP barrier below ensures
        // all writes are visible before main_scan reads pre_ht).
        // Duplicate keys are harmless for anti-join (presence check only).
        #pragma omp parallel for schedule(dynamic, 50000)
        for (int64_t i = 0; i < (int64_t)pre_n; ++i) {
            int32_t a = pre_adsh_col[i];
            int32_t t = pre_tag_col[i];
            int32_t v = pre_ver_col[i];
            uint64_t h = pre_hash(a, t, v);
            for (uint64_t p = 0; p < CAP; ++p) {  // C24: bounded
                uint64_t slot = (h + p) & MASK;
                int32_t expected = 0;
                // Atomically claim slot: CAS(0 → 1). Probes next on failure.
                if (__atomic_compare_exchange_n(&pre_ht[slot].valid, &expected, 1,
                                               false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                    pre_ht[slot].adsh = a;
                    pre_ht[slot].tag  = t;
                    pre_ht[slot].ver  = v;
                    break;
                }
                // Slot taken — probe next (duplicate inserts OK for anti-join)
            }
        }
        // Implicit OMP barrier: all insertions visible before probe phase
    }

    // ── Main scan: filter + anti-join probe + thread-local aggregate ───────
    const int     num_threads = omp_get_max_threads();
    const uint64_t AGG_CAP    = 131072ULL; // 2^17 (C9: full cardinality per thread)
    const uint64_t AGG_MASK   = AGG_CAP - 1;
    const int32_t  DDATE_LO   = 20230101;
    const int32_t  DDATE_HI   = 20231231;

    std::vector<std::vector<AggSlot>> tl_maps(num_threads,
        std::vector<AggSlot>(AGG_CAP, AggSlot{UINT64_MAX, 0, 0.0}));

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            std::vector<AggSlot>& agg = tl_maps[tid];

            const int64_t MORSEL = 100000LL;
            const int64_t total_blocks = (zm_num_blocks > 0)
                ? (int64_t)zm_num_blocks
                : ((int64_t)num_n + MORSEL - 1) / MORSEL;

            #pragma omp for schedule(dynamic, 1)
            for (int64_t blk = 0; blk < total_blocks; ++blk) {
                int64_t row_start, row_end;
                if (zm_num_blocks > 0) {
                    // Zone map skip (C19)
                    const ZoneEntry& ze = zm_entries[blk];
                    if (ze.max_val < DDATE_LO || ze.min_val > DDATE_HI) continue;
                    row_start = blk * MORSEL;
                    row_end   = row_start + ze.block_size;
                    if (row_end > (int64_t)num_n) row_end = (int64_t)num_n;
                } else {
                    row_start = blk * MORSEL;
                    row_end   = std::min(row_start + MORSEL, (int64_t)num_n);
                }

                for (int64_t i = row_start; i < row_end; ++i) {
                    // Filters
                    int32_t ddate = num_ddate_col[i];
                    if (ddate < DDATE_LO || ddate > DDATE_HI) continue;
                    if (num_uom_col[i] != usd_code) continue;
                    double val = num_value_col[i];
                    if (std::isnan(val)) continue;

                    // Anti-join probe (C24: bounded)
                    int32_t ac = num_adsh_col[i];
                    int32_t tc = num_tag_col[i];
                    int32_t vc = num_ver_col[i];
                    uint64_t h = pre_hash(ac, tc, vc);
                    bool found = false;
                    for (uint64_t p = 0; p < CAP; ++p) {
                        uint64_t slot = (h + p) & MASK;
                        if (!pre_ht[slot].valid) break;
                        if (pre_ht[slot].adsh == ac && pre_ht[slot].tag == tc &&
                            pre_ht[slot].ver == vc) {
                            found = true; break;
                        }
                    }
                    if (found) continue; // anti-join: exclude rows WITH a pre match

                    // Aggregate (C15: key includes BOTH tag AND version)
                    uint64_t agg_key = ((uint64_t)(uint32_t)tc << 32) | (uint32_t)vc;
                    uint64_t ah = agg_hash(tc, vc);
                    for (uint64_t p = 0; p < AGG_CAP; ++p) {
                        uint64_t sl = (ah + p) & AGG_MASK;
                        if (agg[sl].key == UINT64_MAX) {
                            agg[sl].key     = agg_key;
                            agg[sl].cnt     = 1;
                            agg[sl].sum_val = val;
                            break;
                        }
                        if (agg[sl].key == agg_key) {
                            agg[sl].cnt++;
                            agg[sl].sum_val += val;
                            break;
                        }
                    }
                }
            }
        } // end parallel
    }

    // ── Merge thread-local aggregation maps ───────────────────────────────
    const uint64_t GMAP_CAP  = 262144ULL; // 2^18, some margin
    const uint64_t GMAP_MASK = GMAP_CAP - 1;
    std::vector<AggSlot> global_map(GMAP_CAP, AggSlot{UINT64_MAX, 0, 0.0});

    {
        GENDB_PHASE("aggregation_merge");

        for (int t = 0; t < num_threads; ++t) {
            for (uint64_t s = 0; s < AGG_CAP; ++s) {
                const AggSlot& src = tl_maps[t][s];
                if (src.key == UINT64_MAX) continue;
                uint64_t h = agg_hash(
                    (int32_t)(src.key >> 32),
                    (int32_t)(src.key & 0xFFFFFFFFULL));
                for (uint64_t p = 0; p < GMAP_CAP; ++p) {
                    uint64_t sl = (h + p) & GMAP_MASK;
                    if (global_map[sl].key == UINT64_MAX) {
                        global_map[sl] = src;
                        break;
                    }
                    if (global_map[sl].key == src.key) {
                        global_map[sl].cnt     += src.cnt;
                        global_map[sl].sum_val += src.sum_val;
                        break;
                    }
                }
            }
        }
    }

    // ── HAVING + TopK + CSV output ────────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct Result {
            int32_t tag_code, ver_code;
            int64_t cnt;
            double  total;
        };

        std::vector<Result> results;
        results.reserve(4096);
        for (uint64_t s = 0; s < GMAP_CAP; ++s) {
            const AggSlot& sl = global_map[s];
            if (sl.key == UINT64_MAX) continue;
            if (sl.cnt <= 10) continue; // HAVING cnt > 10
            results.push_back({
                (int32_t)(sl.key >> 32),
                (int32_t)(sl.key & 0xFFFFFFFFULL),
                sl.cnt,
                sl.sum_val
            });
        }

        // TopK: partial_sort top 100 by cnt DESC (P6)
        const size_t LIMIT = 100;
        if (results.size() > LIMIT) {
            std::partial_sort(results.begin(), results.begin() + LIMIT, results.end(),
                [](const Result& a, const Result& b) { return a.cnt > b.cnt; });
            results.resize(LIMIT);
        } else {
            std::sort(results.begin(), results.end(),
                [](const Result& a, const Result& b) { return a.cnt > b.cnt; });
        }

        // Write CSV (C18: decode via dict)
        std::string out_path = results_dir + "/Q24.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        fprintf(f, "tag,version,cnt,total\n");
        for (const auto& r : results) {
            const std::string& tag_str = (r.tag_code >= 0 && r.tag_code < (int32_t)tag_dict.size())
                ? tag_dict[r.tag_code] : std::to_string(r.tag_code);
            const std::string& ver_str = (r.ver_code >= 0 && r.ver_code < (int32_t)ver_dict.size())
                ? ver_dict[r.ver_code] : std::to_string(r.ver_code);
            fprintf(f, "%s,%s,%ld,%.2f\n",
                tag_str.c_str(), ver_str.c_str(), (long)r.cnt, r.total);
        }
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q24(gendb_dir, results_dir);
    return 0;
}
#endif
