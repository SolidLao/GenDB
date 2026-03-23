// Q3: SEC EDGAR — Sum(value) by (name, cik) WHERE uom=USD AND fy=2022
// HAVING SUM > AVG(cik-level sums); ORDER BY total_value DESC LIMIT 100
// Strategy: single-pass dual aggregate with thread-local hash maps

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <cassert>
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
static constexpr uint32_t BLOCK_SIZE       = 100000;
static constexpr uint32_t SUB_ROWS         = 86135;
static constexpr uint32_t CIK_HT_SIZE      = 16384;   // next_pow2(8886*2)
static constexpr uint32_t CIK_HT_MASK      = CIK_HT_SIZE  - 1;
static constexpr uint32_t NC_HT_SIZE       = 32768;   // next_pow2(16000*2)
static constexpr uint32_t NC_HT_MASK       = NC_HT_SIZE   - 1;
static constexpr int32_t  EMPTY_CIK        = INT32_MIN;
static constexpr int64_t  EMPTY_NC         = INT64_MIN;

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
// Hash-table slot types
// ---------------------------------------------------------------------------
struct CikSlot  { int32_t key; int32_t _pad; long double sum; };   // 24 bytes
struct NCSlot   { int64_t key;               long double sum; };   // 24 bytes

// ---------------------------------------------------------------------------
// mmap helper
// ---------------------------------------------------------------------------
template<typename T>
static const T* mmap_file(const std::string& path, size_t& out_bytes,
                           bool sequential = true) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); out_bytes = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    out_bytes = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { perror("mmap"); out_bytes = 0; return nullptr; }
    if (sequential)
        madvise(ptr, out_bytes, MADV_SEQUENTIAL);
    else
        madvise(ptr, out_bytes, MADV_WILLNEED);
    return reinterpret_cast<const T*>(ptr);
}

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------
static inline uint32_t hash_cik(int32_t k) {
    return (uint32_t)k * 2654435761u;
}
static inline uint32_t hash_nc(int64_t k) {
    uint64_t h = (uint64_t)k;
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    return (uint32_t)h;
}

// ---------------------------------------------------------------------------
// Hash-table insert / accumulate
// ---------------------------------------------------------------------------
static inline void cik_insert(CikSlot* __restrict__ ht, int32_t cik, double val) {
    uint32_t h = hash_cik(cik) & CIK_HT_MASK;
    for (uint32_t p = 0; p < CIK_HT_SIZE; ++p) {
        uint32_t s = (h + p) & CIK_HT_MASK;
        if (__builtin_expect(ht[s].key == cik, 1)) { ht[s].sum += val; return; }
        if (ht[s].key == EMPTY_CIK) { ht[s].key = cik; ht[s].sum = val; return; }
    }
}

static inline void nc_insert(NCSlot* __restrict__ ht, int64_t key, double val) {
    uint32_t h = hash_nc(key) & NC_HT_MASK;
    for (uint32_t p = 0; p < NC_HT_SIZE; ++p) {
        uint32_t s = (h + p) & NC_HT_MASK;
        if (__builtin_expect(ht[s].key == key, 1)) { ht[s].sum += val; return; }
        if (ht[s].key == EMPTY_NC) { ht[s].key = key; ht[s].sum = val; return; }
    }
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

    {
        GENDB_PHASE("data_loading");

        // --- Load USD code from shared/uom.dict ---
        {
            std::string dp = gendb_dir + "/shared/uom.dict";
            int fd = open(dp.c_str(), O_RDONLY);
            if (fd < 0) { perror(dp.c_str()); return; }
            uint32_t n = 0;
            if (read(fd, &n, 4) != 4) { close(fd); return; }
            for (uint32_t i = 0; i < n; i++) {
                uint16_t len = 0;
                read(fd, &len, 2);
                char buf[256] = {};
                read(fd, buf, len);
                if (len == 3 && buf[0]=='U' && buf[1]=='S' && buf[2]=='D')
                    usd_code = (int8_t)i;
            }
            close(fd);
        }
        if (usd_code < 0) { fprintf(stderr, "USD code not found\n"); return; }

        // --- Zone maps ---
        {
            size_t zm_sz = 0;
            const uint8_t* raw = mmap_file<uint8_t>(
                gendb_dir + "/indexes/num_zonemaps.bin", zm_sz, false);
            if (raw) {
                n_blocks_zm = *(const int32_t*)raw;
                zm_blocks   = reinterpret_cast<const ZMBlock*>(raw + 4);
            }
        }

        // --- num columns (sequential) ---
        num_uom   = mmap_file<int8_t> (gendb_dir + "/num/uom.bin",   sz, true);
        num_rows  = sz / sizeof(int8_t);
        num_value = mmap_file<double> (gendb_dir + "/num/value.bin",  sz, true);
        num_adsh  = mmap_file<int32_t>(gendb_dir + "/num/adsh.bin",   sz, true);

        // --- sub columns (random access — load into L3) ---
        sub_fy   = mmap_file<int16_t>(gendb_dir + "/sub/fy.bin",   sz, false);
        sub_cik  = mmap_file<int32_t>(gendb_dir + "/sub/cik.bin",  sz, false);
        sub_name = mmap_file<int32_t>(gendb_dir + "/sub/name.bin", sz, false);
    }

    uint32_t n_blocks = (uint32_t)((num_rows + BLOCK_SIZE - 1) / BLOCK_SIZE);
    int nthreads = omp_get_max_threads();

    // -----------------------------------------------------------------------
    // Allocate thread-local hash maps (long double for precision)
    // -----------------------------------------------------------------------
    std::vector<CikSlot*> tl_cik(nthreads, nullptr);
    std::vector<NCSlot*>  tl_nc (nthreads, nullptr);
    for (int t = 0; t < nthreads; t++) {
        tl_cik[t] = new CikSlot[CIK_HT_SIZE];
        tl_nc[t]  = new NCSlot [NC_HT_SIZE];
        for (uint32_t i = 0; i < CIK_HT_SIZE; i++) tl_cik[t][i].key = EMPTY_CIK;
        for (uint32_t i = 0; i < NC_HT_SIZE;  i++) tl_nc[t][i].key  = EMPTY_NC;
    }

    // -----------------------------------------------------------------------
    // main_scan — parallel morsel-driven, dual-aggregate per thread
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        const int8_t  uc = usd_code;
        const int16_t FY = (int16_t)2022;

        #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
        for (uint32_t b = 0; b < n_blocks; b++) {
            // Zone-map skip: if this block has no USD rows, skip entirely
            if (b < (uint32_t)n_blocks_zm) {
                if (zm_blocks[b].uom_min > uc || zm_blocks[b].uom_max < uc) continue;
            }

            const int tid = omp_get_thread_num();
            CikSlot* __restrict__ cik_ht = tl_cik[tid];
            NCSlot*  __restrict__ nc_ht  = tl_nc[tid];

            const uint64_t row_start = (uint64_t)b * BLOCK_SIZE;
            const uint64_t row_end   = std::min(row_start + BLOCK_SIZE, (uint64_t)num_rows);

            for (uint64_t i = row_start; i < row_end; i++) {
                if (num_uom[i] != uc) continue;
                const double val = num_value[i];
                if (std::isnan(val)) continue;

                const int32_t adsh_code = num_adsh[i];
                if (sub_fy[adsh_code] != FY) continue;

                const int32_t cik  = sub_cik [adsh_code];
                const int32_t name = sub_name[adsh_code];

                cik_insert(cik_ht, cik, val);
                const int64_t nc_key = ((int64_t)name << 32) | (uint32_t)cik;
                nc_insert(nc_ht, nc_key, val);
            }
        }
    }

    // -----------------------------------------------------------------------
    // merge_thread_local_maps — reduce into global tables
    // -----------------------------------------------------------------------
    CikSlot* global_cik = new CikSlot[CIK_HT_SIZE];
    NCSlot*  global_nc  = new NCSlot [NC_HT_SIZE];
    for (uint32_t i = 0; i < CIK_HT_SIZE; i++) global_cik[i].key = EMPTY_CIK;
    for (uint32_t i = 0; i < NC_HT_SIZE;  i++) global_nc[i].key  = EMPTY_NC;

    {
        GENDB_PHASE("merge_thread_local_maps");
        for (int t = 0; t < nthreads; t++) {
            for (uint32_t i = 0; i < CIK_HT_SIZE; i++) {
                if (tl_cik[t][i].key != EMPTY_CIK)
                    cik_insert(global_cik, tl_cik[t][i].key, tl_cik[t][i].sum);
            }
            for (uint32_t i = 0; i < NC_HT_SIZE; i++) {
                if (tl_nc[t][i].key != EMPTY_NC)
                    nc_insert(global_nc, tl_nc[t][i].key, tl_nc[t][i].sum);
            }
            delete[] tl_cik[t];
            delete[] tl_nc[t];
        }
    }

    // -----------------------------------------------------------------------
    // compute_threshold_avg_cik_sums
    // -----------------------------------------------------------------------
    long double threshold = 0.0L;
    {
        GENDB_PHASE("compute_threshold_avg_cik_sums");
        long double total = 0.0L;
        int64_t cnt  = 0;
        for (uint32_t i = 0; i < CIK_HT_SIZE; i++) {
            if (global_cik[i].key != EMPTY_CIK) {
                total += global_cik[i].sum;
                ++cnt;
            }
        }
        threshold = (cnt > 0) ? total / (long double)cnt : 0.0L;
    }
    delete[] global_cik;

    // -----------------------------------------------------------------------
    // having_filter_name_cik_map + collect survivors
    // -----------------------------------------------------------------------
    // Survivor: (name_code, cik, total_value) where total_value > threshold
    struct Survivor { int32_t name_code; int32_t cik; long double sum; };
    std::vector<Survivor> survivors;
    survivors.reserve(512);

    {
        GENDB_PHASE("having_filter_name_cik_map");
        for (uint32_t i = 0; i < NC_HT_SIZE; i++) {
            if (global_nc[i].key != EMPTY_NC && global_nc[i].sum > threshold) {
                int32_t name_code = (int32_t)((uint64_t)global_nc[i].key >> 32);
                int32_t cik       = (int32_t)(global_nc[i].key & 0xFFFFFFFFLL);
                survivors.push_back({name_code, cik, global_nc[i].sum});
            }
        }
    }
    delete[] global_nc;

    // -----------------------------------------------------------------------
    // decode_name_codes — load sub/name.dict, decode only survivors
    // -----------------------------------------------------------------------
    std::vector<std::string> name_dict;
    {
        GENDB_PHASE("decode_name_codes");
        std::string dp = gendb_dir + "/sub/name.dict";
        int fd = open(dp.c_str(), O_RDONLY);
        if (fd >= 0) {
            uint32_t n = 0;
            read(fd, &n, 4);
            name_dict.resize(n);
            for (uint32_t i = 0; i < n; i++) {
                uint16_t len = 0;
                read(fd, &len, 2);
                name_dict[i].resize(len);
                if (len > 0) read(fd, &name_dict[i][0], len);
            }
            close(fd);
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
    // output — write CSV
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
            // Quote names that contain commas or double-quotes (RFC 4180 CSV)
            bool needs_quote = (nm.find(',') != std::string::npos ||
                                nm.find('"') != std::string::npos);
            if (needs_quote) {
                // Escape any embedded double-quotes by doubling them
                std::string escaped;
                escaped.reserve(nm.size() + 2);
                for (char c : nm) {
                    if (c == '"') escaped += '"';
                    escaped += c;
                }
                fprintf(f, "\"%s\",%d,%.2f\n", escaped.c_str(), s.cik, (double)s.sum);
            } else {
                fprintf(f, "%s,%d,%.2f\n", nm.c_str(), s.cik, (double)s.sum);
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
