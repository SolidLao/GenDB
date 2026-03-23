// Q24 iter_2: Sort-merge anti-join with parallel collect + GNU parallel sort
//
// Root cause of iter_1 slowness (71ms):
//   8.9M USD-2023 rows (not 67K as plan estimated), each requiring binary search
//   into 69MB anti array (8.6M entries, exceeds 44MB LLC). 64 threads × 139K rows
//   × 23 binary search levels = 204M random comparisons, many hitting DRAM.
//
// Fix: collect probe entries → parallel sort → single-pass merge
//   - Parallel collect: 64 threads scan 8.9M rows → flat probe buffer (sequential)
//   - GNU parallel sort: parallelizes std::sort across 64 threads (~10-20ms)
//   - Single-pass merge: sequential 69MB scan of anti array at memory bandwidth (~3ms)
//   - Aggregate survivors by tagver_code
//
// Expected: collect ~5ms + parallel_sort ~15ms + merge ~5ms = ~25ms total

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <parallel/algorithm>   // __gnu_parallel::sort

#include "timing_utils.h"

static const size_t TAG_COUNT = 1070663;
static const size_t NUM_ROWS  = 39401761;

// ---------------------------------------------------------------------------
// RAII mmap helper
// ---------------------------------------------------------------------------
struct RawMmap {
    void*  ptr = nullptr;
    size_t sz  = 0;
    int    fd  = -1;

    RawMmap() = default;
    RawMmap(const RawMmap&) = delete;
    RawMmap& operator=(const RawMmap&) = delete;
    ~RawMmap() { close_all(); }

    void open(const std::string& path, int advice = MADV_SEQUENTIAL) {
        close_all();
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st;
        if (fstat(fd, &st) < 0) {
            ::close(fd); fd = -1;
            throw std::runtime_error("fstat failed: " + path);
        }
        sz = (size_t)st.st_size;
        if (sz == 0) return;
        ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
            ptr = nullptr; ::close(fd); fd = -1;
            throw std::runtime_error("mmap failed: " + path);
        }
        madvise(ptr, sz, advice);
    }

    void close_all() {
        if (ptr && sz > 0) munmap(ptr, sz);
        if (fd >= 0) ::close(fd);
        ptr = nullptr; sz = 0; fd = -1;
    }

    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }

    void willneed_rows(size_t row_lo, size_t row_hi, size_t elem_size) const {
        if (!ptr || row_hi <= row_lo) return;
        static const size_t page_sz = (size_t)sysconf(_SC_PAGESIZE);
        size_t byte_off = row_lo * elem_size;
        size_t byte_end = row_hi * elem_size;
        if (byte_end > sz) byte_end = sz;
        if (byte_off >= byte_end) return;
        size_t aligned_off = byte_off & ~(page_sz - 1);
        madvise((char*)ptr + aligned_off, byte_end - aligned_off, MADV_WILLNEED);
    }
};

// ---------------------------------------------------------------------------
// Load USD dict code
// ---------------------------------------------------------------------------
static int8_t load_usd_code(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open " + path);
    uint8_t N;
    if (fread(&N, 1, 1, f) != 1) { fclose(f); throw std::runtime_error("fread N"); }
    for (int i = 0; i < (int)N; i++) {
        int8_t code; uint8_t slen;
        if (fread(&code, 1, 1, f) != 1) break;
        if (fread(&slen, 1, 1, f) != 1) break;
        char buf[256] = {};
        if (fread(buf, 1, slen, f) != (size_t)slen) break;
        if (slen == 3 && memcmp(buf, "USD", 3) == 0) { fclose(f); return code; }
    }
    fclose(f);
    throw std::runtime_error("USD not found in uom_codes.bin");
}

// ---------------------------------------------------------------------------
// ProbeEntry: key (for sort+merge) + tagver_code + value (for aggregate)
// Layout: key@0(8), value@8(8), tagver_code@16(4), pad@20(4) = 24 bytes
// ---------------------------------------------------------------------------
struct ProbeEntry {
    uint64_t key;
    double   value;
    int32_t  tagver_code;
    int32_t  _pad;
};

static_assert(sizeof(ProbeEntry) == 24, "ProbeEntry must be 24 bytes");

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE_MS("total", t_total);

    int8_t usd_code     = 0;
    size_t r_2023_start = 0;
    size_t r_2023_end   = 0;

    RawMmap m_uom, m_ddate, m_adsh, m_tagver, m_value, m_anti;

    // -------------------------------------------------------------------------
    // Phase: data_loading — mmap + binary search for exact USD-2023 row range
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("data_loading");

        usd_code = load_usd_code(gendb_dir + "/indexes/uom_codes.bin");
        printf("[INFO] usd_code = %d\n", (int)usd_code);

        // Binary search: find exact USD row range (num sorted uom_code ASC, ddate ASC)
        m_uom.open(gendb_dir + "/num/uom_code.bin", MADV_RANDOM);
        const int8_t* uom_ptr = m_uom.as<int8_t>();
        size_t r_usd_start = (size_t)(
            std::lower_bound(uom_ptr, uom_ptr + NUM_ROWS, usd_code) - uom_ptr);
        size_t r_usd_end   = (size_t)(
            std::upper_bound(uom_ptr, uom_ptr + NUM_ROWS, usd_code) - uom_ptr);
        printf("[INFO] USD row range: [%zu, %zu) = %zu rows\n",
               r_usd_start, r_usd_end, r_usd_end - r_usd_start);

        // Binary search: find 2023 ddate range within USD rows
        m_ddate.open(gendb_dir + "/num/ddate.bin", MADV_RANDOM);
        const int32_t* ddate_ptr = m_ddate.as<int32_t>();
        r_2023_start = (size_t)(std::lower_bound(
            ddate_ptr + r_usd_start, ddate_ptr + r_usd_end, (int32_t)20230101) - ddate_ptr);
        r_2023_end   = (size_t)(std::upper_bound(
            ddate_ptr + r_usd_start, ddate_ptr + r_usd_end, (int32_t)20231231) - ddate_ptr);
        printf("[INFO] USD-2023 row range: [%zu, %zu) = %zu rows\n",
               r_2023_start, r_2023_end, r_2023_end - r_2023_start);

        // mmap scan columns + async readahead on qualifying range
        m_adsh.open(gendb_dir   + "/num/adsh_code.bin",  MADV_RANDOM);
        m_tagver.open(gendb_dir + "/num/tagver_code.bin", MADV_RANDOM);
        m_value.open(gendb_dir  + "/num/value.bin",       MADV_RANDOM);
        m_adsh.willneed_rows(r_2023_start, r_2023_end, sizeof(int32_t));
        m_tagver.willneed_rows(r_2023_start, r_2023_end, sizeof(int32_t));
        m_value.willneed_rows(r_2023_start, r_2023_end, sizeof(double));

        // mmap anti-join array — SEQUENTIAL for merge pass
        m_anti.open(gendb_dir + "/indexes/pre_adsh_tagver_set.bin", MADV_SEQUENTIAL);

    } // data_loading

    // -------------------------------------------------------------------------
    // Phase: main_scan
    // Steps: parallel collect → parallel sort → single-pass merge → aggregate
    // -------------------------------------------------------------------------

    struct Group {
        int32_t tagver_code;
        int32_t cnt;
        double  total;
    };
    std::vector<Group> result;

    {
        GENDB_PHASE("main_scan");

        const int32_t* adsh_col   = m_adsh.as<int32_t>();
        const int32_t* tagver_col = m_tagver.as<int32_t>();
        const double*  value_col  = m_value.as<double>();
        const size_t   n_rows     = r_2023_end - r_2023_start;

        // --- Step 1: Parallel collect ---
        // Each thread collects its chunk into a thread-local vector,
        // then we concatenate into a single probe buffer.
        const int n_threads = (int)std::thread::hardware_concurrency();
        printf("[INFO] n_threads = %d, qualifying rows = %zu\n", n_threads, n_rows);

        std::vector<std::vector<ProbeEntry>> thread_probes(n_threads);
        const size_t chunk_size = (n_rows + (size_t)n_threads - 1) / (size_t)n_threads;

        {
            std::vector<std::thread> threads;
            threads.reserve(n_threads);
            for (int tid = 0; tid < n_threads; tid++) {
                threads.emplace_back([&, tid]() {
                    size_t lo = r_2023_start + (size_t)tid * chunk_size;
                    size_t hi = std::min(lo + chunk_size, r_2023_end);
                    if (lo >= r_2023_end) return;

                    auto& local = thread_probes[tid];
                    local.reserve((hi - lo) / 2);  // conservative estimate

                    for (size_t i = lo; i < hi; ++i) {
                        const double v = value_col[i];
                        if (__builtin_expect(std::isnan(v), 0)) continue;
                        const int32_t tc = tagver_col[i];
                        if (__builtin_expect((uint32_t)tc >= (uint32_t)TAG_COUNT, 0)) continue;
                        const int32_t  ac  = adsh_col[i];
                        const uint64_t key = ((uint64_t)(uint32_t)ac << 32) | (uint32_t)tc;
                        local.push_back({key, v, tc, 0});
                    }
                });
            }
            for (auto& th : threads) th.join();
        }

        // Concatenate thread-local vectors into one flat probe buffer
        size_t total_probes = 0;
        for (const auto& v : thread_probes) total_probes += v.size();
        printf("[INFO] collected %zu probe entries\n", total_probes);

        std::vector<ProbeEntry> probes;
        probes.reserve(total_probes);
        for (auto& v : thread_probes) {
            probes.insert(probes.end(), v.begin(), v.end());
            v.clear();
            v.shrink_to_fit();
        }
        thread_probes.clear();

        // --- Step 2: GNU parallel sort ---
        // Uses all available OpenMP threads for parallel sort
        __gnu_parallel::sort(probes.begin(), probes.end(),
                             [](const ProbeEntry& a, const ProbeEntry& b) {
                                 return a.key < b.key;
                             });

        // --- Step 3: Single-pass sort-merge anti-join ---
        // Sequential scan of 69MB anti array → hardware prefetch takes over
        const uint64_t* anti_ptr  = m_anti.as<uint64_t>();
        const size_t    n_anti    = (size_t)anti_ptr[0];
        const uint64_t* anti_data = anti_ptr + 1;
        printf("[INFO] anti n_unique = %zu\n", n_anti);

        // --- Step 4: Aggregate survivors ---
        std::unordered_map<int32_t, std::pair<int32_t, double>> agg;
        agg.reserve(512);

        const size_t np = probes.size();
        size_t ai = 0;
        size_t pi = 0;

        while (pi < np) {
            const uint64_t pk = probes[pi].key;
            // Advance anti pointer to first entry >= pk
            while (ai < n_anti && anti_data[ai] < pk) ++ai;

            if (ai < n_anti && anti_data[ai] == pk) {
                // pk found in anti → this (adsh, tagver) has a pre match → block all probes
                while (pi < np && probes[pi].key == pk) ++pi;
            } else {
                // pk not in anti → row survives anti-join → aggregate
                while (pi < np && probes[pi].key == pk) {
                    auto& e = agg[probes[pi].tagver_code];
                    e.first++;
                    e.second += probes[pi].value;
                    ++pi;
                }
            }
        }
        printf("[INFO] raw groups: %zu\n", agg.size());

        // HAVING cnt > 10
        result.reserve(agg.size());
        for (const auto& kv : agg) {
            if (kv.second.first > 10) {
                result.push_back({kv.first, kv.second.first, kv.second.second});
            }
        }
        printf("[INFO] groups after HAVING cnt > 10: %zu\n", result.size());

    } // main_scan

    // -------------------------------------------------------------------------
    // Phase: dim_filter — sort by cnt DESC, limit 100
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("dim_filter");

        std::sort(result.begin(), result.end(),
                  [](const Group& a, const Group& b) { return a.cnt > b.cnt; });
        if (result.size() > 100) result.resize(100);

    } // dim_filter

    // -------------------------------------------------------------------------
    // Phase: output — decode tag/version strings, write CSV
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        RawMmap m_tag_off, m_tag_data, m_ver_off, m_ver_data;
        m_tag_off.open(gendb_dir  + "/tag/tag_offsets.bin",     MADV_RANDOM);
        m_tag_data.open(gendb_dir + "/tag/tag_data.bin",        MADV_SEQUENTIAL);
        m_ver_off.open(gendb_dir  + "/tag/version_offsets.bin", MADV_RANDOM);
        m_ver_data.open(gendb_dir + "/tag/version_data.bin",    MADV_SEQUENTIAL);

        const uint32_t* tag_offsets = m_tag_off.as<uint32_t>();
        const char*     tag_data    = m_tag_data.as<char>();
        const uint32_t* ver_offsets = m_ver_off.as<uint32_t>();
        const char*     ver_data    = m_ver_data.as<char>();

        {
            std::string cmd = "mkdir -p " + results_dir;
            if (system(cmd.c_str()) != 0)
                fprintf(stderr, "Warning: mkdir -p failed\n");
        }

        const std::string out_path = results_dir + "/Q24.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) throw std::runtime_error("Cannot open output: " + out_path);

        fprintf(out, "tag,version,cnt,total\n");
        for (const auto& g : result) {
            const uint32_t tc    = (uint32_t)g.tagver_code;
            const uint32_t tag_s = tag_offsets[tc];
            const uint32_t tag_e = tag_offsets[tc + 1];
            const uint32_t ver_s = ver_offsets[tc];
            const uint32_t ver_e = ver_offsets[tc + 1];
            fprintf(out, "%.*s,%.*s,%d,%.2f\n",
                    (int)(tag_e - tag_s), tag_data + tag_s,
                    (int)(ver_e - ver_s), ver_data + ver_s,
                    g.cnt, g.total);
        }
        fclose(out);
        printf("[INFO] Written %zu rows to %s\n", result.size(), out_path.c_str());

    } // output

    return 0;
}
