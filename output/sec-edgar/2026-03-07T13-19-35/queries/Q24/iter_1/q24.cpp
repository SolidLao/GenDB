// Q24 iter_1: Optimized anti-join + aggregate (tag, version) with HAVING cnt > 10, ORDER BY cnt DESC LIMIT 100
// Key insight: num is sorted by (uom_code ASC, ddate ASC).
// Binary search finds EXACT USD-2023 row range. Parallel scan with 64 threads.
// Anti-join uses mmap'd sorted array (zero-copy) + prefetch-enhanced binary search.
//
// Pipeline:
//   1. mmap uom_code → binary search → USD row range
//   2. mmap ddate → binary search within USD range → 2023 range
//   3. mmap adsh, tagver, value + madvise WILLNEED on qualifying rows
//   4. mmap anti-join sorted array (zero-copy) + warm all pages
//   5. Parallel scan: prefetch-based binary search for anti-join probe
//   6. Merge maps, HAVING cnt > 10, ORDER BY cnt DESC LIMIT 100, decode strings

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

#include "timing_utils.h"

static const size_t TAG_COUNT  = 1070663;
static const size_t NUM_ROWS   = 39401761;

// RAII mmap helper (MADV_RANDOM — no automatic prefetch)
struct RawMmap {
    void*  ptr  = nullptr;
    size_t sz   = 0;
    int    fd   = -1;

    void open(const std::string& path) {
        close_all();
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st;
        if (fstat(fd, &st) < 0) { ::close(fd); fd = -1; throw std::runtime_error("fstat: " + path); }
        sz = (size_t)st.st_size;
        if (sz == 0) return;
        ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { ptr = nullptr; ::close(fd); fd = -1; throw std::runtime_error("mmap failed: " + path); }
        madvise(ptr, sz, MADV_RANDOM);
    }

    void close_all() {
        if (ptr && sz > 0) munmap(ptr, sz);
        if (fd >= 0) ::close(fd);
        ptr = nullptr; sz = 0; fd = -1;
    }

    ~RawMmap() { close_all(); }

    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }

    void willneed_rows(size_t row_lo, size_t row_hi, size_t elem_size) const {
        if (!ptr || row_hi <= row_lo) return;
        static const size_t page_sz = (size_t)sysconf(_SC_PAGESIZE);
        size_t byte_off = row_lo * elem_size;
        size_t byte_end = row_hi * elem_size;
        if (byte_end > sz) byte_end = sz;
        if (byte_off >= byte_end) return;
        size_t aligned_off = byte_off & ~(page_sz - 1);
        size_t aligned_len = byte_end - aligned_off;
        madvise((char*)ptr + aligned_off, aligned_len, MADV_WILLNEED);
    }

    void willneed_all() const {
        if (ptr && sz > 0) madvise(ptr, sz, MADV_WILLNEED);
    }
};

// Load usd_code from uom_codes.bin
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
        if (fread(buf, 1, slen, f) != slen) break;
        if (slen == 3 && memcmp(buf, "USD", 3) == 0) { fclose(f); return code; }
    }
    fclose(f);
    throw std::runtime_error("USD not found in uom_codes.bin");
}

// Prefetch-enhanced binary search for sorted uint64_t array
// Issues prefetch for next two children before branching → hides memory latency
// Returns true if key is found in [data, data+n)
inline bool anti_contains(const uint64_t* __restrict__ data, size_t n, uint64_t key) {
    size_t lo = 0, hi = n;
    // Prefetch-based descent: prefetch both children 2 levels ahead
    while (hi - lo > 16) {
        size_t mid = lo + ((hi - lo) >> 1);
        // Prefetch left and right children's midpoints (2 levels ahead)
        size_t lmid = lo + ((mid - lo) >> 1);
        size_t rmid = mid + ((hi - mid) >> 1);
        __builtin_prefetch(data + lmid, 0, 1);
        __builtin_prefetch(data + rmid, 0, 1);
        if (data[mid] < key)       { lo = mid + 1; }
        else if (data[mid] > key)  { hi = mid; }
        else                       { return true; }
    }
    // Linear scan for last few elements (fits in cache line)
    for (size_t i = lo; i < hi; i++) {
        if (data[i] == key) return true;
        if (data[i] > key)  return false;
    }
    return false;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE_MS("total", t_total);

    size_t r_2023_start = 0, r_2023_end = 0;
    RawMmap m_uom, m_ddate, m_adsh, m_tagver, m_value;
    RawMmap m_anti;
    size_t n_anti = 0;
    int8_t usd_code = 0;

    // -------------------------------------------------------------------------
    // Phase: data_loading
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    usd_code = load_usd_code(gendb_dir + "/indexes/uom_codes.bin");
    printf("[INFO] usd_code = %d\n", (int)usd_code);

    // Binary search for USD row range (num sorted by uom_code ASC, ddate ASC)
    m_uom.open(gendb_dir + "/num/uom_code.bin");
    const int8_t* uom_ptr = m_uom.as<int8_t>();
    size_t r_usd_start = (size_t)(std::lower_bound(uom_ptr, uom_ptr + NUM_ROWS, usd_code) - uom_ptr);
    size_t r_usd_end   = (size_t)(std::upper_bound(uom_ptr, uom_ptr + NUM_ROWS, usd_code) - uom_ptr);
    printf("[INFO] USD row range: [%zu, %zu) = %zu rows\n", r_usd_start, r_usd_end, r_usd_end - r_usd_start);

    // Binary search for 2023 ddate range within USD rows
    m_ddate.open(gendb_dir + "/num/ddate.bin");
    const int32_t* ddate_ptr = m_ddate.as<int32_t>();
    r_2023_start = (size_t)(std::lower_bound(ddate_ptr + r_usd_start, ddate_ptr + r_usd_end, (int32_t)20230101) - ddate_ptr);
    r_2023_end   = (size_t)(std::upper_bound(ddate_ptr + r_usd_start, ddate_ptr + r_usd_end, (int32_t)20231231) - ddate_ptr);
    printf("[INFO] USD-2023 row range: [%zu, %zu) = %zu rows\n", r_2023_start, r_2023_end, r_2023_end - r_2023_start);

    m_adsh.open(gendb_dir   + "/num/adsh_code.bin");
    m_tagver.open(gendb_dir + "/num/tagver_code.bin");
    m_value.open(gendb_dir  + "/num/value.bin");

    // Fire async I/O for qualifying row ranges
    m_uom.willneed_rows(r_2023_start, r_2023_end, sizeof(int8_t));
    m_ddate.willneed_rows(r_2023_start, r_2023_end, sizeof(int32_t));
    m_adsh.willneed_rows(r_2023_start, r_2023_end, sizeof(int32_t));
    m_tagver.willneed_rows(r_2023_start, r_2023_end, sizeof(int32_t));
    m_value.willneed_rows(r_2023_start, r_2023_end, sizeof(double));

    // mmap anti-join sorted array + warm all pages (async I/O for 66MB)
    m_anti.open(gendb_dir + "/indexes/pre_adsh_tagver_set.bin");
    n_anti = (size_t)(m_anti.as<uint64_t>()[0]);
    printf("[INFO] anti n_unique = %zu\n", n_anti);
    m_anti.willneed_all();

    } // data_loading

    // -------------------------------------------------------------------------
    // Phase: build_joins — pre-fault anti array pages with sequential scan
    // Touch all 66MB of anti data to bring them into page cache before parallel scan
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("build_joins");

    // Sequential scan to pre-fault all anti pages (eliminates page faults in parallel scan)
    const uint64_t* anti_data = m_anti.as<uint64_t>() + 1;
    volatile uint64_t chk = 0;
    // Touch every 512th element (= every 4KB page) to trigger page faults sequentially
    for (size_t k = 0; k < n_anti; k += 512) {
        chk ^= anti_data[k];
    }
    // Also touch last element
    chk ^= anti_data[n_anti - 1];
    (void)chk;

    } // build_joins

    // -------------------------------------------------------------------------
    // Phase: main_scan — parallel scan with prefetch-based binary search
    // -------------------------------------------------------------------------
    using GroupMap = std::unordered_map<int32_t, std::pair<int32_t, double>>;

    int n_threads = (int)std::thread::hardware_concurrency();
    if (n_threads < 1) n_threads = 1;
    printf("[INFO] n_threads = %d, qualifying rows = %zu\n", n_threads, r_2023_end - r_2023_start);

    std::vector<GroupMap> thread_maps(n_threads);
    for (auto& m : thread_maps) m.reserve(512);

    {
    GENDB_PHASE("main_scan");

    const uint64_t* anti_data  = m_anti.as<uint64_t>() + 1; // skip n_unique header
    const int32_t* adsh_col   = m_adsh.as<int32_t>();
    const int32_t* tagver_col = m_tagver.as<int32_t>();
    const double*  value_col  = m_value.as<double>();

    std::atomic<size_t> next_chunk(0);
    const size_t CHUNK = 8192; // smaller chunks → better cache reuse for anti hot-path

    auto worker = [&](int tid) {
        GroupMap& my_map = thread_maps[tid];

        while (true) {
            size_t chunk_idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
            size_t lo = r_2023_start + chunk_idx * CHUNK;
            if (lo >= r_2023_end) break;
            size_t hi = std::min(lo + CHUNK, r_2023_end);

            for (size_t i = lo; i < hi; i++) {
                // Prefetch next iteration's column data
                if (__builtin_expect(i + 8 < hi, 1)) {
                    __builtin_prefetch(&value_col[i + 8],  0, 1);
                    __builtin_prefetch(&tagver_col[i + 8], 0, 1);
                    __builtin_prefetch(&adsh_col[i + 8],   0, 1);
                }

                // value IS NOT NULL
                const double v = value_col[i];
                if (__builtin_expect(std::isnan(v), 0)) continue;

                const int32_t tc = tagver_col[i];
                // Guard unmappable tagver_codes
                if (__builtin_expect(tc < 0 || (uint32_t)tc >= (uint32_t)TAG_COUNT, 0)) continue;

                // Anti-join probe: prefetch-enhanced binary search in sorted array
                const uint64_t key = ((uint64_t)(uint32_t)adsh_col[i] << 32) | (uint32_t)tc;
                if (__builtin_expect(anti_contains(anti_data, n_anti, key), 1)) continue;

                auto& entry = my_map[tc];
                entry.first++;
                entry.second += v;
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(n_threads);
    for (int t = 0; t < n_threads; t++) threads.emplace_back(worker, t);
    for (auto& th : threads) th.join();

    } // main_scan

    // -------------------------------------------------------------------------
    // Phase: dim_filter — merge + HAVING + sort
    // -------------------------------------------------------------------------
    struct Group {
        int32_t tagver_code;
        int32_t cnt;
        double  total;
    };
    std::vector<Group> result;

    {
    GENDB_PHASE("dim_filter");

    GroupMap& merged = thread_maps[0];
    for (int t = 1; t < n_threads; t++) {
        for (const auto& kv : thread_maps[t]) {
            auto& entry = merged[kv.first];
            entry.first  += kv.second.first;
            entry.second += kv.second.second;
        }
        thread_maps[t].clear();
    }
    printf("[INFO] raw groups: %zu\n", merged.size());

    result.reserve(merged.size());
    for (const auto& kv : merged) {
        if (kv.second.first > 10) {
            result.push_back({kv.first, kv.second.first, kv.second.second});
        }
    }
    printf("[INFO] groups after HAVING cnt > 10: %zu\n", result.size());

    std::sort(result.begin(), result.end(), [](const Group& a, const Group& b) {
        return a.cnt > b.cnt;
    });
    if (result.size() > 100) result.resize(100);
    } // dim_filter

    // -------------------------------------------------------------------------
    // Phase: output — decode tag/version strings, write CSV
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("output");

    RawMmap m_tag_off, m_tag_data, m_ver_off, m_ver_data;
    m_tag_off.open(gendb_dir  + "/tag/tag_offsets.bin");
    m_tag_data.open(gendb_dir + "/tag/tag_data.bin");
    m_ver_off.open(gendb_dir  + "/tag/version_offsets.bin");
    m_ver_data.open(gendb_dir + "/tag/version_data.bin");

    const uint32_t* tag_offsets = m_tag_off.as<uint32_t>();
    const char*     tag_data    = m_tag_data.as<char>();
    const uint32_t* ver_offsets = m_ver_off.as<uint32_t>();
    const char*     ver_data    = m_ver_data.as<char>();

    {
        std::string mkdir_cmd = "mkdir -p " + results_dir;
        if (system(mkdir_cmd.c_str()) != 0)
            fprintf(stderr, "Warning: mkdir -p failed\n");
    }

    const std::string out_path = results_dir + "/Q24.csv";
    FILE* out = fopen(out_path.c_str(), "w");
    if (!out) throw std::runtime_error("Cannot open output file: " + out_path);

    fprintf(out, "tag,version,cnt,total\n");
    for (const auto& g : result) {
        const uint32_t tc = (uint32_t)g.tagver_code;
        const uint32_t tag_start = tag_offsets[tc];
        const uint32_t tag_end   = tag_offsets[tc + 1];
        const std::string tag_str(tag_data + tag_start, tag_end - tag_start);
        const uint32_t ver_start = ver_offsets[tc];
        const uint32_t ver_end   = ver_offsets[tc + 1];
        const std::string ver_str(ver_data + ver_start, ver_end - ver_start);

        fprintf(out, "%s,%s,%d,%.2f\n",
                tag_str.c_str(), ver_str.c_str(), g.cnt, g.total);
    }
    fclose(out);
    printf("[INFO] Written %zu rows to %s\n", result.size(), out_path.c_str());
    } // output

    return 0;
}
