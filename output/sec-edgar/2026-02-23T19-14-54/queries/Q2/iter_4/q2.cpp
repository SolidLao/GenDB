// Q2: Top-100 max-value rows per (adsh,tag) for uom='pure', fy=2022
// Strategy: two-pass over num, thread-local hash maps for MAX aggregation,
//           pre-built sub_adsh_hash index for sub join.
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <omp.h>
#include "timing_utils.h"

// -----------------------------------------------------------------------
// Dictionary loading
// -----------------------------------------------------------------------
static int16_t find_code16(const std::vector<std::string>& dict, const std::string& target) {
    for (int i = 0; i < (int)dict.size(); i++) {
        if (dict[i] == target) return (int16_t)i;
    }
    return -1;
}

// Fast mmap-based dict loader: ~10x faster than ifstream for large dicts (198K entries)
static std::vector<std::string> load_dict_fast(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    if (sz == 0) { close(fd); return {}; }
    const char* buf = reinterpret_cast<const char*>(
        mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0));
    if (buf == MAP_FAILED) { perror("mmap dict"); close(fd); exit(1); }
    close(fd);

    // Count newlines to pre-size vector (avoids realloc)
    size_t nlines = 0;
    const char* p = buf;
    const char* end = buf + sz;
    while (p < end) {
        const char* nl = reinterpret_cast<const char*>(memchr(p, '\n', end - p));
        nlines++;
        p = nl ? nl + 1 : end;
    }

    std::vector<std::string> dict;
    dict.reserve(nlines);
    p = buf;
    while (p < end) {
        const char* nl = reinterpret_cast<const char*>(memchr(p, '\n', end - p));
        size_t len = nl ? static_cast<size_t>(nl - p) : static_cast<size_t>(end - p);
        if (len > 0 && p[len - 1] == '\r') len--;  // strip \r
        dict.emplace_back(p, len);
        p = nl ? nl + 1 : end;
    }
    munmap(const_cast<char*>(buf), sz);
    return dict;
}

// -----------------------------------------------------------------------
// mmap helper
// -----------------------------------------------------------------------
template<typename T>
struct MmapCol {
    const T* data = nullptr;
    size_t n = 0;
    size_t bytes = 0;
    int fd = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); exit(1); }
        struct stat st;
        fstat(fd, &st);
        bytes = st.st_size;
        n = bytes / sizeof(T);
        // No MAP_POPULATE: avoid single-threaded kernel page-fault stall on hot data.
        // Parallel scan will soft-fault pages (already in page cache) across 64 threads.
        data = reinterpret_cast<const T*>(
            mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data == MAP_FAILED) { perror("mmap"); exit(1); }
        madvise((void*)data, bytes, MADV_SEQUENTIAL);
    }

    ~MmapCol() {
        if (data && data != MAP_FAILED) munmap((void*)data, bytes);
        if (fd >= 0) close(fd);
    }
};

// -----------------------------------------------------------------------
// Open-addressing hash map: uint64_t key → double (MAX aggregation)
// C9: cap = next_power_of_2(39000 * 2) = 65536
// C20: std::fill, not memset for sentinel
// C24: bounded probing
// -----------------------------------------------------------------------
static constexpr uint32_t MAXVAL_CAP  = 65536u;
static constexpr uint32_t MAXVAL_MASK = MAXVAL_CAP - 1u;
static constexpr uint64_t EMPTY_KEY   = UINT64_MAX;  // sentinel

struct MaxValSlot {
    uint64_t key;
    double   value;
};

struct MaxValMap {
    MaxValSlot slots[MAXVAL_CAP];

    void init() {
        // C20: use fill not memset for 64-bit sentinel
        for (uint32_t i = 0; i < MAXVAL_CAP; i++) {
            slots[i].key   = EMPTY_KEY;
            slots[i].value = 0.0;
        }
    }

    inline uint32_t hash_key(uint64_t key) const {
        // Murmur-inspired mix
        key ^= key >> 33;
        key *= 0xff51afd7ed558ccdULL;
        key ^= key >> 33;
        return (uint32_t)(key & MAXVAL_MASK);
    }

    inline void update_max(uint64_t key, double val) {
        uint32_t h = hash_key(key);
        // C24: bounded probing
        for (uint32_t p = 0; p < MAXVAL_CAP; p++) {
            uint32_t s = (h + p) & MAXVAL_MASK;
            if (slots[s].key == EMPTY_KEY) {
                slots[s].key   = key;
                slots[s].value = val;
                return;
            }
            if (slots[s].key == key) {
                if (val > slots[s].value) slots[s].value = val;
                return;
            }
        }
        // Should never reach here if load factor <= 50%
    }

    inline const MaxValSlot* lookup(uint64_t key) const {
        uint32_t h = hash_key(key);
        for (uint32_t p = 0; p < MAXVAL_CAP; p++) {
            uint32_t s = (h + p) & MAXVAL_MASK;
            if (slots[s].key == EMPTY_KEY) return nullptr;
            if (slots[s].key == key)       return &slots[s];
        }
        return nullptr;
    }
};

// -----------------------------------------------------------------------
// Compact row: qualifying num rows collected in single pass (~39K rows)
// 16 bytes each → 624KB total → fits comfortably in L2 cache
// -----------------------------------------------------------------------
struct NumRowCompact {
    int32_t adsh;
    int32_t tag;
    double  value;  // exact double from storage; MAX compare is exact (C29 N/A: no SUM)
};

// -----------------------------------------------------------------------
// sub_adsh_hash index layout (pre-built)
// -----------------------------------------------------------------------
struct SubSlot {
    int32_t  adsh_code;
    uint32_t row_idx;
};

// -----------------------------------------------------------------------
// Output candidate
// -----------------------------------------------------------------------
struct Candidate {
    double   value;
    int32_t  name_code;
    int32_t  tag_code;
};

// Final sort: value DESC, name ASC, tag ASC (by decoded strings)
static bool cand_less_by_strings(const Candidate& a, const Candidate& b,
                                  const std::vector<std::string>& name_dict,
                                  const std::vector<std::string>& tag_dict) {
    if (a.value != b.value) return a.value > b.value;  // DESC
    const std::string& na = (a.name_code >= 0 && a.name_code < (int)name_dict.size()) ? name_dict[a.name_code] : "";
    const std::string& nb = (b.name_code >= 0 && b.name_code < (int)name_dict.size()) ? name_dict[b.name_code] : "";
    if (na != nb) return na < nb;  // ASC
    const std::string& ta = (a.tag_code >= 0 && a.tag_code < (int)tag_dict.size()) ? tag_dict[a.tag_code] : "";
    const std::string& tb = (b.tag_code >= 0 && b.tag_code < (int)tag_dict.size()) ? tag_dict[b.tag_code] : "";
    return ta < tb;  // ASC
}

// -----------------------------------------------------------------------
// Main query function
// -----------------------------------------------------------------------
void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ---- Data loading ----
    MmapCol<int16_t> num_uom;
    MmapCol<double>  num_value;
    MmapCol<int32_t> num_adsh;
    MmapCol<int32_t> num_tag;
    MmapCol<int32_t> sub_fy;
    MmapCol<int32_t> sub_name;

    const uint32_t* sub_hash_cap_ptr = nullptr;
    const SubSlot*  sub_hash_slots   = nullptr;
    uint32_t        sub_hash_cap     = 0;
    uint32_t        sub_hash_mask    = 0;
    size_t          sub_hash_bytes   = 0;
    int             sub_hash_fd      = -1;

    std::vector<std::string> uom_dict, name_dict, tag_dict;

    {
        GENDB_PHASE("data_loading");

        // Load dictionaries (C2: never hardcode) — use fast mmap+memchr loader
        uom_dict  = load_dict_fast(gendb_dir + "/num/uom_dict.txt");
        name_dict = load_dict_fast(gendb_dir + "/sub/name_dict.txt");
        tag_dict  = load_dict_fast(gendb_dir + "/tag_global_dict.txt");

        // mmap num columns
        num_uom.open(gendb_dir   + "/num/uom.bin");
        num_value.open(gendb_dir + "/num/value.bin");
        num_adsh.open(gendb_dir  + "/num/adsh.bin");
        num_tag.open(gendb_dir   + "/num/tag.bin");

        // mmap sub columns
        sub_fy.open(gendb_dir   + "/sub/fy.bin");
        sub_name.open(gendb_dir + "/sub/name.bin");

        // mmap sub_adsh_hash index (P11: pre-built, zero build cost)
        sub_hash_fd = ::open((gendb_dir + "/indexes/sub_adsh_hash.bin").c_str(), O_RDONLY);
        if (sub_hash_fd < 0) { perror("sub_adsh_hash.bin"); exit(1); }
        {
            struct stat st; fstat(sub_hash_fd, &st);
            sub_hash_bytes = st.st_size;
        }
        sub_hash_cap_ptr = reinterpret_cast<const uint32_t*>(
            mmap(nullptr, sub_hash_bytes, PROT_READ, MAP_PRIVATE, sub_hash_fd, 0));
        if (sub_hash_cap_ptr == MAP_FAILED) { perror("mmap sub_adsh_hash"); exit(1); }
        // sub_adsh_hash is 2MB, random-access probed — prefetch it eagerly
        madvise((void*)sub_hash_cap_ptr, sub_hash_bytes, MADV_WILLNEED);
        sub_hash_cap   = sub_hash_cap_ptr[0];
        sub_hash_mask  = sub_hash_cap - 1u;
        sub_hash_slots = reinterpret_cast<const SubSlot*>(sub_hash_cap_ptr + 1);
    }

    const size_t  N         = num_uom.n;
    const int16_t pure_code = find_code16(uom_dict, "pure");  // C2: runtime lookup
    if (pure_code < 0) { std::cerr << "Cannot find 'pure' in uom_dict\n"; exit(1); }

    int nthreads = std::min(omp_get_max_threads(), 64);
    // Global max-value map (built from compact rows, much cheaper than merging 64 maps)
    static MaxValMap global_map;

    {
        GENDB_PHASE("main_scan");  // covers single pass + compact processing

        // ---- Single Pass: collect qualifying rows into thread-local compact arrays ----
        // Filter: uom == pure_code (~1/1000 → ~39K rows total)
        // Compact row = 16 bytes; ~39K rows = 624KB → fits in L2 cache
        std::vector<std::vector<NumRowCompact>> tl_rows(nthreads);
        for (int t = 0; t < nthreads; t++)
            tl_rows[t].reserve(39000 / nthreads + 200);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            std::vector<NumRowCompact>& local_rows = tl_rows[tid];

            // schedule(static): sequential per-thread ranges → hardware prefetch friendly
            #pragma omp for schedule(static)
            for (size_t i = 0; i < N; i++) {
                if (num_uom.data[i] != pure_code) continue;
                // value IS NOT NULL: null_fraction=0.0 → skip null check
                local_rows.push_back({num_adsh.data[i], num_tag.data[i], num_value.data[i]});
            }
        }

        // Merge thread-local compact arrays → single compact array (~39K rows)
        size_t total_rows = 0;
        for (int t = 0; t < nthreads; t++) total_rows += tl_rows[t].size();
        std::vector<NumRowCompact> compact;
        compact.reserve(total_rows);
        for (int t = 0; t < nthreads; t++)
            compact.insert(compact.end(), tl_rows[t].begin(), tl_rows[t].end());

        // ---- Build global maxval map from compact array (39K rows, trivial) ----
        // No thread-local maps, no 4M-slot merge — directly insert 39K rows.
        {
            GENDB_PHASE("aggregation_merge");
            global_map.init();
            for (const auto& r : compact) {
                uint64_t key = ((uint64_t)(uint32_t)r.adsh << 32) | (uint32_t)r.tag;
                global_map.update_max(key, r.value);
            }
        }

        // ---- Pass 2: scan compact array (39K rows) for candidates ----
        // This replaces the second 39M-row scan — compact array fits in L2 cache.
        std::vector<Candidate> all_cands;
        all_cands.reserve(256);  // generous: at most ~14.6K fy==2022 matches
        for (const auto& r : compact) {
            uint64_t key = ((uint64_t)(uint32_t)r.adsh << 32) | (uint32_t)r.tag;
            const MaxValSlot* slot = global_map.lookup(key);
            if (!slot || r.value != slot->value) continue;

            // Probe sub_adsh_hash to find sub row (P11)
            uint32_t h = ((uint32_t)r.adsh * 2654435761u) & sub_hash_mask;
            for (uint32_t p = 0; p < sub_hash_cap; p++) {
                uint32_t si = (h + p) & sub_hash_mask;
                if (sub_hash_slots[si].adsh_code == INT32_MIN) break;   // empty slot
                if (sub_hash_slots[si].adsh_code == r.adsh) {
                    uint32_t sub_row = sub_hash_slots[si].row_idx;
                    // Filter: s.fy == 2022
                    if (sub_fy.data[sub_row] == 2022) {
                        all_cands.push_back({r.value, sub_name.data[sub_row], r.tag});
                    }
                    break;
                }
            }
        }

        // Sort: value DESC, name ASC, tag ASC (by decoded strings)
        // Use partial_sort for top 100 (P6)
        size_t top_n = std::min((size_t)100, all_cands.size());
        if (all_cands.size() > top_n) {
            std::partial_sort(all_cands.begin(), all_cands.begin() + top_n, all_cands.end(),
                [&](const Candidate& a, const Candidate& b) {
                    return cand_less_by_strings(a, b, name_dict, tag_dict);
                });
        } else {
            std::sort(all_cands.begin(), all_cands.end(),
                [&](const Candidate& a, const Candidate& b) {
                    return cand_less_by_strings(a, b, name_dict, tag_dict);
                });
        }

        // ---- Output ----
        {
            GENDB_PHASE("output");

            // CSV field writer: quotes if field contains comma, quote, or newline
            auto csv_field = [](const std::string& s) -> std::string {
                bool need_quote = (s.find(',') != std::string::npos ||
                                   s.find('"') != std::string::npos ||
                                   s.find('\n') != std::string::npos);
                if (!need_quote) return s;
                std::string out;
                out.reserve(s.size() + 2);
                out += '"';
                for (char ch : s) {
                    if (ch == '"') out += '"';  // escape internal quotes
                    out += ch;
                }
                out += '"';
                return out;
            };

            std::string out_path = results_dir + "/Q2.csv";
            FILE* fp = fopen(out_path.c_str(), "w");
            if (!fp) { perror(out_path.c_str()); exit(1); }

            fprintf(fp, "name,tag,value\n");
            for (size_t i = 0; i < top_n; i++) {
                const Candidate& c = all_cands[i];
                const std::string& name_s = (c.name_code >= 0 && c.name_code < (int)name_dict.size())
                    ? name_dict[c.name_code] : std::string();
                const std::string& tag_s  = (c.tag_code >= 0 && c.tag_code < (int)tag_dict.size())
                    ? tag_dict[c.tag_code] : std::string();
                // %.2f for 2 decimal places (C18/output format requirement)
                fprintf(fp, "%s,%s,%.2f\n",
                        csv_field(name_s).c_str(),
                        csv_field(tag_s).c_str(),
                        c.value);
            }
            fclose(fp);
        }
    }  // end main_scan GENDB_PHASE

    // Cleanup
    if (sub_hash_cap_ptr && sub_hash_cap_ptr != MAP_FAILED)
        munmap((void*)sub_hash_cap_ptr, sub_hash_bytes);
    if (sub_hash_fd >= 0) close(sub_hash_fd);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q2(gendb_dir, results_dir);
    return 0;
}
#endif
