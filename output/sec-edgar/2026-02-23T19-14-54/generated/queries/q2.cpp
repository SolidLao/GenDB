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
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <omp.h>
#include "timing_utils.h"

namespace {

// -----------------------------------------------------------------------
// Dictionary loading
// -----------------------------------------------------------------------
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    if (!f) { std::cerr << "Cannot open dict: " << path << "\n"; exit(1); }
    std::string line;
    while (std::getline(f, line)) {
        // strip trailing \r if any
        if (!line.empty() && line.back() == '\r') line.pop_back();
        dict.push_back(line);
    }
    return dict;
}

static int16_t find_code16(const std::vector<std::string>& dict, const std::string& target) {
    for (int i = 0; i < (int)dict.size(); i++) {
        if (dict[i] == target) return (int16_t)i;
    }
    return -1;
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
        posix_fadvise(fd, 0, bytes, POSIX_FADV_SEQUENTIAL);
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
} // end anonymous namespace

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

        // Load dictionaries (C2: never hardcode)
        uom_dict  = load_dict(gendb_dir + "/num/uom_dict.txt");
        name_dict = load_dict(gendb_dir + "/sub/name_dict.txt");
        tag_dict  = load_dict(gendb_dir + "/tag_global_dict.txt");

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

    // ---- Pass 1: Build thread-local MAX(value) maps per (adsh_code, tag_code) ----
    // Global merged map (sequential merge after parallel pass)
    static MaxValMap global_map;

    int nthreads = std::min(omp_get_max_threads(), 64);

    // P22: Use static storage for thread-local maps to avoid repeated mmap/munmap
    // page-fault overhead. glibc uses mmap for >128KB allocs — delete/new each call
    // causes 16K anonymous page faults per map (~63ms total). Static pages persist
    // across calls: hot-run init() is pure write bandwidth (~1ms parallel).
    static MaxValMap tl_maps_storage[64];

    {
        GENDB_PHASE("main_scan");  // covers both passes

        // Pass 1: parallel scan — init maps AND scan in same parallel region.
        // Parallel init: each thread initializes its own 1MB map (NUMA first-touch,
        // distributes page faults across 64 cores vs single-threaded 65ms stall).
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            MaxValMap* local_map = &tl_maps_storage[tid];
            local_map->init();  // parallel first-touch: 1MB per thread simultaneously

            // schedule(static): sequential per-thread ranges → better prefetch than dynamic
            #pragma omp for schedule(static)
            for (size_t i = 0; i < N; i++) {
                if (num_uom.data[i] != pure_code) continue;
                // value IS NOT NULL: null_fraction=0.0 → skip null check
                int32_t  ac  = num_adsh.data[i];
                int32_t  tc  = num_tag.data[i];
                double   val = num_value.data[i];
                // C15: key includes BOTH adsh_code AND tag_code
                uint64_t key = ((uint64_t)(uint32_t)ac << 32) | (uint32_t)tc;
                local_map->update_max(key, val);
            }
        }

        // Sequential merge into global map.
        // After parallel pass, each thread's map is in L3 (recently written/read).
        // 64 maps × 65536 slots = 4M iterations; global_map (1MB) fits in L3.
        {
            GENDB_PHASE("aggregation_merge");
            global_map.init();  // reset global map for this call
            for (int t = 0; t < nthreads; t++) {
                const MaxValMap* lm = &tl_maps_storage[t];
                for (uint32_t s = 0; s < MAXVAL_CAP; s++) {
                    if (lm->slots[s].key == EMPTY_KEY) continue;
                    global_map.update_max(lm->slots[s].key, lm->slots[s].value);
                }
            }
        }

        // Pass 2: probe global map + sub_adsh_hash, collect candidates
        // Thread-local candidate vectors
        std::vector<std::vector<Candidate>> tl_cands(nthreads);

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            std::vector<Candidate>& local_cands = tl_cands[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < N; i++) {
                if (num_uom.data[i] != pure_code) continue;
                int32_t  ac  = num_adsh.data[i];
                int32_t  tc  = num_tag.data[i];
                double   val = num_value.data[i];
                uint64_t key = ((uint64_t)(uint32_t)ac << 32) | (uint32_t)tc;

                // Check if this row is the max for its (adsh, tag) group
                const MaxValSlot* slot = global_map.lookup(key);
                if (!slot || val != slot->value) continue;

                // Probe sub_adsh_hash to find sub row (P11)
                uint32_t h = ((uint32_t)ac * 2654435761u) & sub_hash_mask;
                for (uint32_t p = 0; p < sub_hash_cap; p++) {
                    uint32_t si = (h + p) & sub_hash_mask;
                    if (sub_hash_slots[si].adsh_code == INT32_MIN) break;   // empty slot
                    if (sub_hash_slots[si].adsh_code == ac) {
                        uint32_t sub_row = sub_hash_slots[si].row_idx;
                        // Filter: s.fy == 2022
                        if (sub_fy.data[sub_row] == 2022) {
                            local_cands.push_back({val, sub_name.data[sub_row], tc});
                        }
                        break;
                    }
                }
            }
        }

        // Merge candidates from all threads
        std::vector<Candidate> all_cands;
        for (int t = 0; t < nthreads; t++) {
            for (auto& c : tl_cands[t]) {
                all_cands.push_back(c);
        }   }

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
