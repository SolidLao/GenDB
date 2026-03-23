#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"
#include "cli_params.h"

// ── Dictionary loader ──────────────────────────────────────────────
struct Dict {
    std::vector<std::string> entries;
    void load(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const char* base = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        uint32_t count = *(const uint32_t*)base;
        const uint32_t* offsets = (const uint32_t*)(base + 4);
        const char* strs = base + 4 + (count + 1) * 4;
        entries.resize(count);
        for (uint32_t i = 0; i < count; i++)
            entries[i] = std::string(strs + offsets[i], offsets[i+1] - offsets[i]);
        munmap((void*)base, st.st_size);
        ::close(fd);
    }
    uint16_t find_code(const std::string& val) const {
        for (uint16_t i = 0; i < (uint16_t)entries.size(); i++)
            if (entries[i] == val) return i;
        return 0xFFFF;
    }
};

// ── mmap helper ────────────────────────────────────────────────────
template<typename T>
struct Col {
    const T* data;
    size_t count;
    size_t fsize;
    int fd;
    Col() : data(nullptr), count(0), fsize(0), fd(-1) {}
    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        fsize = st.st_size;
        count = fsize / sizeof(T);
        data = (const T*)mmap(nullptr, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise((void*)data, fsize, MADV_SEQUENTIAL);
        madvise((void*)data, fsize, MADV_HUGEPAGE);
    }
    void close() {
        if (data) { munmap((void*)data, fsize); data = nullptr; }
        if (fd >= 0) { ::close(fd); fd = -1; }
    }
};

// ── Hash functions (must match build_indexes.cpp exactly) ──────────
inline uint64_t hash_u32(uint32_t a) {
    uint64_t x = a;
    x = (x ^ (x >> 16)) * 0x45d9f3b;
    x = (x ^ (x >> 16)) * 0x45d9f3b;
    x = x ^ (x >> 16);
    return x;
}
inline uint64_t hash_u32x2(uint32_t a, uint32_t b) {
    uint64_t x = (static_cast<uint64_t>(a) << 32) | b;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}
inline uint64_t hash_u32x3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = hash_u32x2(a, b);
    h ^= hash_u32(c) + 0x9e3779b97f4a7c15ULL + (h << 12) + (h >> 4);
    return h;
}

// ── Hash index (pre_join) ──────────────────────────────────────────
struct HashEntry {
    uint64_t key_hash;
    uint32_t value;
    uint32_t extra;
    uint8_t  occupied;
    uint8_t  pad[7];
};
static_assert(sizeof(HashEntry) == 24, "HashEntry must be 24 bytes");

struct HashIndex {
    const HashEntry* entries;
    uint32_t capacity;
    uint32_t mask;
    size_t fsize;
    int fd;

    HashIndex() : entries(nullptr), capacity(0), mask(0), fsize(0), fd(-1) {}

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        fsize = st.st_size;
        const char* base = (const char*)mmap(nullptr, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise((void*)base, fsize, MADV_HUGEPAGE);
        capacity = *(const uint32_t*)base;
        mask = capacity - 1;
        entries = (const HashEntry*)(base + 8);
    }

    // Returns true if key NOT found (anti-join qualifier)
    inline bool not_found(uint32_t adsh, uint32_t tag, uint32_t version) const {
        uint64_t h = hash_u32x3(adsh, tag, version);
        uint32_t slot = (uint32_t)h & mask;
        while (true) {
            const HashEntry& e = entries[slot];
            if (!e.occupied) return true;   // empty slot → not found
            if (e.key_hash == h) return false; // found → doesn't qualify
            slot = (slot + 1) & mask;
        }
    }

    void close() {
        if (entries) {
            munmap((void*)((const char*)entries - 8), fsize);
            entries = nullptr;
        }
        if (fd >= 0) { ::close(fd); fd = -1; }
    }
};

// ── Aggregation state ──────────────────────────────────────────────
struct AggState {
    int64_t count;
    double sum;
};

struct GroupResult {
    uint32_t tag_code;
    uint32_t version_code;
    int64_t cnt;
    double total;
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params...]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    // Parse parameters
    int64_t param_limit = gendb::parse_int_arg(argc, argv, "--limit", 100);
    std::string param_uom = gendb::parse_string_arg(argc, argv, "--uom_eq", "USD");
    int32_t param_ddate_lower = (int32_t)gendb::parse_int_arg(argc, argv, "--ddate_lower", 20230101);
    int32_t param_ddate_upper = (int32_t)gendb::parse_int_arg(argc, argv, "--ddate_upper", 20231231);

    // Declare mmap objects at outer scope (RAII timer scoping: munmap outside timer)
    Col<uint16_t> col_uom;
    Col<int32_t> col_ddate;
    Col<uint8_t> col_value_null;
    Col<uint32_t> col_adsh, col_tag, col_version;
    Col<double> col_value;
    HashIndex idx;
    Dict dict_uom, dict_tag, dict_version;

    {
    GENDB_PHASE("total");

    // ── Load dictionaries & resolve params ─────────────────────
    uint16_t usd_code;
    {
        GENDB_PHASE("data_loading");

        dict_uom.load(gendb_dir + "/dictionaries/uom.dict");
        dict_tag.load(gendb_dir + "/dictionaries/tag.dict");
        dict_version.load(gendb_dir + "/dictionaries/version.dict");

        usd_code = dict_uom.find_code(param_uom);
        if (usd_code == 0xFFFF) {
            fprintf(stderr, "UOM code not found for '%s'\n", param_uom.c_str());
            return 1;
        }

        // Load pre_join index
        idx.open(gendb_dir + "/indexes/pre_join.idx");

        // mmap num columns
        col_uom.open(gendb_dir + "/num/uom.bin");
        col_ddate.open(gendb_dir + "/num/ddate.bin");
        col_value_null.open(gendb_dir + "/num/value_null.bin");
        col_adsh.open(gendb_dir + "/num/adsh.bin");
        col_tag.open(gendb_dir + "/num/tag.bin");
        col_version.open(gendb_dir + "/num/version.bin");
        col_value.open(gendb_dir + "/num/value.bin");
    }

    const size_t N = col_uom.count;
    const int num_threads = omp_get_max_threads();

    // ── Parallel scan + filter + anti-join + aggregate ─────────
    std::vector<std::unordered_map<uint64_t, AggState>> thread_maps(num_threads);
    // Pre-reserve for expected ~20K groups
    for (auto& m : thread_maps) m.reserve(32768);

    {
        GENDB_PHASE("main_scan");

        const uint16_t* uom_data = col_uom.data;
        const int32_t* ddate_data = col_ddate.data;
        const uint8_t* vnull_data = col_value_null.data;
        const uint32_t* adsh_data = col_adsh.data;
        const uint32_t* tag_data = col_tag.data;
        const uint32_t* ver_data = col_version.data;
        const double* val_data = col_value.data;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local_map = thread_maps[tid];

            #pragma omp for schedule(dynamic, 100000)
            for (size_t i = 0; i < N; i++) {
                // Filter: uom == usd_code (most selective per byte)
                if (uom_data[i] != usd_code) continue;
                // Filter: value_null == 0
                if (vnull_data[i] != 0) continue;
                // Filter: ddate BETWEEN lower AND upper
                int32_t dd = ddate_data[i];
                if (dd < param_ddate_lower || dd > param_ddate_upper) continue;
                // Anti-join: NOT found in pre
                uint32_t a = adsh_data[i], t = tag_data[i], v = ver_data[i];
                if (!idx.not_found(a, t, v)) continue;
                // Aggregate
                uint64_t key = ((uint64_t)t << 32) | v;
                auto& agg = local_map[key];
                agg.count++;
                agg.sum += val_data[i];
            }
        }
    }

    // ── Merge thread-local maps ────────────────────────────────
    std::unordered_map<uint64_t, AggState> global_map;
    {
        GENDB_PHASE("aggregation");
        global_map.reserve(32768);
        for (auto& m : thread_maps) {
            for (auto& [key, agg] : m) {
                auto& g = global_map[key];
                g.count += agg.count;
                g.sum += agg.sum;
            }
        }
    }

    // ── HAVING + collect results ───────────────────────────────
    std::vector<GroupResult> results;
    {
        GENDB_PHASE("having_filter");
        results.reserve(global_map.size());
        for (auto& [key, agg] : global_map) {
            if (agg.count > 10) {
                uint32_t t = (uint32_t)(key >> 32);
                uint32_t v = (uint32_t)(key & 0xFFFFFFFF);
                results.push_back({t, v, agg.count, agg.sum});
            }
        }
    }

    // ── Top-K sort ─────────────────────────────────────────────
    {
        GENDB_PHASE("sort");
        size_t k = std::min((size_t)param_limit, results.size());
        if (k < results.size()) {
            std::partial_sort(results.begin(), results.begin() + k, results.end(),
                [](const GroupResult& a, const GroupResult& b) {
                    return a.cnt > b.cnt;
                });
            results.resize(k);
        } else {
            std::sort(results.begin(), results.end(),
                [](const GroupResult& a, const GroupResult& b) {
                    return a.cnt > b.cnt;
                });
        }
    }

    // ── Output ─────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q24.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        fprintf(fp, "tag,version,cnt,total\n");
        for (auto& r : results) {
            fprintf(fp, "%s,%s,%ld,%.2f\n",
                    dict_tag.entries[r.tag_code].c_str(),
                    dict_version.entries[r.version_code].c_str(),
                    (long)r.cnt, r.total);
        }
        fclose(fp);
    }

    } // end total timer

    // Cleanup outside timer (RAII scoping for TLB shootdown)
    col_uom.close();
    col_ddate.close();
    col_value_null.close();
    col_adsh.close();
    col_tag.close();
    col_version.close();
    col_value.close();
    idx.close();

    return 0;
}
