// Q6: Direct pre index probing with software prefetching
// Packed uint64_t group keys, open-addressing agg maps

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <filesystem>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"
#include "mmap_utils.h"

static inline uint64_t hashKey3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= (uint64_t)c * 3266489917ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}

struct OffsetRange { uint64_t start, end; };

static std::vector<OffsetRange> loadOffsetTable(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    uint32_t n;
    (void)::read(fd, &n, 4);
    std::vector<OffsetRange> v(n);
    (void)::read(fd, v.data(), n * sizeof(OffsetRange));
    ::close(fd);
    return v;
}

struct Dict {
    const uint64_t* offsets;
    const char* data;
    size_t num_off;
    size_t off_file_size, data_file_size;
    int off_fd, data_fd;

    Dict() : offsets(nullptr), data(nullptr), num_off(0), off_fd(-1), data_fd(-1) {}

    void open(const std::string& off_path, const std::string& data_path) {
        struct stat st;
        off_fd = ::open(off_path.c_str(), O_RDONLY);
        fstat(off_fd, &st);
        off_file_size = st.st_size;
        num_off = off_file_size / sizeof(uint64_t);
        offsets = (const uint64_t*)mmap(nullptr, off_file_size, PROT_READ, MAP_PRIVATE, off_fd, 0);
        data_fd = ::open(data_path.c_str(), O_RDONLY);
        fstat(data_fd, &st);
        data_file_size = st.st_size;
        data = (const char*)mmap(nullptr, data_file_size, PROT_READ, MAP_PRIVATE, data_fd, 0);
    }

    std::string get(size_t i) const {
        return std::string(data + offsets[i], offsets[i + 1] - offsets[i]);
    }

    int findCode(const char* val, size_t vlen) const {
        for (size_t i = 0; i + 1 < num_off; i++) {
            size_t len = offsets[i + 1] - offsets[i];
            if (len == vlen && memcmp(data + offsets[i], val, len) == 0)
                return (int)i;
        }
        return -1;
    }

    void close_all() {
        if (offsets) munmap((void*)offsets, off_file_size);
        if (data) munmap((void*)data, data_file_size);
        if (off_fd >= 0) ::close(off_fd);
        if (data_fd >= 0) ::close(data_fd);
    }
    ~Dict() { close_all(); }
};

struct IndexEntry {
    uint32_t sub_fk;
    uint32_t tag_code;
    uint32_t version_code;
    uint32_t row_idx;
};

static inline uint64_t packKey(uint32_t nid, uint32_t tc, uint32_t pc) {
    return ((uint64_t)nid << 38) | ((uint64_t)tc << 20) | pc;
}

struct AggSlot {
    uint64_t key;
    double sum;
    uint64_t count;
};

static constexpr uint64_t EMPTY_KEY = UINT64_MAX;

struct OpenAggMap {
    AggSlot* slots;
    size_t capacity;
    size_t mask;
    size_t count;

    OpenAggMap() : slots(nullptr), capacity(0), mask(0), count(0) {}

    void init(size_t cap) {
        capacity = 16;
        while (capacity < cap) capacity <<= 1;
        mask = capacity - 1;
        slots = (AggSlot*)malloc(capacity * sizeof(AggSlot));
        for (size_t i = 0; i < capacity; i++) slots[i].key = EMPTY_KEY;
        count = 0;
    }

    ~OpenAggMap() { if (slots) free(slots); }

    static inline size_t hash_key(uint64_t k) {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return k;
    }

    inline void insert(uint64_t gk, double val) {
        size_t idx = hash_key(gk) & mask;
        while (true) {
            AggSlot& s = slots[idx];
            if (s.key == EMPTY_KEY) {
                s.key = gk;
                s.sum = val;
                s.count = 1;
                count++;
                if (count * 10 > capacity * 7) resize();
                return;
            }
            if (s.key == gk) {
                s.sum += val;
                s.count++;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    void resize() {
        size_t old_cap = capacity;
        AggSlot* old_slots = slots;
        capacity <<= 1;
        mask = capacity - 1;
        slots = (AggSlot*)malloc(capacity * sizeof(AggSlot));
        for (size_t i = 0; i < capacity; i++) slots[i].key = EMPTY_KEY;
        count = 0;
        for (size_t i = 0; i < old_cap; i++) {
            if (old_slots[i].key != EMPTY_KEY) {
                size_t idx = hash_key(old_slots[i].key) & mask;
                while (slots[idx].key != EMPTY_KEY) idx = (idx + 1) & mask;
                slots[idx] = old_slots[i];
                count++;
            }
        }
        free(old_slots);
    }
};

static void writeCSVField(FILE* f, const std::string& s) {
    bool needQuote = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { needQuote = true; break; }
    }
    if (needQuote) {
        fputc('"', f);
        for (char c : s) {
            if (c == '"') fputc('"', f);
            fputc(c, f);
        }
        fputc('"', f);
    } else {
        fwrite(s.data(), 1, s.size(), f);
    }
}

int main(int argc, char* argv[]) {
    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    const std::string num_dir = gendb_dir + "/num";
    const std::string sub_dir = gendb_dir + "/sub";
    const std::string pre_dir = gendb_dir + "/pre";
    const std::string dict_dir = gendb_dir + "/dicts";

    std::vector<uint32_t> sub_fk_to_name_id;
    std::vector<std::string> name_strings;
    uint64_t usd_start = 0, usd_end = 0;
    size_t sub_rows = 0;
    std::vector<uint8_t> fy2023_bits;
    uint8_t isCode = 0;

    uint64_t idx_num_buckets = 0;
    const uint64_t* idx_bucket_offsets = nullptr;
    const IndexEntry* idx_entries = nullptr;
    void* idx_mmap_base = nullptr;
    size_t idx_file_size = 0;
    int idx_fd = -1;

    const uint8_t* pre_stmt_code_data = nullptr;
    const uint32_t* pre_plabel_code_data = nullptr;
    void* stmt_mmap = nullptr; size_t stmt_mmap_size = 0; int stmt_fd_val = -1;
    void* plabel_mmap = nullptr; size_t plabel_mmap_size = 0; int plabel_fd_val = -1;

    Dict tag_dict, plabel_dict;

    GENDB_PHASE("total");

    // ======= DATA LOADING =======
    {
        GENDB_PHASE("data_loading");

        // 1. Filter sub by fy=2023
        {
            GENDB_PHASE("dim_filter");
            gendb::MmapColumn<int16_t> fy_col(sub_dir + "/fy.bin");
            sub_rows = fy_col.count;
            fy2023_bits.resize((sub_rows + 7) / 8, 0);
            for (size_t i = 0; i < sub_rows; i++) {
                if (fy_col[i] == 2023)
                    fy2023_bits[i >> 3] |= (1 << (i & 7));
            }
        }

        // 2. Build name_id map
        {
            gendb::MmapColumn<uint64_t> name_off_col(sub_dir + "/name_offsets.bin");
            struct stat st;
            int nfd = ::open((sub_dir + "/name_data.bin").c_str(), O_RDONLY);
            fstat(nfd, &st);
            const char* name_data = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, nfd, 0);

            std::unordered_map<std::string, uint32_t> name_to_id;
            name_to_id.reserve(sub_rows);
            sub_fk_to_name_id.resize(sub_rows);

            for (size_t i = 0; i < sub_rows; i++) {
                const char* ptr = name_data + name_off_col[i];
                size_t len = name_off_col[i + 1] - name_off_col[i];
                std::string nm(ptr, len);
                auto it = name_to_id.find(nm);
                if (it == name_to_id.end()) {
                    uint32_t id = (uint32_t)name_strings.size();
                    name_to_id[nm] = id;
                    name_strings.push_back(std::move(nm));
                    sub_fk_to_name_id[i] = id;
                } else {
                    sub_fk_to_name_id[i] = it->second;
                }
            }
            munmap((void*)name_data, st.st_size);
            ::close(nfd);
        }

        // 3. Find dict codes
        uint8_t usdCode = 0;
        {
            Dict uom_dict;
            uom_dict.open(num_dir + "/uom_dict_offsets.bin", num_dir + "/uom_dict_data.bin");
            int c = uom_dict.findCode("USD", 3);
            usdCode = (uint8_t)c;

            Dict stmt_dict;
            stmt_dict.open(pre_dir + "/stmt_dict_offsets.bin", pre_dir + "/stmt_dict_data.bin");
            c = stmt_dict.findCode("IS", 2);
            isCode = (uint8_t)c;
        }

        // 4. Mmap pre-built index + pre columns
        {
            GENDB_PHASE("build_joins");

            struct stat st;
            idx_fd = ::open((gendb_dir + "/indexes/pre_by_adsh_tag_ver.idx").c_str(), O_RDONLY);
            fstat(idx_fd, &st);
            idx_file_size = st.st_size;
            idx_mmap_base = mmap(nullptr, idx_file_size, PROT_READ, MAP_PRIVATE, idx_fd, 0);
            madvise(idx_mmap_base, idx_file_size, MADV_RANDOM);

            const char* base = (const char*)idx_mmap_base;
            idx_num_buckets = *(const uint64_t*)(base);
            idx_bucket_offsets = (const uint64_t*)(base + 16);
            idx_entries = (const IndexEntry*)(base + 16 + (idx_num_buckets + 1) * 8);

            stmt_fd_val = ::open((pre_dir + "/stmt_code.bin").c_str(), O_RDONLY);
            fstat(stmt_fd_val, &st);
            stmt_mmap_size = st.st_size;
            stmt_mmap = mmap(nullptr, stmt_mmap_size, PROT_READ, MAP_PRIVATE, stmt_fd_val, 0);
            pre_stmt_code_data = (const uint8_t*)stmt_mmap;

            plabel_fd_val = ::open((gendb_dir + "/column_versions/pre.plabel.dict/codes.bin").c_str(), O_RDONLY);
            fstat(plabel_fd_val, &st);
            plabel_mmap_size = st.st_size;
            plabel_mmap = mmap(nullptr, plabel_mmap_size, PROT_READ, MAP_PRIVATE, plabel_fd_val, 0);
            pre_plabel_code_data = (const uint32_t*)plabel_mmap;
        }

        // 5. Get USD range
        {
            auto uom_ranges = loadOffsetTable(num_dir + "/uom_offsets.bin");
            usd_start = uom_ranges[usdCode].start;
            usd_end = uom_ranges[usdCode].end;
        }

        // 6. Preload output dicts
        tag_dict.open(dict_dir + "/tag_dict_offsets.bin", dict_dir + "/tag_dict_data.bin");
        madvise((void*)tag_dict.offsets, tag_dict.off_file_size, MADV_WILLNEED);
        madvise((void*)tag_dict.data, tag_dict.data_file_size, MADV_WILLNEED);
        plabel_dict.open(
            gendb_dir + "/column_versions/pre.plabel.dict/dict.offsets",
            gendb_dir + "/column_versions/pre.plabel.dict/dict.data"
        );
        madvise((void*)plabel_dict.offsets, plabel_dict.off_file_size, MADV_WILLNEED);
        madvise((void*)plabel_dict.data, plabel_dict.data_file_size, MADV_WILLNEED);
    }

    // ======= MAIN SCAN =======
    int nthreads = omp_get_max_threads();

    {
        GENDB_PHASE("main_scan");

        gendb::MmapColumn<uint32_t> num_sfk(num_dir + "/sub_fk.bin");
        gendb::MmapColumn<uint32_t> num_tc(num_dir + "/tag_code.bin");
        gendb::MmapColumn<uint32_t> num_vc(num_dir + "/version_code.bin");
        gendb::MmapColumn<double> num_val(num_dir + "/value.bin");

        num_sfk.advise_sequential();
        num_tc.advise_sequential();
        num_vc.advise_sequential();
        num_val.advise_sequential();

        std::vector<OpenAggMap> thread_maps(nthreads);
        for (auto& m : thread_maps) m.init(8192);

        const uint8_t* fy_bits = fy2023_bits.data();
        const uint32_t* sfk_ptr = num_sfk.data;
        const uint32_t* tc_ptr = num_tc.data;
        const uint32_t* vc_ptr = num_vc.data;
        const double* val_ptr = num_val.data;
        const uint32_t* name_map = sub_fk_to_name_id.data();

        const uint64_t idx_mask = idx_num_buckets - 1;
        const uint64_t* boff = idx_bucket_offsets;
        const IndexEntry* ient = idx_entries;
        const uint8_t* stmt_codes = pre_stmt_code_data;
        const uint32_t* plabel_codes = pre_plabel_code_data;
        const uint8_t is_code = isCode;

        // Prefetch distance for hiding index latency
        constexpr int PREFETCH_DIST = 8;
        const size_t MORSEL = 100000;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            OpenAggMap& local_map = thread_maps[tid];

            #pragma omp for schedule(dynamic, 1) nowait
            for (size_t morsel_start = usd_start; morsel_start < usd_end; morsel_start += MORSEL) {
                size_t morsel_end = std::min(morsel_start + MORSEL, usd_end);

                // Process with software prefetching
                for (size_t i = morsel_start; i < morsel_end; i++) {
                    // Prefetch ahead: compute hash and prefetch bucket_offsets
                    if (i + PREFETCH_DIST < morsel_end) {
                        size_t pi = i + PREFETCH_DIST;
                        uint32_t psfk = sfk_ptr[pi];
                        if (fy_bits[psfk >> 3] & (1 << (psfk & 7))) {
                            uint64_t pbucket = hashKey3(psfk, tc_ptr[pi], vc_ptr[pi]) & idx_mask;
                            __builtin_prefetch(&boff[pbucket], 0, 0);
                        }
                    }

                    double val = val_ptr[i];
                    if (std::isnan(val)) continue;

                    uint32_t sfk = sfk_ptr[i];
                    if (!(fy_bits[sfk >> 3] & (1 << (sfk & 7)))) continue;

                    uint32_t tc = tc_ptr[i];
                    uint32_t vc = vc_ptr[i];

                    uint64_t bucket = hashKey3(sfk, tc, vc) & idx_mask;
                    uint64_t bStart = boff[bucket];
                    uint64_t bEnd = boff[bucket + 1];

                    for (uint64_t j = bStart; j < bEnd; j++) {
                        const IndexEntry& e = ient[j];
                        if (e.sub_fk == sfk && e.tag_code == tc && e.version_code == vc) {
                            if (stmt_codes[e.row_idx] != is_code) continue;
                            uint32_t nid = name_map[sfk];
                            uint32_t pc = plabel_codes[e.row_idx];
                            local_map.insert(packKey(nid, tc, pc), val);
                        }
                    }
                }
            }
        }

        // ======= OUTPUT =======
        {
            GENDB_PHASE("output");

            OpenAggMap& merged = thread_maps[0];
            for (int t = 1; t < nthreads; t++) {
                OpenAggMap& src = thread_maps[t];
                for (size_t i = 0; i < src.capacity; i++) {
                    if (src.slots[i].key != EMPTY_KEY) {
                        AggSlot& s = src.slots[i];
                        size_t idx = OpenAggMap::hash_key(s.key) & merged.mask;
                        while (true) {
                            AggSlot& ms = merged.slots[idx];
                            if (ms.key == EMPTY_KEY) {
                                ms = s;
                                merged.count++;
                                if (merged.count * 10 > merged.capacity * 7) merged.resize();
                                break;
                            }
                            if (ms.key == s.key) {
                                ms.sum += s.sum;
                                ms.count += s.count;
                                break;
                            }
                            idx = (idx + 1) & merged.mask;
                        }
                    }
                }
            }

            struct ResultRow {
                uint64_t key;
                double total_value;
                uint64_t cnt;
            };

            std::vector<ResultRow> results;
            results.reserve(merged.count);
            for (size_t i = 0; i < merged.capacity; i++) {
                if (merged.slots[i].key != EMPTY_KEY)
                    results.push_back({merged.slots[i].key, merged.slots[i].sum, merged.slots[i].count});
            }

            size_t limit = std::min((size_t)200, results.size());
            std::partial_sort(results.begin(), results.begin() + limit, results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    return a.total_value > b.total_value;
                });

            std::string outpath = results_dir + "/Q6.csv";
            FILE* fout = fopen(outpath.c_str(), "w");
            fprintf(fout, "name,stmt,tag,plabel,total_value,cnt\n");

            for (size_t i = 0; i < limit; i++) {
                const auto& r = results[i];
                uint32_t nid = (uint32_t)(r.key >> 38);
                uint32_t tc = (uint32_t)((r.key >> 20) & 0x3FFFF);
                uint32_t pc = (uint32_t)(r.key & 0xFFFFF);
                writeCSVField(fout, name_strings[nid]);
                fprintf(fout, ",IS,");
                writeCSVField(fout, tag_dict.get(tc));
                fputc(',', fout);
                writeCSVField(fout, plabel_dict.get(pc));
                fprintf(fout, ",%.2f,%lu\n", r.total_value, (unsigned long)r.cnt);
            }
            fclose(fout);
        }
    }

    // Cleanup
    if (idx_mmap_base) munmap(idx_mmap_base, idx_file_size);
    if (idx_fd >= 0) ::close(idx_fd);
    if (stmt_mmap) munmap(stmt_mmap, stmt_mmap_size);
    if (stmt_fd_val >= 0) ::close(stmt_fd_val);
    if (plabel_mmap) munmap(plabel_mmap, plabel_mmap_size);
    if (plabel_fd_val >= 0) ::close(plabel_fd_val);

    return 0;
}
