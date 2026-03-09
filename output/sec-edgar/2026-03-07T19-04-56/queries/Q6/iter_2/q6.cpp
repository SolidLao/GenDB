// Q6: Direct-indexed pre lookup by sub_fk
// Instead of probing 275MB index OR building a hash map, use a direct array
// indexed by sub_fk (max 86135). Each entry stores a list of (tag_code, version_code, plabel_code)
// from IS-filtered, fy2023-filtered pre rows. O(1) lookup, ~12 entries per sub_fk on average.
// Total structure: ~200K entries × 12 bytes = ~2.4MB, trivially fits in L2 cache.

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

    ~Dict() {
        if (offsets) munmap((void*)offsets, off_file_size);
        if (data) munmap((void*)data, data_file_size);
        if (off_fd >= 0) ::close(off_fd);
        if (data_fd >= 0) ::close(data_fd);
    }
};

// ---- Direct-indexed pre entry structure ----
struct PreEntry {
    uint32_t tag_code;
    uint32_t version_code;
    uint32_t plabel_code;
};

// ---- Aggregation Map ----
static inline uint64_t packAggKey(uint32_t name_id, uint32_t tag_code, uint32_t plabel_code) {
    return ((uint64_t)name_id << 38) | ((uint64_t)tag_code << 20) | (uint64_t)plabel_code;
}

struct AggSlot {
    uint64_t key;
    double sum;
    uint64_t count;
};

static constexpr uint64_t AGG_EMPTY = UINT64_MAX;

static inline size_t aggHash(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

struct OpenAggMap {
    AggSlot* slots;
    size_t capacity, mask, count;

    OpenAggMap() : slots(nullptr), capacity(0), mask(0), count(0) {}

    void init(size_t cap) {
        capacity = 16;
        while (capacity < cap) capacity <<= 1;
        mask = capacity - 1;
        slots = (AggSlot*)malloc(capacity * sizeof(AggSlot));
        for (size_t i = 0; i < capacity; i++) slots[i].key = AGG_EMPTY;
        count = 0;
    }

    ~OpenAggMap() { if (slots) free(slots); }

    inline void insert(uint64_t gk, double val) {
        size_t idx = aggHash(gk) & mask;
        while (true) {
            AggSlot& s = slots[idx];
            if (s.key == AGG_EMPTY) {
                s.key = gk; s.sum = val; s.count = 1;
                count++;
                if (count * 10 > capacity * 7) resize();
                return;
            }
            if (s.key == gk) {
                s.sum += val; s.count++; return;
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
        for (size_t i = 0; i < capacity; i++) slots[i].key = AGG_EMPTY;
        count = 0;
        for (size_t i = 0; i < old_cap; i++) {
            if (old_slots[i].key != AGG_EMPTY) {
                size_t idx = aggHash(old_slots[i].key) & mask;
                while (slots[idx].key != AGG_EMPTY) idx = (idx + 1) & mask;
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
        for (char c : s) { if (c == '"') fputc('"', f); fputc(c, f); }
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
    uint8_t usdCode = 0;

    // Direct-indexed pre lookup: pre_index[sub_fk] = start index into pre_entries
    // pre_counts[sub_fk] = number of entries for this sub_fk
    std::vector<uint32_t> pre_offsets;  // prefix sum, size sub_rows+1
    std::vector<PreEntry> pre_entries;  // all entries sorted by sub_fk

    Dict tag_dict, plabel_dict;

    GENDB_PHASE("total");

    // ======= DATA LOADING + BUILD =======
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

        // 2. Build name_id map (deduplicate sub names)
        {
            gendb::MmapColumn<uint64_t> name_off_col(sub_dir + "/name_offsets.bin");
            struct stat st;
            int nfd = ::open((sub_dir + "/name_data.bin").c_str(), O_RDONLY);
            fstat(nfd, &st);
            const char* name_data = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, nfd, 0);
            size_t name_data_size = st.st_size;

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
            munmap((void*)name_data, name_data_size);
            ::close(nfd);
        }

        // 3. Find dict codes
        uint8_t isCode = 0;
        {
            Dict uom_dict;
            uom_dict.open(num_dir + "/uom_dict_offsets.bin", num_dir + "/uom_dict_data.bin");
            usdCode = (uint8_t)uom_dict.findCode("USD", 3);

            Dict stmt_dict;
            stmt_dict.open(pre_dir + "/stmt_dict_offsets.bin", pre_dir + "/stmt_dict_data.bin");
            isCode = (uint8_t)stmt_dict.findCode("IS", 2);
        }

        // 4. Build direct-indexed pre lookup from IS-filtered, fy2023-filtered pre rows
        {
            GENDB_PHASE("build_joins");

            // Get IS row range via stmt_offsets
            auto stmt_ranges = loadOffsetTable(pre_dir + "/stmt_offsets.bin");
            uint64_t is_start = stmt_ranges[isCode].start;
            uint64_t is_end = stmt_ranges[isCode].end;

            // Mmap pre columns
            gendb::MmapColumn<uint32_t> pre_sfk(pre_dir + "/sub_fk.bin");
            gendb::MmapColumn<uint32_t> pre_tc(pre_dir + "/tag_code.bin");
            gendb::MmapColumn<uint32_t> pre_vc(pre_dir + "/version_code.bin");
            gendb::MmapColumn<uint32_t> pre_plabel(gendb_dir + "/column_versions/pre.plabel.dict/codes.bin");

            const uint32_t* psfk = pre_sfk.data;
            const uint32_t* ptc = pre_tc.data;
            const uint32_t* pvc = pre_vc.data;
            const uint32_t* ppc = pre_plabel.data;
            const uint8_t* fy_bits = fy2023_bits.data();

            // Count entries per sub_fk
            std::vector<uint32_t> counts(sub_rows, 0);
            for (uint64_t r = is_start; r < is_end; r++) {
                uint32_t sfk = psfk[r];
                if (fy_bits[sfk >> 3] & (1 << (sfk & 7)))
                    counts[sfk]++;
            }

            // Build prefix sum
            pre_offsets.resize(sub_rows + 1);
            pre_offsets[0] = 0;
            for (size_t i = 0; i < sub_rows; i++)
                pre_offsets[i + 1] = pre_offsets[i] + counts[i];
            uint32_t total_entries = pre_offsets[sub_rows];

            // Fill entries
            pre_entries.resize(total_entries);
            std::vector<uint32_t> pos(sub_rows, 0);  // current insertion position per sub_fk

            for (uint64_t r = is_start; r < is_end; r++) {
                uint32_t sfk = psfk[r];
                if (!(fy_bits[sfk >> 3] & (1 << (sfk & 7)))) continue;
                uint32_t idx = pre_offsets[sfk] + pos[sfk]++;
                pre_entries[idx] = {ptc[r], pvc[r], ppc[r]};
            }
        }

        // 5. Get USD range via uom_offsets
        {
            auto uom_ranges = loadOffsetTable(num_dir + "/uom_offsets.bin");
            usd_start = uom_ranges[usdCode].start;
            usd_end = uom_ranges[usdCode].end;
        }

        // 6. Preload output dicts
        tag_dict.open(dict_dir + "/tag_dict_offsets.bin", dict_dir + "/tag_dict_data.bin");
        plabel_dict.open(
            gendb_dir + "/column_versions/pre.plabel.dict/dict.offsets",
            gendb_dir + "/column_versions/pre.plabel.dict/dict.data"
        );
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
        const uint32_t* pre_off = pre_offsets.data();
        const PreEntry* pre_ent = pre_entries.data();

        const size_t MORSEL = 100000;

        // Thread-local compact entry lists for fast merge
        std::vector<std::vector<AggSlot>> thread_entries(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            OpenAggMap& local_map = thread_maps[tid];

            #pragma omp for schedule(dynamic, 1) nowait
            for (size_t morsel_start = usd_start; morsel_start < usd_end; morsel_start += MORSEL) {
                size_t morsel_end = std::min(morsel_start + MORSEL, usd_end);

                for (size_t i = morsel_start; i < morsel_end; i++) {
                    double val = val_ptr[i];
                    if (std::isnan(val)) continue;

                    uint32_t sfk = sfk_ptr[i];
                    if (!(fy_bits[sfk >> 3] & (1 << (sfk & 7)))) continue;

                    uint32_t tc = tc_ptr[i];
                    uint32_t vc = vc_ptr[i];
                    uint32_t nid = name_map[sfk];

                    // Direct lookup: O(1) to find entries for this sub_fk
                    uint32_t pstart = pre_off[sfk];
                    uint32_t pend = pre_off[sfk + 1];

                    for (uint32_t p = pstart; p < pend; p++) {
                        const PreEntry& pe = pre_ent[p];
                        if (pe.tag_code == tc && pe.version_code == vc) {
                            local_map.insert(packAggKey(nid, tc, pe.plabel_code), val);
                        }
                    }
                }
            }

            // Extract compact entries in parallel (avoids scanning empty slots sequentially)
            auto& entries = thread_entries[tid];
            entries.reserve(local_map.count);
            for (size_t i = 0; i < local_map.capacity; i++) {
                if (local_map.slots[i].key != AGG_EMPTY)
                    entries.push_back(local_map.slots[i]);
            }
        }

        // ======= OUTPUT =======
        {
            GENDB_PHASE("output");

            // Merge compact entry lists into thread 0's map
            OpenAggMap& merged = thread_maps[0];
            for (int t = 1; t < nthreads; t++) {
                for (const auto& s : thread_entries[t]) {
                    size_t idx = aggHash(s.key) & merged.mask;
                    while (true) {
                        AggSlot& ms = merged.slots[idx];
                        if (ms.key == AGG_EMPTY) {
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

            struct ResultRow {
                uint64_t key;
                double total_value;
                uint64_t cnt;
            };

            // Extract results from merged map using compact entries
            std::vector<ResultRow> results;
            size_t total_unique = merged.count;
            results.reserve(total_unique);
            for (size_t i = 0; i < merged.capacity; i++) {
                if (merged.slots[i].key != AGG_EMPTY)
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

    return 0;
}
