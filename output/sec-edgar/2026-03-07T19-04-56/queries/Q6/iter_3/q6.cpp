// Q6: Direct-indexed pre lookup with parallel reduction merge
// Key optimization: merge thread-local agg maps using parallel reduction tree
// instead of sequential merge into single map.

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

struct PreEntry {
    uint32_t tag_code;
    uint32_t version_code;
    uint32_t plabel_code;
};

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

    // Merge another slot into this map (for reduction)
    inline void merge_slot(const AggSlot& s) {
        size_t idx = aggHash(s.key) & mask;
        while (true) {
            AggSlot& ms = slots[idx];
            if (ms.key == AGG_EMPTY) {
                ms = s;
                count++;
                if (count * 10 > capacity * 7) resize();
                return;
            }
            if (ms.key == s.key) {
                ms.sum += s.sum;
                ms.count += s.count;
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

    std::vector<uint32_t> pre_offsets;
    std::vector<PreEntry> pre_entries;

    Dict tag_dict, plabel_dict;

    // Pre-open num columns
    gendb::MmapColumn<uint32_t> num_sfk(num_dir + "/sub_fk.bin");
    gendb::MmapColumn<uint32_t> num_tc(num_dir + "/tag_code.bin");
    gendb::MmapColumn<uint32_t> num_vc(num_dir + "/version_code.bin");
    gendb::MmapColumn<double> num_val(num_dir + "/value.bin");

    GENDB_PHASE("total");

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
            size_t name_data_size = st.st_size;

            std::unordered_map<std::string_view, uint32_t> name_to_id;
            name_to_id.reserve(sub_rows);
            sub_fk_to_name_id.resize(sub_rows);

            for (size_t i = 0; i < sub_rows; i++) {
                std::string_view nm(name_data + name_off_col[i], name_off_col[i + 1] - name_off_col[i]);
                auto it = name_to_id.find(nm);
                if (it == name_to_id.end()) {
                    uint32_t id = (uint32_t)name_strings.size();
                    name_to_id[nm] = id;
                    name_strings.emplace_back(nm);
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

        // Get USD range
        {
            auto uom_ranges = loadOffsetTable(num_dir + "/uom_offsets.bin");
            usd_start = uom_ranges[usdCode].start;
            usd_end = uom_ranges[usdCode].end;
        }

        // Prefetch num columns
        num_sfk.prefetch();
        num_tc.prefetch();
        num_vc.prefetch();
        num_val.prefetch();

        // 4. Build direct-indexed pre lookup
        {
            GENDB_PHASE("build_joins");

            auto stmt_ranges = loadOffsetTable(pre_dir + "/stmt_offsets.bin");
            uint64_t is_start = stmt_ranges[isCode].start;
            uint64_t is_end = stmt_ranges[isCode].end;

            gendb::MmapColumn<uint32_t> pre_sfk(pre_dir + "/sub_fk.bin");
            gendb::MmapColumn<uint32_t> pre_tc(pre_dir + "/tag_code.bin");
            gendb::MmapColumn<uint32_t> pre_vc(pre_dir + "/version_code.bin");
            gendb::MmapColumn<uint32_t> pre_plabel(gendb_dir + "/column_versions/pre.plabel.dict/codes.bin");

            const uint32_t* psfk = pre_sfk.data;
            const uint32_t* ptc = pre_tc.data;
            const uint32_t* pvc = pre_vc.data;
            const uint32_t* ppc = pre_plabel.data;
            const uint8_t* fy_bits = fy2023_bits.data();

            std::vector<uint32_t> counts(sub_rows, 0);
            for (uint64_t r = is_start; r < is_end; r++) {
                uint32_t sfk = psfk[r];
                if (fy_bits[sfk >> 3] & (1 << (sfk & 7)))
                    counts[sfk]++;
            }

            pre_offsets.resize(sub_rows + 1);
            pre_offsets[0] = 0;
            for (size_t i = 0; i < sub_rows; i++)
                pre_offsets[i + 1] = pre_offsets[i] + counts[i];
            uint32_t total_entries = pre_offsets[sub_rows];

            pre_entries.resize(total_entries);
            memset(counts.data(), 0, sub_rows * sizeof(uint32_t));

            for (uint64_t r = is_start; r < is_end; r++) {
                uint32_t sfk = psfk[r];
                if (!(fy_bits[sfk >> 3] & (1 << (sfk & 7)))) continue;
                uint32_t idx = pre_offsets[sfk] + counts[sfk]++;
                pre_entries[idx] = {ptc[r], pvc[r], ppc[r]};
            }
        }

        tag_dict.open(dict_dir + "/tag_dict_offsets.bin", dict_dir + "/tag_dict_data.bin");
        plabel_dict.open(
            gendb_dir + "/column_versions/pre.plabel.dict/dict.offsets",
            gendb_dir + "/column_versions/pre.plabel.dict/dict.data"
        );
    }

    // ======= MAIN SCAN + AGGREGATE =======
    int nthreads = omp_get_max_threads();

    // Use partitioned aggregation: partition by hash bits to enable parallel merge
    // 256 partitions, each thread writes to its own partition set
    static constexpr int NUM_PARTS = 256;
    static constexpr int PART_SHIFT = 56;  // top 8 bits of aggHash for partition

    struct PartMap {
        OpenAggMap map;
    };

    // thread_parts[tid][part] — each thread has its own map per partition
    // After scan, merge same-partition maps across threads
    // This is too memory-intensive with 64 threads × 256 parts.

    // Better approach: use thread-local maps, extract compact entries,
    // then merge using parallel partitioned approach
    std::vector<std::vector<AggSlot>> thread_entries(nthreads);

    {
        GENDB_PHASE("main_scan");

        std::vector<OpenAggMap> thread_maps(nthreads);
        for (auto& m : thread_maps) m.init(16384);

        const uint8_t* fy_bits = fy2023_bits.data();
        const uint32_t* sfk_ptr = num_sfk.data;
        const uint32_t* tc_ptr = num_tc.data;
        const uint32_t* vc_ptr = num_vc.data;
        const double* val_ptr = num_val.data;
        const uint32_t* name_map = sub_fk_to_name_id.data();
        const uint32_t* pre_off = pre_offsets.data();
        const PreEntry* pre_ent = pre_entries.data();

        const size_t MORSEL = 131072;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            OpenAggMap& local_map = thread_maps[tid];

            #pragma omp for schedule(dynamic, 1) nowait
            for (size_t morsel_start = usd_start; morsel_start < usd_end; morsel_start += MORSEL) {
                size_t morsel_end = std::min(morsel_start + MORSEL, usd_end);

                for (size_t i = morsel_start; i < morsel_end; i++) {
                    uint32_t sfk = sfk_ptr[i];
                    if (!(fy_bits[sfk >> 3] & (1 << (sfk & 7)))) continue;

                    double val = val_ptr[i];
                    if (std::isnan(val)) continue;

                    uint32_t pstart = pre_off[sfk];
                    uint32_t pend = pre_off[sfk + 1];
                    if (pstart == pend) continue;

                    uint32_t tc = tc_ptr[i];
                    uint32_t vc = vc_ptr[i];
                    uint32_t nid = name_map[sfk];

                    for (uint32_t p = pstart; p < pend; p++) {
                        const PreEntry& pe = pre_ent[p];
                        if (pe.tag_code == tc && pe.version_code == vc) {
                            local_map.insert(packAggKey(nid, tc, pe.plabel_code), val);
                        }
                    }
                }
            }

            // Extract compact entries
            auto& entries = thread_entries[tid];
            entries.reserve(local_map.count);
            for (size_t i = 0; i < local_map.capacity; i++) {
                if (local_map.slots[i].key != AGG_EMPTY)
                    entries.push_back(local_map.slots[i]);
            }
        }
        // thread_maps destructors run here, freeing memory
    }

    // ======= PARALLEL MERGE + OUTPUT =======
    {
        GENDB_PHASE("output");

        // Partition entries by hash bits, then merge partitions in parallel
        // Each partition gets its own small map
        size_t total_entries = 0;
        for (int t = 0; t < nthreads; t++) total_entries += thread_entries[t].size();

        // Assign entries to partitions
        std::vector<std::vector<AggSlot>> part_entries(NUM_PARTS);
        for (int t = 0; t < nthreads; t++) {
            for (const auto& s : thread_entries[t]) {
                size_t h = aggHash(s.key);
                int part = (h >> PART_SHIFT) & (NUM_PARTS - 1);
                part_entries[part].push_back(s);
            }
        }
        // Free thread entries
        for (int t = 0; t < nthreads; t++) {
            std::vector<AggSlot>().swap(thread_entries[t]);
        }

        // Merge each partition in parallel
        std::vector<std::vector<AggSlot>> part_results(NUM_PARTS);

        #pragma omp parallel for schedule(dynamic, 4)
        for (int part = 0; part < NUM_PARTS; part++) {
            auto& pe = part_entries[part];
            if (pe.empty()) continue;

            OpenAggMap pmap;
            pmap.init(pe.size() * 2);
            for (const auto& s : pe) {
                pmap.merge_slot(s);
            }

            auto& pr = part_results[part];
            pr.reserve(pmap.count);
            for (size_t i = 0; i < pmap.capacity; i++) {
                if (pmap.slots[i].key != AGG_EMPTY)
                    pr.push_back(pmap.slots[i]);
            }
        }

        // Collect all results
        struct ResultRow {
            uint64_t key;
            double total_value;
            uint64_t cnt;
        };

        std::vector<ResultRow> results;
        results.reserve(total_entries); // upper bound
        for (int part = 0; part < NUM_PARTS; part++) {
            for (const auto& s : part_results[part]) {
                results.push_back({s.key, s.sum, s.count});
            }
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

    return 0;
}
