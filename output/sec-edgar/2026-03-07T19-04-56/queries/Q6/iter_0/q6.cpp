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

// ---- Hash function matching pre_by_adsh_tag_ver index ----
static inline uint64_t hashKey3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= (uint64_t)c * 3266489917ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}

// ---- Offset table: uint32_t num_entries, then (uint64_t start, uint64_t end) pairs ----
struct OffsetRange { uint64_t start, end; };

static std::vector<OffsetRange> loadOffsetTable(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    uint32_t n;
    ::read(fd, &n, 4);
    std::vector<OffsetRange> v(n);
    ::read(fd, v.data(), n * sizeof(OffsetRange));
    ::close(fd);
    return v;
}

// ---- Varlen string helpers ----
struct VarlenCol {
    const uint64_t* offsets; // N+1 entries
    const char* data;
    size_t num_rows;
    size_t off_file_size, data_file_size;
    int off_fd, data_fd;

    VarlenCol() : offsets(nullptr), data(nullptr), num_rows(0), off_fd(-1), data_fd(-1) {}

    void open(const std::string& off_path, const std::string& data_path) {
        struct stat st;
        off_fd = ::open(off_path.c_str(), O_RDONLY);
        fstat(off_fd, &st);
        off_file_size = st.st_size;
        num_rows = off_file_size / sizeof(uint64_t) - 1;
        offsets = (const uint64_t*)mmap(nullptr, off_file_size, PROT_READ, MAP_PRIVATE, off_fd, 0);

        data_fd = ::open(data_path.c_str(), O_RDONLY);
        fstat(data_fd, &st);
        data_file_size = st.st_size;
        data = (const char*)mmap(nullptr, data_file_size, PROT_READ, MAP_PRIVATE, data_fd, 0);
    }

    std::string get(size_t i) const {
        return std::string(data + offsets[i], offsets[i + 1] - offsets[i]);
    }

    // Return (ptr, len) without allocation
    std::pair<const char*, size_t> get_view(size_t i) const {
        return {data + offsets[i], offsets[i + 1] - offsets[i]};
    }

    void close() {
        if (offsets) munmap((void*)offsets, off_file_size);
        if (data) munmap((void*)data, data_file_size);
        if (off_fd >= 0) ::close(off_fd);
        if (data_fd >= 0) ::close(data_fd);
        offsets = nullptr; data = nullptr;
        off_fd = data_fd = -1;
    }

    ~VarlenCol() { close(); }
};

// ---- Dictionary: offsets + data ----
struct Dict {
    std::vector<uint64_t> offsets;
    std::vector<char> data;

    void load(const std::string& off_path, const std::string& data_path) {
        // Read offsets
        int fd = ::open(off_path.c_str(), O_RDONLY);
        struct stat st;
        fstat(fd, &st);
        size_t n = st.st_size / sizeof(uint64_t);
        offsets.resize(n);
        ::read(fd, offsets.data(), st.st_size);
        ::close(fd);
        // Read data
        fd = ::open(data_path.c_str(), O_RDONLY);
        fstat(fd, &st);
        data.resize(st.st_size);
        ::read(fd, data.data(), st.st_size);
        ::close(fd);
    }

    size_t num_entries() const { return offsets.size() - 1; }

    std::string get(size_t i) const {
        return std::string(data.data() + offsets[i], offsets[i + 1] - offsets[i]);
    }

    int findCode(const std::string& val) const {
        for (size_t i = 0; i + 1 < offsets.size(); i++) {
            size_t len = offsets[i + 1] - offsets[i];
            if (len == val.size() && memcmp(data.data() + offsets[i], val.data(), len) == 0)
                return (int)i;
        }
        return -1;
    }
};

// ---- Pre hash map (bucket chain) ----
struct PreEntry {
    uint32_t sub_fk;
    uint32_t tag_code;
    uint32_t version_code;
    uint32_t plabel_id;
};

struct PreHashMap {
    std::vector<uint64_t> bucket_offsets; // numBuckets + 1
    std::vector<PreEntry> entries;
    size_t mask;

    void build(const uint32_t* sfk, const uint32_t* tc, const uint32_t* vc,
               const uint32_t* plbl_ids, size_t count) {
        // Determine bucket count (power of 2, >= 2 * count)
        size_t numBuckets = 16;
        while (numBuckets < count * 2) numBuckets <<= 1;
        mask = numBuckets - 1;

        // Count per bucket
        std::vector<uint64_t> counts(numBuckets, 0);
        for (size_t i = 0; i < count; i++) {
            uint64_t b = hashKey3(sfk[i], tc[i], vc[i]) & mask;
            counts[b]++;
        }

        // Prefix sum
        bucket_offsets.resize(numBuckets + 1);
        bucket_offsets[0] = 0;
        for (size_t i = 0; i < numBuckets; i++)
            bucket_offsets[i + 1] = bucket_offsets[i] + counts[i];

        // Scatter
        entries.resize(count);
        std::vector<uint64_t> pos(numBuckets);
        for (size_t i = 0; i < numBuckets; i++) pos[i] = bucket_offsets[i];
        for (size_t i = 0; i < count; i++) {
            uint64_t b = hashKey3(sfk[i], tc[i], vc[i]) & mask;
            entries[pos[b]++] = {sfk[i], tc[i], vc[i], plbl_ids[i]};
        }
    }
};

// ---- Aggregation key & value ----
struct GroupKey {
    uint32_t name_id;
    uint32_t tag_code;
    uint32_t plabel_id;
    bool operator==(const GroupKey& o) const {
        return name_id == o.name_id && tag_code == o.tag_code && plabel_id == o.plabel_id;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        uint64_t h = (uint64_t)k.name_id * 0x9E3779B97F4A7C15ULL;
        h ^= (uint64_t)k.tag_code * 2246822519ULL;
        h ^= (uint64_t)k.plabel_id * 3266489917ULL;
        h ^= h >> 16;
        h *= 0x45d9f3b37197344dULL;
        h ^= h >> 16;
        return h;
    }
};

struct AggValue {
    double sum;
    uint64_t count;
};

// ---- CSV quoting ----
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

    // Result structures
    std::vector<uint32_t> sub_fk_to_name_id;
    std::vector<std::string> name_strings;   // name_id → string
    std::vector<std::string> plabel_strings; // plabel_id → string
    PreHashMap pre_map;
    uint64_t usd_start = 0, usd_end = 0;
    Dict tag_dict;

    // fy2023 bitset
    std::vector<uint8_t> fy2023_bits;
    size_t sub_rows = 0;

    GENDB_PHASE("total");

    // ======= DATA LOADING =======
    {
        GENDB_PHASE("data_loading");

        // 1. Load sub/fy.bin → build fy2023 bitset
        {
            GENDB_PHASE("dim_filter");
            gendb::MmapColumn<int16_t> fy_col(sub_dir + "/fy.bin");
            sub_rows = fy_col.count;
            fy2023_bits.resize((sub_rows + 7) / 8, 0);
            for (size_t i = 0; i < sub_rows; i++) {
                if (fy_col[i] == 2023) {
                    fy2023_bits[i >> 3] |= (1 << (i & 7));
                }
            }
        }

        // 2. Build name_id map: sub_fk → name_id
        {
            GENDB_PHASE("build_joins");
            VarlenCol name_col;
            name_col.open(sub_dir + "/name_offsets.bin", sub_dir + "/name_data.bin");

            std::unordered_map<std::string, uint32_t> name_to_id;
            name_to_id.reserve(sub_rows);
            sub_fk_to_name_id.resize(sub_rows);

            for (size_t i = 0; i < sub_rows; i++) {
                std::string nm = name_col.get(i);
                auto it = name_to_id.find(nm);
                if (it == name_to_id.end()) {
                    uint32_t id = (uint32_t)name_strings.size();
                    name_strings.push_back(std::move(nm));
                    name_to_id[name_strings.back()] = id;
                    sub_fk_to_name_id[i] = id;
                } else {
                    sub_fk_to_name_id[i] = it->second;
                }
            }
        }

        // 3. Find dictionary codes
        uint8_t usdCode = 0;
        uint8_t isCode = 0;
        {
            Dict uom_dict;
            uom_dict.load(num_dir + "/uom_dict_offsets.bin", num_dir + "/uom_dict_data.bin");
            int c = uom_dict.findCode("USD");
            if (c < 0) { fprintf(stderr, "USD not found in uom_dict\n"); return 1; }
            usdCode = (uint8_t)c;

            Dict stmt_dict;
            stmt_dict.load(pre_dir + "/stmt_dict_offsets.bin", pre_dir + "/stmt_dict_data.bin");
            c = stmt_dict.findCode("IS");
            if (c < 0) { fprintf(stderr, "IS not found in stmt_dict\n"); return 1; }
            isCode = (uint8_t)c;
        }

        // 4. Load tag_dict for output decoding
        tag_dict.load(dict_dir + "/tag_dict_offsets.bin", dict_dir + "/tag_dict_data.bin");

        // 5. Get IS range from stmt_offsets
        {
            auto stmt_ranges = loadOffsetTable(pre_dir + "/stmt_offsets.bin");
            uint64_t is_start = stmt_ranges[isCode].start;
            uint64_t is_end = stmt_ranges[isCode].end;
            size_t is_count = is_end - is_start;

            // Load pre columns for IS range
            gendb::MmapColumn<uint32_t> pre_sfk(pre_dir + "/sub_fk.bin");
            gendb::MmapColumn<uint32_t> pre_tc(pre_dir + "/tag_code.bin");
            gendb::MmapColumn<uint32_t> pre_vc(pre_dir + "/version_code.bin");

            // Load plabel varlen for the whole pre table (we need global row indices)
            VarlenCol plabel_col;
            plabel_col.open(pre_dir + "/plabel_offsets.bin", pre_dir + "/plabel_data.bin");

            // Build plabel_id map and pre hash map data arrays
            std::unordered_map<std::string, uint32_t> plabel_to_id;
            plabel_to_id.reserve(is_count / 4); // estimated unique plabels

            std::vector<uint32_t> sfk_arr(is_count);
            std::vector<uint32_t> tc_arr(is_count);
            std::vector<uint32_t> vc_arr(is_count);
            std::vector<uint32_t> plbl_arr(is_count);

            for (size_t i = 0; i < is_count; i++) {
                size_t row = is_start + i;
                sfk_arr[i] = pre_sfk[row];
                tc_arr[i] = pre_tc[row];
                vc_arr[i] = pre_vc[row];

                std::string pl = plabel_col.get(row);
                auto it = plabel_to_id.find(pl);
                if (it == plabel_to_id.end()) {
                    uint32_t id = (uint32_t)plabel_strings.size();
                    plabel_strings.push_back(std::move(pl));
                    plabel_to_id[plabel_strings.back()] = id;
                    plbl_arr[i] = id;
                } else {
                    plbl_arr[i] = it->second;
                }
            }

            pre_map.build(sfk_arr.data(), tc_arr.data(), vc_arr.data(),
                          plbl_arr.data(), is_count);
        }

        // 6. Get USD range from uom_offsets
        {
            auto uom_ranges = loadOffsetTable(num_dir + "/uom_offsets.bin");
            usd_start = uom_ranges[usdCode].start;
            usd_end = uom_ranges[usdCode].end;
        }
    }

    // ======= MAIN SCAN =======
    // Mmap num columns
    gendb::MmapColumn<uint32_t> num_sfk(num_dir + "/sub_fk.bin");
    gendb::MmapColumn<uint32_t> num_tc(num_dir + "/tag_code.bin");
    gendb::MmapColumn<uint32_t> num_vc(num_dir + "/version_code.bin");
    gendb::MmapColumn<double> num_val(num_dir + "/value.bin");

    // Prefetch num columns for USD range
    num_sfk.prefetch();
    num_tc.prefetch();
    num_vc.prefetch();
    num_val.prefetch();

    int nthreads = omp_get_max_threads();
    std::vector<std::unordered_map<GroupKey, AggValue, GroupKeyHash>> thread_maps(nthreads);
    for (auto& m : thread_maps) m.reserve(8192);

    {
        GENDB_PHASE("main_scan");

        const uint8_t* fy_bits = fy2023_bits.data();
        const uint32_t* sfk_ptr = num_sfk.data;
        const uint32_t* tc_ptr = num_tc.data;
        const uint32_t* vc_ptr = num_vc.data;
        const double* val_ptr = num_val.data;
        const uint32_t* name_map = sub_fk_to_name_id.data();

        const size_t pre_mask = pre_map.mask;
        const uint64_t* pre_bucket_off = pre_map.bucket_offsets.data();
        const PreEntry* pre_entries = pre_map.entries.data();

        const size_t MORSEL = 100000;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local_map = thread_maps[tid];

            #pragma omp for schedule(dynamic, 1) nowait
            for (size_t morsel_start = usd_start; morsel_start < usd_end; morsel_start += MORSEL) {
                size_t morsel_end = std::min(morsel_start + MORSEL, usd_end);

                for (size_t i = morsel_start; i < morsel_end; i++) {
                    double val = val_ptr[i];
                    if (std::isnan(val)) continue;

                    uint32_t sfk = sfk_ptr[i];
                    // Check fy2023 bitset
                    if (!(fy_bits[sfk >> 3] & (1 << (sfk & 7)))) continue;

                    uint32_t tc = tc_ptr[i];
                    uint32_t vc = vc_ptr[i];

                    // Probe pre hash map
                    uint64_t bucket = hashKey3(sfk, tc, vc) & pre_mask;
                    uint64_t bStart = pre_bucket_off[bucket];
                    uint64_t bEnd = pre_bucket_off[bucket + 1];

                    for (uint64_t j = bStart; j < bEnd; j++) {
                        const PreEntry& pe = pre_entries[j];
                        if (pe.sub_fk == sfk && pe.tag_code == tc && pe.version_code == vc) {
                            uint32_t nid = name_map[sfk];
                            GroupKey gk{nid, tc, pe.plabel_id};
                            auto& agg = local_map[gk];
                            agg.sum += val;
                            agg.count++;
                        }
                    }
                }
            }
        }
    }

    // ======= OUTPUT =======
    {
        GENDB_PHASE("output");

        // Merge thread-local maps
        auto& merged = thread_maps[0];
        for (int t = 1; t < nthreads; t++) {
            for (auto& [key, val] : thread_maps[t]) {
                auto& agg = merged[key];
                agg.sum += val.sum;
                agg.count += val.count;
            }
            thread_maps[t].clear();
        }

        // Collect results for sorting
        struct ResultRow {
            uint32_t name_id;
            uint32_t tag_code;
            uint32_t plabel_id;
            double total_value;
            uint64_t cnt;
        };

        std::vector<ResultRow> results;
        results.reserve(merged.size());
        for (auto& [key, val] : merged) {
            results.push_back({key.name_id, key.tag_code, key.plabel_id, val.sum, val.count});
        }

        // Partial sort for top 200
        size_t limit = std::min((size_t)200, results.size());
        std::partial_sort(results.begin(), results.begin() + limit, results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                return a.total_value > b.total_value;
            });

        // Write CSV
        std::string outpath = results_dir + "/Q6.csv";
        FILE* fout = fopen(outpath.c_str(), "w");
        fprintf(fout, "name,stmt,tag,plabel,total_value,cnt\n");

        for (size_t i = 0; i < limit; i++) {
            const auto& r = results[i];
            writeCSVField(fout, name_strings[r.name_id]);
            fprintf(fout, ",IS,");
            writeCSVField(fout, tag_dict.get(r.tag_code));
            fputc(',', fout);
            writeCSVField(fout, plabel_strings[r.plabel_id]);
            fprintf(fout, ",%.2f,%lu\n", r.total_value, (unsigned long)r.cnt);
        }

        fclose(fout);
    }

    return 0;
}
