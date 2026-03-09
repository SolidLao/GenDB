// Q3: SUM(value) GROUP BY (name, cik) with HAVING > AVG, top 100 DESC
// Strategy: uom_offsets range scan, bitset dimension filter, parallel hash aggregation
// Key insight: GROUP BY (name, cik) — same CIK can have different names

#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"

// ── mmap helper ──────────────────────────────────────────────────────
struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    void open(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (data == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path.c_str()); exit(1); }
        ::close(fd);
    }
    ~MmapFile() { if (data && data != MAP_FAILED) munmap((void*)data, size); }
};

template<typename T>
struct Col {
    MmapFile f;
    size_t count = 0;
    const T* ptr = nullptr;
    void load(const std::string& path) {
        f.open(path);
        count = f.size / sizeof(T);
        ptr = reinterpret_cast<const T*>(f.data);
    }
    const T& operator[](size_t i) const { return ptr[i]; }
};

// ── Result row ───────────────────────────────────────────────────────
struct Result {
    std::string name;
    int32_t cik;
    long double total_value;
};

// ── Main ─────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // ── Phase 1: Load sub dimension ──────────────────────────────────
    Col<int16_t> sub_fy;
    Col<int32_t> sub_cik;
    MmapFile sub_name_offsets_f, sub_name_data_f;
    size_t sub_rows;

    {
        GENDB_PHASE("data_loading");
        sub_fy.load(gendb_dir + "/sub/fy.bin");
        sub_cik.load(gendb_dir + "/sub/cik.bin");
        sub_name_offsets_f.open(gendb_dir + "/sub/name_offsets.bin");
        sub_name_data_f.open(gendb_dir + "/sub/name_data.bin");
        sub_rows = sub_fy.count;
    }

    // ── Phase 2: Build group structures ──────────────────────────────
    // Group by (name, cik) — same CIK can map to different names
    // Assign a dense group_id to each unique (name, cik) pair
    // Then create sub_fk → group_id mapping for fast inner-loop lookup

    struct GroupInfo {
        std::string name;
        int32_t cik;
    };
    std::vector<GroupInfo> groups;                        // group_id → (name, cik)
    std::vector<int32_t> sub_to_group;                   // sub_row → group_id (-1 if fy!=2022)

    {
        GENDB_PHASE("dim_filter");

        const uint64_t* name_offsets = reinterpret_cast<const uint64_t*>(sub_name_offsets_f.data);
        const char* name_data = sub_name_data_f.data;

        sub_to_group.resize(sub_rows, -1);

        // Map (name, cik) → group_id
        // Use string key = name + "\0" + cik_bytes for uniqueness
        std::unordered_map<std::string, int32_t> key_to_gid;
        key_to_gid.reserve(10000);

        for (size_t i = 0; i < sub_rows; i++) {
            if (sub_fy[i] == 2022) {
                int32_t cik = sub_cik[i];
                uint64_t off_start = name_offsets[i];
                uint64_t off_end = name_offsets[i + 1];
                std::string name(name_data + off_start, off_end - off_start);

                // Composite key for grouping
                std::string key = name;
                key.push_back('\0');
                key.append(reinterpret_cast<const char*>(&cik), sizeof(cik));

                auto it = key_to_gid.find(key);
                if (it == key_to_gid.end()) {
                    int32_t gid = (int32_t)groups.size();
                    key_to_gid[key] = gid;
                    groups.push_back({std::move(name), cik});
                    sub_to_group[i] = gid;
                } else {
                    sub_to_group[i] = it->second;
                }
            }
        }
    }

    // ── Phase 3: Resolve USD code & get range ────────────────────────
    uint64_t usdStart = 0, usdEnd = 0;
    {
        GENDB_PHASE("build_joins");

        MmapFile dict_off_f, dict_data_f;
        dict_off_f.open(gendb_dir + "/num/uom_dict_offsets.bin");
        dict_data_f.open(gendb_dir + "/num/uom_dict_data.bin");
        const uint64_t* dict_offsets = reinterpret_cast<const uint64_t*>(dict_off_f.data);
        const char* dict_data = dict_data_f.data;
        size_t num_dict_entries = dict_off_f.size / sizeof(uint64_t) - 1;

        uint8_t usdCode = 255;
        for (size_t i = 0; i < num_dict_entries; i++) {
            std::string_view sv(dict_data + dict_offsets[i], dict_offsets[i+1] - dict_offsets[i]);
            if (sv == "USD") { usdCode = (uint8_t)i; break; }
        }
        if (usdCode == 255) { fprintf(stderr, "USD not found in dictionary\n"); return 1; }

        MmapFile uom_off_f;
        uom_off_f.open(gendb_dir + "/num/uom_offsets.bin");
        const char* uom_ptr = uom_off_f.data;
        uint32_t numEntries;
        memcpy(&numEntries, uom_ptr, 4);
        struct Range { uint64_t start, end; };
        const Range* ranges = reinterpret_cast<const Range*>(uom_ptr + 4);
        usdStart = ranges[usdCode].start;
        usdEnd = ranges[usdCode].end;
    }

    // ── Phase 4: Parallel scan aggregate ─────────────────────────────
    // Aggregate by group_id (dense int32_t), thread-local arrays
    size_t num_groups = groups.size();
    std::vector<long double> global_sums(num_groups, 0.0L);

    {
        GENDB_PHASE("main_scan");

        Col<uint32_t> num_sub_fk;
        Col<double> num_value;
        num_sub_fk.load(gendb_dir + "/num/sub_fk.bin");
        num_value.load(gendb_dir + "/num/value.bin");

        const uint32_t* __restrict__ sub_fk_ptr = num_sub_fk.ptr;
        const double* __restrict__ value_ptr = num_value.ptr;
        const int32_t* __restrict__ stg_ptr = sub_to_group.data();

        int n_threads = omp_get_max_threads();
        // Thread-local dense arrays for aggregation (each ~num_groups * 16 bytes)
        std::vector<std::vector<long double>> tl_sums(n_threads);
        for (auto& v : tl_sums) v.resize(num_groups, 0.0L);

        int64_t total_rows = (int64_t)(usdEnd - usdStart);
        static constexpr int64_t MORSEL = 100000;

        #pragma omp parallel num_threads(n_threads)
        {
            int tid = omp_get_thread_num();
            long double* __restrict__ local = tl_sums[tid].data();

            #pragma omp for schedule(dynamic, 1) nowait
            for (int64_t m = 0; m < total_rows; m += MORSEL) {
                int64_t end = std::min(m + MORSEL, total_rows);
                int64_t base = (int64_t)usdStart;

                for (int64_t j = m; j < end; j++) {
                    int64_t i = base + j;
                    double val = value_ptr[i];
                    if (std::isnan(val)) continue;
                    uint32_t sfk = sub_fk_ptr[i];
                    int32_t gid = stg_ptr[sfk];
                    if (gid < 0) continue;
                    local[gid] += (long double)val;
                }
            }
        }

        // Merge thread-local arrays
        for (int t = 0; t < n_threads; t++) {
            for (size_t g = 0; g < num_groups; g++) {
                global_sums[g] += tl_sums[t][g];
            }
        }
    }

    // ── Phase 5: Compute HAVING threshold & filter+topk ──────────────
    {
        GENDB_PHASE("output");

        // Compute threshold = AVG of per-group sums
        // The HAVING subquery groups by cik only, so we need to first
        // aggregate by cik, then compute AVG of those cik-level sums
        std::unordered_map<int32_t, long double> cik_sums;
        cik_sums.reserve(10000);
        for (size_t g = 0; g < num_groups; g++) {
            cik_sums[groups[g].cik] += global_sums[g];
        }

        long double total_sum = 0.0L;
        for (auto& [cik, val] : cik_sums) {
            total_sum += val;
        }
        long double threshold = ((int64_t)cik_sums.size() > 0) ?
            (total_sum / (long double)cik_sums.size()) : 0.0L;

        // Now filter: the main query groups by (name, cik) and computes SUM per group.
        // HAVING condition: SUM(n.value) > threshold
        // But HAVING references the main query's SUM, which is per (name, cik) group.
        // Wait - re-read the SQL:
        //   HAVING SUM(n.value) > (SELECT AVG(sub_total) FROM (
        //     SELECT SUM(n2.value) AS sub_total ... GROUP BY s2.cik
        //   ))
        // The subquery groups by cik (single column), the main query groups by (name, cik).
        // HAVING applies per (name, cik) group: each group's SUM must exceed the threshold.
        // Threshold = AVG of per-cik sums (from subquery).

        std::vector<Result> results;
        results.reserve(num_groups);
        for (size_t g = 0; g < num_groups; g++) {
            if (global_sums[g] > threshold) {
                results.push_back({groups[g].name, groups[g].cik, global_sums[g]});
            }
        }

        // Top 100 by total_value DESC
        size_t limit = std::min((size_t)100, results.size());
        std::partial_sort(results.begin(), results.begin() + limit, results.end(),
            [](const Result& a, const Result& b) { return a.total_value > b.total_value; });

        // Write CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror("fopen"); return 1; }
        fprintf(fp, "name,cik,total_value\n");
        for (size_t i = 0; i < limit; i++) {
            const auto& r = results[i];
            // Quote name if it contains comma or double-quote
            bool needs_quote = r.name.find(',') != std::string::npos ||
                               r.name.find('"') != std::string::npos;
            if (needs_quote) {
                std::string escaped;
                escaped.reserve(r.name.size() + 4);
                escaped += '"';
                for (char c : r.name) {
                    if (c == '"') escaped += '"';
                    escaped += c;
                }
                escaped += '"';
                fprintf(fp, "%s,%d,%.2Lf\n", escaped.c_str(), r.cik, r.total_value);
            } else {
                fprintf(fp, "%s,%d,%.2Lf\n", r.name.c_str(), r.cik, r.total_value);
            }
        }
        fclose(fp);
    }

    return 0;
}
