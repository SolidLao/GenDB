// Q3: SELECT s.name, s.cik, SUM(n.value) AS total_value
//     ... HAVING SUM(n.value) > AVG(cik-level sums) ORDER BY total_value DESC LIMIT 100
// Optimized: double (not long double), morsel-driven parallel scan, fy2022 bitmap pre-filter

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <atomic>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"

using namespace gendb;

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    // Create output directory
    {
        std::string cmd = "mkdir -p " + results_dir;
        system(cmd.c_str());
    }

    auto t0 = std::chrono::high_resolution_clock::now();

    // ========== Phase 1: Data Loading ==========
    auto t_load = std::chrono::high_resolution_clock::now();

    // Sub dimension columns
    MmapColumn<int16_t> sub_fy(gendb_dir + "/sub/fy.bin");
    MmapColumn<int32_t> sub_cik(gendb_dir + "/sub/cik.bin");
    MmapColumn<uint64_t> name_offsets(gendb_dir + "/sub/name_offsets.bin");

    // Name data (raw bytes)
    int name_data_fd = open((gendb_dir + "/sub/name_data.bin").c_str(), O_RDONLY);
    struct stat name_data_st;
    fstat(name_data_fd, &name_data_st);
    const char* name_data = (const char*)mmap(nullptr, name_data_st.st_size,
                                               PROT_READ, MAP_PRIVATE, name_data_fd, 0);

    size_t sub_rows = sub_fy.count;

    // Resolve USD dictionary code
    MmapColumn<uint64_t> uom_dict_offsets(gendb_dir + "/num/uom_dict_offsets.bin");
    int uom_data_fd = open((gendb_dir + "/num/uom_dict_data.bin").c_str(), O_RDONLY);
    struct stat uom_data_st;
    fstat(uom_data_fd, &uom_data_st);
    const char* uom_data = (const char*)mmap(nullptr, uom_data_st.st_size,
                                              PROT_READ, MAP_PRIVATE, uom_data_fd, 0);

    uint8_t usd_code = 255;
    for (size_t i = 0; i + 1 < uom_dict_offsets.count; i++) {
        uint64_t s = uom_dict_offsets[i], e = uom_dict_offsets[i + 1];
        if (e - s == 3 && memcmp(uom_data + s, "USD", 3) == 0) {
            usd_code = (uint8_t)i;
            break;
        }
    }

    // Load uom_offsets to get USD row range
    struct UomRange { uint64_t start, end; };
    uint32_t num_uom_entries;
    std::vector<UomRange> uom_ranges;
    {
        int fd = open((gendb_dir + "/num/uom_offsets.bin").c_str(), O_RDONLY);
        read(fd, &num_uom_entries, 4);
        uom_ranges.resize(num_uom_entries);
        read(fd, uom_ranges.data(), num_uom_entries * sizeof(UomRange));
        close(fd);
    }
    uint64_t usd_start = uom_ranges[usd_code].start;
    uint64_t usd_end = uom_ranges[usd_code].end;

    // Mmap num columns
    MmapColumn<uint32_t> num_sub_fk(gendb_dir + "/num/sub_fk.bin");
    MmapColumn<double> num_value(gendb_dir + "/num/value.bin");

    // Advise sequential on USD range only
    {
        size_t ps = 4096;
        // sub_fk
        size_t fk_start_byte = usd_start * sizeof(uint32_t);
        size_t fk_end_byte = usd_end * sizeof(uint32_t);
        size_t fk_page = (fk_start_byte / ps) * ps;
        madvise((char*)num_sub_fk.data + fk_page, fk_end_byte - fk_page, MADV_SEQUENTIAL);
        // value
        size_t v_start_byte = usd_start * sizeof(double);
        size_t v_end_byte = usd_end * sizeof(double);
        size_t v_page = (v_start_byte / ps) * ps;
        madvise((char*)num_value.data + v_page, v_end_byte - v_page, MADV_SEQUENTIAL);
    }

    auto t_load_end = std::chrono::high_resolution_clock::now();
    double data_loading_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load).count();
    std::printf("[TIMING] data_loading: %.2f ms\n", data_loading_ms);

    // ========== Phase 2: Dim Filter — build groups and sub_to_group ==========
    auto t_dim = std::chrono::high_resolution_clock::now();

    // Group by (name, cik). Assign dense group_ids.
    // sub_to_group[sub_row] = group_id or -1
    // fy2022_bitmap[sub_row] = 1 if fy==2022, 0 otherwise (86KB, fits L2)

    struct GroupInfo {
        std::string name;
        int32_t cik;
    };
    std::vector<GroupInfo> groups;

    // Use unordered_map with (cik, name_view) as key for O(1) dedup
    struct PairHash {
        size_t operator()(const std::pair<int32_t, std::string_view>& p) const {
            size_t h1 = std::hash<int32_t>{}(p.first);
            size_t h2 = std::hash<std::string_view>{}(p.second);
            return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL);
        }
    };
    std::unordered_map<std::pair<int32_t, std::string_view>, int32_t, PairHash> key_to_group;
    key_to_group.reserve(16384);

    std::vector<int32_t> sub_to_group(sub_rows, -1);
    std::vector<uint8_t> fy2022_bitmap(sub_rows, 0);

    for (size_t i = 0; i < sub_rows; i++) {
        if (sub_fy[i] != 2022) continue;
        fy2022_bitmap[i] = 1;
        int32_t cik = sub_cik[i];
        uint64_t off = name_offsets[i];
        uint64_t next_off = name_offsets[i + 1];
        std::string_view name_sv(name_data + off, next_off - off);

        auto key = std::make_pair(cik, name_sv);
        auto it = key_to_group.find(key);
        int32_t gid;
        if (it == key_to_group.end()) {
            gid = (int32_t)groups.size();
            groups.push_back({std::string(name_sv), cik});
            key_to_group[key] = gid;
        } else {
            gid = it->second;
        }
        sub_to_group[i] = gid;
    }

    int32_t num_groups = (int32_t)groups.size();

    auto t_dim_end = std::chrono::high_resolution_clock::now();
    double dim_filter_ms = std::chrono::duration<double, std::milli>(t_dim_end - t_dim).count();
    std::printf("[TIMING] dim_filter: %.2f ms\n", dim_filter_ms);

    // ========== Phase 3: Parallel Scan + Aggregate ==========
    auto t_scan = std::chrono::high_resolution_clock::now();

    int nthreads = omp_get_max_threads();

    const uint32_t* fk_ptr = num_sub_fk.data;
    const double* val_ptr = num_value.data;
    const int32_t* stg_ptr = sub_to_group.data();
    const uint8_t* bm_ptr = fy2022_bitmap.data();

    // Thread-local long double arrays for aggregation
    // ~10K groups × 16 bytes = 160KB per thread
    std::vector<std::vector<long double>> tl_sums(nthreads);
    for (int t = 0; t < nthreads; t++) {
        tl_sums[t].assign(num_groups, 0.0L);
    }

    const uint32_t MORSEL = 131072;
    std::atomic<uint64_t> next_row{usd_start};

    // Morsel-driven parallel scan
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        long double* local_sum = tl_sums[tid].data();

        while (true) {
            uint64_t start = next_row.fetch_add(MORSEL, std::memory_order_relaxed);
            if (start >= usd_end) break;
            uint64_t end = std::min(start + (uint64_t)MORSEL, usd_end);

            for (uint64_t i = start; i < end; i++) {
                uint32_t fk = fk_ptr[i];
                if (!bm_ptr[fk]) continue;
                double v = val_ptr[i];
                if (std::isnan(v)) continue;
                int32_t gid = stg_ptr[fk];
                local_sum[gid] += v;
            }
        }
    }

    // Merge thread-local arrays sequentially
    std::vector<long double> global_sums(num_groups, 0.0L);
    for (int t = 0; t < nthreads; t++) {
        const long double* src = tl_sums[t].data();
        for (int32_t g = 0; g < num_groups; g++) {
            global_sums[g] += src[g];
        }
    }

    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double main_scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan).count();
    std::printf("[TIMING] main_scan: %.2f ms\n", main_scan_ms);

    // ========== Phase 4: HAVING + Output ==========
    auto t_out = std::chrono::high_resolution_clock::now();

    // Re-aggregate by cik for HAVING threshold
    std::unordered_map<int32_t, long double> cik_totals;
    cik_totals.reserve(num_groups);
    for (int32_t g = 0; g < num_groups; g++) {
        cik_totals[groups[g].cik] += global_sums[g];
    }

    // Threshold = AVG(cik-level sums)
    long double cik_sum = 0.0L;
    for (auto& [cik, total] : cik_totals) {
        cik_sum += total;
    }
    long double threshold = cik_sum / (long double)cik_totals.size();

    // Filter groups by HAVING: group's SUM > threshold
    struct Result {
        int32_t gid;
        long double total;
    };
    std::vector<Result> results;
    results.reserve(1024);
    for (int32_t g = 0; g < num_groups; g++) {
        if (global_sums[g] > threshold) {
            results.push_back({g, global_sums[g]});
        }
    }

    // Partial sort for top 100
    size_t limit = std::min((size_t)100, results.size());
    std::partial_sort(results.begin(), results.begin() + limit, results.end(),
        [](const Result& a, const Result& b) { return a.total > b.total; });
    results.resize(limit);

    // Write CSV output (quote names containing commas)
    std::string out_path = results_dir + "/Q3.csv";
    FILE* fp = fopen(out_path.c_str(), "w");
    fprintf(fp, "name,cik,total_value\n");
    for (size_t i = 0; i < results.size(); i++) {
        auto& r = results[i];
        const std::string& name = groups[r.gid].name;
        if (name.find(',') != std::string::npos) {
            fprintf(fp, "\"%s\",%d,%.2Lf\n", name.c_str(), groups[r.gid].cik, r.total);
        } else {
            fprintf(fp, "%s,%d,%.2Lf\n", name.c_str(), groups[r.gid].cik, r.total);
        }
    }
    fclose(fp);

    auto t_out_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_out_end - t_out).count();
    std::printf("[TIMING] output: %.2f ms\n", output_ms);

    double total_ms = std::chrono::duration<double, std::milli>(t_out_end - t0).count();
    std::printf("[TIMING] total: %.2f ms\n", total_ms);

    // Cleanup mmap'd raw data
    munmap((void*)name_data, name_data_st.st_size);
    close(name_data_fd);
    munmap((void*)uom_data, uom_data_st.st_size);
    close(uom_data_fd);

    return 0;
}
