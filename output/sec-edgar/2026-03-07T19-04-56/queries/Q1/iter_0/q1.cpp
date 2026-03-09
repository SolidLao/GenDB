#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <bitset>
#include <omp.h>
#include "mmap_utils.h"
#include "timing_utils.h"

static constexpr size_t BITSET_SIZE = 86135;
static constexpr int MAX_STMT = 9;
static constexpr int MAX_RFILE = 2;

struct AggState {
    uint64_t count;
    int64_t sum_line;
    std::bitset<BITSET_SIZE> distinct_sub;
    AggState() : count(0), sum_line(0) {}
};

struct Range { uint64_t start, end; };

// Load dictionary: returns vector of strings
static std::vector<std::string> loadDict(const std::string& offsets_path, const std::string& data_path) {
    // Read offsets
    gendb::MmapColumn<uint64_t> offsets(offsets_path);
    // Read data
    int fd = ::open(data_path.c_str(), O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t dsz = st.st_size;
    std::vector<char> data(dsz);
    if (dsz > 0) ::read(fd, data.data(), dsz);
    ::close(fd);

    size_t n = offsets.count - 1; // num entries
    std::vector<std::string> result(n);
    for (size_t i = 0; i < n; i++) {
        result[i] = std::string(data.data() + offsets[i], offsets[i+1] - offsets[i]);
    }
    return result;
}

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];
    std::string pre = gendb_dir + "/pre/";

    double total_ms = 0, data_loading_ms = 0, scan_ms = 0, output_ms = 0;
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // =========== Phase: Data Loading ===========
    auto t_load_start = std::chrono::high_resolution_clock::now();

    // Load dictionaries
    auto stmt_dict = loadDict(pre + "stmt_dict_offsets.bin", pre + "stmt_dict_data.bin");
    auto rfile_dict = loadDict(pre + "rfile_dict_offsets.bin", pre + "rfile_dict_data.bin");

    // Find empty-string stmt code
    int empty_stmt_code = -1;
    for (size_t i = 0; i < stmt_dict.size(); i++) {
        if (stmt_dict[i].empty()) { empty_stmt_code = (int)i; break; }
    }

    // Load stmt_offsets index
    std::ifstream sof(pre + "stmt_offsets.bin", std::ios::binary);
    uint32_t numStmtEntries; sof.read((char*)&numStmtEntries, 4);
    std::vector<Range> stmt_ranges(numStmtEntries);
    sof.read((char*)stmt_ranges.data(), numStmtEntries * sizeof(Range));
    sof.close();

    // Collect valid (non-null) code ranges
    struct CodeRange { int code; uint64_t start, end; };
    std::vector<CodeRange> valid_ranges;
    for (uint32_t c = 0; c < numStmtEntries; c++) {
        if ((int)c == empty_stmt_code) continue;
        if (stmt_ranges[c].end > stmt_ranges[c].start) {
            valid_ranges.push_back({(int)c, stmt_ranges[c].start, stmt_ranges[c].end});
        }
    }

    // mmap columns
    gendb::MmapColumn<uint8_t> rfile_code_col(pre + "rfile_code.bin");
    gendb::MmapColumn<uint32_t> sub_fk_col(pre + "sub_fk.bin");
    gendb::MmapColumn<int16_t> line_col(pre + "line.bin");

    auto t_load_end = std::chrono::high_resolution_clock::now();
    data_loading_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] data_loading: %.2f ms\n", data_loading_ms);

    // =========== Phase: Scan + Aggregate ===========
    auto t_scan_start = std::chrono::high_resolution_clock::now();

    int nthreads = omp_get_max_threads();

    // Thread-local aggregation states
    std::vector<std::vector<AggState>> tls(nthreads, std::vector<AggState>(MAX_STMT * MAX_RFILE));

    // Build work units: split each valid range into morsels
    struct WorkUnit { int code; uint64_t start, end; };
    std::vector<WorkUnit> work;
    constexpr uint64_t MORSEL = 100000;
    for (auto& vr : valid_ranges) {
        for (uint64_t s = vr.start; s < vr.end; s += MORSEL) {
            uint64_t e = std::min(s + MORSEL, vr.end);
            work.push_back({vr.code, s, e});
        }
    }

    const uint8_t* rfile_data = rfile_code_col.data;
    const uint32_t* sub_fk_data = sub_fk_col.data;
    const int16_t* line_data = line_col.data;

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local = tls[tid];

        #pragma omp for schedule(dynamic)
        for (size_t wi = 0; wi < work.size(); wi++) {
            int sc = work[wi].code;
            uint64_t s = work[wi].start;
            uint64_t e = work[wi].end;

            for (uint64_t i = s; i < e; i++) {
                uint8_t rc = rfile_data[i];
                int idx = sc * MAX_RFILE + rc;
                auto& agg = local[idx];
                agg.count++;
                agg.sum_line += line_data[i];
                agg.distinct_sub.set(sub_fk_data[i]);
            }
        }
    }

    // Merge thread-local states
    auto& merged = tls[0];
    for (int t = 1; t < nthreads; t++) {
        for (int g = 0; g < MAX_STMT * MAX_RFILE; g++) {
            merged[g].count += tls[t][g].count;
            merged[g].sum_line += tls[t][g].sum_line;
            merged[g].distinct_sub |= tls[t][g].distinct_sub;
        }
    }

    auto t_scan_end = std::chrono::high_resolution_clock::now();
    scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] main_scan: %.2f ms\n", scan_ms);

    // =========== Phase: Output ===========
    auto t_out_start = std::chrono::high_resolution_clock::now();

    // Collect results
    struct Result {
        std::string stmt, rfile;
        uint64_t cnt;
        uint64_t num_filings;
        double avg_line_num;
    };
    std::vector<Result> results;

    for (int sc = 0; sc < MAX_STMT; sc++) {
        for (int rc = 0; rc < MAX_RFILE; rc++) {
            auto& agg = merged[sc * MAX_RFILE + rc];
            if (agg.count == 0) continue;
            Result r;
            r.stmt = stmt_dict[sc];
            r.rfile = rfile_dict[rc];
            r.cnt = agg.count;
            r.num_filings = agg.distinct_sub.count();
            r.avg_line_num = (double)agg.sum_line / (double)agg.count;
            results.push_back(r);
        }
    }

    // Sort by cnt DESC
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.cnt > b.cnt;
    });

    // Write CSV
    std::string out_path = results_dir + "/Q1.csv";
    FILE* fout = fopen(out_path.c_str(), "w");
    fprintf(fout, "stmt,rfile,cnt,num_filings,avg_line_num\n");
    for (auto& r : results) {
        fprintf(fout, "%s,%s,%lu,%lu,%.2f\n", r.stmt.c_str(), r.rfile.c_str(), r.cnt, r.num_filings, r.avg_line_num);
    }
    fclose(fout);

    auto t_out_end = std::chrono::high_resolution_clock::now();
    output_ms = std::chrono::duration<double, std::milli>(t_out_end - t_out_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    double timing_ms = total_ms - output_ms;
    printf("[TIMING] timing_ms: %.2f ms\n", timing_ms);

    return 0;
}
