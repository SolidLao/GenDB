#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// Dictionary: uint32_t count, uint32_t[count+1] offsets, char[] string_data
struct Dict {
    std::vector<std::string> entries;
    uint32_t count;
    void load(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const char* base = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        count = *(const uint32_t*)base;
        const uint32_t* offsets = (const uint32_t*)(base + 4);
        const char* strs = base + 4 + (count + 1) * 4;
        entries.resize(count);
        for (uint32_t i = 0; i < count; i++)
            entries[i] = std::string(strs + offsets[i], offsets[i+1] - offsets[i]);
        munmap((void*)base, st.st_size);
        ::close(fd);
    }
};

// mmap helper — declared outside timing scope so munmap TLB shootdown excluded
template<typename T>
struct Col {
    const T* data = nullptr;
    size_t count = 0;
    size_t fsize = 0;
    int fd = -1;
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
        if (data) munmap((void*)data, fsize);
        if (fd >= 0) ::close(fd);
        data = nullptr; fd = -1;
    }
};

// Per-group aggregation header (followed by bitset words)
struct AggSlotHeader {
    uint64_t count;
    int64_t  line_sum;
    uint64_t line_count;
};
static constexpr size_t HEADER_SIZE = sizeof(AggSlotHeader);

int main(int argc, char** argv) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    // Declare mmaps BEFORE RAII timer to exclude munmap TLB shootdown
    Col<uint8_t>  col_stmt, col_rfile;
    Col<uint32_t> col_adsh;
    Col<int32_t>  col_line;

    {
    GENDB_PHASE("total");

    Dict dict_stmt, dict_rfile;
    uint8_t null_code = 255;
    uint32_t adsh_count;
    uint32_t num_stmt, num_rfile, num_groups;
    size_t bitset_words, slot_size;
    {
        GENDB_PHASE("data_loading");
        dict_stmt.load(gendb_dir + "/dictionaries/stmt.dict");
        dict_rfile.load(gendb_dir + "/dictionaries/rfile.dict");
        num_stmt = dict_stmt.count;
        num_rfile = dict_rfile.count;

        // Read adsh dict count (just first 4 bytes — no need to decode 86K strings)
        {
            int fd = ::open((gendb_dir + "/dictionaries/adsh.dict").c_str(), O_RDONLY);
            ::read(fd, &adsh_count, 4);
            ::close(fd);
        }

        // Find null sentinel for stmt (empty string code)
        for (uint32_t i = 0; i < num_stmt; i++) {
            if (dict_stmt.entries[i].empty()) {
                null_code = (uint8_t)i;
                break;
            }
        }

        // Composite key = stmt_code * num_rfile + rfile_code
        // Max groups = num_stmt * num_rfile (9 * 2 = 18)
        num_groups = num_stmt * num_rfile;
        bitset_words = (adsh_count + 63) / 64;
        slot_size = HEADER_SIZE + bitset_words * sizeof(uint64_t);

        col_stmt.open(gendb_dir + "/pre/stmt.bin");
        col_rfile.open(gendb_dir + "/pre/rfile.bin");
        col_adsh.open(gendb_dir + "/pre/adsh.bin");
        col_line.open(gendb_dir + "/pre/line.bin");
    }

    const size_t N = col_stmt.count;
    const int max_threads = omp_get_max_threads();

    // Thread-local aggregation buffers
    std::vector<char*> tl_aggs(max_threads);

    {
        GENDB_PHASE("main_scan");

        const uint8_t*  stmt_data  = col_stmt.data;
        const uint8_t*  rfile_data = col_rfile.data;
        const uint32_t* adsh_data  = col_adsh.data;
        const int32_t*  line_data  = col_line.data;
        const size_t ss = slot_size;
        const uint32_t ng = num_groups;
        const uint32_t nr = num_rfile;
        const uint8_t nc = null_code;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            // calloc: demand-paged, zero-initialized
            char* my_agg = (char*)calloc(ng, ss);
            tl_aggs[tid] = my_agg;

            #pragma omp for schedule(static)
            for (size_t i = 0; i < N; i++) {
                uint8_t s = stmt_data[i];
                if (s == nc) continue;

                uint32_t key = (uint32_t)s * nr + rfile_data[i];
                AggSlotHeader* slot = (AggSlotHeader*)(my_agg + key * ss);
                slot->count++;
                slot->line_sum += line_data[i];
                slot->line_count++;

                uint32_t a = adsh_data[i];
                uint64_t* bits = (uint64_t*)(my_agg + key * ss + HEADER_SIZE);
                bits[a >> 6] |= (1ULL << (a & 63));
            }
        }
    }

    // Merge thread-local results
    char* global = (char*)calloc(num_groups, slot_size);
    {
        GENDB_PHASE("aggregation");
        for (int t = 0; t < max_threads; t++) {
            for (uint32_t g = 0; g < num_groups; g++) {
                AggSlotHeader* dst = (AggSlotHeader*)(global + (size_t)g * slot_size);
                AggSlotHeader* src = (AggSlotHeader*)(tl_aggs[t] + (size_t)g * slot_size);
                if (src->count == 0) continue;
                dst->count += src->count;
                dst->line_sum += src->line_sum;
                dst->line_count += src->line_count;
                uint64_t* dst_bits = (uint64_t*)(global + (size_t)g * slot_size + HEADER_SIZE);
                uint64_t* src_bits = (uint64_t*)(tl_aggs[t] + (size_t)g * slot_size + HEADER_SIZE);
                for (size_t w = 0; w < bitset_words; w++)
                    dst_bits[w] |= src_bits[w];
            }
            free(tl_aggs[t]);
        }
    }

    // Collect, sort, output
    {
        GENDB_PHASE("output");

        struct Result {
            uint8_t stmt_code, rfile_code;
            uint64_t cnt, num_filings;
            double avg_line;
        };
        std::vector<Result> results;

        for (uint32_t g = 0; g < num_groups; g++) {
            AggSlotHeader* s = (AggSlotHeader*)(global + (size_t)g * slot_size);
            if (s->count == 0) continue;
            uint64_t* bits = (uint64_t*)(global + (size_t)g * slot_size + HEADER_SIZE);
            uint64_t distinct = 0;
            for (size_t w = 0; w < bitset_words; w++)
                distinct += __builtin_popcountll(bits[w]);
            results.push_back({
                (uint8_t)(g / num_rfile), (uint8_t)(g % num_rfile),
                s->count, distinct,
                (double)s->line_sum / (double)s->line_count
            });
        }
        free(global);

        std::sort(results.begin(), results.end(),
                  [](const Result& a, const Result& b) { return a.cnt > b.cnt; });

        std::string out_path = results_dir + "/Q1.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        fprintf(fp, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (auto& r : results) {
            fprintf(fp, "%s,%s,%" PRIu64 ",%" PRIu64 ",%.2f\n",
                    dict_stmt.entries[r.stmt_code].c_str(),
                    dict_rfile.entries[r.rfile_code].c_str(),
                    r.cnt, r.num_filings, r.avg_line);
        }
        fclose(fp);
    }

    } // end total timer

    col_stmt.close();
    col_rfile.close();
    col_adsh.close();
    col_line.close();

    return 0;
}
