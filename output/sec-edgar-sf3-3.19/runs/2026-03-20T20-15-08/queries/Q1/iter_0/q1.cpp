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

// ── Dictionary loader ──────────────────────────────────────────────
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
    uint8_t find_null_code() const {
        for (uint8_t i = 0; i < (uint8_t)entries.size(); i++)
            if (entries[i].empty()) return i;
        return 255;
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
        if (data) munmap((void*)data, fsize);
        if (fd >= 0) ::close(fd);
        data = nullptr; fd = -1;
    }
};

// ── Per-group aggregation ──────────────────────────────────────────
// AggSlot is dynamically sized (bitset depends on adsh dict count).
// We use a flat byte buffer and index into it manually.
struct AggSlotHeader {
    uint64_t count;
    int64_t  line_sum;
    uint64_t line_count;
    // followed by uint64_t adsh_bits[bitset_words]
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

    Dict dict_stmt, dict_rfile, dict_adsh;
    uint8_t null_code;
    size_t max_adsh, bitset_words, slot_size;
    int num_groups;
    {
        GENDB_PHASE("data_loading");
        dict_stmt.load(gendb_dir + "/dictionaries/stmt.dict");
        dict_rfile.load(gendb_dir + "/dictionaries/rfile.dict");
        dict_adsh.load(gendb_dir + "/dictionaries/adsh.dict");
        null_code = dict_stmt.find_null_code();
        max_adsh = dict_adsh.count;
        bitset_words = (max_adsh + 63) / 64;
        slot_size = HEADER_SIZE + bitset_words * sizeof(uint64_t);
        // Composite key: (stmt << 8) | rfile — max 256 groups
        num_groups = 256;

        col_stmt.open(gendb_dir + "/pre/stmt.bin");
        col_rfile.open(gendb_dir + "/pre/rfile.bin");
        col_adsh.open(gendb_dir + "/pre/adsh.bin");
        col_line.open(gendb_dir + "/pre/line.bin");
    }

    const size_t N = col_stmt.count;

    const int num_threads = omp_get_max_threads();

    // Allocate thread-local aggregation arrays (flat byte buffers)
    std::vector<char*> tl_aggs(num_threads);

    {
        GENDB_PHASE("main_scan");

        const uint8_t*  stmt_data  = col_stmt.data;
        const uint8_t*  rfile_data = col_rfile.data;
        const uint32_t* adsh_data  = col_adsh.data;
        const int32_t*  line_data  = col_line.data;
        const size_t ss = slot_size;
        const int ng = num_groups;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            char* my_agg = (char*)calloc(ng, ss);
            tl_aggs[tid] = my_agg;

            #pragma omp for schedule(static)
            for (size_t i = 0; i < N; i++) {
                uint8_t s = stmt_data[i];
                if (s == null_code) continue;

                uint32_t key = ((uint32_t)s << 8) | rfile_data[i];
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

    // ── Merge: parallel across groups ──────────────────────────
    char* global = (char*)calloc(num_groups, slot_size);
    {
        GENDB_PHASE("aggregation");
        #pragma omp parallel for schedule(static)
        for (int g = 0; g < num_groups; g++) {
            AggSlotHeader* dst = (AggSlotHeader*)(global + (size_t)g * slot_size);
            uint64_t* dst_bits = (uint64_t*)(global + (size_t)g * slot_size + HEADER_SIZE);
            for (int t = 0; t < num_threads; t++) {
                AggSlotHeader* src = (AggSlotHeader*)(tl_aggs[t] + (size_t)g * slot_size);
                if (src->count == 0) continue;
                dst->count += src->count;
                dst->line_sum += src->line_sum;
                dst->line_count += src->line_count;
                uint64_t* src_bits = (uint64_t*)(tl_aggs[t] + (size_t)g * slot_size + HEADER_SIZE);
                for (size_t w = 0; w < bitset_words; w++)
                    dst_bits[w] |= src_bits[w];
            }
        }
        for (int t = 0; t < num_threads; t++)
            free(tl_aggs[t]);
    }

    // ── Collect non-empty groups, sort, output ─────────────────
    {
        GENDB_PHASE("output");

        struct Result {
            uint8_t stmt_code, rfile_code;
            uint64_t cnt;
            uint64_t num_filings;
            double avg_line;
        };
        std::vector<Result> results;

        for (int g = 0; g < num_groups; g++) {
            AggSlotHeader* s = (AggSlotHeader*)(global + (size_t)g * slot_size);
            if (s->count == 0) continue;
            uint64_t* bits = (uint64_t*)(global + (size_t)g * slot_size + HEADER_SIZE);
            uint64_t distinct = 0;
            for (size_t w = 0; w < bitset_words; w++)
                distinct += __builtin_popcountll(bits[w]);
            results.push_back({
                (uint8_t)(g >> 8), (uint8_t)(g & 0xFF),
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
