#include "timing_utils.h"
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <algorithm>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

struct MMapFile {
    void* ptr;
    size_t size;
    int fd_;
    MMapFile() : ptr(nullptr), size(0), fd_(-1) {}
    void open(const std::string& path) {
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd_, &st);
        size = st.st_size;
        if (size == 0) { ptr = nullptr; return; }
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path.c_str()); exit(1); }
        madvise(ptr, size, MADV_SEQUENTIAL);
        madvise(ptr, size, MADV_HUGEPAGE);
    }
    void close() {
        if (ptr && size > 0) munmap(ptr, size);
        if (fd_ >= 0) ::close(fd_);
        ptr = nullptr; size = 0; fd_ = -1;
    }
    ~MMapFile() { close(); }
    MMapFile(const MMapFile&) = delete;
    MMapFile& operator=(const MMapFile&) = delete;
};

int main(int argc, char* argv[]) {
    (void)argc;
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    constexpr int64_t NUM_ORDERS = 15000000;
    constexpr int MAX_CUSTKEY = 1500000;

    // Declare mmap objects at outer scope (RAII scoping - munmap after timing)
    MMapFile mmap_custkey, mmap_offsets, mmap_data;

    {
        GENDB_PHASE("total");

        const int32_t* o_custkey;
        const uint32_t* o_comment_offsets;
        const char* o_comment_data;

        int num_threads;

        // Use mmap for per-thread arrays too - anonymous mmap is zero-filled by OS
        // and can use huge pages, much faster than calloc for 384MB
        int32_t** per_thread_counts;

        {
            GENDB_PHASE("data_loading");
            mmap_custkey.open(gendb_dir + "/orders/o_custkey.bin");
            mmap_offsets.open(gendb_dir + "/orders/o_comment_offsets.bin");
            mmap_data.open(gendb_dir + "/orders/o_comment_data.bin");

            o_custkey = (const int32_t*)mmap_custkey.ptr;
            o_comment_offsets = (const uint32_t*)mmap_offsets.ptr;
            o_comment_data = (const char*)mmap_data.ptr;

            num_threads = omp_get_max_threads();
            per_thread_counts = (int32_t**)malloc(num_threads * sizeof(int32_t*));

            // Allocate per-thread arrays using mmap with huge pages
            size_t arr_bytes = (size_t)(MAX_CUSTKEY + 1) * sizeof(int32_t);
            for (int t = 0; t < num_threads; t++) {
                per_thread_counts[t] = (int32_t*)mmap(nullptr, arr_bytes,
                    PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                madvise(per_thread_counts[t], arr_bytes, MADV_HUGEPAGE);
            }
        }

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel
            {
                int tid = omp_get_thread_num();
                int32_t* my_counts = per_thread_counts[tid];

                #pragma omp for schedule(dynamic, 65536)
                for (int64_t i = 0; i < NUM_ORDERS; i++) {
                    uint32_t off_start = o_comment_offsets[i];
                    uint32_t off_end = o_comment_offsets[i + 1];
                    const char* s = o_comment_data + off_start;
                    size_t len = off_end - off_start;

                    // Two-phase memmem: find "special" then "requests"
                    const char* p = (const char*)memmem(s, len, "special", 7);
                    if (p) {
                        size_t remaining = len - (size_t)(p - s) - 7;
                        if (memmem(p + 7, remaining, "requests", 8) != nullptr) {
                            continue; // Matches pattern -> exclude
                        }
                    }

                    my_counts[o_custkey[i]]++;
                }
            }
        }

        // Merge per-thread counts into first array
        {
            GENDB_PHASE("merge_counts");
            int32_t* merged = per_thread_counts[0];
            #pragma omp parallel for schedule(static)
            for (int64_t k = 1; k <= MAX_CUSTKEY; k++) {
                int32_t sum = merged[k];
                for (int t = 1; t < num_threads; t++) {
                    sum += per_thread_counts[t][k];
                }
                merged[k] = sum;
            }
        }

        int32_t* c_count = per_thread_counts[0];

        // Build histogram
        // First find max c_count
        int max_c = 0;
        {
            GENDB_PHASE("build_histogram");
            for (int64_t k = 1; k <= MAX_CUSTKEY; k++) {
                if (c_count[k] > max_c) max_c = c_count[k];
            }
        }

        std::vector<int64_t> custdist(max_c + 1, 0);
        for (int64_t k = 1; k <= MAX_CUSTKEY; k++) {
            custdist[c_count[k]]++;
        }

        // Sort: custdist DESC, c_count DESC
        struct Row { int32_t cc; int64_t cd; };
        std::vector<Row> rows;
        rows.reserve(max_c + 1);
        for (int i = 0; i <= max_c; i++) {
            if (custdist[i] > 0) {
                rows.push_back({i, custdist[i]});
            }
        }
        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            if (a.cd != b.cd) return a.cd > b.cd;
            return a.cc > b.cc;
        });

        {
            GENDB_PHASE("output");
            std::string outpath = results_dir + "/Q13.csv";
            FILE* f = fopen(outpath.c_str(), "w");
            fprintf(f, "c_count,custdist\n");
            for (auto& r : rows) {
                fprintf(f, "%d,%ld\n", r.cc, r.cd);
            }
            fclose(f);
        }

        // Free per-thread arrays
        size_t arr_bytes = (size_t)(MAX_CUSTKEY + 1) * sizeof(int32_t);
        for (int t = 0; t < num_threads; t++) {
            munmap(per_thread_counts[t], arr_bytes);
        }
        free(per_thread_counts);
    }

    return 0;
}
