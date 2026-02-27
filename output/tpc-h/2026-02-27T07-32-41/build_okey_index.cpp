// build_okey_index.cpp: Build lineitem secondary index sorted by orderkey
// Creates gendb/lineitem_by_okey/ with all key columns in orderkey-sorted order
// Also creates okey_offsets.bin for direct orderkey-based access
// Usage: ./build_okey_index <gendb_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <omp.h>
#include "mmap_utils.h"
using namespace gendb;

static void mkdir_p(const char* path) {
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char* p = tmp+1; *p; p++) {
        if (*p == '/') { *p = '\0'; mkdir(tmp, 0755); *p = '/'; }
    }
    mkdir(tmp, 0755);
}

// Write a column to disk (sequential write)
template<typename T>
static void write_column(const char* path, const T* data, size_t count) {
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { fprintf(stderr, "Cannot open %s: %s\n", path, strerror(errno)); exit(1); }
    size_t total = count * sizeof(T);
    const char* buf = (const char*)data;
    while (total > 0) {
        ssize_t written = write(fd, buf, total > (1<<26) ? (1<<26) : total);
        if (written <= 0) { fprintf(stderr, "Write error: %s\n", strerror(errno)); exit(1); }
        buf += written; total -= written;
    }
    close(fd);
}

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    std::string gd = argv[1];

    double t0 = ({ struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); ts.tv_sec * 1e3 + ts.tv_nsec * 1e-6; });

    const int MAX_OKEY = 60000001;

    printf("[build_okey_index] Loading lineitem orderkey column...\n");
    MmapColumn<int32_t> l_ok(gd+"/lineitem/l_orderkey.bin");
    size_t n = l_ok.count;
    printf("[build_okey_index] n=%zu rows\n", n);

    // === Phase 1: Counting sort ===
    // count[k] = number of lineitem rows with orderkey k
    printf("[build_okey_index] Phase 1: counting...\n");
    uint32_t* count = (uint32_t*)calloc(MAX_OKEY, sizeof(uint32_t));
    if (!count) { fprintf(stderr, "OOM\n"); return 1; }

    const int32_t* OK = l_ok.data;

    // Parallel count with thread-local arrays to avoid atomic overhead
    int nth = omp_get_max_threads();
    uint32_t** local_counts = new uint32_t*[nth];
    for (int t = 0; t < nth; t++) {
        local_counts[t] = (uint32_t*)calloc(MAX_OKEY, sizeof(uint32_t));
    }

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        uint32_t* lc = local_counts[tid];
        #pragma omp for schedule(static) nowait
        for (size_t i = 0; i < n; i++) {
            int32_t k = OK[i];
            if (k > 0 && k < MAX_OKEY) lc[k]++;
        }
    }

    // Merge local counts
    #pragma omp parallel for schedule(static)
    for (int k = 0; k < MAX_OKEY; k++) {
        uint32_t s = 0;
        for (int t = 0; t < nth; t++) s += local_counts[t][k];
        count[k] = s;
    }
    for (int t = 0; t < nth; t++) { free(local_counts[t]); }
    delete[] local_counts;

    // === Phase 2: Compute okey_offsets (prefix sums) ===
    printf("[build_okey_index] Phase 2: prefix sums...\n");
    uint32_t* okey_offsets = (uint32_t*)malloc((size_t)MAX_OKEY * sizeof(uint32_t));
    if (!okey_offsets) { fprintf(stderr, "OOM\n"); return 1; }

    okey_offsets[0] = 0;
    for (int k = 1; k < MAX_OKEY; k++)
        okey_offsets[k] = okey_offsets[k-1] + count[k-1];

    // positions[] = working copy of okey_offsets for scatter
    uint32_t* positions = (uint32_t*)malloc((size_t)MAX_OKEY * sizeof(uint32_t));
    memcpy(positions, okey_offsets, (size_t)MAX_OKEY * sizeof(uint32_t));
    free(count);

    // Create output directory
    std::string outdir = gd + "/lineitem_by_okey";
    mkdir_p(outdir.c_str());

    // === Phase 3: Scatter passes — one per column ===
    // Reset positions before each column scatter
    // Since positions are modified in-place, we restore from okey_offsets each time

    // Helper: scatter column T from input to output using positions[]
    auto scatter_col = [&](auto* in_data, auto* out_data, size_t nn) {
        // Reset positions to okey_offsets
        memcpy(positions, okey_offsets, (size_t)MAX_OKEY * sizeof(uint32_t));
        for (size_t i = 0; i < nn; i++) {
            int32_t k = OK[i];
            if (k > 0 && k < MAX_OKEY) {
                out_data[positions[k]++] = in_data[i];
            }
        }
    };

    // l_orderkey (sorted, for verification)
    printf("[build_okey_index] Scattering l_orderkey...\n");
    {
        int32_t* out = (int32_t*)malloc(n * sizeof(int32_t));
        scatter_col(l_ok.data, out, n);
        write_column((outdir+"/l_orderkey.bin").c_str(), out, n);
        free(out);
    }

    // l_quantity
    printf("[build_okey_index] Scattering l_quantity...\n");
    {
        MmapColumn<uint8_t> col(gd+"/lineitem/l_quantity.bin");
        uint8_t* out = (uint8_t*)malloc(n);
        scatter_col(col.data, out, n);
        write_column((outdir+"/l_quantity.bin").c_str(), out, n);
        free(out);
    }

    // l_extprice
    printf("[build_okey_index] Scattering l_extprice...\n");
    {
        MmapColumn<int32_t> col(gd+"/lineitem/l_extprice.bin");
        int32_t* out = (int32_t*)malloc(n * sizeof(int32_t));
        scatter_col(col.data, out, n);
        write_column((outdir+"/l_extprice.bin").c_str(), out, n);
        free(out);
    }

    // l_discount
    printf("[build_okey_index] Scattering l_discount...\n");
    {
        MmapColumn<uint8_t> col(gd+"/lineitem/l_discount.bin");
        uint8_t* out = (uint8_t*)malloc(n);
        scatter_col(col.data, out, n);
        write_column((outdir+"/l_discount.bin").c_str(), out, n);
        free(out);
    }

    // l_shipdate
    printf("[build_okey_index] Scattering l_shipdate...\n");
    {
        MmapColumn<uint16_t> col(gd+"/lineitem/l_shipdate.bin");
        uint16_t* out = (uint16_t*)malloc(n * sizeof(uint16_t));
        scatter_col(col.data, out, n);
        write_column((outdir+"/l_shipdate.bin").c_str(), out, n);
        free(out);
    }

    // l_partkey
    printf("[build_okey_index] Scattering l_partkey...\n");
    {
        MmapColumn<int32_t> col(gd+"/lineitem/l_partkey.bin");
        int32_t* out = (int32_t*)malloc(n * sizeof(int32_t));
        scatter_col(col.data, out, n);
        write_column((outdir+"/l_partkey.bin").c_str(), out, n);
        free(out);
    }

    // l_suppkey
    printf("[build_okey_index] Scattering l_suppkey...\n");
    {
        MmapColumn<int32_t> col(gd+"/lineitem/l_suppkey.bin");
        int32_t* out = (int32_t*)malloc(n * sizeof(int32_t));
        scatter_col(col.data, out, n);
        write_column((outdir+"/l_suppkey.bin").c_str(), out, n);
        free(out);
    }

    // Write okey_offsets
    printf("[build_okey_index] Writing okey_offsets...\n");
    write_column((outdir+"/okey_offsets.bin").c_str(), okey_offsets, MAX_OKEY);

    free(okey_offsets);
    free(positions);

    double t1 = ({ struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); ts.tv_sec * 1e3 + ts.tv_nsec * 1e-6; });
    printf("[build_okey_index] Done. Total time: %.1f ms\n", t1-t0);
    printf("[INGEST_TIME_MS] %.1f\n", t1-t0);

    return 0;
}
