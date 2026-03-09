// Build lineitem.l_net_price column version
// net_price[i] = l_extendedprice[i] * (1.0 - l_discount[i])
// Output: double[59986052] at <gendb_dir>/column_versions/lineitem.l_net_price/net_price.bin
// Usage: ./build_ext_net_price <gendb_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>

static void* mmap_ro(const char* path, size_t* out_sz) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    *out_sz = st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    return p;
}

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: build_ext_net_price <gendb_dir>\n"); return 1; }
    std::string gd = argv[1];

    // Read l_extendedprice and l_discount
    size_t ep_sz = 0, dc_sz = 0;
    const double* ep = (const double*)mmap_ro((gd + "/lineitem/l_extendedprice.bin").c_str(), &ep_sz);
    const double* dc = (const double*)mmap_ro((gd + "/lineitem/l_discount.bin").c_str(), &dc_sz);

    int64_t nrows = (int64_t)(ep_sz / sizeof(double));
    fprintf(stderr, "Row count: %ld\n", nrows);
    if (nrows != 59986052) {
        fprintf(stderr, "WARNING: expected 59986052 rows, got %ld\n", nrows);
    }

    // Advise sequential reads
    madvise((void*)ep, ep_sz, MADV_SEQUENTIAL);
    madvise((void*)dc, dc_sz, MADV_SEQUENTIAL);

    // Create output directory
    std::string outdir = gd + "/column_versions/lineitem.l_net_price";
    mkdir(outdir.c_str(), 0755);

    // Allocate output buffer and compute
    std::string outpath = outdir + "/net_price.bin";
    FILE* f = fopen(outpath.c_str(), "wb");
    if (!f) { perror("fopen output"); return 1; }

    // Process in chunks to avoid large malloc
    const int64_t CHUNK = 4096 * 1024; // 4M rows at a time
    double* buf = (double*)malloc(CHUNK * sizeof(double));
    if (!buf) { perror("malloc"); return 1; }

    for (int64_t start = 0; start < nrows; start += CHUNK) {
        int64_t end = start + CHUNK;
        if (end > nrows) end = nrows;
        int64_t n = end - start;
        for (int64_t i = 0; i < n; i++) {
            buf[i] = ep[start + i] * (1.0 - dc[start + i]);
        }
        if (fwrite(buf, sizeof(double), n, f) != (size_t)n) {
            perror("fwrite"); return 1;
        }
    }
    fclose(f);
    free(buf);

    munmap((void*)ep, ep_sz);
    munmap((void*)dc, dc_sz);

    fprintf(stderr, "Written: %s (%ld rows, %.1f MB)\n",
            outpath.c_str(), nrows, nrows * 8.0 / (1024*1024));
    return 0;
}
