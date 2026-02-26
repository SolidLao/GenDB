// build_ext_compact.cpp
// Builds compact integer encodings for Q1 columns that have integer semantics
// but are stored as double in the base tables.
//
// Encodings built:
//   lineitem.l_quantity.uint8   : uint8_t  = (uint8_t)round(qty_f64)    values 1..50
//   lineitem.l_discount.uint8   : uint8_t  = (uint8_t)round(disc_f64*100) values 0..10
//   lineitem.l_tax.uint8        : uint8_t  = (uint8_t)round(tax_f64*100)  values 0..8
//   lineitem.l_extendedprice.int32 : int32_t = (int32_t)round(price_f64*100) max ~5,495,000
//
// Usage: ./build_ext_compact <gendb_dir>
// Output: <gendb_dir>/column_versions/{id}/
//
// These compact encodings reduce the Q1 column scan from 2.04 GB to 0.54 GB (3.7x),
// and enable pure integer arithmetic in the inner loop (eliminating long double / x87).

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cassert>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

static const double* mmap_doubles(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { fprintf(stderr, "Cannot open %s: %s\n", path.c_str(), strerror(errno)); exit(1); }
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    n_rows = sz / sizeof(double);
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed for %s\n", path.c_str()); exit(1); }
    madvise(ptr, sz, MADV_SEQUENTIAL);
    close(fd);
    return (const double*)ptr;
}

// Write a binary file from a buffer
static bool write_file(const std::string& path, const void* buf, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot create %s: %s\n", path.c_str(), strerror(errno)); return false; }
    size_t written = fwrite(buf, 1, bytes, f);
    fclose(f);
    if (written != bytes) { fprintf(stderr, "Short write to %s\n", path.c_str()); return false; }
    return true;
}

static void mkdir_p(const std::string& path) {
    std::string cmd = "mkdir -p " + path;
    if (system(cmd.c_str()) != 0) {
        fprintf(stderr, "Failed to create directory: %s\n", path.c_str());
        exit(1);
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir = argv[1];
    const std::string li = gendb_dir + "/lineitem/";
    const std::string cv = gendb_dir + "/column_versions/";

    // -------------------------------------------------------------------------
    // 1. l_quantity -> uint8_t (values 1..50, stored as exact integer)
    // -------------------------------------------------------------------------
    {
        printf("Building lineitem.l_quantity.uint8 ...\n");
        size_t n;
        const double* src = mmap_doubles(li + "l_quantity.bin", n);
        printf("  Rows: %zu\n", n);

        uint8_t* dst = (uint8_t*)malloc(n);
        if (!dst) { fprintf(stderr, "OOM\n"); exit(1); }

        uint8_t mn = 255, mx = 0;
        for (size_t i = 0; i < n; i++) {
            uint8_t v = (uint8_t)(src[i] + 0.5); // round to nearest integer
            dst[i] = v;
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }
        printf("  Value range: [%u, %u]\n", (unsigned)mn, (unsigned)mx);

        mkdir_p(cv + "lineitem.l_quantity.uint8");
        if (!write_file(cv + "lineitem.l_quantity.uint8/qty.bin", dst, n))
            exit(1);
        printf("  Written: %s (%zu bytes)\n", (cv + "lineitem.l_quantity.uint8/qty.bin").c_str(), n);
        free(dst);
        munmap((void*)src, n * sizeof(double));
    }

    // -------------------------------------------------------------------------
    // 2. l_discount -> uint8_t (values 0..10, representing 0.00..0.10 in units of 0.01)
    // -------------------------------------------------------------------------
    {
        printf("Building lineitem.l_discount.uint8 ...\n");
        size_t n;
        const double* src = mmap_doubles(li + "l_discount.bin", n);

        uint8_t* dst = (uint8_t*)malloc(n);
        if (!dst) { fprintf(stderr, "OOM\n"); exit(1); }

        uint8_t mn = 255, mx = 0;
        for (size_t i = 0; i < n; i++) {
            uint8_t v = (uint8_t)(src[i] * 100.0 + 0.5); // round to nearest centesimal
            dst[i] = v;
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }
        printf("  Value range: [%u, %u] (×0.01 → %.2f..%.2f)\n",
               (unsigned)mn, (unsigned)mx, mn*0.01, mx*0.01);

        mkdir_p(cv + "lineitem.l_discount.uint8");
        if (!write_file(cv + "lineitem.l_discount.uint8/disc.bin", dst, n))
            exit(1);
        printf("  Written: %s (%zu bytes)\n", (cv + "lineitem.l_discount.uint8/disc.bin").c_str(), n);
        free(dst);
        munmap((void*)src, n * sizeof(double));
    }

    // -------------------------------------------------------------------------
    // 3. l_tax -> uint8_t (values 0..8, representing 0.00..0.08 in units of 0.01)
    // -------------------------------------------------------------------------
    {
        printf("Building lineitem.l_tax.uint8 ...\n");
        size_t n;
        const double* src = mmap_doubles(li + "l_tax.bin", n);

        uint8_t* dst = (uint8_t*)malloc(n);
        if (!dst) { fprintf(stderr, "OOM\n"); exit(1); }

        uint8_t mn = 255, mx = 0;
        for (size_t i = 0; i < n; i++) {
            uint8_t v = (uint8_t)(src[i] * 100.0 + 0.5);
            dst[i] = v;
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }
        printf("  Value range: [%u, %u] (×0.01 → %.2f..%.2f)\n",
               (unsigned)mn, (unsigned)mx, mn*0.01, mx*0.01);

        mkdir_p(cv + "lineitem.l_tax.uint8");
        if (!write_file(cv + "lineitem.l_tax.uint8/tax.bin", dst, n))
            exit(1);
        printf("  Written: %s (%zu bytes)\n", (cv + "lineitem.l_tax.uint8/tax.bin").c_str(), n);
        free(dst);
        munmap((void*)src, n * sizeof(double));
    }

    // -------------------------------------------------------------------------
    // 4. l_extendedprice -> int32_t (values in centiprice units, i.e. ×100)
    //    TPC-H: l_extendedprice = l_quantity × retailprice, max ≈ 50 × 1099.00 = 54,950.00
    //    As int32_t ×100: max = 5,495,000 << INT32_MAX (2,147,483,647) — safe
    // -------------------------------------------------------------------------
    {
        printf("Building lineitem.l_extendedprice.int32 ...\n");
        size_t n;
        const double* src = mmap_doubles(li + "l_extendedprice.bin", n);

        int32_t* dst = (int32_t*)malloc(n * sizeof(int32_t));
        if (!dst) { fprintf(stderr, "OOM\n"); exit(1); }

        int32_t mn = INT32_MAX, mx = INT32_MIN;
        for (size_t i = 0; i < n; i++) {
            int32_t v = (int32_t)(src[i] * 100.0 + 0.5);
            dst[i] = v;
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }
        printf("  Value range: [%d, %d] (×0.01 → %.2f..%.2f)\n",
               mn, mx, mn*0.01, mx*0.01);

        mkdir_p(cv + "lineitem.l_extendedprice.int32");
        if (!write_file(cv + "lineitem.l_extendedprice.int32/price.bin", dst, n * sizeof(int32_t)))
            exit(1);
        printf("  Written: %s (%zu bytes)\n",
               (cv + "lineitem.l_extendedprice.int32/price.bin").c_str(), n * sizeof(int32_t));
        free(dst);
        munmap((void*)src, n * sizeof(double));
    }

    printf("\nAll compact columns built successfully.\n");
    printf("Data reduction: 4×480MB double → 3×60MB uint8 + 1×240MB int32 = 420MB total\n");
    printf("(vs original 1920MB for these 4 columns — 4.6× smaller)\n");
    return 0;
}
