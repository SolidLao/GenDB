// Build storage extension: orders.o_orderkey.year_idx
// Produces a direct-indexed uint8_t array: year_idx = okey_year[orderkey]
// year_idx = (year - 1992), values 0-6 for 1992-1998, 0xFF for unused slots.
//
// Usage: ./build_ext_okey_year <gendb_dir>

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

static inline int32_t extract_year(int32_t days) {
    int32_t jdn = days + 2440588;
    int32_t a   = jdn + 32044;
    int32_t b2  = (4*a + 3) / 146097;
    int32_t c   = a - (146097*b2) / 4;
    int32_t d2  = (4*c + 3) / 1461;
    int32_t e   = c - (1461*d2) / 4;
    int32_t m   = (5*e + 2) / 153;
    return 100*b2 + d2 - 4800 + (m < 10 ? 0 : 1);
}

static const int32_t* mmap_i32(const std::string& path, int64_t* count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    *count = st.st_size / 4;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    return (const int32_t*)p;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: build_ext_okey_year <gendb_dir>\n");
        return 1;
    }
    const std::string gendb = argv[1];

    int64_t norders = 0;
    const int32_t* okey = mmap_i32(gendb + "/orders/o_orderkey.bin", &norders);

    int64_t ndate = 0;
    const int32_t* odate = mmap_i32(gendb + "/orders/o_orderdate.bin", &ndate);

    if (norders != ndate) {
        fprintf(stderr, "Row count mismatch: okey=%ld odate=%ld\n", norders, ndate);
        return 1;
    }
    printf("norders = %ld\n", norders);

    // Find max orderkey
    int32_t max_okey = 0;
    for (int64_t i = 0; i < norders; i++)
        if (okey[i] > max_okey) max_okey = okey[i];
    printf("max_okey = %d (array size = %.1f MB)\n", max_okey, (max_okey + 1) / 1e6);

    // Allocate direct lookup array — 0xFF = unused slot
    size_t arr_sz = (size_t)(max_okey + 1);
    uint8_t* year_arr = (uint8_t*)malloc(arr_sz);
    if (!year_arr) { perror("malloc"); return 1; }
    memset(year_arr, 0xFF, arr_sz);

    int year_counts[8] = {};
    for (int64_t i = 0; i < norders; i++) {
        int32_t ok = okey[i];
        int32_t yr = extract_year(odate[i]) - 1992;
        if (yr >= 0 && yr <= 6) {
            year_arr[ok] = (uint8_t)yr;
            year_counts[yr]++;
        }
    }

    printf("Year distribution (1992-1998):\n");
    for (int y = 0; y < 7; y++)
        printf("  %d: %d orders\n", 1992 + y, year_counts[y]);

    // Create output directory
    const std::string outdir = gendb + "/column_versions/orders.o_orderkey.year_idx";
    if (system(("mkdir -p " + outdir).c_str()) != 0) {
        fprintf(stderr, "mkdir failed\n"); return 1;
    }

    // Write: 4-byte header (max_okey as int32_t) followed by raw uint8_t array
    const std::string outpath = outdir + "/year_idx.bin";
    FILE* f = fopen(outpath.c_str(), "wb");
    if (!f) { perror("fopen"); return 1; }
    fwrite(&max_okey, sizeof(int32_t), 1, f);
    fwrite(year_arr, 1, arr_sz, f);
    fclose(f);

    printf("Written to %s\n", outpath.c_str());
    printf("File size: %.2f MB\n", (arr_sz + 4) / 1e6);

    free(year_arr);
    return 0;
}
