// Build persistent orderkey->year_offset nibble file for Q9 (and any query needing o_year).
// Output: column_versions/orders.o_orderkey_to_year_nibble/oky_nibble.bin
//   Format: uint8_t[(MAX_ORDERKEY/2)+1], nibble-packed.
//   For orderkey ok:
//     byte_idx = ok >> 1
//     shift    = (ok & 1) << 2    // 0 for even, 4 for odd
//     nibble   = (oky_nibble[byte_idx] >> shift) & 0xF
//   Values: 0x0-0x6 = year_offset (0 = 1992, 6 = 1998), 0xF = not_found
// Stored as raw file; on hot runs the OS keeps it in page cache.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

// Howard Hinnant civil_from_days — inverse of date_to_days
static int year_from_days(int32_t z) {
    z += 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int y = (int)yoe + era * 400;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp = (5*doy + 2) / 153;
    if (mp >= 10) y++;
    return y;
}

template<typename T>
static const T* mmap_ro(const char* path, size_t& n_out) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, sz, MADV_SEQUENTIAL);
    close(fd);
    n_out = sz / sizeof(T);
    return reinterpret_cast<const T*>(p);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    const char* db = argv[1];
    auto t0 = std::chrono::steady_clock::now();

    // Read orders columns
    char path_ok[512], path_od[512];
    snprintf(path_ok, sizeof(path_ok), "%s/orders/o_orderkey.bin", db);
    snprintf(path_od, sizeof(path_od), "%s/orders/o_orderdate.bin", db);

    size_t n_ok, n_od;
    const int32_t* o_orderkey = mmap_ro<int32_t>(path_ok, n_ok);
    const int32_t* o_orderdate = mmap_ro<int32_t>(path_od, n_od);

    if (n_ok != n_od) {
        fprintf(stderr, "Row count mismatch: orderkey=%zu orderdate=%zu\n", n_ok, n_od);
        return 1;
    }
    size_t n_orders = n_ok;
    printf("Orders rows: %zu\n", n_orders);

    // Find max orderkey
    int32_t max_ok = 0;
    for (size_t i = 0; i < n_orders; i++) {
        if (o_orderkey[i] > max_ok) max_ok = o_orderkey[i];
    }
    printf("Max orderkey: %d\n", max_ok);

    // Nibble array: (max_ok/2) + 1 bytes, init 0xFF
    size_t nibble_bytes = (size_t)(max_ok / 2) + 2; // +2 for safety
    uint8_t* nibble = (uint8_t*)malloc(nibble_bytes);
    if (!nibble) { perror("malloc"); return 1; }
    memset(nibble, 0xFF, nibble_bytes);

    constexpr int BASE_YEAR = 1992;
    constexpr int MAX_YEAR_OFF = 6;  // 1998 - 1992

    int in_range = 0, out_range = 0;
    for (size_t i = 0; i < n_orders; i++) {
        int32_t ok = o_orderkey[i];
        if (ok <= 0 || ok > max_ok) continue;
        int yr = year_from_days(o_orderdate[i]);
        int yr_off = yr - BASE_YEAR;
        if (yr_off < 0 || yr_off > MAX_YEAR_OFF) { out_range++; continue; }
        in_range++;
        // Write nibble
        int byte_idx = ok >> 1;
        int shift = (ok & 1) << 2;  // 0 or 4
        nibble[byte_idx] = (nibble[byte_idx] & ~(0xF << shift)) | ((yr_off & 0xF) << shift);
    }
    printf("In-range entries: %d, out-of-range: %d\n", in_range, out_range);

    // Write output file
    char out_dir[512], out_file[512];
    snprintf(out_dir,  sizeof(out_dir),  "%s/column_versions/orders.o_orderkey_to_year_nibble", db);
    snprintf(out_file, sizeof(out_file), "%s/oky_nibble.bin", out_dir);

    // mkdir -p (already created externally, but check)
    FILE* f = fopen(out_file, "wb");
    if (!f) { perror(out_file); return 1; }
    size_t written = fwrite(nibble, 1, nibble_bytes, f);
    fclose(f);
    free(nibble);

    auto t1 = std::chrono::steady_clock::now();
    long ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1-t0).count();
    printf("Written %zu bytes to %s in %ldms\n", written, out_file, ms);
    printf("max_ok=%d nibble_bytes=%zu\n", max_ok, nibble_bytes);
    return 0;
}
