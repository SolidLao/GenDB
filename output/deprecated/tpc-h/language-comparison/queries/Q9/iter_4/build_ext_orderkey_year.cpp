// Build storage extension: orderkey -> year_offset direct lookup
// Eliminates orders_orderkey_lookup.bin (229MB) + o_orderdate.bin (57MB) + year_from_days()
// Output: uint8_t[max_orderkey+1] where entry[ok] = year_from_days(orderdate) - 1992

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

static inline int year_from_days(int z) {
    z += 719468;
    int era = (z >= 0 ? z : z - 146097) / 146097;
    int doe = z - era * 146097;
    int yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int y = yoe + era * 400;
    int doy = doe - (365*yoe + yoe/4 - yoe/100);
    int mp = (5*doy + 2) / 153;
    int m = mp + (mp < 10 ? 3 : -9);
    return y + (m <= 2);
}

int main(int argc, char** argv) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    char path[4096];

    // Read orders_orderkey_lookup.bin
    snprintf(path, sizeof(path), "%s/indexes/orders_orderkey_lookup.bin", argv[1]);
    int fd1 = open(path, O_RDONLY);
    struct stat st1; fstat(fd1, &st1);
    auto* lookup_raw = (const char*)mmap(nullptr, st1.st_size, PROT_READ, MAP_PRIVATE, fd1, 0);
    close(fd1);
    uint32_t max_ok = *(const uint32_t*)lookup_raw;
    const int32_t* lookup = (const int32_t*)(lookup_raw + 4);

    // Read o_orderdate.bin
    snprintf(path, sizeof(path), "%s/orders/o_orderdate.bin", argv[1]);
    int fd2 = open(path, O_RDONLY);
    struct stat st2; fstat(fd2, &st2);
    const int32_t* orderdate = (const int32_t*)mmap(nullptr, st2.st_size, PROT_READ, MAP_PRIVATE, fd2, 0);
    close(fd2);
    uint32_t n_orders = st2.st_size / 4;

    // Build year_offset array
    uint32_t n_entries = max_ok + 1;
    auto* year_off = new uint8_t[n_entries];
    memset(year_off, 0xFF, n_entries); // sentinel for invalid

    uint32_t valid = 0;
    int min_year = 9999, max_year = 0;
    for (uint32_t ok = 0; ok < n_entries; ok++) {
        int32_t row = lookup[ok];
        if (row >= 0 && (uint32_t)row < n_orders) {
            int y = year_from_days(orderdate[row]);
            year_off[ok] = (uint8_t)(y - 1992);
            valid++;
            if (y < min_year) min_year = y;
            if (y > max_year) max_year = y;
        }
    }

    // Write output
    snprintf(path, sizeof(path), "%s/column_versions/orders.orderkey_year.direct_lookup/year_offset.bin", argv[1]);
    FILE* fp = fopen(path, "wb");
    // Header: uint32_t max_orderkey
    fwrite(&max_ok, 4, 1, fp);
    fwrite(year_off, 1, n_entries, fp);
    fclose(fp);

    printf("max_orderkey=%u, valid_orders=%u, n_orders=%u, year_range=[%d,%d]\n",
           max_ok, valid, n_orders, min_year, max_year);
    printf("Output size: %u bytes (%.1f MB)\n", 4 + n_entries, (4 + n_entries) / 1048576.0);

    delete[] year_off;
    munmap((void*)lookup_raw, st1.st_size);
    munmap((void*)orderdate, st2.st_size);
    return 0;
}
