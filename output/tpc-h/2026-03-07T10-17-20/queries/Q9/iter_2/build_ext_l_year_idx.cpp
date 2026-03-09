// Build lineitem.l_year_idx column version
// Precomputes year_idx (uint8_t) for every lineitem row by looking up
// l_orderkey[i] in the existing orders.o_orderkey.year_idx extension.
// Output: uint8_t[n_lineitem_rows], stored sequentially.
// Sequential access during scan replaces random 60MB okey_year_arr access.
//
// Usage: ./build_ext_l_year_idx <gendb_dir>

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>

static void* mmap_ro(const char* path, size_t* out_size) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st;
    fstat(fd, &st);
    *out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, *out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    return ptr;
}

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: build_ext_l_year_idx <gendb_dir>\n"); return 1; }
    const std::string gendb = argv[1];

    // --- Load existing orders.o_orderkey.year_idx extension ---
    std::string yidx_path = gendb + "/column_versions/orders.o_orderkey.year_idx/year_idx.bin";
    size_t yidx_sz = 0;
    void* yidx_ptr = mmap_ro(yidx_path.c_str(), &yidx_sz);
    int32_t max_okey = *(const int32_t*)yidx_ptr;
    const uint8_t* okey_year_arr = (const uint8_t*)((const char*)yidx_ptr + 4);
    printf("max_okey = %d\n", max_okey);

    // --- Load l_orderkey.bin ---
    std::string okey_path = gendb + "/lineitem/l_orderkey.bin";
    size_t okey_sz = 0;
    const int32_t* l_orderkey = (const int32_t*)mmap_ro(okey_path.c_str(), &okey_sz);
    int64_t n_rows = (int64_t)(okey_sz / sizeof(int32_t));
    printf("lineitem rows = %ld\n", n_rows);

    // --- Build output ---
    // Create output directory
    std::string out_dir = gendb + "/column_versions/lineitem.l_year_idx";
    mkdir(out_dir.c_str(), 0755);

    // Allocate output buffer
    uint8_t* year_idx_out = (uint8_t*)malloc((size_t)n_rows);
    if (!year_idx_out) { perror("malloc"); return 1; }

    // Fill: for each lineitem row, look up year_idx from okey_year_arr
    for (int64_t i = 0; i < n_rows; i++) {
        int32_t okey = l_orderkey[i];
        if (okey >= 0 && okey <= max_okey) {
            year_idx_out[i] = okey_year_arr[okey];
        } else {
            year_idx_out[i] = 0xFF;
        }
    }

    // Write output
    std::string out_path = out_dir + "/year_idx.bin";
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { perror(out_path.c_str()); return 1; }
    size_t written = fwrite(year_idx_out, 1, (size_t)n_rows, f);
    fclose(f);
    free(year_idx_out);

    if (written != (size_t)n_rows) {
        fprintf(stderr, "Short write: %zu of %ld bytes\n", written, n_rows);
        return 1;
    }

    printf("Written %ld bytes to %s\n", n_rows, out_path.c_str());

    // Count valid entries (non-0xFF)
    // Re-read output to verify
    size_t check_sz = 0;
    const uint8_t* check = (const uint8_t*)mmap_ro(out_path.c_str(), &check_sz);
    int64_t valid_count = 0;
    for (int64_t i = 0; i < n_rows; i++) {
        if (check[i] != 0xFF) valid_count++;
    }
    munmap((void*)check, check_sz);
    printf("Valid year_idx entries: %ld / %ld (expected ~%ld)\n",
           valid_count, n_rows, n_rows); // all lineitem rows should have valid orders

    munmap(yidx_ptr, yidx_sz);
    munmap((void*)l_orderkey, okey_sz);
    return 0;
}
