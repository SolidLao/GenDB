// Build storage extension: sorted_l_qty_uint8.bin
// Converts sorted_l_quantity.bin (double, 8 bytes/row) to uint8_t (1 byte/row).
// l_quantity in TPC-H is always an integer in [1, 50] — fits in uint8.
// Reduces per-row quantity storage from 8 bytes to 1 byte (8x compression).

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir = argv[1];

    const std::string src_path = gendb_dir + "/column_versions/lineitem.sorted_by_okey/sorted_l_quantity.bin";
    const std::string dst_path = gendb_dir + "/column_versions/lineitem.sorted_by_okey/sorted_l_qty_uint8.bin";

    // mmap source file
    int src_fd = open(src_path.c_str(), O_RDONLY);
    if (src_fd < 0) { perror("open src"); return 1; }
    struct stat st;
    fstat(src_fd, &st);
    size_t src_size = st.st_size;
    int64_t nrows = (int64_t)(src_size / sizeof(double));
    fprintf(stderr, "[BUILD] Reading %ld rows from %s (%.1f MB)\n",
            nrows, src_path.c_str(), src_size / 1e6);

    const double* src = (const double*)mmap(nullptr, src_size, PROT_READ, MAP_PRIVATE, src_fd, 0);
    if (src == MAP_FAILED) { perror("mmap src"); return 1; }
    madvise((void*)src, src_size, MADV_SEQUENTIAL);
    close(src_fd);

    // allocate output buffer
    uint8_t* dst = new uint8_t[nrows];

    // Convert: validate all values are integers in [1, 50]
    int64_t out_of_range = 0;
    for (int64_t i = 0; i < nrows; i++) {
        double v = src[i];
        uint8_t u = (uint8_t)(v + 0.5);  // round to nearest integer
        if (u < 1 || u > 50) out_of_range++;
        dst[i] = u;
    }
    munmap((void*)src, src_size);

    if (out_of_range > 0) {
        fprintf(stderr, "[WARN] %ld values out of [1,50] range\n", out_of_range);
    }

    // Write output file
    int dst_fd = open(dst_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dst_fd < 0) { perror("open dst"); return 1; }
    size_t dst_size = (size_t)nrows;
    ssize_t written = write(dst_fd, dst, dst_size);
    if (written != (ssize_t)dst_size) { perror("write"); return 1; }
    close(dst_fd);
    delete[] dst;

    fprintf(stderr, "[BUILD] Wrote %ld rows to %s (%.1f MB)\n",
            nrows, dst_path.c_str(), dst_size / 1e6);
    fprintf(stderr, "[BUILD] Compression: %.1f MB -> %.1f MB (8x reduction)\n",
            src_size / 1e6, dst_size / 1e6);
    return 0;
}
