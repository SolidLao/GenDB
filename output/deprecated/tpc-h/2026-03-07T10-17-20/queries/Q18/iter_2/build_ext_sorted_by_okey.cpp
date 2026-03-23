// build_ext_sorted_by_okey.cpp
// Builds lineitem.sorted_by_okey storage extension.
// 2-pass LSD radix sort (16-bit passes) on l_orderkey (ascending).
// Writes sorted column files for cache-friendly sequential aggregation in Q18.
//
// Output files in <gendb_dir>/column_versions/lineitem.sorted_by_okey/:
//   sorted_l_orderkey.bin  — int32_t[N], sorted ascending
//   sorted_l_quantity.bin  — double[N], reordered to match sorted orderkeys

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>
#include <chrono>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

static void* mmap_ro(const char* path, size_t& out_sz) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); return nullptr; }
    struct stat st; fstat(fd, &st);
    out_sz = (size_t)st.st_size;
    void* p = mmap(nullptr, out_sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return p;
}

static bool write_file(const std::string& path, const void* data, size_t nbytes) {
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); return false; }
    size_t written = fwrite(data, 1, nbytes, fp);
    fclose(fp);
    if (written != nbytes) { fprintf(stderr, "Write error: %s\n", path.c_str()); return false; }
    fprintf(stderr, "[build_ext] wrote %s (%.1f MB)\n", path.c_str(), nbytes / 1e6);
    return true;
}

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    std::string gd(argv[1]);
    auto t0 = std::chrono::steady_clock::now();

    // ── mmap input columns ────────────────────────────────────────────────────
    size_t lok_sz, lqt_sz;
    void* lok_raw = mmap_ro((gd + "/lineitem/l_orderkey.bin").c_str(), lok_sz);
    void* lqt_raw = mmap_ro((gd + "/lineitem/l_quantity.bin").c_str(), lqt_sz);
    if (!lok_raw || !lqt_raw) return 1;

    const int64_t  N   = (int64_t)(lok_sz / sizeof(int32_t));
    const int32_t* lok = (const int32_t*)lok_raw;
    const double*  lqt = (const double*)lqt_raw;
    fprintf(stderr, "[build_ext] N=%ld lineitem rows\n", N);

    // ── Double-buffered 2-pass LSD radix sort ─────────────────────────────────
    // ka/qa = active buffer (after 2 passes: sorted result)
    // kb/qb = scratch buffer
    std::vector<int32_t> ka(N), kb(N);
    std::vector<double>  qa(N), qb(N);

    // Initialize from mmap input
    for (int64_t i = 0; i < N; i++) { ka[i] = lok[i]; qa[i] = lqt[i]; }

    std::vector<uint64_t> cnt(65536), pfx(65536);

    // ── Pass 1: sort by lower 16 bits — (ka,qa) → (kb,qb) ───────────────────
    for (auto& c : cnt) c = 0;
    for (int64_t i = 0; i < N; i++) cnt[(uint32_t)ka[i] & 0xFFFFu]++;
    pfx[0] = 0;
    for (int i = 1; i < 65536; i++) pfx[i] = pfx[i-1] + cnt[i-1];
    for (int64_t i = 0; i < N; i++) {
        uint32_t b = (uint32_t)ka[i] & 0xFFFFu;
        uint64_t pos = pfx[b]++;
        kb[pos] = ka[i];
        qb[pos] = qa[i];
    }

    // ── Pass 2: sort by upper 16 bits — (kb,qb) → (ka,qa) ───────────────────
    for (auto& c : cnt) c = 0;
    for (int64_t i = 0; i < N; i++) cnt[((uint32_t)kb[i] >> 16) & 0xFFFFu]++;
    pfx[0] = 0;
    for (int i = 1; i < 65536; i++) pfx[i] = pfx[i-1] + cnt[i-1];
    for (int64_t i = 0; i < N; i++) {
        uint32_t b = ((uint32_t)kb[i] >> 16) & 0xFFFFu;
        uint64_t pos = pfx[b]++;
        ka[pos] = kb[i];
        qa[pos] = qb[i];
    }
    // ka, qa are now sorted ascending by l_orderkey

    // ── Verify sort and count unique orderkeys ────────────────────────────────
    int64_t n_unique = 0;
    {
        int32_t prev = INT32_MIN;
        for (int64_t i = 0; i < N; i++) {
            if (ka[i] < prev) {
                fprintf(stderr, "ERROR: sort violated at row %ld (prev=%d cur=%d)\n",
                        i, prev, ka[i]);
                return 1;
            }
            if (ka[i] != prev) { n_unique++; prev = ka[i]; }
        }
    }
    fprintf(stderr, "[build_ext] unique orderkeys=%ld\n", n_unique);

    // ── Write output files ────────────────────────────────────────────────────
    std::string out_dir = gd + "/column_versions/lineitem.sorted_by_okey";
    std::filesystem::create_directories(out_dir);

    if (!write_file(out_dir + "/sorted_l_orderkey.bin",
                    ka.data(), (size_t)N * sizeof(int32_t))) return 1;
    if (!write_file(out_dir + "/sorted_l_quantity.bin",
                    qa.data(), (size_t)N * sizeof(double))) return 1;

    munmap(lok_raw, lok_sz);
    munmap(lqt_raw, lqt_sz);

    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    fprintf(stderr, "[build_ext] done in %.0f ms  row_count=%ld  unique_keys=%ld\n",
            ms, N, n_unique);
    return 0;
}
