// Q6: Forecasting Revenue Change — iter_1 (optimized filter order + unsigned tricks)
// SUM(l_extendedprice * l_discount) WHERE l_shipdate IN [1994-01-01, 1995-01-01)
//   AND l_discount BETWEEN 0.05 AND 0.07 (encoded: 5..7)
//   AND l_quantity < 24
//
// Key improvements vs iter_0:
//   1. Filter order: l_discount FIRST (21% pass) → l_quantity SECOND (47%) → l_shipdate THIRD (99% in-block)
//      → reduces avg bytes read per row from 6.0 to 2.4 (60% reduction in memory traffic)
//   2. Unsigned range tricks fuse each two-sided comparison into one branch
//   3. MADV_WILLNEED on qualifying row range only (avoids 800MB useless readahead)
//   4. Cache-line-padded per-thread accumulators (eliminates false sharing)

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <thread>
#include <string>
#include <chrono>

// ── GENDB_PHASE timing ────────────────────────────────────────────────────────
#ifdef GENDB_PROFILE
struct PhaseTimer {
    const char* name;
    std::chrono::high_resolution_clock::time_point t0;
    PhaseTimer(const char* n) : name(n), t0(std::chrono::high_resolution_clock::now()) {}
    ~PhaseTimer() {
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
        fprintf(stderr, "[GENDB_PHASE] %s: %.3f ms\n", name, ms);
    }
};
#define GENDB_PHASE(name) PhaseTimer _phase_timer_##__LINE__(name)
#else
#define GENDB_PHASE(name)
#endif

// ── Cache-line-padded accumulator — prevents false sharing across threads ─────
struct alignas(64) PaddedDouble {
    double val = 0.0;
    char   pad[56];  // total struct = 64 bytes = one cache line
};

// ── mmap helper ───────────────────────────────────────────────────────────────
struct MmapFile {
    void*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const char* path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        return true;
    }

    // Prefetch just the qualifying byte range — avoid readahead on unneeded data
    void willneed(size_t byte_lo, size_t byte_len) {
        if (data && byte_len > 0)
            madvise((char*)data + byte_lo, byte_len, MADV_WILLNEED);
    }

    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ── Phase: data_loading — zone map + column mmap ──────────────────────
    struct ZoneEntry { int32_t mn, mx; };
    uint32_t num_blocks = 0, block_size_zm = 0;
    std::vector<ZoneEntry> zones;

    MmapFile f_discount, f_quantity, f_shipdate, f_extprice;
    {
        GENDB_PHASE("data_loading");

        // Load zone map (fits in L1 cache — 4808 bytes)
        std::string zm_path = gendb_dir + "/lineitem/l_shipdate_zone_map.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (!zf) { perror(zm_path.c_str()); return 1; }
        fread(&num_blocks,    4, 1, zf);
        fread(&block_size_zm, 4, 1, zf);
        zones.resize(num_blocks);
        fread(zones.data(), sizeof(ZoneEntry), num_blocks, zf);
        fclose(zf);

        // mmap column files (no madvise yet — defer until qualifying range known)
        if (!f_discount.open((gendb_dir + "/lineitem/l_discount.bin").c_str()))      return 1;
        if (!f_quantity.open((gendb_dir + "/lineitem/l_quantity.bin").c_str()))      return 1;
        if (!f_shipdate.open((gendb_dir + "/lineitem/l_shipdate.bin").c_str()))      return 1;
        if (!f_extprice.open((gendb_dir + "/lineitem/l_extendedprice.bin").c_str())) return 1;
    }

    const int8_t*  l_discount      = (const int8_t*) f_discount.data;
    const int8_t*  l_quantity      = (const int8_t*) f_quantity.data;
    const int32_t* l_shipdate      = (const int32_t*)f_shipdate.data;
    const double*  l_extendedprice = (const double*) f_extprice.data;

    const size_t total_rows = f_shipdate.size / sizeof(int32_t);
    const size_t block_size = (size_t)block_size_zm;

    // Date constants (days since epoch)
    constexpr int32_t LO = 8766;  // 1994-01-01
    constexpr int32_t HI = 9131;  // 1995-01-01

    // ── Phase: dim_filter / zone_map_prune ────────────────────────────────
    // Linear scan of 600 entries (L1-resident) finds contiguous qualifying window
    uint32_t block_lo = num_blocks, block_hi = 0;
    {
        GENDB_PHASE("dim_filter");
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].mx < LO || zones[b].mn >= HI) continue;
            if (b < block_lo) block_lo = b;
            block_hi = b + 1;  // iterate ascending: always captures last qualifying block
        }
    }

    if (block_lo >= block_hi) {
        // No qualifying blocks
        std::string mkdirp = "mkdir -p \"" + results_dir + "\"";
        system(mkdirp.c_str());
        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        fprintf(out, "revenue\n0.00\n");
        fclose(out);
        return 0;
    }

    const size_t row_lo = (size_t)block_lo * block_size;
    const size_t row_hi = std::min((size_t)block_hi * block_size, total_rows);
    const size_t nrows  = row_hi - row_lo;

    // ── MADV_WILLNEED on qualifying ranges only ────────────────────────────
    // Only prefetch the ~1.9% of each file that contains 1994 data.
    // This avoids triggering readahead on 57MB+57MB+229MB+458MB of unneeded data.
    {
        f_discount.willneed(row_lo * 1,              nrows * 1);              // int8_t
        f_quantity.willneed(row_lo * 1,              nrows * 1);              // int8_t
        f_shipdate.willneed(row_lo * sizeof(int32_t), nrows * sizeof(int32_t)); // int32_t
        f_extprice.willneed(row_lo * sizeof(double),  nrows * sizeof(double));  // double
    }

    // ── Phase: main_scan (parallel, morsel-driven) ────────────────────────
    double total_revenue = 0.0;
    {
        GENDB_PHASE("main_scan");

        const int nthreads = (int)std::thread::hardware_concurrency();
        // Cache-line-padded per-thread sums — each in its own 64B cache line
        std::vector<PaddedDouble> partial(nthreads);

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        const size_t chunk = (nrows + nthreads - 1) / nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t t_lo = row_lo + (size_t)t * chunk;
                const size_t t_hi = std::min(t_lo + chunk, row_hi);
                if (t_lo >= t_hi) return;

                double rev_local = 0.0;

                // Block range for this thread's row slice
                const uint32_t b_start = (uint32_t)(t_lo / block_size);
                const uint32_t b_end   = (uint32_t)((t_hi - 1) / block_size);

                for (uint32_t b = b_start; b <= b_end; b++) {
                    // Per-block zone check — skip if entirely outside [LO, HI)
                    if (zones[b].mx < LO || zones[b].mn >= HI) continue;

                    const size_t r_lo = std::max((size_t)b * block_size, t_lo);
                    const size_t r_hi = std::min((size_t)(b + 1) * block_size, t_hi);

                    // ── CRITICAL INNER LOOP ──────────────────────────────
                    // Filter order: discount → quantity → shipdate
                    // avg bytes read per row: 2.4 (vs 6.0 with old order) — 60% less traffic
                    for (size_t i = r_lo; i < r_hi; i++) {
                        // FILTER 1: discount in [5,7] — unsigned trick fuses two comparisons
                        // (uint8_t)(disc - 5) <= 2u  ←→  disc >= 5 && disc <= 7
                        // 79% of rows rejected here after reading only 1 byte
                        const uint8_t disc_u = (uint8_t)(l_discount[i] - (int8_t)5);
                        if (disc_u > 2u) continue;

                        // FILTER 2: quantity < 24 — single unsigned byte comparison
                        // 53% of remaining rows rejected here after reading 1 more byte
                        if ((uint8_t)l_quantity[i] >= 24u) continue;

                        // FILTER 3: shipdate in [LO, HI) — unsigned trick fuses two comparisons
                        // (uint32_t)(sd - LO) < 365u  ←→  sd >= LO && sd < HI
                        // ~99% pass within qualifying blocks — hint branch predictor
                        const uint32_t sd_off = (uint32_t)(l_shipdate[i] - LO);
                        if (__builtin_expect(sd_off >= (uint32_t)(HI - LO), 0)) continue;

                        // ACCUMULATE: l_extendedprice * (l_discount * 0.01)
                        // Re-read discount as signed int8 for the multiply
                        // (compiler may reuse cached register value)
                        const int8_t disc = l_discount[i];
                        rev_local += l_extendedprice[i] * (disc * 0.01);
                    }
                }

                partial[t].val = rev_local;
            });
        }
        for (auto& th : threads) th.join();

        // Final scalar reduction across nthreads partial sums
        for (int t = 0; t < nthreads; t++) total_revenue += partial[t].val;
    }

    // ── Phase: output ─────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string mkdirp = "mkdir -p \"" + results_dir + "\"";
        system(mkdirp.c_str());

        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }
        fprintf(out, "revenue\n%.2f\n", total_revenue);
        fclose(out);
    }

    // ── Total timing ──────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
