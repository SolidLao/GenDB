// Q6: Forecasting Revenue Change — iter_2
// Optimization: dual-path scan
//   Interior blocks (zone fully inside [LO,HI)): skip l_shipdate load/check entirely
//   Boundary blocks (straddle date boundary): full three-filter check with shipdate first
// No madvise calls per plan (avoids iter_1 regression).
//
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate <  DATE '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

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

// ── GENDB_PHASE timing ─────────────────────────────────────────────────────
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

// ── mmap helper (no madvise) ───────────────────────────────────────────────
struct MmapFile {
    void*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const char* path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st;
        if (fstat(fd, &st) < 0) { perror("fstat"); return false; }
        size = (size_t)st.st_size;
        if (size == 0) { data = nullptr; return true; }
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        // No madvise — plan deliberately omits it to avoid iter_1 regression
        return true;
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) ::close(fd);
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

    // ── Phase: data_loading ────────────────────────────────────────────────
    struct ZoneEntry { int32_t mn, mx; };
    uint32_t num_blocks = 0, block_size_zm = 0;
    std::vector<ZoneEntry> zones;

    MmapFile f_shipdate, f_discount, f_quantity, f_extprice;
    {
        GENDB_PHASE("data_loading");

        // Load zone map (fits in L1 cache — 4808 bytes)
        {
            std::string zm_path = gendb_dir + "/lineitem/l_shipdate_zone_map.bin";
            FILE* zf = fopen(zm_path.c_str(), "rb");
            if (!zf) { perror(zm_path.c_str()); return 1; }
            fread(&num_blocks,    4, 1, zf);
            fread(&block_size_zm, 4, 1, zf);
            zones.resize(num_blocks);
            fread(zones.data(), sizeof(ZoneEntry), num_blocks, zf);
            fclose(zf);
        }

        // mmap column files
        if (!f_shipdate.open((gendb_dir + "/lineitem/l_shipdate.bin").c_str()))      return 1;
        if (!f_discount.open((gendb_dir + "/lineitem/l_discount.bin").c_str()))      return 1;
        if (!f_quantity.open((gendb_dir + "/lineitem/l_quantity.bin").c_str()))      return 1;
        if (!f_extprice.open((gendb_dir + "/lineitem/l_extendedprice.bin").c_str())) return 1;
    }

    const int32_t* l_shipdate      = (const int32_t*)f_shipdate.data;
    const int8_t*  l_discount      = (const int8_t*) f_discount.data;
    const int8_t*  l_quantity      = (const int8_t*) f_quantity.data;
    const double*  l_extendedprice = (const double*)  f_extprice.data;

    const size_t total_rows = f_shipdate.size / sizeof(int32_t);
    const size_t block_size = (size_t)block_size_zm;

    constexpr int32_t LO = 8766;  // 1994-01-01
    constexpr int32_t HI = 9131;  // 1995-01-01

    // ── Phase: zone_map_prune — identify qualifying block range ────────────
    uint32_t block_lo = num_blocks, block_hi = 0;
    {
        GENDB_PHASE("dim_filter");
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].mx < LO || zones[b].mn >= HI) continue;
            if (b < block_lo) block_lo = b;
            if (b > block_hi || block_hi == 0) block_hi = b + 1;
        }
        // Ensure block_hi captures all qualifying blocks correctly
        // Re-scan to get correct max
        if (block_lo < num_blocks) {
            block_hi = block_lo;
            for (uint32_t b = block_lo; b < num_blocks; b++) {
                if (zones[b].mx >= LO && zones[b].mn < HI) block_hi = b + 1;
            }
        }
    }

    if (block_lo >= num_blocks || block_lo >= block_hi) {
        // No qualifying blocks
        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (out) { fprintf(out, "revenue\n0.00\n"); fclose(out); }
        return 0;
    }

    const size_t row_lo = (size_t)block_lo * block_size;
    const size_t row_hi = std::min((size_t)block_hi * block_size, total_rows);

    // ── Phase: main_scan (parallel, morsel-driven) ─────────────────────────
    double total_revenue = 0.0;
    {
        GENDB_PHASE("main_scan");

        const int nthreads = (int)std::thread::hardware_concurrency();
        std::vector<double> partial(nthreads, 0.0);
        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        // Divide qualifying block range [block_lo, block_hi) across threads
        const uint32_t num_qual_blocks = block_hi - block_lo;
        const uint32_t blocks_per_thread = (num_qual_blocks + (uint32_t)nthreads - 1) / (uint32_t)nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                uint32_t b_start = block_lo + (uint32_t)t * blocks_per_thread;
                uint32_t b_end   = std::min(b_start + blocks_per_thread, block_hi);
                if (b_start >= b_end) return;

                double rev_local = 0.0;

                for (uint32_t b = b_start; b < b_end; b++) {
                    // Skip blocks entirely outside the date range
                    if (zones[b].mx < LO || zones[b].mn >= HI) continue;

                    const size_t r_lo = (size_t)b * block_size;
                    const size_t r_hi = std::min(r_lo + block_size, total_rows);

                    // ── Dual-path dispatch ─────────────────────────────────
                    if (zones[b].mn >= LO && zones[b].mx < HI) {
                        // INTERIOR block: zone guarantees all rows pass l_shipdate filter.
                        // Skip l_shipdate load entirely — int8-only hot path for SIMD.
                        for (size_t i = r_lo; i < r_hi; i++) {
                            int8_t disc = l_discount[i];
                            if (disc < 5 || disc > 7) continue;
                            int8_t qty  = l_quantity[i];
                            if (qty >= 24) continue;
                            rev_local += l_extendedprice[i] * (disc * 0.01);
                        }
                    } else {
                        // BOUNDARY block: straddles a date boundary — full three-filter check.
                        // Shipdate checked first (most selective at boundaries).
                        for (size_t i = r_lo; i < r_hi; i++) {
                            int32_t sd = l_shipdate[i];
                            if (sd < LO || sd >= HI) continue;
                            int8_t disc = l_discount[i];
                            if (disc < 5 || disc > 7) continue;
                            int8_t qty  = l_quantity[i];
                            if (qty >= 24) continue;
                            rev_local += l_extendedprice[i] * (disc * 0.01);
                        }
                    }
                }

                partial[t] = rev_local;
            });
        }
        for (auto& th : threads) th.join();

        // Sequential reduction
        for (int t = 0; t < nthreads; t++) total_revenue += partial[t];
    }

    // ── Phase: output ─────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        // Ensure results_dir exists
        {
            std::string cmd = "mkdir -p \"" + results_dir + "\"";
            system(cmd.c_str());
        }
        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { perror(out_path.c_str()); return 1; }
        fprintf(out, "revenue\n%.2f\n", total_revenue);
        fclose(out);
    }

    // ── Total timing ───────────────────────────────────────────────────────
    {
        auto t_total_end = std::chrono::high_resolution_clock::now();
        double total_ms = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
        fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);
    }

    return 0;
}
