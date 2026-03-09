// Q6: Forecasting Revenue Change — iter_3
// Optimizations vs iter_2:
//   1. Thread count capped at min(hardware_concurrency(), N_qualifying_blocks) ~12 not 64
//      Eliminates ~50ms of thread lifecycle overhead from 64 threads doing trivial work
//   2. system("mkdir -p ...") replaced with std::filesystem::create_directories()
//      Eliminates 2-5ms fork+exec overhead from the measured run duration
//   3. No madvise — per plan (avoids unnecessary kernel readahead on non-qualifying data)
//   4. Filter order: sd -> disc -> qty (iter_0 order, NOT dual-path which regressed in iter_2)
//   5. Block-strided thread dispatch: thread t processes blocks block_lo+t, block_lo+t+nthreads, ...
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
#include <filesystem>

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
        // No madvise — plan deliberately omits it to avoid readahead on non-qualifying blocks
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

        // Load zone map (4808 bytes — fits in L1 cache)
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

        // mmap column files — no madvise per plan
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

    // ── Phase: zone_map_prune — find qualifying block range [block_lo, block_hi) ──
    uint32_t block_lo = num_blocks, block_hi = 0;
    {
        GENDB_PHASE("dim_filter");
        // Linear scan to find first and last qualifying block
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].mx < LO || zones[b].mn >= HI) continue;
            if (block_lo == num_blocks) block_lo = b;
            block_hi = b + 1;
        }
    }

    if (block_lo >= num_blocks || block_lo >= block_hi) {
        // No qualifying blocks — output zero
        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (out) { fprintf(out, "revenue\n0.00\n"); fclose(out); }
        return 0;
    }

    // ── Phase: build_joins (N/A for Q6) ───────────────────────────────────
    // No joins in Q6 — skip this phase

    // ── Phase: main_scan (block-parallel, thread count capped to N qualifying blocks) ──
    double total_revenue = 0.0;
    {
        GENDB_PHASE("main_scan");

        // CRITICAL: cap threads to number of qualifying blocks (~12), not hardware_concurrency() (64)
        // Spawning 64 threads for ~12 blocks means each thread processes ~1 block in microseconds
        // while thread creation/join costs ~50-100µs each = 6-12ms of pure lifecycle overhead
        const int N = (int)(block_hi - block_lo);
        const int hw = (int)std::thread::hardware_concurrency();
        const int nthreads = std::min(hw, N);

        std::vector<double> partial(nthreads, 0.0);
        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        // Block-strided dispatch: thread t processes blocks block_lo+t, block_lo+t+nthreads, ...
        // This ensures each thread handles one or more complete blocks
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                double rev_local = 0.0;

                for (int b = (int)block_lo + t; b < (int)block_hi; b += nthreads) {
                    // Per-block zone map guard (also catches any gaps in the range)
                    if (zones[b].mx < LO || zones[b].mn >= HI) continue;

                    const size_t r_lo = (size_t)b * block_size;
                    const size_t r_hi = std::min(r_lo + block_size, total_rows);

                    // Inner loop — filter order from iter_0 (sd -> disc -> qty)
                    // Do NOT reorder (iter_1 regression) or use dual-path (iter_2 regression)
                    for (size_t i = r_lo; i < r_hi; i++) {
                        int32_t sd = l_shipdate[i];
                        if (sd < LO || sd >= HI) continue;
                        int8_t disc = l_discount[i];
                        if (disc < 5 || disc > 7) continue;
                        int8_t qty = l_quantity[i];
                        if (qty >= 24) continue;
                        rev_local += l_extendedprice[i] * (disc * 0.01);
                    }
                }

                partial[t] = rev_local;
            });
        }
        for (auto& th : threads) th.join();

        // Sequential reduction of ~12 thread-local doubles
        for (int t = 0; t < nthreads; t++) total_revenue += partial[t];
    }

    // ── Phase: output ──────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        // Use std::filesystem::create_directories — eliminates fork+exec overhead of system()
        // system("mkdir -p ...") costs 2-5ms via fork+execve+wait; this is O(1) syscall
        std::filesystem::create_directories(results_dir);

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
