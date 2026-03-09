// Q6: Forecasting Revenue Change
// SUM(l_extendedprice * l_discount) WHERE l_shipdate IN [1994-01-01, 1995-01-01)
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <thread>
#include <string>
#include <chrono>
#include <atomic>

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
        if (data == MAP_FAILED) { perror("mmap"); data=nullptr; return false; }
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
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

    // ── Phase: data_loading ────────────────────────────────────────────────
    double t_load_start_ms, t_load_end_ms;
    {
        GENDB_PHASE("data_loading");
        t_load_start_ms = std::chrono::duration<double,std::milli>(
            std::chrono::high_resolution_clock::now() - t_total_start).count();

        // (files mmap'd below, timing captured via GENDB_PHASE)
        t_load_end_ms = 0; // unused, GENDB_PHASE handles it
    }

    // ── Load zone map ──────────────────────────────────────────────────────
    struct ZoneEntry { int32_t mn, mx; };
    uint32_t num_blocks = 0, block_size_zm = 0;
    std::vector<ZoneEntry> zones;
    {
        GENDB_PHASE("data_loading/zone_map");
        std::string zm_path = gendb_dir + "/lineitem/l_shipdate_zone_map.bin";
        FILE* zf = fopen(zm_path.c_str(), "rb");
        if (!zf) { perror(zm_path.c_str()); return 1; }
        fread(&num_blocks,    4, 1, zf);
        fread(&block_size_zm, 4, 1, zf);
        zones.resize(num_blocks);
        fread(zones.data(), sizeof(ZoneEntry), num_blocks, zf);
        fclose(zf);
    }

    // ── mmap columns ──────────────────────────────────────────────────────
    MmapFile f_shipdate, f_discount, f_quantity, f_extprice;
    {
        GENDB_PHASE("data_loading/columns");
        if (!f_shipdate.open((gendb_dir + "/lineitem/l_shipdate.bin").c_str()))     return 1;
        if (!f_discount.open((gendb_dir + "/lineitem/l_discount.bin").c_str()))     return 1;
        if (!f_quantity.open((gendb_dir + "/lineitem/l_quantity.bin").c_str()))     return 1;
        if (!f_extprice.open((gendb_dir + "/lineitem/l_extendedprice.bin").c_str())) return 1;
    }

    const int32_t* l_shipdate     = (const int32_t*)f_shipdate.data;
    const int8_t*  l_discount     = (const int8_t*) f_discount.data;
    const int8_t*  l_quantity     = (const int8_t*) f_quantity.data;
    const double*  l_extendedprice= (const double*) f_extprice.data;

    const size_t total_rows = f_shipdate.size / sizeof(int32_t);
    const size_t block_size = (size_t)block_size_zm;

    // ── Phase: dim_filter / zone_map_prune ────────────────────────────────
    uint32_t block_lo = num_blocks, block_hi = 0;
    {
        GENDB_PHASE("dim_filter");
        constexpr int32_t LO = 8766;  // 1994-01-01
        constexpr int32_t HI = 9131;  // 1995-01-01
        for (uint32_t b = 0; b < num_blocks; b++) {
            if (zones[b].mx < LO || zones[b].mn >= HI) continue;
            if (b < block_lo) block_lo = b;
            if (b >= block_hi) block_hi = b + 1;
        }
    }

    if (block_lo >= block_hi) {
        // No qualifying blocks — output zero
        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        fprintf(out, "revenue\n0.00\n");
        fclose(out);
        return 0;
    }

    const size_t row_lo = (size_t)block_lo * block_size;
    const size_t row_hi = std::min((size_t)block_hi * block_size, total_rows);

    // ── Phase: main_scan (parallel) ───────────────────────────────────────
    double total_revenue = 0.0;
    {
        GENDB_PHASE("main_scan");

        constexpr int32_t LO = 8766;
        constexpr int32_t HI = 9131;

        const int nthreads = (int)std::thread::hardware_concurrency();
        std::vector<double> partial(nthreads, 0.0);
        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        const size_t total_scan = row_hi - row_lo;
        const size_t chunk = (total_scan + nthreads - 1) / nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                size_t t_lo = row_lo + (size_t)t * chunk;
                size_t t_hi = std::min(t_lo + chunk, row_hi);
                if (t_lo >= t_hi) return;

                // Align to block boundaries for zone-map check
                double rev_local = 0.0;

                // Determine which block t_lo starts in
                uint32_t b_start = (uint32_t)(t_lo / block_size);
                uint32_t b_end   = (uint32_t)((t_hi - 1) / block_size);

                for (uint32_t b = b_start; b <= b_end; b++) {
                    // Per-block zone check
                    if (zones[b].mx < LO || zones[b].mn >= HI) continue;

                    size_t r_lo = std::max((size_t)b * block_size, t_lo);
                    size_t r_hi = std::min((size_t)(b + 1) * block_size, t_hi);

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

        for (int t = 0; t < nthreads; t++) total_revenue += partial[t];
    }

    // ── Phase: output ─────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        // Ensure results_dir exists
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
