// Q6: Forecasting Revenue Change — Iteration 5
// Plan: zone_map_prune → parallel_pread_load → parallel_filter_scan → aggregate_reduce
//
// Key change from iter_4: replace mmap with pread() into pre-allocated anonymous
// buffers to eliminate kernel-mode page-fault overhead (~8-21ms for 4200 pages).
//
// Revenue formula: SUM(l_extendedprice * (l_discount * 0.01))
// Filters: l_shipdate in [8766, 9131), l_discount in [5,7], l_quantity < 24

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cassert>
#include <algorithm>
#include <string>
#include <chrono>

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

// ─── Timing macros ─────────────────────────────────────────────────────────
#ifdef GENDB_PROFILE
#  define TS_NOW() std::chrono::steady_clock::now()
#  define TS_MS(a,b) (std::chrono::duration<double,std::milli>((b)-(a)).count())
#  define LOG_PHASE(name, ms) fprintf(stderr, "[GENDB_PHASE] %s: %.3f ms\n", name, ms)
#else
#  define TS_NOW() std::chrono::steady_clock::now()
#  define TS_MS(a,b) (0.0)
#  define LOG_PHASE(name, ms) do {} while(0)
#endif

// ─── Constants ─────────────────────────────────────────────────────────────
static constexpr int32_t  LO           = 8766;   // 1994-01-01
static constexpr int32_t  HI           = 9131;   // 1995-01-01
static constexpr int8_t   DISC_LO      = 5;
static constexpr int8_t   DISC_HI      = 7;
static constexpr int8_t   QTY_THRESH   = 24;
static constexpr size_t   TOTAL_ROWS   = 59986052ULL;
static constexpr int       N_IO        = 4;       // one thread per column
static constexpr int       N_SCAN      = 64;      // scan threads

// ─── Zone Map Entry ────────────────────────────────────────────────────────
struct ZoneEntry { int32_t mn, mx; };

// ─── Cache-line padded per-thread accumulator ──────────────────────────────
struct alignas(64) Accum {
    double sum;
    char   _pad[56]; // 64 - 8 = 56
};
static Accum g_accum[N_SCAN];

// ─── Global column buffers (qualifying range only) ─────────────────────────
static int32_t* g_shipdate  = nullptr;
static int8_t*  g_discount  = nullptr;
static int8_t*  g_quantity  = nullptr;
static double*  g_extprice  = nullptr;
static size_t   g_count     = 0;   // number of rows in qualifying range

// ─── IO thread argument ────────────────────────────────────────────────────
struct IOArg {
    const char* path;
    void*       buf;
    size_t      nbytes;
    off_t       file_offset;
    int         status;  // 0 = ok, -1 = error
};

static void* io_thread(void* arg) {
    IOArg* a = (IOArg*)arg;
    int fd = open(a->path, O_RDONLY);
    if (fd < 0) { a->status = -1; return nullptr; }
    ssize_t n = pread(fd, a->buf, a->nbytes, a->file_offset);
    close(fd);
    a->status = (n == (ssize_t)a->nbytes) ? 0 : -1;
    return nullptr;
}

// ─── Scan thread argument ──────────────────────────────────────────────────
struct ScanArg {
    int    tid;
    size_t j_start;
    size_t j_end;
};

static void* scan_thread(void* arg) {
    ScanArg* a = (ScanArg*)arg;
    const int32_t* sd  = g_shipdate;
    const int8_t*  dc  = g_discount;
    const int8_t*  qt  = g_quantity;
    const double*  ep  = g_extprice;
    double rev = 0.0;

    for (size_t j = a->j_start; j < a->j_end; j++) {
        int32_t s = sd[j];
        if (s < LO | s >= HI) continue;
        int8_t  d = dc[j];
        if (d < DISC_LO | d > DISC_HI) continue;
        if (qt[j] >= QTY_THRESH) continue;
        rev += ep[j] * (d * 0.01);
    }

    g_accum[a->tid].sum = rev;
    return nullptr;
}

// ─── Aligned allocator ─────────────────────────────────────────────────────
static void* alloc64(size_t nbytes) {
    void* p = nullptr;
    if (posix_memalign(&p, 64, nbytes) != 0) {
        fprintf(stderr, "posix_memalign(%zu) failed\n", nbytes);
        exit(1);
    }
    return p;
}

// ─── mkdir without fork/exec ────────────────────────────────────────────────
static void safe_mkdir(const char* path) {
    if (mkdir(path, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "mkdir(%s): %s\n", path, strerror(errno));
        exit(1);
    }
}

// ─── Main ──────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    auto t_total = TS_NOW();

    // ── Phase: data_loading — zone map via fread ──────────────────────────
    auto t_load = TS_NOW();

    std::string zm_path = gendb_dir + "/lineitem/l_shipdate_zone_map.bin";
    FILE* zf = fopen(zm_path.c_str(), "rb");
    if (!zf) { fprintf(stderr, "Cannot open: %s\n", zm_path.c_str()); return 1; }
    uint32_t num_blocks = 0, block_size = 0;
    fread(&num_blocks, 4, 1, zf);
    fread(&block_size, 4, 1, zf);
    ZoneEntry* zones = (ZoneEntry*)malloc(num_blocks * sizeof(ZoneEntry));
    fread(zones, sizeof(ZoneEntry), num_blocks, zf);
    fclose(zf);

    LOG_PHASE("data_loading", TS_MS(t_load, TS_NOW()));

    // ── Phase: dim_filter — zone map prune ────────────────────────────────
    auto t_dim = TS_NOW();

    uint32_t block_lo = num_blocks, block_hi = 0;
    for (uint32_t b = 0; b < num_blocks; b++) {
        if (zones[b].mx < LO || zones[b].mn >= HI) continue;
        if (b < block_lo) block_lo = b;
        if (b + 1 > block_hi) block_hi = b + 1;
    }

    LOG_PHASE("dim_filter", TS_MS(t_dim, TS_NOW()));

    if (block_lo >= block_hi) {
        // No qualifying blocks
        safe_mkdir(results_dir.c_str());
        std::string out_path = results_dir + "/Q6.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        fprintf(out, "revenue\n0.00\n");
        fclose(out);
        free(zones);
        return 0;
    }

    size_t row_lo = (size_t)block_lo * block_size;
    size_t row_hi = std::min((size_t)block_hi * block_size, TOTAL_ROWS);
    g_count = row_hi - row_lo;

#ifdef GENDB_PROFILE
    fprintf(stderr, "[GENDB_PHASE] zone_map: blocks [%u,%u), rows [%zu,%zu), count=%zu\n",
            block_lo, block_hi, row_lo, row_hi, g_count);
#endif

    // ── Phase: build_joins — allocate + parallel pread ───────────────────
    auto t_pread = TS_NOW();

    g_shipdate = (int32_t*)alloc64(g_count * sizeof(int32_t));
    g_discount = (int8_t*) alloc64(g_count * sizeof(int8_t));
    g_quantity = (int8_t*) alloc64(g_count * sizeof(int8_t));
    g_extprice = (double*) alloc64(g_count * sizeof(double));

    std::string sd_path  = gendb_dir + "/lineitem/l_shipdate.bin";
    std::string dc_path  = gendb_dir + "/lineitem/l_discount.bin";
    std::string qt_path  = gendb_dir + "/lineitem/l_quantity.bin";
    std::string ep_path  = gendb_dir + "/lineitem/l_extendedprice.bin";

    IOArg io[N_IO];
    io[0] = { sd_path.c_str(),  g_shipdate, g_count * sizeof(int32_t), (off_t)(row_lo * sizeof(int32_t)), 0 };
    io[1] = { dc_path.c_str(),  g_discount, g_count * sizeof(int8_t),  (off_t)(row_lo * sizeof(int8_t)),  0 };
    io[2] = { qt_path.c_str(),  g_quantity, g_count * sizeof(int8_t),  (off_t)(row_lo * sizeof(int8_t)),  0 };
    io[3] = { ep_path.c_str(),  g_extprice, g_count * sizeof(double),  (off_t)(row_lo * sizeof(double)),  0 };

    pthread_t io_tids[N_IO];
    for (int i = 0; i < N_IO; i++)
        pthread_create(&io_tids[i], nullptr, io_thread, &io[i]);
    for (int i = 0; i < N_IO; i++) {
        pthread_join(io_tids[i], nullptr);
        if (io[i].status != 0) {
            fprintf(stderr, "IO error on column %d\n", i);
            return 1;
        }
    }

    LOG_PHASE("build_joins", TS_MS(t_pread, TS_NOW()));

    // ── Phase: main_scan — 64-thread parallel filter + accumulate ────────
    auto t_scan = TS_NOW();

    ScanArg scan_args[N_SCAN];
    pthread_t scan_tids[N_SCAN];
    size_t chunk = (g_count + N_SCAN - 1) / N_SCAN;

    for (int t = 0; t < N_SCAN; t++) {
        scan_args[t].tid     = t;
        scan_args[t].j_start = std::min((size_t)t * chunk, g_count);
        scan_args[t].j_end   = std::min((size_t)(t + 1) * chunk, g_count);
        pthread_create(&scan_tids[t], nullptr, scan_thread, &scan_args[t]);
    }
    for (int t = 0; t < N_SCAN; t++)
        pthread_join(scan_tids[t], nullptr);

    double revenue = 0.0;
    for (int t = 0; t < N_SCAN; t++)
        revenue += g_accum[t].sum;

    LOG_PHASE("main_scan", TS_MS(t_scan, TS_NOW()));

    // ── Phase: output ─────────────────────────────────────────────────────
    auto t_output = TS_NOW();

    safe_mkdir(results_dir.c_str());
    std::string out_path = results_dir + "/Q6.csv";
    FILE* out = fopen(out_path.c_str(), "w");
    if (!out) { perror(out_path.c_str()); return 1; }
    fprintf(out, "revenue\n%.2f\n", revenue);
    fclose(out);

    auto t_output_end = TS_NOW();
    LOG_PHASE("output", TS_MS(t_output, t_output_end));

    // ── Total timing ───────────────────────────────────────────────────────
    double total_ms  = TS_MS(t_total, t_output_end);
    double output_ms = TS_MS(t_output, t_output_end);
    LOG_PHASE("total", total_ms);
#ifdef GENDB_PROFILE
    fprintf(stderr, "[GENDB_PHASE] timing_ms: %.3f ms\n", total_ms - output_ms);
#endif

    // Cleanup
    free(g_shipdate);
    free(g_discount);
    free(g_quantity);
    free(g_extprice);
    free(zones);

    return 0;
}
