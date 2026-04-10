#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <filesystem>
#include <algorithm>
#include <thread>
#include <chrono>
#include <mutex>
#include <cfloat>

namespace fs = std::filesystem;

static std::mutex g_print_mtx;
static void report(const char* idx, const char* msg) {
    std::lock_guard<std::mutex> lk(g_print_mtx);
    printf("[%-30s] %s\n", idx, msg);
    fflush(stdout);
}

static uint64_t read_row_count(const std::string& meta_path) {
    FILE* fp = fopen(meta_path.c_str(), "r");
    if (!fp) { perror(meta_path.c_str()); exit(1); }
    uint64_t n = 0;
    fscanf(fp, "row_count=%lu", &n);
    fclose(fp);
    return n;
}

template<typename T>
static std::vector<T> read_bin(const std::string& path, size_t count) {
    std::vector<T> v(count);
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) { perror(path.c_str()); exit(1); }
    size_t rd = fread(v.data(), sizeof(T), count, fp);
    if (rd != count) { fprintf(stderr, "Short read: %s (%zu/%zu)\n", path.c_str(), rd, count); exit(1); }
    fclose(fp);
    return v;
}

template<typename T>
static void write_bin(const std::string& path, const std::vector<T>& v) {
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); exit(1); }
    if (!v.empty()) fwrite(v.data(), sizeof(T), v.size(), fp);
    fclose(fp);
}

// ============================================================
// Zone map for int32_t columns (same format as existing build_indexes.cpp)
// ============================================================
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
};

static void build_zone_map_i32(const std::string& storage_dir,
                                const std::string& table,
                                const std::string& col,
                                uint32_t block_size) {
    std::string idx_path = storage_dir + "/indexes/" + table + "_" + col + "_zonemap.bin";
    if (fs::exists(idx_path)) {
        report((table + "_" + col + "_zm").c_str(), "SKIP (already exists)");
        return;
    }

    std::string meta_path = storage_dir + "/" + table + "/meta.txt";
    uint64_t nrows = read_row_count(meta_path);

    auto vals = read_bin<int32_t>(storage_dir + "/" + table + "/" + col + ".bin", nrows);

    uint32_t nblocks = (uint32_t)((nrows + block_size - 1) / block_size);
    std::vector<ZoneMapEntry> zm(nblocks);

    for (uint32_t b = 0; b < nblocks; b++) {
        uint64_t start = (uint64_t)b * block_size;
        uint64_t end = std::min(start + block_size, nrows);
        int32_t mn = vals[start], mx = vals[start];
        for (uint64_t i = start + 1; i < end; i++) {
            if (vals[i] < mn) mn = vals[i];
            if (vals[i] > mx) mx = vals[i];
        }
        zm[b] = { mn, mx };
    }

    write_bin(idx_path, zm);

    FILE* mfp = fopen((storage_dir + "/indexes/" + table + "_" + col + "_zonemap_meta.txt").c_str(), "w");
    fprintf(mfp, "block_size=%u\nnum_blocks=%u\nentry_size=8\nformat=min_i32,max_i32\n",
            block_size, nblocks);
    fclose(mfp);

    char msg[256];
    snprintf(msg, sizeof(msg), "done: %u blocks of %u rows", nblocks, block_size);
    report((table + "_" + col + "_zm").c_str(), msg);
}

// ============================================================
// Zone map for double columns: (min_double, max_double) per block
// ============================================================
struct ZoneMapDoubleEntry {
    double min_val;
    double max_val;
};

static void build_zone_map_double(const std::string& storage_dir,
                                   const std::string& table,
                                   const std::string& col,
                                   uint32_t block_size) {
    std::string idx_path = storage_dir + "/indexes/" + table + "_" + col + "_zonemap.bin";
    if (fs::exists(idx_path)) {
        report((table + "_" + col + "_zm").c_str(), "SKIP (already exists)");
        return;
    }

    std::string meta_path = storage_dir + "/" + table + "/meta.txt";
    uint64_t nrows = read_row_count(meta_path);

    auto vals = read_bin<double>(storage_dir + "/" + table + "/" + col + ".bin", nrows);

    uint32_t nblocks = (uint32_t)((nrows + block_size - 1) / block_size);
    std::vector<ZoneMapDoubleEntry> zm(nblocks);

    for (uint32_t b = 0; b < nblocks; b++) {
        uint64_t start = (uint64_t)b * block_size;
        uint64_t end = std::min(start + block_size, nrows);
        double mn = vals[start], mx = vals[start];
        for (uint64_t i = start + 1; i < end; i++) {
            if (vals[i] < mn) mn = vals[i];
            if (vals[i] > mx) mx = vals[i];
        }
        zm[b] = { mn, mx };
    }

    write_bin(idx_path, zm);

    FILE* mfp = fopen((storage_dir + "/indexes/" + table + "_" + col + "_zonemap_meta.txt").c_str(), "w");
    fprintf(mfp, "block_size=%u\nnum_blocks=%u\nentry_size=16\nformat=min_f64,max_f64\n",
            block_size, nblocks);
    fclose(mfp);

    char msg[256];
    snprintf(msg, sizeof(msg), "done: %u blocks of %u rows (double)", nblocks, block_size);
    report((table + "_" + col + "_zm").c_str(), msg);
}

// ============================================================
// Main
// ============================================================
int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <storage_dir>\n", argv[0]);
        return 1;
    }
    std::string dir = argv[1];
    fs::create_directories(dir + "/indexes");

    auto t0 = std::chrono::high_resolution_clock::now();

    // Build new zone maps in parallel
    std::thread t1([&]{ build_zone_map_double(dir, "lineitem", "l_quantity", 65536); });
    std::thread t2([&]{ build_zone_map_double(dir, "lineitem", "l_discount", 65536); });
    std::thread t3([&]{ build_zone_map_i32(dir, "part", "p_size", 65536); });

    t1.join(); t2.join(); t3.join();

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t0).count();
    printf("\nExtend index building complete in %.1f seconds\n", elapsed);
    return 0;
}
