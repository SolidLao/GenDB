#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <atomic>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "timing_utils.h"

// Zone map entry: min, max (int32_t), start_row (uint32_t)
struct ZoneMapEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint32_t start_row;
};

static constexpr int32_t  SHIP_LO   = 8766;   // 1994-01-01
static constexpr int32_t  SHIP_HI   = 9131;   // 1995-01-01 (exclusive)
static constexpr uint8_t  DISC_LO   = 5;
static constexpr uint8_t  DISC_HI   = 7;
static constexpr uint8_t  QTY_MAX   = 24;      // < 24
static constexpr uint32_t BLOCK_SZ  = 100000;

// Helper: mmap a file, return pointer and size (no MAP_POPULATE — caller controls faulting)
template<typename T>
static const T* mmap_file(const std::string& path, size_t& n_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t bytes = st.st_size;
    void* ptr = mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    n_elements = bytes / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

// Helper: mmap a small file with MAP_POPULATE (for small index/lookup files that should load immediately)
template<typename T>
static const T* mmap_file_populate(const std::string& path, size_t& n_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t bytes = st.st_size;
    void* ptr = mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    n_elements = bytes / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const std::string base = gendb_dir + "/lineitem/";

    // -------------------------------------------------------------------------
    // Phase 0: Data loading — mmap columns + selective madvise via zone maps
    // -------------------------------------------------------------------------
    const ZoneMapEntry* zm_entries = nullptr;
    uint32_t            zm_nblocks = 0;
    const int32_t*      l_shipdate       = nullptr;
    const uint8_t*      l_discount_code  = nullptr;
    const uint8_t*      l_quantity_code  = nullptr;
    const double*       l_extprice       = nullptr;
    const double*       disc_lut         = nullptr;
    size_t total_rows = 0;
    std::vector<uint32_t> qual_blocks;   // computed once, shared between phases

    {
        GENDB_PHASE("data_loading");

        // -- Zone map (small ~7KB, load immediately with MAP_POPULATE) --
        {
            int fd = open((base + "indexes/shipdate_zonemap.bin").c_str(), O_RDONLY);
            if (fd < 0) { perror("shipdate_zonemap.bin"); exit(1); }
            struct stat st; fstat(fd, &st);
            void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
            close(fd);
            const uint8_t* raw = reinterpret_cast<const uint8_t*>(ptr);
            zm_nblocks = *reinterpret_cast<const uint32_t*>(raw);
            zm_entries = reinterpret_cast<const ZoneMapEntry*>(raw + sizeof(uint32_t));
        }

        // -- Identify qualifying blocks BEFORE mmap'ing large columns --
        qual_blocks.reserve(zm_nblocks);
        for (uint32_t b = 0; b < zm_nblocks; b++) {
            const auto& e = zm_entries[b];
            if (e.max_val < SHIP_LO) continue;
            if (e.min_val >= SHIP_HI) break;  // sorted — no more qualifying
            qual_blocks.push_back(b);
        }

        // -- Discount lookup table (256 doubles = 2 KB, load immediately) --
        {
            size_t n;
            disc_lut = mmap_file_populate<double>(base + "l_discount_lookup.bin", n);
        }

        // -- Main columns: mmap WITHOUT MAP_POPULATE — only qualifying blocks get faulted in --
        {
            size_t n;
            l_shipdate      = mmap_file<int32_t>(base + "l_shipdate.bin", n);      total_rows = n;
            l_discount_code = mmap_file<uint8_t>(base + "l_discount.bin", n);
            l_quantity_code = mmap_file<uint8_t>(base + "l_quantity.bin", n);
            l_extprice      = mmap_file<double> (base + "l_extendedprice.bin", n);
        }

        // -- Selective MADV_WILLNEED on qualifying blocks only (~132MB vs 840MB full scan) --
        // Column-by-column order ensures sequential HDD reads per column file
        #pragma omp parallel for schedule(static) num_threads(4)
        for (int col = 0; col < 4; col++) {
            for (uint32_t b : qual_blocks) {
                const auto& e = zm_entries[b];
                uint32_t start = e.start_row;
                uint32_t count = BLOCK_SZ;
                if (start + count > (uint32_t)total_rows) count = total_rows - start;
                switch (col) {
                    case 0: madvise((void*)(l_shipdate      + start), count * sizeof(int32_t), MADV_WILLNEED); break;
                    case 1: madvise((void*)(l_discount_code + start), count * sizeof(uint8_t), MADV_WILLNEED); break;
                    case 2: madvise((void*)(l_quantity_code + start), count * sizeof(uint8_t), MADV_WILLNEED); break;
                    case 3: madvise((void*)(l_extprice      + start), count * sizeof(double),  MADV_WILLNEED); break;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 1-3: Zone-map-guided morsel scan with thread-local accumulation
    // -------------------------------------------------------------------------
    double global_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        std::atomic<uint32_t> block_idx{0};
        uint32_t nqual = (uint32_t)qual_blocks.size();

        #pragma omp parallel reduction(+:global_revenue)
        {
            double partial = 0.0;
            while (true) {
                uint32_t bi = block_idx.fetch_add(1, std::memory_order_relaxed);
                if (bi >= nqual) break;

                uint32_t b     = qual_blocks[bi];
                uint32_t start = zm_entries[b].start_row;
                uint32_t end   = start + BLOCK_SZ;
                if (end > (uint32_t)total_rows) end = (uint32_t)total_rows;

                const int32_t* sd  = l_shipdate      + start;
                const uint8_t* dc  = l_discount_code + start;
                const uint8_t* qc  = l_quantity_code + start;
                const double*  ep  = l_extprice      + start;
                uint32_t       len = end - start;

                for (uint32_t i = 0; i < len; i++) {
                    int32_t s  = sd[i];
                    uint8_t d  = dc[i];
                    uint8_t q  = qc[i];
                    if (s >= SHIP_LO & s < SHIP_HI & d >= DISC_LO & d <= DISC_HI & q < QTY_MAX) {
                        partial += ep[i] * disc_lut[d];
                    }
                }
            }
            global_revenue += partial;
        }
    }

    // -------------------------------------------------------------------------
    // Phase 4: Output
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }
        fprintf(f, "revenue\n");
        fprintf(f, "%.2f\n", global_revenue);
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
