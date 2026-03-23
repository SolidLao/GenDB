#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <algorithm>
#include <fstream>
#include <sstream>

namespace fs = std::filesystem;

// ============================================================
// Read a binary column file into a vector
// ============================================================
template<typename T>
static std::vector<T> read_column(const std::string& path, size_t expected_rows = 0) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Failed to open %s\n", path.c_str()); exit(1); }
    fseek(f, 0, SEEK_END);
    size_t fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    size_t count = fsize / sizeof(T);
    if (expected_rows > 0 && count != expected_rows) {
        fprintf(stderr, "Warning: %s has %zu elements, expected %zu\n", path.c_str(), count, expected_rows);
    }
    std::vector<T> data(count);
    size_t rd = fread(data.data(), sizeof(T), count, f);
    (void)rd;
    fclose(f);
    return data;
}

// ============================================================
// Read row count from meta.json
// ============================================================
static size_t read_row_count(const std::string& table_dir) {
    std::ifstream f(table_dir + "/meta.json");
    std::string line;
    std::getline(f, line);
    // Parse: {"row_count": 12345}
    auto pos = line.find(":");
    if (pos == std::string::npos) { fprintf(stderr, "Bad meta.json in %s\n", table_dir.c_str()); exit(1); }
    return (size_t)std::stoll(line.substr(pos + 1));
}

// ============================================================
// Write binary data
// ============================================================
static void write_binary(const std::string& path, const void* data, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Failed to write %s\n", path.c_str()); exit(1); }
    fwrite(data, 1, bytes, f);
    fclose(f);
}

// ============================================================
// Build lineitem_orderkey_index (dense_range)
// Format: array of (uint32_t start, uint32_t count) indexed by orderkey
// Index[orderkey] = {start_row_in_lineitem, num_rows_for_this_orderkey}
// ============================================================
static void build_lineitem_orderkey_index(const std::string& gendb_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    printf("  Building lineitem_orderkey_index...\n");

    std::string li_dir = gendb_dir + "/lineitem";
    size_t nrows = read_row_count(li_dir);
    auto orderkeys = read_column<int32_t>(li_dir + "/l_orderkey.bin", nrows);

    // Find max orderkey
    int32_t max_ok = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (orderkeys[i] > max_ok) max_ok = orderkeys[i];
    }
    printf("    max orderkey = %d, lineitem rows = %zu\n", max_ok, nrows);

    // Build dense index: for each orderkey, store (start, count)
    // Since lineitem is sorted by l_orderkey, we can scan once
    size_t index_size = (size_t)max_ok + 1;
    struct RangeEntry { uint32_t start; uint32_t count; };
    std::vector<RangeEntry> index(index_size, {0, 0});

    size_t i = 0;
    while (i < nrows) {
        int32_t ok = orderkeys[i];
        uint32_t start = (uint32_t)i;
        uint32_t count = 0;
        while (i < nrows && orderkeys[i] == ok) { i++; count++; }
        index[ok] = {start, count};
    }

    // Write index: first write max_key as uint32_t header, then the array
    std::string idx_dir = gendb_dir + "/indexes";
    fs::create_directories(idx_dir);

    FILE* f = fopen((idx_dir + "/lineitem_orderkey_index.bin").c_str(), "wb");
    uint32_t header = (uint32_t)max_ok;
    fwrite(&header, sizeof(uint32_t), 1, f);
    fwrite(index.data(), sizeof(RangeEntry), index_size, f);
    fclose(f);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("    done in %.1f s (index size: %zu entries)\n",
           std::chrono::duration<double>(t1 - t0).count(), index_size);
}

// ============================================================
// Build orders_orderkey_lookup (dense_lookup)
// Format: array of int32_t indexed by orderkey, value = row index (-1 if missing)
// ============================================================
static void build_orders_orderkey_lookup(const std::string& gendb_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    printf("  Building orders_orderkey_lookup...\n");

    std::string ord_dir = gendb_dir + "/orders";
    size_t nrows = read_row_count(ord_dir);
    auto orderkeys = read_column<int32_t>(ord_dir + "/o_orderkey.bin", nrows);

    int32_t max_ok = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (orderkeys[i] > max_ok) max_ok = orderkeys[i];
    }
    printf("    max orderkey = %d, orders rows = %zu\n", max_ok, nrows);

    size_t index_size = (size_t)max_ok + 1;
    std::vector<int32_t> lookup(index_size, -1);

    for (size_t i = 0; i < nrows; i++) {
        lookup[orderkeys[i]] = (int32_t)i;
    }

    std::string idx_dir = gendb_dir + "/indexes";
    fs::create_directories(idx_dir);

    FILE* f = fopen((idx_dir + "/orders_orderkey_lookup.bin").c_str(), "wb");
    uint32_t header = (uint32_t)max_ok;
    fwrite(&header, sizeof(uint32_t), 1, f);
    fwrite(lookup.data(), sizeof(int32_t), index_size, f);
    fclose(f);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("    done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Build customer_custkey_lookup (dense_lookup)
// ============================================================
static void build_customer_custkey_lookup(const std::string& gendb_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    printf("  Building customer_custkey_lookup...\n");

    std::string cust_dir = gendb_dir + "/customer";
    size_t nrows = read_row_count(cust_dir);
    auto custkeys = read_column<int32_t>(cust_dir + "/c_custkey.bin", nrows);

    int32_t max_ck = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (custkeys[i] > max_ck) max_ck = custkeys[i];
    }

    size_t index_size = (size_t)max_ck + 1;
    std::vector<int32_t> lookup(index_size, -1);

    for (size_t i = 0; i < nrows; i++) {
        lookup[custkeys[i]] = (int32_t)i;
    }

    std::string idx_dir = gendb_dir + "/indexes";
    fs::create_directories(idx_dir);

    FILE* f = fopen((idx_dir + "/customer_custkey_lookup.bin").c_str(), "wb");
    uint32_t header = (uint32_t)max_ck;
    fwrite(&header, sizeof(uint32_t), 1, f);
    fwrite(lookup.data(), sizeof(int32_t), index_size, f);
    fclose(f);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("    done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Build supplier_suppkey_lookup (dense_lookup)
// ============================================================
static void build_supplier_suppkey_lookup(const std::string& gendb_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    printf("  Building supplier_suppkey_lookup...\n");

    std::string supp_dir = gendb_dir + "/supplier";
    size_t nrows = read_row_count(supp_dir);
    auto suppkeys = read_column<int32_t>(supp_dir + "/s_suppkey.bin", nrows);

    int32_t max_sk = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (suppkeys[i] > max_sk) max_sk = suppkeys[i];
    }

    size_t index_size = (size_t)max_sk + 1;
    std::vector<int32_t> lookup(index_size, -1);

    for (size_t i = 0; i < nrows; i++) {
        lookup[suppkeys[i]] = (int32_t)i;
    }

    std::string idx_dir = gendb_dir + "/indexes";

    FILE* f = fopen((idx_dir + "/supplier_suppkey_lookup.bin").c_str(), "wb");
    uint32_t header = (uint32_t)max_sk;
    fwrite(&header, sizeof(uint32_t), 1, f);
    fwrite(lookup.data(), sizeof(int32_t), index_size, f);
    fclose(f);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("    done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Build partsupp_pk_index (dense_range on ps_partkey)
// Format: header (uint32_t max_partkey), then array of (uint32_t start, uint32_t count) per partkey
// partsupp is sorted by (ps_partkey, ps_suppkey) so entries for same partkey are contiguous
// ============================================================
static void build_partsupp_pk_index(const std::string& gendb_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    printf("  Building partsupp_pk_index...\n");

    std::string ps_dir = gendb_dir + "/partsupp";
    size_t nrows = read_row_count(ps_dir);
    auto partkeys = read_column<int32_t>(ps_dir + "/ps_partkey.bin", nrows);

    int32_t max_pk = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (partkeys[i] > max_pk) max_pk = partkeys[i];
    }

    size_t index_size = (size_t)max_pk + 1;
    struct RangeEntry { uint32_t start; uint32_t count; };
    std::vector<RangeEntry> index(index_size, {0, 0});

    // Scan sorted partkeys
    size_t i = 0;
    while (i < nrows) {
        int32_t pk = partkeys[i];
        uint32_t start = (uint32_t)i;
        uint32_t count = 0;
        while (i < nrows && partkeys[i] == pk) { i++; count++; }
        index[pk] = {start, count};
    }

    std::string idx_dir = gendb_dir + "/indexes";

    FILE* f = fopen((idx_dir + "/partsupp_pk_index.bin").c_str(), "wb");
    uint32_t header = (uint32_t)max_pk;
    fwrite(&header, sizeof(uint32_t), 1, f);
    fwrite(index.data(), sizeof(RangeEntry), index_size, f);
    fclose(f);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("    done in %.1f s\n", std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================
// Build lineitem_shipdate_zonemap
// Format: header (uint32_t num_blocks, uint32_t block_size),
//         then array of (int32_t min_date, int32_t max_date) per block
// ============================================================
static void build_lineitem_shipdate_zonemap(const std::string& gendb_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    printf("  Building lineitem_shipdate_zonemap...\n");

    std::string li_dir = gendb_dir + "/lineitem";
    size_t nrows = read_row_count(li_dir);
    auto shipdates = read_column<int32_t>(li_dir + "/l_shipdate.bin", nrows);

    const uint32_t block_size = 100000;
    uint32_t num_blocks = (uint32_t)((nrows + block_size - 1) / block_size);

    struct ZoneEntry { int32_t min_date; int32_t max_date; };
    std::vector<ZoneEntry> zones(num_blocks);

    for (uint32_t b = 0; b < num_blocks; b++) {
        size_t start = (size_t)b * block_size;
        size_t end = std::min(start + block_size, nrows);
        int32_t mn = shipdates[start], mx = shipdates[start];
        for (size_t i = start + 1; i < end; i++) {
            if (shipdates[i] < mn) mn = shipdates[i];
            if (shipdates[i] > mx) mx = shipdates[i];
        }
        zones[b] = {mn, mx};
    }

    std::string idx_dir = gendb_dir + "/indexes";

    FILE* f = fopen((idx_dir + "/lineitem_shipdate_zonemap.bin").c_str(), "wb");
    fwrite(&num_blocks, sizeof(uint32_t), 1, f);
    fwrite(&block_size, sizeof(uint32_t), 1, f);
    fwrite(zones.data(), sizeof(ZoneEntry), num_blocks, f);
    fclose(f);

    auto t1 = std::chrono::high_resolution_clock::now();
    printf("    done in %.1f s (%u blocks)\n",
           std::chrono::duration<double>(t1 - t0).count(), num_blocks);
}

// ============================================================
// Main
// ============================================================
int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    printf("Building indexes in %s\n", gendb_dir.c_str());
    auto t0 = std::chrono::high_resolution_clock::now();

    fs::create_directories(gendb_dir + "/indexes");

    // Build indexes in parallel
    std::thread t1([&]() { build_lineitem_orderkey_index(gendb_dir); });
    std::thread t2([&]() { build_orders_orderkey_lookup(gendb_dir); });
    std::thread t3([&]() { build_customer_custkey_lookup(gendb_dir); });
    std::thread t4([&]() { build_supplier_suppkey_lookup(gendb_dir); });
    std::thread t5([&]() { build_partsupp_pk_index(gendb_dir); });
    std::thread t6([&]() { build_lineitem_shipdate_zonemap(gendb_dir); });

    t1.join(); t2.join(); t3.join(); t4.join(); t5.join(); t6.join();

    auto tend = std::chrono::high_resolution_clock::now();
    printf("\nTotal index building time: %.1f s\n",
           std::chrono::duration<double>(tend - t0).count());
    return 0;
}
