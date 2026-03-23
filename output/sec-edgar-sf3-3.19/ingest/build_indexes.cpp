// build_indexes.cpp — Build hash indexes from binary columnar data
#include <iostream>
#include <fstream>
#include <vector>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <filesystem>

namespace fs = std::filesystem;

// ============================================================================
// Mmap helpers
// ============================================================================
template<typename T>
struct MmapColumn {
    const T* data = nullptr;
    uint32_t rows = 0;
    int fd = -1;
    size_t file_size = 0;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open " << path << "\n"; return false; }
        struct stat st;
        fstat(fd, &st);
        file_size = st.st_size;
        rows = static_cast<uint32_t>(file_size / sizeof(T));
        data = static_cast<const T*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        if (data == MAP_FAILED) { std::cerr << "mmap failed: " << path << "\n"; return false; }
        return true;
    }

    ~MmapColumn() {
        if (data && data != MAP_FAILED) munmap(const_cast<T*>(data), file_size);
        if (fd >= 0) close(fd);
    }
};

uint32_t read_row_count(const std::string& dir) {
    std::ifstream in(dir + "/row_count.bin", std::ios::binary);
    uint32_t n;
    in.read(reinterpret_cast<char*>(&n), 4);
    return n;
}

// ============================================================================
// Hash function for index keys
// ============================================================================
inline uint64_t hash_u32(uint32_t a) {
    uint64_t x = a;
    x = (x ^ (x >> 16)) * 0x45d9f3b;
    x = (x ^ (x >> 16)) * 0x45d9f3b;
    x = x ^ (x >> 16);
    return x;
}

inline uint64_t hash_u32x2(uint32_t a, uint32_t b) {
    uint64_t x = (static_cast<uint64_t>(a) << 32) | b;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}

inline uint64_t hash_u32x3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = hash_u32x2(a, b);
    h ^= hash_u32(c) + 0x9e3779b97f4a7c15ULL + (h << 12) + (h >> 4);
    return h;
}

// ============================================================================
// Open-addressing hash table with linear probing
// Stored format:
//   uint32_t capacity
//   uint32_t num_entries
//   Entry[capacity] where Entry = { uint64_t key_hash, uint32_t value, uint32_t extra, uint8_t occupied }
// For single-value: value = row_id, extra = 0
// For multi-value: value = offset into row_ids array, extra = count
// ============================================================================
struct HashEntry {
    uint64_t key_hash;
    uint32_t value;
    uint32_t extra;
    uint8_t  occupied;
    uint8_t  pad[7]; // align to 24 bytes
};
static_assert(sizeof(HashEntry) == 24, "HashEntry must be 24 bytes");

void write_hash_table(const std::string& path, const std::vector<HashEntry>& table, uint32_t num_entries) {
    std::ofstream out(path, std::ios::binary);
    uint32_t capacity = static_cast<uint32_t>(table.size());
    out.write(reinterpret_cast<const char*>(&capacity), 4);
    out.write(reinterpret_cast<const char*>(&num_entries), 4);
    out.write(reinterpret_cast<const char*>(table.data()), table.size() * sizeof(HashEntry));
}

// ============================================================================
// Build sub_adsh_lookup: dense array adsh_code -> sub_row_id
// ============================================================================
void build_sub_adsh_lookup(const std::string& storage_dir) {
    auto ts = std::chrono::high_resolution_clock::now();

    uint32_t nrows = read_row_count(storage_dir + "/sub");
    MmapColumn<uint32_t> adsh_col;
    adsh_col.open(storage_dir + "/sub/adsh.bin");

    // Find max adsh code
    uint32_t max_code = 0;
    for (uint32_t i = 0; i < nrows; i++) {
        if (adsh_col.data[i] > max_code) max_code = adsh_col.data[i];
    }

    // Build dense lookup: adsh_code -> sub_row_id (UINT32_MAX if not found)
    std::vector<uint32_t> lookup(max_code + 1, UINT32_MAX);
    for (uint32_t i = 0; i < nrows; i++) {
        lookup[adsh_col.data[i]] = i;
    }

    // Write
    std::ofstream out(storage_dir + "/indexes/sub_adsh_lookup.idx", std::ios::binary);
    uint32_t size = static_cast<uint32_t>(lookup.size());
    out.write(reinterpret_cast<const char*>(&size), 4);
    out.write(reinterpret_cast<const char*>(lookup.data()), lookup.size() * 4);

    auto te = std::chrono::high_resolution_clock::now();
    std::cout << "sub_adsh_lookup: " << size << " entries, "
              << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms\n";
}

// ============================================================================
// Build tag_pk: hash(tag_code, version_code) -> tag_row_id
// ============================================================================
void build_tag_pk(const std::string& storage_dir) {
    auto ts = std::chrono::high_resolution_clock::now();

    uint32_t nrows = read_row_count(storage_dir + "/tag");
    MmapColumn<uint32_t> tag_col, ver_col;
    tag_col.open(storage_dir + "/tag/tag.bin");
    ver_col.open(storage_dir + "/tag/version.bin");

    // Build hash table with 2x capacity
    uint32_t capacity = nrows * 2;
    // Round up to power of 2 for fast modulo
    uint32_t cap = 1;
    while (cap < capacity) cap <<= 1;

    std::vector<HashEntry> table(cap);
    memset(table.data(), 0, cap * sizeof(HashEntry));

    uint32_t mask = cap - 1;
    for (uint32_t i = 0; i < nrows; i++) {
        uint64_t h = hash_u32x2(tag_col.data[i], ver_col.data[i]);
        uint32_t slot = static_cast<uint32_t>(h) & mask;
        while (table[slot].occupied) {
            slot = (slot + 1) & mask;
        }
        table[slot].key_hash = h;
        table[slot].value = i;
        table[slot].extra = 0;
        table[slot].occupied = 1;
    }

    write_hash_table(storage_dir + "/indexes/tag_pk.idx", table, nrows);

    auto te = std::chrono::high_resolution_clock::now();
    std::cout << "tag_pk: " << nrows << " entries in " << cap << " slots, "
              << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms\n";
}

// ============================================================================
// Build pre_join: hash(adsh_code, tag_code, version_code) -> (offset, count)
// into sorted row_ids array
// ============================================================================
void build_pre_join(const std::string& storage_dir) {
    auto ts = std::chrono::high_resolution_clock::now();

    uint32_t nrows = read_row_count(storage_dir + "/pre");
    MmapColumn<uint32_t> adsh_col, tag_col, ver_col;
    adsh_col.open(storage_dir + "/pre/adsh.bin");
    tag_col.open(storage_dir + "/pre/tag.bin");
    ver_col.open(storage_dir + "/pre/version.bin");

    std::cout << "pre_join: sorting " << nrows << " rows...\n";

    // Create index array sorted by (adsh, tag, version)
    std::vector<uint32_t> sorted_ids(nrows);
    std::iota(sorted_ids.begin(), sorted_ids.end(), 0);

    std::sort(sorted_ids.begin(), sorted_ids.end(), [&](uint32_t a, uint32_t b) {
        if (adsh_col.data[a] != adsh_col.data[b]) return adsh_col.data[a] < adsh_col.data[b];
        if (tag_col.data[a] != tag_col.data[b]) return tag_col.data[a] < tag_col.data[b];
        return ver_col.data[a] < ver_col.data[b];
    });

    // Find group boundaries and build hash table
    // Count distinct keys first
    uint32_t num_groups = 0;
    {
        uint32_t i = 0;
        while (i < nrows) {
            num_groups++;
            uint32_t a = adsh_col.data[sorted_ids[i]];
            uint32_t t = tag_col.data[sorted_ids[i]];
            uint32_t v = ver_col.data[sorted_ids[i]];
            uint32_t j = i + 1;
            while (j < nrows && adsh_col.data[sorted_ids[j]] == a
                             && tag_col.data[sorted_ids[j]] == t
                             && ver_col.data[sorted_ids[j]] == v) j++;
            i = j;
        }
    }

    std::cout << "pre_join: " << num_groups << " distinct keys\n";

    // Build hash table
    uint32_t cap = 1;
    while (cap < num_groups * 2) cap <<= 1;

    std::vector<HashEntry> table(cap);
    memset(table.data(), 0, cap * sizeof(HashEntry));
    uint32_t mask = cap - 1;

    {
        uint32_t i = 0;
        while (i < nrows) {
            uint32_t a = adsh_col.data[sorted_ids[i]];
            uint32_t t = tag_col.data[sorted_ids[i]];
            uint32_t v = ver_col.data[sorted_ids[i]];
            uint32_t j = i + 1;
            while (j < nrows && adsh_col.data[sorted_ids[j]] == a
                             && tag_col.data[sorted_ids[j]] == t
                             && ver_col.data[sorted_ids[j]] == v) j++;

            uint64_t h = hash_u32x3(a, t, v);
            uint32_t slot = static_cast<uint32_t>(h) & mask;
            while (table[slot].occupied) {
                slot = (slot + 1) & mask;
            }
            table[slot].key_hash = h;
            table[slot].value = i;          // offset into sorted_ids
            table[slot].extra = j - i;      // count
            table[slot].occupied = 1;

            i = j;
        }
    }

    // Write hash table
    write_hash_table(storage_dir + "/indexes/pre_join.idx", table, num_groups);

    // Write sorted row IDs
    {
        std::ofstream out(storage_dir + "/indexes/pre_join_rowids.idx", std::ios::binary);
        uint32_t n = nrows;
        out.write(reinterpret_cast<const char*>(&n), 4);
        out.write(reinterpret_cast<const char*>(sorted_ids.data()), nrows * 4);
    }

    auto te = std::chrono::high_resolution_clock::now();
    std::cout << "pre_join: " << num_groups << " groups in " << cap << " slots, "
              << std::chrono::duration_cast<std::chrono::milliseconds>(te - ts).count() << "ms\n";
}

// ============================================================================
// Main
// ============================================================================
int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <storage_dir>\n";
        return 1;
    }
    std::string storage_dir = argv[1];

    auto t0 = std::chrono::high_resolution_clock::now();

    fs::create_directories(storage_dir + "/indexes");

    // Build indexes in parallel
    std::thread t1([&]{ build_sub_adsh_lookup(storage_dir); });
    std::thread t2([&]{ build_tag_pk(storage_dir); });
    std::thread t3([&]{ build_pre_join(storage_dir); });

    t1.join();
    t2.join();
    t3.join();

    auto t_end = std::chrono::high_resolution_clock::now();
    std::cout << "\nTotal index build time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t0).count() << "ms\n";
    return 0;
}
