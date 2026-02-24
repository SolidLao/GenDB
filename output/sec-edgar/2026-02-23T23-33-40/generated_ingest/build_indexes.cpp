// build_indexes.cpp — SEC EDGAR Index Construction
// Usage: ./build_indexes <gendb_dir>
//
// Builds three pre-built hash indexes from binary columnar data:
//
//   1. indexes/sub_adsh_index.bin
//      Hash table: adsh_code (int32_t) -> sub_row (int32_t)
//      Layout: uint32_t cap | cap × SubSlot{int32_t adsh_code, int32_t sub_row}
//      Sentinel: adsh_code = -1
//      Hash fn: hash_i32(k)
//
//   2. indexes/tag_index.bin
//      Hash table: (tag_code, version_code) -> tag_row
//      Layout: uint32_t cap | cap × TagSlot{int32_t tag_code, int32_t version_code, int32_t tag_row}
//      Sentinel: tag_code = -1
//      Hash fn: hash_i32x2(a, b)
//
//   3. indexes/pre_join_index.bin
//      Multi-value hash table: (adsh_code, tag_code, version_code) -> pre_rows[]
//      Layout: uint32_t cap | cap × PreSlot{int32_t adsh_code, int32_t tag_code,
//              int32_t version_code, uint32_t payload_offset, uint32_t payload_count}
//              | uint32_t payload_pool[]
//      pool_start = file_ptr + 4 + cap * sizeof(PreSlot)  (cap*20 bytes)
//      Sentinel: adsh_code = -1
//      Hash fn: hash_i32x3(a, b, c)

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

namespace fs = std::filesystem;
using clk = std::chrono::steady_clock;
static double elapsed_s(clk::time_point t0) {
    return std::chrono::duration<double>(clk::now() - t0).count();
}

// ============================================================
// Hash functions (MUST match exactly in query code)
// ============================================================

// Single int32_t key
static inline uint32_t hash_i32(int32_t k) {
    uint32_t x = (uint32_t)k;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = ((x >> 16) ^ x) * 0x45d9f3bU;
    x = (x >> 16) ^ x;
    return x;
}

// Two int32_t keys
static inline uint32_t hash_i32x2(int32_t a, int32_t b) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL
               ^ (uint64_t)(uint32_t)b * 2246822519ULL;
    return (uint32_t)(h ^ (h >> 32));
}

// Three int32_t keys
static inline uint32_t hash_i32x3(int32_t a, int32_t b, int32_t c) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL;
    h ^= (uint64_t)(uint32_t)b * 2246822519ULL;
    h ^= (uint64_t)(uint32_t)c * 3266489917ULL;
    return (uint32_t)(h ^ (h >> 32));
}

// next power of 2 >= x
static inline uint32_t next_pow2(uint64_t x) {
    if (x == 0) return 1;
    --x;
    x |= x >> 1; x |= x >> 2; x |= x >> 4;
    x |= x >> 8; x |= x >> 16; x |= x >> 32;
    return (uint32_t)(x + 1);
}

// ============================================================
// mmap helper (read-only)
// ============================================================
struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open " << path << "\n"; return false; }
        struct stat st; fstat(fd, &st);
        size = st.st_size;
        if (size == 0) return true;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { data = nullptr; perror("mmap"); return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }
    size_t count32() const { return size / 4; }
    size_t count16() const { return size / 2; }
    const int32_t* as_i32() const { return reinterpret_cast<const int32_t*>(data); }
    ~MmapFile() {
        if (data && size) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
};

// ============================================================
// Index 1: sub_adsh_index
// Key: adsh_code (int32_t) -> sub_row (int32_t)
// ============================================================
struct SubSlot { int32_t adsh_code; int32_t sub_row; };
static_assert(sizeof(SubSlot) == 8, "SubSlot must be 8 bytes");

static void build_sub_adsh_index(const std::string& gendb_dir)
{
    auto t0 = clk::now();
    std::cerr << "[sub_adsh_index] building...\n";

    MmapFile f; f.open(gendb_dir + "/sub/adsh.bin");
    size_t n_rows = f.count32();
    const int32_t* adsh = f.as_i32();

    // C9: cap = next_pow2(n * 2) for <= 50% load factor
    uint32_t cap  = next_pow2((uint64_t)n_rows * 2);
    uint32_t mask = cap - 1;

    // Allocate hash table with sentinel adsh_code = -1
    std::vector<SubSlot> ht(cap);
    for (auto& s : ht) { s.adsh_code = -1; s.sub_row = -1; }

    for (uint32_t row = 0; row < (uint32_t)n_rows; ++row) {
        int32_t key = adsh[row];
        uint32_t slot = hash_i32(key) & mask;
        // C24: bounded probing
        for (uint32_t probe = 0; probe < cap; ++probe) {
            uint32_t s = (slot + probe) & mask;
            if (ht[s].adsh_code == -1) {
                ht[s].adsh_code = key;
                ht[s].sub_row   = (int32_t)row;
                break;
            }
        }
    }

    // Write: [uint32_t cap][SubSlot * cap]
    std::string out_path = gendb_dir + "/indexes/sub_adsh_index.bin";
    std::ofstream out(out_path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(&cap), 4);
    out.write(reinterpret_cast<const char*>(ht.data()), cap * sizeof(SubSlot));

    std::cerr << "[sub_adsh_index] done: " << n_rows << " rows, cap=" << cap
              << " (" << (cap * sizeof(SubSlot) + 4) / (1024*1024) << "MB)"
              << " in " << elapsed_s(t0) << "s\n"
              << "  -> " << out_path << "\n\n";
}

// ============================================================
// Index 2: tag_index
// Key: (tag_code, version_code) -> tag_row
// ============================================================
struct TagSlot { int32_t tag_code; int32_t version_code; int32_t tag_row; };
static_assert(sizeof(TagSlot) == 12, "TagSlot must be 12 bytes");

static void build_tag_index(const std::string& gendb_dir)
{
    auto t0 = clk::now();
    std::cerr << "[tag_index] building...\n";

    MmapFile f_tag, f_ver;
    f_tag.open(gendb_dir + "/tag/tag.bin");
    f_ver.open(gendb_dir + "/tag/version.bin");

    size_t n_rows = f_tag.count32();
    if (n_rows != f_ver.count32()) {
        std::cerr << "ERROR: tag row count mismatch!\n"; return;
    }
    const int32_t* tags     = f_tag.as_i32();
    const int32_t* versions = f_ver.as_i32();

    uint32_t cap  = next_pow2((uint64_t)n_rows * 2);
    uint32_t mask = cap - 1;

    std::vector<TagSlot> ht(cap);
    for (auto& s : ht) { s.tag_code = -1; s.version_code = -1; s.tag_row = -1; }

    for (uint32_t row = 0; row < (uint32_t)n_rows; ++row) {
        int32_t tk = tags[row];
        int32_t vk = versions[row];
        uint32_t slot = hash_i32x2(tk, vk) & mask;
        for (uint32_t probe = 0; probe < cap; ++probe) {
            uint32_t s = (slot + probe) & mask;
            if (ht[s].tag_code == -1) {
                ht[s].tag_code     = tk;
                ht[s].version_code = vk;
                ht[s].tag_row      = (int32_t)row;
                break;
            }
        }
    }

    std::string out_path = gendb_dir + "/indexes/tag_index.bin";
    std::ofstream out(out_path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(&cap), 4);
    out.write(reinterpret_cast<const char*>(ht.data()), cap * sizeof(TagSlot));

    std::cerr << "[tag_index] done: " << n_rows << " rows, cap=" << cap
              << " (" << (cap * sizeof(TagSlot) + 4) / (1024*1024) << "MB)"
              << " in " << elapsed_s(t0) << "s\n"
              << "  -> " << out_path << "\n\n";
}

// ============================================================
// Index 3: pre_join_index (multi-value)
// Key: (adsh_code, tag_code, version_code) -> pre_rows[]
//
// Build strategy:
//   1. Load pre/adsh.bin, pre/tag.bin, pre/version.bin
//   2. Sort row indices by (adsh, tag, version) key
//   3. Scan sorted groups: assign payload_offset and payload_count
//   4. Build open-addressing hash table with (offset, count) per unique key
//   5. Write: [uint32_t cap][PreSlot * cap][payload pool]
// ============================================================
struct PreSlot {
    int32_t  adsh_code;
    int32_t  tag_code;
    int32_t  version_code;
    uint32_t payload_offset;
    uint32_t payload_count;
};
static_assert(sizeof(PreSlot) == 20, "PreSlot must be 20 bytes");

static void build_pre_join_index(const std::string& gendb_dir)
{
    auto t0 = clk::now();
    std::cerr << "[pre_join_index] building...\n";

    MmapFile f_adsh, f_tag, f_ver;
    f_adsh.open(gendb_dir + "/pre/adsh.bin");
    f_tag.open(gendb_dir + "/pre/tag.bin");
    f_ver.open(gendb_dir + "/pre/version.bin");

    size_t n_rows = f_adsh.count32();
    std::cerr << "  pre rows: " << n_rows << "\n";

    const int32_t* adsh     = f_adsh.as_i32();
    const int32_t* tags     = f_tag.as_i32();
    const int32_t* versions = f_ver.as_i32();

    // Step 1: Create sort index [row_id] sorted by (adsh, tag, version)
    std::vector<uint32_t> order(n_rows);
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < (uint32_t)n_rows; ++i) order[i] = i;

    std::cerr << "  sorting " << n_rows << " rows...\n";
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
        if (adsh[a] != adsh[b]) return adsh[a] < adsh[b];
        if (tags[a] != tags[b]) return tags[a] < tags[b];
        return versions[a] < versions[b];
    });
    std::cerr << "  sort done (" << elapsed_s(t0) << "s)\n";

    // Step 2: Build payload pool and collect unique keys with (offset, count)
    struct KeyEntry {
        int32_t  adsh_code, tag_code, version_code;
        uint32_t payload_offset, payload_count;
    };
    std::vector<KeyEntry> unique_keys;
    std::vector<uint32_t> payload_pool;
    unique_keys.reserve(n_rows / 2);
    payload_pool.reserve(n_rows);

    for (size_t i = 0; i < n_rows; ) {
        size_t j = i + 1;
        int32_t a0 = adsh[order[i]], t0k = tags[order[i]], v0 = versions[order[i]];
        while (j < n_rows && adsh[order[j]] == a0 &&
               tags[order[j]] == t0k && versions[order[j]] == v0) ++j;
        // rows [i, j) share the same key
        KeyEntry ke;
        ke.adsh_code      = a0;
        ke.tag_code       = t0k;
        ke.version_code   = v0;
        ke.payload_offset = (uint32_t)payload_pool.size();
        ke.payload_count  = (uint32_t)(j - i);
        unique_keys.push_back(ke);
        for (size_t r = i; r < j; ++r) payload_pool.push_back(order[r]);
        i = j;
    }
    std::vector<uint32_t>().swap(order); // free sort order

    size_t n_unique = unique_keys.size();
    std::cerr << "  unique keys: " << n_unique
              << ", payload pool: " << payload_pool.size()
              << " (" << elapsed_s(t0) << "s)\n";

    // Step 3: Build open-addressing hash table
    // C9: cap = next_pow2(n_unique * 2)
    uint32_t cap  = next_pow2((uint64_t)n_unique * 2);
    uint32_t mask = cap - 1;
    std::cerr << "  hash table: cap=" << cap
              << " (" << (uint64_t)cap * sizeof(PreSlot) / (1024*1024) << "MB)\n";

    std::vector<PreSlot> ht(cap);
    for (auto& s : ht) { s.adsh_code = -1; s.tag_code = -1; s.version_code = -1;
                          s.payload_offset = 0; s.payload_count = 0; }

    for (const auto& ke : unique_keys) {
        uint32_t slot = hash_i32x3(ke.adsh_code, ke.tag_code, ke.version_code) & mask;
        for (uint32_t probe = 0; probe < cap; ++probe) {
            uint32_t s = (slot + probe) & mask;
            if (ht[s].adsh_code == -1) {
                ht[s].adsh_code      = ke.adsh_code;
                ht[s].tag_code       = ke.tag_code;
                ht[s].version_code   = ke.version_code;
                ht[s].payload_offset = ke.payload_offset;
                ht[s].payload_count  = ke.payload_count;
                break;
            }
        }
    }
    std::vector<KeyEntry>().swap(unique_keys); // free

    // Step 4: Write file
    // Layout: [uint32_t cap][PreSlot * cap][uint32_t pool...]
    std::string out_path = gendb_dir + "/indexes/pre_join_index.bin";
    std::ofstream out(out_path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(&cap), 4);
    out.write(reinterpret_cast<const char*>(ht.data()), cap * sizeof(PreSlot));
    out.write(reinterpret_cast<const char*>(payload_pool.data()),
              payload_pool.size() * 4);

    size_t total_bytes = 4 + (size_t)cap * sizeof(PreSlot) + payload_pool.size() * 4;
    std::cerr << "[pre_join_index] done: " << n_rows << " rows, cap=" << cap
              << " unique_keys=" << n_unique
              << " total_file=" << total_bytes / (1024*1024) << "MB"
              << " in " << elapsed_s(t0) << "s\n"
              << "  -> " << out_path << "\n\n";
}

// ============================================================
// main
// ============================================================
int main(int argc, char** argv)
{
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }
    std::string gendb_dir = argv[1];
    fs::create_directories(gendb_dir + "/indexes");

    auto t_global = clk::now();
    std::cerr << "=== SEC EDGAR Index Building ===\n"
              << "  gendb_dir = " << gendb_dir << "\n"
              << "  threads   = " << omp_get_max_threads() << "\n\n";

    build_sub_adsh_index(gendb_dir);
    build_tag_index(gendb_dir);
    build_pre_join_index(gendb_dir);

    std::cerr << "=== Total index build time: " << elapsed_s(t_global) << "s ===\n";
    return 0;
}
