#include <iostream>
#include <fstream>
#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <thread>
#include <filesystem>

namespace fs = std::filesystem;

// ============================================================
// Binary column readers
// ============================================================
template<typename T>
std::vector<T> readColumn(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "Cannot open: " << path << "\n"; exit(1); }
    size_t sz = f.tellg();
    f.seekg(0);
    std::vector<T> data(sz / sizeof(T));
    f.read((char*)data.data(), sz);
    return data;
}

uint64_t readRowCount(const std::string& dir) {
    std::ifstream f(dir + "/_row_count.bin", std::ios::binary);
    uint64_t count = 0;
    f.read((char*)&count, sizeof(uint64_t));
    return count;
}

// ============================================================
// Hash function for composite keys
// ============================================================
static inline uint64_t hashKey2(uint32_t a, uint32_t b) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}

static inline uint64_t hashKey3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= (uint64_t)c * 3266489917ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}

// ============================================================
// Build UOM offset table for num
// Maps each uom_code value to (start_row, end_row) in sorted num table
// ============================================================
void buildUomOffsets(const std::string& gendbDir) {
    std::cerr << "Building uom_offsets...\n";
    auto uom = readColumn<uint8_t>(gendbDir + "/num/uom_code.bin");
    uint64_t N = uom.size();

    // Find max uom_code
    uint8_t maxCode = 0;
    for (auto c : uom) if (c > maxCode) maxCode = c;

    // Build offset table: for each code, (start, end)
    // Format: uint32_t num_entries, then num_entries * (uint8_t code, uint64_t start, uint64_t end)
    // Simpler: array of (start, end) indexed by code value
    size_t numEntries = maxCode + 1;
    std::vector<uint64_t> starts(numEntries, UINT64_MAX);
    std::vector<uint64_t> ends(numEntries, 0);

    for (uint64_t i = 0; i < N; i++) {
        uint8_t c = uom[i];
        if (starts[c] == UINT64_MAX) starts[c] = i;
        ends[c] = i + 1;
    }

    // Write: uint32_t num_entries, then pairs of (uint64_t start, uint64_t end)
    std::string path = gendbDir + "/num/uom_offsets.bin";
    std::ofstream f(path, std::ios::binary);
    uint32_t ne = (uint32_t)numEntries;
    f.write((const char*)&ne, sizeof(uint32_t));
    for (size_t i = 0; i < numEntries; i++) {
        uint64_t s = (starts[i] == UINT64_MAX) ? 0 : starts[i];
        uint64_t e = ends[i];
        f.write((const char*)&s, sizeof(uint64_t));
        f.write((const char*)&e, sizeof(uint64_t));
    }
    std::cerr << "  uom_offsets: " << numEntries << " entries\n";
}

// ============================================================
// Build STMT offset table for pre
// ============================================================
void buildStmtOffsets(const std::string& gendbDir) {
    std::cerr << "Building stmt_offsets...\n";
    auto stmt = readColumn<uint8_t>(gendbDir + "/pre/stmt_code.bin");
    uint64_t N = stmt.size();

    uint8_t maxCode = 0;
    for (auto c : stmt) if (c > maxCode) maxCode = c;

    size_t numEntries = maxCode + 1;
    std::vector<uint64_t> starts(numEntries, UINT64_MAX);
    std::vector<uint64_t> ends(numEntries, 0);

    for (uint64_t i = 0; i < N; i++) {
        uint8_t c = stmt[i];
        if (starts[c] == UINT64_MAX) starts[c] = i;
        ends[c] = i + 1;
    }

    std::string path = gendbDir + "/pre/stmt_offsets.bin";
    std::ofstream f(path, std::ios::binary);
    uint32_t ne = (uint32_t)numEntries;
    f.write((const char*)&ne, sizeof(uint32_t));
    for (size_t i = 0; i < numEntries; i++) {
        uint64_t s = (starts[i] == UINT64_MAX) ? 0 : starts[i];
        uint64_t e = ends[i];
        f.write((const char*)&s, sizeof(uint64_t));
        f.write((const char*)&e, sizeof(uint64_t));
    }
    std::cerr << "  stmt_offsets: " << numEntries << " entries\n";
}

// ============================================================
// Build ddate zone maps for num
// For each block of 100000 rows, store (min_ddate, max_ddate)
// ============================================================
void buildDdateZoneMaps(const std::string& gendbDir) {
    std::cerr << "Building ddate zone maps...\n";
    auto ddate = readColumn<int32_t>(gendbDir + "/num/ddate.bin");
    uint64_t N = ddate.size();
    const uint64_t blockSize = 100000;
    uint64_t numBlocks = (N + blockSize - 1) / blockSize;

    // Format: uint64_t num_blocks, uint64_t block_size, then numBlocks * (int32_t min, int32_t max)
    std::string path = gendbDir + "/indexes/num_ddate_zonemap.bin";
    std::ofstream f(path, std::ios::binary);
    f.write((const char*)&numBlocks, sizeof(uint64_t));
    f.write((const char*)&blockSize, sizeof(uint64_t));

    for (uint64_t b = 0; b < numBlocks; b++) {
        uint64_t start = b * blockSize;
        uint64_t end = std::min(start + blockSize, N);
        int32_t mn = ddate[start], mx = ddate[start];
        for (uint64_t i = start + 1; i < end; i++) {
            if (ddate[i] < mn) mn = ddate[i];
            if (ddate[i] > mx) mx = ddate[i];
        }
        f.write((const char*)&mn, sizeof(int32_t));
        f.write((const char*)&mx, sizeof(int32_t));
    }
    std::cerr << "  ddate zone maps: " << numBlocks << " blocks\n";
}

// ============================================================
// Build tag PK index: hash(tag_code, version_code) -> tag table row
// ============================================================
void buildTagPkIndex(const std::string& gendbDir) {
    std::cerr << "Building tag PK index...\n";
    auto tagCode = readColumn<uint32_t>(gendbDir + "/tag/tag_code.bin");
    auto verCode = readColumn<uint32_t>(gendbDir + "/tag/version_code.bin");
    uint64_t N = tagCode.size();

    // Hash table: open addressing with linear probing
    // Size: next power of 2 >= 2*N
    uint64_t tableSize = 1;
    while (tableSize < 2 * N) tableSize <<= 1;

    // Each slot: (tag_code: uint32, version_code: uint32, row_idx: uint32, occupied: uint8)
    // Pack as: tag_code, version_code, row_idx (0xFFFFFFFF = empty)
    struct Slot {
        uint32_t tag_code;
        uint32_t version_code;
        uint32_t row_idx;
    };
    std::vector<Slot> table(tableSize);
    for (auto& s : table) { s.tag_code = 0; s.version_code = 0; s.row_idx = UINT32_MAX; }

    for (uint64_t i = 0; i < N; i++) {
        uint64_t h = hashKey2(tagCode[i], verCode[i]) & (tableSize - 1);
        while (table[h].row_idx != UINT32_MAX) {
            h = (h + 1) & (tableSize - 1);
        }
        table[h].tag_code = tagCode[i];
        table[h].version_code = verCode[i];
        table[h].row_idx = (uint32_t)i;
    }

    // Write: uint64_t table_size, then table_size * Slot
    std::string path = gendbDir + "/indexes/tag_pk_index.idx";
    std::ofstream f(path, std::ios::binary);
    f.write((const char*)&tableSize, sizeof(uint64_t));
    f.write((const char*)table.data(), tableSize * sizeof(Slot));
    std::cerr << "  tag PK index: " << N << " entries in " << tableSize << " slots\n";
}

// ============================================================
// Build pre hash index: (sub_fk, tag_code, version_code) -> list of pre rows
// Format: bucket-based hash table
//   uint64_t num_buckets
//   uint64_t total_entries
//   uint64_t bucket_offsets[num_buckets + 1]  (cumulative)
//   Entry entries[total_entries]  where Entry = {sub_fk, tag_code, version_code, row_idx}
// ============================================================
void buildPreHashIndex(const std::string& gendbDir) {
    std::cerr << "Building pre hash index...\n";
    auto sub_fk = readColumn<uint32_t>(gendbDir + "/pre/sub_fk.bin");
    auto tag_code = readColumn<uint32_t>(gendbDir + "/pre/tag_code.bin");
    auto ver_code = readColumn<uint32_t>(gendbDir + "/pre/version_code.bin");
    uint64_t N = sub_fk.size();

    // Choose number of buckets: next power of 2 >= N
    uint64_t numBuckets = 1;
    while (numBuckets < N) numBuckets <<= 1;

    // Count entries per bucket
    std::vector<uint32_t> bucketCounts(numBuckets, 0);
    for (uint64_t i = 0; i < N; i++) {
        uint64_t h = hashKey3(sub_fk[i], tag_code[i], ver_code[i]) & (numBuckets - 1);
        bucketCounts[h]++;
    }

    // Build cumulative offsets
    std::vector<uint64_t> bucketOffsets(numBuckets + 1);
    bucketOffsets[0] = 0;
    for (uint64_t i = 0; i < numBuckets; i++) {
        bucketOffsets[i + 1] = bucketOffsets[i] + bucketCounts[i];
    }

    // Fill entries
    struct Entry {
        uint32_t sub_fk;
        uint32_t tag_code;
        uint32_t version_code;
        uint32_t row_idx;
    };
    std::vector<Entry> entries(N);
    std::vector<uint32_t> bucketPos(numBuckets, 0); // current position within each bucket

    for (uint64_t i = 0; i < N; i++) {
        uint64_t h = hashKey3(sub_fk[i], tag_code[i], ver_code[i]) & (numBuckets - 1);
        uint64_t pos = bucketOffsets[h] + bucketPos[h];
        entries[pos] = { sub_fk[i], tag_code[i], ver_code[i], (uint32_t)i };
        bucketPos[h]++;
    }

    // Write index
    std::string path = gendbDir + "/indexes/pre_by_adsh_tag_ver.idx";
    std::ofstream f(path, std::ios::binary);
    f.write((const char*)&numBuckets, sizeof(uint64_t));
    uint64_t totalEntries = N;
    f.write((const char*)&totalEntries, sizeof(uint64_t));
    f.write((const char*)bucketOffsets.data(), (numBuckets + 1) * sizeof(uint64_t));
    f.write((const char*)entries.data(), N * sizeof(Entry));

    std::cerr << "  pre hash index: " << N << " entries in " << numBuckets << " buckets\n";
    // Report avg bucket occupancy
    uint32_t maxBucket = *std::max_element(bucketCounts.begin(), bucketCounts.end());
    uint64_t nonEmpty = 0;
    for (auto c : bucketCounts) if (c > 0) nonEmpty++;
    std::cerr << "  non-empty buckets: " << nonEmpty << ", max bucket: " << maxBucket << "\n";
}

// ============================================================
// Main
// ============================================================
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: build_indexes <gendb_dir>\n";
        return 1;
    }
    std::string gendbDir = argv[1];
    auto t0 = std::chrono::steady_clock::now();

    fs::create_directories(gendbDir + "/indexes");

    // Build all indexes
    buildUomOffsets(gendbDir);
    buildStmtOffsets(gendbDir);
    buildDdateZoneMaps(gendbDir);
    buildTagPkIndex(gendbDir);
    buildPreHashIndex(gendbDir);

    auto t1 = std::chrono::steady_clock::now();
    std::cerr << "Total index build time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << " ms\n";
    return 0;
}
