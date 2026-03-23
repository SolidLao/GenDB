// Q24 iter_1: Flat uint64_t hash set for anti-join + parallel num scan
// Key optimizations:
//   1. Flat open-addressing hash set using packed uint64_t keys (no heap allocs)
//   2. Key = (adsh_id<<40) | (tag_id<<20) | ver_id - fits in 60 bits
//   3. 32 threads for parallel num scan
//   4. mmap for both binary files
#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <thread>
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "timing_utils.h"

using namespace std;

#pragma pack(push, 1)
struct NumRow { uint32_t adsh_id, tag_id, version_id, ddate; double value; };
struct PreRow { uint32_t adsh_id, tag_id, version_id, plabel_id, line; char stmt[3], rfile[2]; uint8_t inpth, negating; };
#pragma pack(pop)

struct VarDict {
    uint32_t count = 0; vector<const char*> strings; vector<char> data_buf;
    void load(const string& path) {
        FILE* f = fopen(path.c_str(), "rb"); fread(&count, 4, 1, f);
        vector<uint32_t> offsets(count+1); fread(offsets.data(), 4, count+1, f);
        uint32_t ds = offsets[count]; data_buf.resize(ds); fread(data_buf.data(), 1, ds, f); fclose(f);
        strings.resize(count); for (uint32_t i = 0; i < count; i++) strings[i] = data_buf.data() + offsets[i];
    }
    const char* get(uint32_t id) const { return (id < count) ? strings[id] : ""; }
};

// Flat open-addressing hash set for uint64_t keys
// Uses UINT64_MAX as the empty sentinel
// No heap allocations after construction
struct FlatHashSet64 {
    vector<uint64_t> slots;
    size_t mask;
    static constexpr uint64_t EMPTY = UINT64_MAX;

    FlatHashSet64(size_t capacity_hint) {
        size_t cap = 1;
        while (cap < capacity_hint * 2) cap <<= 1;
        slots.assign(cap, EMPTY);
        mask = cap - 1;
    }

    inline void insert(uint64_t key) {
        // Mix for better distribution
        uint64_t h = key ^ (key >> 33);
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        size_t idx = h & mask;
        while (slots[idx] != EMPTY && slots[idx] != key) idx = (idx + 1) & mask;
        slots[idx] = key;
    }

    inline bool contains(uint64_t key) const {
        uint64_t h = key ^ (key >> 33);
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        size_t idx = h & mask;
        while (slots[idx] != EMPTY && slots[idx] != key) idx = (idx + 1) & mask;
        return slots[idx] == key;
    }
};

inline uint64_t pack_key(uint32_t adsh, uint32_t tag, uint32_t ver) {
    // adsh < 2^24, tag < 2^20, ver < 2^20
    return ((uint64_t)adsh << 40) | ((uint64_t)(tag & 0xFFFFF) << 20) | (ver & 0xFFFFF);
}

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];

    VarDict tag_dict, version_dict;
    tag_dict.load(gendb_dir + "/tag.dict");
    version_dict.load(gendb_dir + "/version.dict");

    // mmap pre.bin and build flat hash set of all (adsh, tag, ver)
    FlatHashSet64 pre_set(10000000); // 9.6M pre rows
    {
        int fd = open((gendb_dir + "/pre.bin").c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const char* m = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise((void*)m, st.st_size, MADV_SEQUENTIAL);
        close(fd);

        uint64_t count; memcpy(&count, m, 8);
        const PreRow* rows = reinterpret_cast<const PreRow*>(m + 8);
        for (uint64_t i = 0; i < count; i++) {
            pre_set.insert(pack_key(rows[i].adsh_id, rows[i].tag_id, rows[i].version_id));
        }
        munmap((void*)m, st.st_size);
    }

    // mmap num_usd.bin
    int fd = open((gendb_dir + "/num_usd.bin").c_str(), O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t file_size = st.st_size;
    const char* mapped = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise((void*)mapped, file_size, MADV_SEQUENTIAL);
    close(fd);

    uint64_t total_rows;
    memcpy(&total_rows, mapped, 8);
    const NumRow* rows = reinterpret_cast<const NumRow*>(mapped + 8);

    // Parallel scan: filter by ddate range, anti-join, group by (tag_id, version_id)
    int num_threads = 32;
    struct ThreadData {
        unordered_map<uint64_t, pair<uint64_t,double>> groups; // (tag<<20|ver) → (cnt, sum)
    };
    vector<ThreadData> tdata(num_threads);
    for (auto& td : tdata) td.groups.reserve(1024);

    auto worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end = (uint64_t)(tid+1) * total_rows / num_threads;
        auto& groups = tdata[tid].groups;

        for (uint64_t i = start; i < end; i++) {
            const NumRow& r = rows[i];
            // Filter: ddate in [20230101, 20231231]
            if (r.ddate < 20230101 || r.ddate > 20231231) continue;
            // Anti-join: skip if (adsh, tag, ver) in pre
            if (pre_set.contains(pack_key(r.adsh_id, r.tag_id, r.version_id))) continue;
            // Group by (tag_id, version_id)
            uint64_t gkey = ((uint64_t)r.tag_id << 20) | r.version_id;
            auto& g = groups[gkey];
            g.first++;
            g.second += r.value;
        }
    };

    vector<thread> threads;
    for (int i = 0; i < num_threads; i++) threads.emplace_back(worker, i);
    for (auto& t : threads) t.join();

    munmap((void*)mapped, file_size);

    // Merge
    unordered_map<uint64_t, pair<uint64_t,double>> merged;
    merged.reserve(10000);
    for (auto& td : tdata) {
        for (auto& [k, v] : td.groups) {
            auto& m = merged[k];
            m.first += v.first;
            m.second += v.second;
        }
    }

    // Filter HAVING cnt > 10, collect results
    struct Result { string tag, ver; uint64_t cnt; double total; };
    vector<Result> results;
    for (auto& [k, v] : merged) {
        if (v.first <= 10) continue;
        uint32_t tag_id = (uint32_t)(k >> 20);
        uint32_t ver_id = (uint32_t)(k & 0xFFFFF);
        results.push_back({tag_dict.get(tag_id), version_dict.get(ver_id), v.first, v.second});
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.cnt > b.cnt;
    });
    if (results.size() > 100) results.resize(100);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q24.csv").c_str(), "w");
        fprintf(f, "tag,version,cnt,total\n");
        for (auto& r : results) {
            fprintf(f, "%s,%s,%lu,%.2f\n", r.tag.c_str(), r.ver.c_str(), r.cnt, r.total);
        }
        fclose(f);
    }
    return 0;
}
