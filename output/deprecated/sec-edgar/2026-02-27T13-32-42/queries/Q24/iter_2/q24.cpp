// Q24 iter_2: Use pre-built pre_atv_fset.bin for fast anti-join.
// Key improvement over iter_1:
//   - Pre-built flat hash set (128MB) loaded via mmap instead of building from 248MB pre.bin
//   - Eliminates 9.6M hash insertions + 248MB file parse → saves ~200-250ms
//   - Parallel dict loading (tag + version in parallel)
// pre_atv_fset.bin: uint64_t cap | cap × uint64_t slots (UINT64_MAX = empty)
// Contains pack_key(adsh, tag, ver) for every row in pre.bin
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
#include <chrono>
#include "timing_utils.h"

using namespace std;
using hrc = chrono::high_resolution_clock;
static inline double ms_since(hrc::time_point t) { return chrono::duration<double,milli>(hrc::now()-t).count(); }

#pragma pack(push, 1)
struct NumRow { uint32_t adsh_id, tag_id, version_id, ddate; double value; };
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

static const uint64_t FSET_EMPTY = UINT64_MAX;

inline uint64_t pack_key(uint32_t a, uint32_t t, uint32_t v) {
    return ((uint64_t)a << 40) | ((uint64_t)(t & 0xFFFFF) << 20) | (v & 0xFFFFF);
}

inline bool fset_contains(const uint64_t* slots, uint64_t cap, uint64_t key) {
    uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
    uint64_t idx = h & (cap - 1);
    while (slots[idx] != FSET_EMPTY && slots[idx] != key) idx = (idx + 1) & (cap - 1);
    return slots[idx] == key;
}

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];
    auto t0 = hrc::now();

    // Load dicts in parallel
    VarDict tag_dict, version_dict;
    {
        vector<thread> dt;
        dt.emplace_back([&]() { tag_dict.load(gendb_dir + "/tag.dict"); });
        dt.emplace_back([&]() { version_dict.load(gendb_dir + "/version.dict"); });
        for (auto& t : dt) t.join();
    }
    fprintf(stderr, "[phase] dicts: %.1f ms\n", ms_since(t0));

    // Load pre_atv_fset.bin via mmap + MADV_WILLNEED
    uint64_t fset_cap = 0;
    const uint64_t* fset_slots = nullptr;
    size_t fset_size = 0;
    void* fset_mmap = nullptr;
    {
        int ffd = open((gendb_dir + "/pre_atv_fset.bin").c_str(), O_RDONLY);
        if (ffd < 0) { fprintf(stderr, "ERROR: pre_atv_fset.bin not found\n"); return 1; }
        struct stat fst; fstat(ffd, &fst);
        fset_size = fst.st_size;
        fset_mmap = mmap(nullptr, fset_size, PROT_READ, MAP_PRIVATE, ffd, 0);
        madvise(fset_mmap, fset_size, MADV_WILLNEED);
        close(ffd);
        memcpy(&fset_cap, fset_mmap, 8);
        fset_slots = reinterpret_cast<const uint64_t*>((const char*)fset_mmap + 8);
    }
    fprintf(stderr, "[phase] fset loaded: %.1f ms (cap=%lu, %.0f MB)\n",
            ms_since(t0), (unsigned long)fset_cap, (double)fset_size / (1024*1024));

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
    fprintf(stderr, "[phase] num_usd: %.1f ms (rows=%lu)\n", ms_since(t0), (unsigned long)total_rows);

    // Parallel scan: 32 threads, ddate filter + anti-join + group by (tag, ver)
    int num_threads = 32;
    struct ThreadData {
        unordered_map<uint64_t, pair<uint64_t,double>> groups;
    };
    vector<ThreadData> tdata(num_threads);
    for (auto& td : tdata) td.groups.reserve(1024);

    auto worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end = (uint64_t)(tid+1) * total_rows / num_threads;
        auto& groups = tdata[tid].groups;

        for (uint64_t i = start; i < end; i++) {
            const NumRow& r = rows[i];
            if (r.ddate < 20230101 || r.ddate > 20231231) continue;
            if (fset_contains(fset_slots, fset_cap, pack_key(r.adsh_id, r.tag_id, r.version_id))) continue;
            uint64_t gkey = ((uint64_t)r.tag_id << 20) | r.version_id;
            auto& g = groups[gkey];
            g.first++;
            g.second += r.value;
        }
    };

    {
        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) threads.emplace_back(worker, i);
        for (auto& t : threads) t.join();
    }
    munmap((void*)mapped, file_size);
    fprintf(stderr, "[phase] scan: %.1f ms\n", ms_since(t0));

    // Merge thread-local groups
    unordered_map<uint64_t, pair<uint64_t,double>> merged;
    merged.reserve(10000);
    for (auto& td : tdata) {
        for (auto& [k, v] : td.groups) {
            auto& m = merged[k];
            m.first += v.first;
            m.second += v.second;
        }
    }

    munmap(fset_mmap, fset_size);
    fprintf(stderr, "[phase] merge: %.1f ms\n", ms_since(t0));

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
