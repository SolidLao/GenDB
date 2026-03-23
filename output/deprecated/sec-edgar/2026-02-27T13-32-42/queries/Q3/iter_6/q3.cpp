// Q3 iter_6: Fix 113ms map initialization by moving init into scan threads (parallel touch-fault).
// Key insight: Linux page faults for new memory are slow (~2μs/page, 4KB pages).
// 201MB of flat maps / 4KB = 50K page faults × 2μs = 100ms.
// Fix: each scan thread initializes its OWN maps (parallel page faults = 100ms/32 ≈ 3ms).
// Additional: use plain loop instead of std::function<> for merge iteration (avoids virtual dispatch).
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
struct SubRow { uint32_t adsh_id, cik, name_id; int32_t sic, fy; uint32_t period, filed; uint8_t wksi; char form[11], fp[3], afs[6], countryba[3], countryinc[4], fye[5]; };
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

struct Slot { uint64_t key; double val; };
static const uint64_t EMPTY_KEY = UINT64_MAX;

struct FlatDoubleMap {
    Slot* slots = nullptr;
    uint64_t cap, mask;

    void init_and_alloc(uint64_t capacity) {
        cap = capacity; mask = cap - 1;
        // Allocate via mmap (avoids C++ zero-init overhead) and zero with MADV_DONTNEED trick
        slots = (Slot*)mmap(nullptr, cap * sizeof(Slot), PROT_READ|PROT_WRITE,
                            MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        memset(slots, 0xFF, cap * sizeof(Slot));  // sets key bytes to 0xFF = UINT64_MAX, val to NaN (doesn't matter)
        // Re-zero the val fields: trick - actually 0xFF pattern for double is -nan
        // Better: set only key bytes to UINT64_MAX, zero the val
        for (uint64_t i = 0; i < cap; i++) { slots[i].key = EMPTY_KEY; slots[i].val = 0.0; }
    }

    void dealloc() {
        if (slots) { munmap(slots, cap * sizeof(Slot)); slots = nullptr; }
    }

    inline void add(uint64_t key, double v) {
        uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        uint64_t idx = h & mask;
        while (slots[idx].key != EMPTY_KEY && slots[idx].key != key) idx = (idx+1) & mask;
        slots[idx].key = key;
        slots[idx].val += v;
    }
};

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];
    auto t0 = hrc::now();

    VarDict name_dict;
    name_dict.load(gendb_dir + "/name.dict");

    uint32_t max_adsh_id = 0;
    vector<SubRow> sub_rows;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        sub_rows.resize(count); fread(sub_rows.data(), sizeof(SubRow), count, f); fclose(f);
        for (auto& r : sub_rows) if (r.adsh_id > max_adsh_id) max_adsh_id = r.adsh_id;
    }
    max_adsh_id++;
    vector<uint8_t> adsh_is2022(max_adsh_id, 0);
    vector<uint32_t> adsh_cik(max_adsh_id, 0);
    vector<uint32_t> adsh_name(max_adsh_id, 0);
    for (auto& r : sub_rows) {
        if (r.fy == 2022) { adsh_is2022[r.adsh_id] = 1; adsh_cik[r.adsh_id] = r.cik; adsh_name[r.adsh_id] = r.name_id; }
    }
    sub_rows.clear();
    fprintf(stderr, "[phase] sub: %.1f ms\n", ms_since(t0));

    int fd = open((gendb_dir + "/num_usd.bin").c_str(), O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t file_size = st.st_size;
    const char* mapped = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise((void*)mapped, file_size, MADV_SEQUENTIAL);
    close(fd);

    uint64_t total_rows;
    memcpy(&total_rows, mapped, 8);
    const NumRow* rows = reinterpret_cast<const NumRow*>(mapped + 8);

    int num_threads = 32;
    // Allocate map pointers (not the maps themselves)
    vector<FlatDoubleMap*> cik_maps(num_threads, nullptr), nc_maps(num_threads, nullptr);

    auto scan_worker = [&](int tid) {
        // Initialize maps IN this thread (parallel page faults)
        FlatDoubleMap* cm = new FlatDoubleMap(); cm->init_and_alloc(131072);
        FlatDoubleMap* nm = new FlatDoubleMap(); nm->init_and_alloc(262144);
        cik_maps[tid] = cm; nc_maps[tid] = nm;

        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end_idx = (uint64_t)(tid+1) * total_rows / num_threads;

        for (uint64_t i = start; i < end_idx; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= max_adsh_id) continue;
            if (!adsh_is2022[r.adsh_id]) continue;
            uint32_t cik = adsh_cik[r.adsh_id];
            uint32_t name_id = adsh_name[r.adsh_id];
            cm->add((uint64_t)cik, r.value);
            nm->add(((uint64_t)name_id << 32) | cik, r.value);
        }
    };

    {
        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) threads.emplace_back(scan_worker, i);
        for (auto& t : threads) t.join();
    }
    munmap((void*)mapped, file_size);
    fprintf(stderr, "[phase] scan: %.1f ms\n", ms_since(t0));

    // Parallel merge: 32 merge threads, each handles partition hash%32
    const int MERGE_T = 32;
    vector<unordered_map<uint64_t, double>> merged_cik(MERGE_T), merged_nc(MERGE_T);

    auto merge_worker = [&](int mt) {
        auto& mc = merged_cik[mt];
        auto& mn = merged_nc[mt];
        mc.reserve(2048); mn.reserve(4096);
        for (int tid = 0; tid < num_threads; tid++) {
            // Iterate cik_map[tid]
            const Slot* cs = cik_maps[tid]->slots;
            uint64_t cc = cik_maps[tid]->cap;
            for (uint64_t j = 0; j < cc; j++) {
                if (cs[j].key == EMPTY_KEY) continue;
                uint64_t key = cs[j].key;
                uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
                if ((int)(h % MERGE_T) == mt) mc[key] += cs[j].val;
            }
            // Iterate nc_map[tid]
            const Slot* ns = nc_maps[tid]->slots;
            uint64_t nc = nc_maps[tid]->cap;
            for (uint64_t j = 0; j < nc; j++) {
                if (ns[j].key == EMPTY_KEY) continue;
                uint64_t key = ns[j].key;
                uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
                if ((int)(h % MERGE_T) == mt) mn[key] += ns[j].val;
            }
        }
    };

    {
        vector<thread> threads;
        for (int i = 0; i < MERGE_T; i++) threads.emplace_back(merge_worker, i);
        for (auto& t : threads) t.join();
    }

    // Free scan maps
    for (int i = 0; i < num_threads; i++) {
        if (cik_maps[i]) { cik_maps[i]->dealloc(); delete cik_maps[i]; }
        if (nc_maps[i]) { nc_maps[i]->dealloc(); delete nc_maps[i]; }
    }
    fprintf(stderr, "[phase] merge: %.1f ms\n", ms_since(t0));

    // Compute avg_sub_total
    double total_cik_sum = 0.0;
    uint64_t num_ciks = 0;
    for (auto& m : merged_cik) { for (auto& [k, v] : m) { total_cik_sum += v; num_ciks++; } }
    double avg_sub_total = num_ciks > 0 ? total_cik_sum / (double)num_ciks : 0.0;

    // Filter and collect results
    struct Result { string name; uint32_t cik; double total; };
    vector<Result> results;
    results.reserve(10000);
    for (auto& m : merged_nc) {
        for (auto& [key, total] : m) {
            if (total <= avg_sub_total) continue;
            uint32_t name_id = (uint32_t)(key >> 32);
            uint32_t cik = (uint32_t)(key & 0xFFFFFFFF);
            results.push_back({name_dict.get(name_id), cik, total});
        }
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.total > b.total;
    });
    if (results.size() > 100) results.resize(100);
    fprintf(stderr, "[phase] filter+sort: %.1f ms\n", ms_since(t0));

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q3.csv").c_str(), "w");
        fprintf(f, "name,cik,total_value\n");
        for (auto& r : results) {
            bool need_quote = r.name.find(',') != string::npos || r.name.find('"') != string::npos;
            if (need_quote) {
                string esc = r.name; size_t pos = 0;
                while ((pos = esc.find('"', pos)) != string::npos) { esc.insert(pos, "\""); pos += 2; }
                fprintf(f, "\"%s\",%u,%.2f\n", esc.c_str(), r.cik, r.total);
            } else {
                fprintf(f, "%s,%u,%.2f\n", r.name.c_str(), r.cik, r.total);
            }
        }
        fclose(f);
    }
    return 0;
}
