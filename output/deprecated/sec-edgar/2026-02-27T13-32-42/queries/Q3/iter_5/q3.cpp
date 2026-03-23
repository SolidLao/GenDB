// Q3 iter_5: Full parallelism with flat open-addressing hash maps.
// With --financial $100 tolerance, we don't need exact float ordering.
// Strategy:
//   1. 32 scan threads each maintain thread-local flat maps:
//      - cik_sum: uint32_t cik → double (cap 131072 for ~30K entries)
//      - name_cik: uint64_t (name_id<<32|cik) → double (cap 262144 for ~90K entries)
//   2. Parallel merge: 32 merge threads each handle partition hash(key)%32
//      Each merge thread iterates all 32 scan maps, collects its partition
//   3. Compute avg_sub_total, filter, sort, output
#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <thread>
#include <atomic>
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

// Flat open-addressing hash map: uint64_t key → double value
// Using UINT64_MAX as empty sentinel, accumulates doubles
struct FlatDoubleMap {
    struct Slot { uint64_t key; double val; };
    vector<Slot> slots;
    uint64_t cap, mask;
    static const uint64_t EMPTY = UINT64_MAX;

    void init(uint64_t capacity) {  // capacity must be power of 2
        cap = capacity; mask = cap - 1;
        slots.assign(cap, {EMPTY, 0.0});
    }

    inline void add(uint64_t key, double v) {
        uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        uint64_t idx = h & mask;
        while (slots[idx].key != EMPTY && slots[idx].key != key) idx = (idx+1) & mask;
        slots[idx].key = key;
        slots[idx].val += v;
    }

    // Iterate all entries, calling fn(key, val)
    void each(const function<void(uint64_t,double)>& fn) const {
        for (auto& s : slots) if (s.key != EMPTY) fn(s.key, s.val);
    }
};

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];
    auto t0 = hrc::now();

    VarDict name_dict;
    name_dict.load(gendb_dir + "/name.dict");

    // Load sub → dense arrays indexed by adsh_id
    uint32_t max_adsh_id = 0;
    vector<SubRow> sub_rows;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        sub_rows.resize(count); fread(sub_rows.data(), sizeof(SubRow), count, f); fclose(f);
        for (auto& r : sub_rows) if (r.adsh_id > max_adsh_id) max_adsh_id = r.adsh_id;
    }
    max_adsh_id++;
    // Compact arrays: fy=2022 flag, cik, name_id
    vector<uint8_t> adsh_is2022(max_adsh_id, 0);
    vector<uint32_t> adsh_cik(max_adsh_id, 0);
    vector<uint32_t> adsh_name(max_adsh_id, 0);
    for (auto& r : sub_rows) {
        if (r.fy == 2022) { adsh_is2022[r.adsh_id] = 1; adsh_cik[r.adsh_id] = r.cik; adsh_name[r.adsh_id] = r.name_id; }
    }
    sub_rows.clear();
    fprintf(stderr, "[phase] sub loaded: %.1f ms\n", ms_since(t0));

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

    // Parallel scan: 32 threads with thread-local flat maps
    int num_threads = 32;
    // cik_sum: cik → double (cap 131072 = 128K, load factor ~23% for 30K CIKs)
    // name_cik: (name_id<<32|cik) → double (cap 262144 = 256K, load factor ~35% for 90K)
    vector<FlatDoubleMap> cik_maps(num_threads), nc_maps(num_threads);
    for (int i = 0; i < num_threads; i++) { cik_maps[i].init(131072); nc_maps[i].init(262144); }
    fprintf(stderr, "[phase] maps init: %.1f ms\n", ms_since(t0));

    auto scan_worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end_idx = (uint64_t)(tid+1) * total_rows / num_threads;
        auto& cik_m = cik_maps[tid];
        auto& nc_m = nc_maps[tid];

        for (uint64_t i = start; i < end_idx; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= max_adsh_id) continue;
            if (!adsh_is2022[r.adsh_id]) continue;
            uint32_t cik = adsh_cik[r.adsh_id];
            uint32_t name_id = adsh_name[r.adsh_id];
            cik_m.add((uint64_t)cik, r.value);
            nc_m.add(((uint64_t)name_id << 32) | cik, r.value);
        }
    };

    {
        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) threads.emplace_back(scan_worker, i);
        for (auto& t : threads) t.join();
    }
    munmap((void*)mapped, file_size);
    fprintf(stderr, "[phase] scan done: %.1f ms\n", ms_since(t0));

    // Parallel merge: 32 merge threads, each handles partition hash(key)%32
    // Thread mt collects entries from all scan maps where (h>>32)&31 == mt
    const int MERGE_T = 32;
    vector<unordered_map<uint64_t, double>> merged_cik(MERGE_T), merged_nc(MERGE_T);
    for (auto& m : merged_cik) m.reserve(2048);
    for (auto& m : merged_nc) m.reserve(4096);

    auto merge_worker = [&](int mt) {
        auto& mc = merged_cik[mt];
        auto& mn = merged_nc[mt];
        for (int tid = 0; tid < num_threads; tid++) {
            cik_maps[tid].each([&](uint64_t key, double val) {
                uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
                if ((int)(h % MERGE_T) == mt) mc[key] += val;
            });
            nc_maps[tid].each([&](uint64_t key, double val) {
                uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
                if ((int)(h % MERGE_T) == mt) mn[key] += val;
            });
        }
    };

    {
        vector<thread> threads;
        for (int i = 0; i < MERGE_T; i++) threads.emplace_back(merge_worker, i);
        for (auto& t : threads) t.join();
    }
    fprintf(stderr, "[phase] merge done: %.1f ms\n", ms_since(t0));

    // Compute avg_sub_total from all merged_cik partitions
    double total_cik_sum = 0.0;
    uint64_t num_ciks = 0;
    for (auto& m : merged_cik) { for (auto& [k, v] : m) { total_cik_sum += v; num_ciks++; } }
    double avg_sub_total = num_ciks > 0 ? total_cik_sum / (double)num_ciks : 0.0;
    fprintf(stderr, "[phase] avg computed: %.1f ms (num_ciks=%lu, avg=%.2f)\n", ms_since(t0), (unsigned long)num_ciks, avg_sub_total);

    // Filter name_cik pairs above avg_sub_total
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
