// Q6 iter_3: Use pre-built pre_is.bin (sorted IS rows, ~20MB) instead of double-reading 248MB pre.bin.
// Key improvements over iter_2:
//   1. Single small file read (~20MB) vs 2× 248MB pre.bin reads + sort → saves ~150ms I/O
//   2. Pre-sorted by (adsh_id, tv) → no sorting step needed
//   3. Better merge reserve sizing to avoid rehashing
//   4. Phase timing added to identify bottlenecks
//
// pre_is.bin format:
//   8 bytes: uint64_t max_adsh
//   4*max_adsh bytes: adsh_count[adsh_id]
//   4*(max_adsh+1) bytes: adsh_start[adsh_id]
//   N * 12 bytes: PreISEntry { uint64_t tv; uint32_t plabel_id; }
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
#include <chrono>
#include "timing_utils.h"

using namespace std;
using hrc = chrono::high_resolution_clock;
static inline double ms_since(hrc::time_point t) { return chrono::duration<double,milli>(hrc::now()-t).count(); }

#pragma pack(push, 1)
struct SubRow { uint32_t adsh_id, cik, name_id; int32_t sic, fy; uint32_t period, filed; uint8_t wksi; char form[11], fp[3], afs[6], countryba[3], countryinc[4], fye[5]; };
struct NumRow { uint32_t adsh_id, tag_id, version_id, ddate; double value; };
struct PreISEntry { uint64_t tv; uint32_t plabel_id; };
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

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];
    auto t0 = hrc::now();

    VarDict name_dict, tag_dict, plabel_dict;
    name_dict.load(gendb_dir + "/name.dict");
    tag_dict.load(gendb_dir + "/tag.dict");
    plabel_dict.load(gendb_dir + "/plabel.dict");
    fprintf(stderr, "[phase] dicts loaded: %.1f ms\n", ms_since(t0));

    // Load sub: adsh_id → {fy, name_id}
    uint32_t max_adsh = 0;
    vector<SubRow> sub_rows;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        sub_rows.resize(count); fread(sub_rows.data(), sizeof(SubRow), count, f); fclose(f);
        for (auto& r : sub_rows) if (r.adsh_id > max_adsh) max_adsh = r.adsh_id;
    }
    max_adsh++;
    vector<int32_t> adsh_fy(max_adsh, -1);
    vector<uint32_t> adsh_name(max_adsh, 0);
    for (auto& r : sub_rows) { adsh_fy[r.adsh_id] = r.fy; adsh_name[r.adsh_id] = r.name_id; }
    sub_rows.clear();
    fprintf(stderr, "[phase] sub loaded: %.1f ms\n", ms_since(t0));

    // Load pre_is.bin: pre-sorted IS rows
    // Format: uint64_t max_adsh | uint32_t adsh_count[max_adsh] | uint32_t adsh_start[max_adsh+1] | PreISEntry[N]
    vector<uint32_t> adsh_count, adsh_start;
    const PreISEntry* pre_is_data = nullptr;
    size_t pre_is_file_size = 0;
    void* pre_is_mmap = nullptr;
    uint64_t total_is_entries = 0;

    {
        int fd = open((gendb_dir + "/pre_is.bin").c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "ERROR: pre_is.bin not found in %s\n", gendb_dir.c_str()); return 1; }
        struct stat st; fstat(fd, &st);
        pre_is_file_size = st.st_size;
        pre_is_mmap = mmap(nullptr, pre_is_file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(pre_is_mmap, pre_is_file_size, MADV_SEQUENTIAL);
        close(fd);

        const char* m = (const char*)pre_is_mmap;
        uint64_t stored_max_adsh; memcpy(&stored_max_adsh, m, 8); m += 8;
        uint32_t smaxA = (uint32_t)stored_max_adsh;

        adsh_count.resize(smaxA);
        memcpy(adsh_count.data(), m, 4 * smaxA); m += 4 * smaxA;

        adsh_start.resize(smaxA + 1);
        memcpy(adsh_start.data(), m, 4 * (smaxA + 1)); m += 4 * (smaxA + 1);

        total_is_entries = adsh_start[smaxA];
        pre_is_data = reinterpret_cast<const PreISEntry*>(m);

        // Adjust max_adsh if needed
        if (smaxA < max_adsh) max_adsh = smaxA;
    }
    madvise(pre_is_mmap, pre_is_file_size, MADV_WILLNEED);
    fprintf(stderr, "[phase] pre_is.bin loaded: %.1f ms (total_is=%lu)\n", ms_since(t0), (unsigned long)total_is_entries);

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
    fprintf(stderr, "[phase] num_usd mmap ready: %.1f ms (rows=%lu)\n", ms_since(t0), (unsigned long)total_rows);

    // Parallel scan: 32 threads with thread-local flat hash maps
    // Group key: (name_id<<40 | tag_id<<20 | plabel_id)
    struct GroupVal { double sum; uint64_t cnt; };

    int num_threads = 32;
    // Flat open-addressing hash map for group aggregation
    // Using unordered_map but with large reserve to avoid rehash
    vector<unordered_map<uint64_t, GroupVal>*> tdata(num_threads, nullptr);

    auto worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end_idx = (uint64_t)(tid+1) * total_rows / num_threads;
        auto* groups = new unordered_map<uint64_t, GroupVal>();
        groups->reserve(131072);  // 128K capacity, power of 2

        for (uint64_t i = start; i < end_idx; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= (uint32_t)max_adsh) continue;
            if (adsh_fy[r.adsh_id] != 2023) continue;

            uint32_t adsh = r.adsh_id;
            uint32_t cnt = adsh_count[adsh];
            if (cnt == 0) continue;

            // Binary search in pre_is for this adsh's IS entries
            uint64_t tv = ((uint64_t)r.tag_id << 20) | r.version_id;
            uint32_t lo = adsh_start[adsh];
            uint32_t hi = lo + cnt;

            // Binary search for tv
            uint32_t lo2 = lo, hi2 = hi;
            while (lo2 < hi2) {
                uint32_t mid = (lo2 + hi2) >> 1;
                if (pre_is_data[mid].tv < tv) lo2 = mid + 1;
                else hi2 = mid;
            }
            if (lo2 >= hi || pre_is_data[lo2].tv != tv) continue;

            uint32_t name_id = adsh_name[adsh];
            for (uint32_t j = lo2; j < hi && pre_is_data[j].tv == tv; j++) {
                uint32_t plabel_id = pre_is_data[j].plabel_id;
                uint64_t gk = ((uint64_t)name_id << 40) | ((uint64_t)(r.tag_id & 0xFFFFF) << 20) | (plabel_id & 0xFFFFF);
                auto& g = (*groups)[gk];
                g.sum += r.value;
                g.cnt++;
            }
        }
        tdata[tid] = groups;
    };

    {
        auto t_scan = hrc::now();
        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) threads.emplace_back(worker, i);
        for (auto& t : threads) t.join();
        fprintf(stderr, "[phase] num scan done: %.1f ms\n", ms_since(t0));
    }

    munmap((void*)mapped, file_size);

    // Merge thread-local flat maps into global unordered_map
    unordered_map<uint64_t, GroupVal> merged;
    merged.reserve(1000000);  // generous reserve to avoid rehash
    for (int tid = 0; tid < num_threads; tid++) {
        if (!tdata[tid]) continue;
        for (auto& [k, v] : *tdata[tid]) {
            auto& m = merged[k];
            m.sum += v.sum;
            m.cnt += v.cnt;
        }
        delete tdata[tid];
        tdata[tid] = nullptr;
    }
    fprintf(stderr, "[phase] merge done: %.1f ms (groups=%zu)\n", ms_since(t0), merged.size());

    munmap(pre_is_mmap, pre_is_file_size);

    // Collect results (use ids, defer string lookup to output time)
    struct Result { uint32_t name_id, tag_id, plabel_id; double total; uint64_t cnt; };
    vector<Result> results;
    results.reserve(merged.size());
    for (auto& [k, v] : merged) {
        uint32_t name_id = (uint32_t)(k >> 40);
        uint32_t tag_id = (uint32_t)((k >> 20) & 0xFFFFF);
        uint32_t plabel_id = (uint32_t)(k & 0xFFFFF);
        results.push_back({name_id, tag_id, plabel_id, v.sum, v.cnt});
    }
    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.total > b.total;
    });
    if (results.size() > 200) results.resize(200);
    fprintf(stderr, "[phase] sort done: %.1f ms\n", ms_since(t0));

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q6.csv").c_str(), "w");
        fprintf(f, "name,stmt,tag,plabel,total_value,cnt\n");
        for (auto& r : results) {
            const char* name = name_dict.get(r.name_id);
            const char* tag = tag_dict.get(r.tag_id);
            const char* plabel = plabel_dict.get(r.plabel_id);
            // Fast CSV escape: check for comma/quote
            bool name_q = (strchr(name, ',') || strchr(name, '"'));
            bool plabel_q = (strchr(plabel, ',') || strchr(plabel, '"'));
            if (name_q || plabel_q) {
                string nm(name), pl(plabel);
                auto esc = [](string& s) {
                    if (s.find(',') != string::npos || s.find('"') != string::npos) {
                        string r = "\"";
                        for (char c : s) { if (c == '"') r += '"'; r += c; }
                        r += '"'; s = r;
                    }
                };
                esc(nm); esc(pl);
                fprintf(f, "%s,IS,%s,%s,%.2f,%lu\n", nm.c_str(), tag, pl.c_str(), r.total, r.cnt);
            } else {
                fprintf(f, "%s,IS,%s,%s,%.2f,%lu\n", name, tag, plabel, r.total, r.cnt);
            }
        }
        fclose(f);
    }
    return 0;
}
