// Q6 iter_1: Sorted vector for pre_IS + flat hash map + parallel num scan
// Key optimizations:
//   1. Sort pre_IS rows by packed key; use binary-search index (no nested maps!)
//   2. 32 threads for parallel num scan with thread-local aggregation
//   3. mmap for large binary files
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
struct SubRow { uint32_t adsh_id, cik, name_id; int32_t sic, fy; uint32_t period, filed; uint8_t wksi; char form[11], fp[3], afs[6], countryba[3], countryinc[4], fye[5]; };
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

// Pack (adsh, tag, ver) into 64-bit key
// adsh < 2^24, tag < 2^20, ver < 2^20
inline uint64_t pack3(uint32_t a, uint32_t t, uint32_t v) {
    return ((uint64_t)a << 40) | ((uint64_t)(t & 0xFFFFF) << 20) | (v & 0xFFFFF);
}

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];

    VarDict name_dict, tag_dict, plabel_dict;
    name_dict.load(gendb_dir + "/name.dict");
    tag_dict.load(gendb_dir + "/tag.dict");
    plabel_dict.load(gendb_dir + "/plabel.dict");

    // Load sub fy=2023 into dense array: adsh_id → name_id
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

    // Load pre_IS rows into sorted vector + build hash index
    // PreISEntry: packed_key → plabel_id
    struct PreISEntry { uint64_t key; uint32_t plabel_id; };
    vector<PreISEntry> pre_is;
    pre_is.reserve(2000000);

    {
        int fd = open((gendb_dir + "/pre.bin").c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const char* m = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise((void*)m, st.st_size, MADV_SEQUENTIAL);
        close(fd);

        uint64_t count; memcpy(&count, m, 8);
        const PreRow* rows = reinterpret_cast<const PreRow*>(m + 8);
        for (uint64_t i = 0; i < count; i++) {
            const PreRow& r = rows[i];
            if (r.stmt[0] != 'I' || r.stmt[1] != 'S') continue;
            uint64_t k = pack3(r.adsh_id, r.tag_id, r.version_id);
            pre_is.push_back({k, r.plabel_id});
        }
        munmap((void*)m, st.st_size);
    }

    // Sort by key for binary-search lookup
    sort(pre_is.begin(), pre_is.end(), [](const PreISEntry& a, const PreISEntry& b) {
        return a.key < b.key;
    });

    // Build index: packed_key → (start_offset, count)
    unordered_map<uint64_t, pair<uint32_t,uint32_t>> pre_idx;
    pre_idx.reserve(pre_is.size());
    {
        uint32_t i = 0;
        while (i < (uint32_t)pre_is.size()) {
            uint64_t k = pre_is[i].key;
            uint32_t start = i;
            while (i < (uint32_t)pre_is.size() && pre_is[i].key == k) i++;
            pre_idx[k] = {start, i - start};
        }
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

    // Parallel scan: 32 threads with thread-local group maps
    // Group key: (name_id<<40 | tag_id<<20 | plabel_id) → (sum, cnt)
    int num_threads = 32;
    struct GroupVal { double sum; uint64_t cnt; };
    struct ThreadData { unordered_map<uint64_t, GroupVal> groups; };
    vector<ThreadData> tdata(num_threads);
    for (auto& td : tdata) td.groups.reserve(4096);

    auto worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end_idx = (uint64_t)(tid+1) * total_rows / num_threads;
        auto& groups = tdata[tid].groups;

        for (uint64_t i = start; i < end_idx; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= (uint32_t)max_adsh) continue;
            if (adsh_fy[r.adsh_id] != 2023) continue;

            uint64_t k = pack3(r.adsh_id, r.tag_id, r.version_id);
            auto it = pre_idx.find(k);
            if (it == pre_idx.end()) continue;

            uint32_t name_id = adsh_name[r.adsh_id];
            uint32_t ps = it->second.first, pc = it->second.second;

            // For each matching pre_IS entry (usually just 1)
            for (uint32_t j = ps; j < ps + pc; j++) {
                uint32_t plabel_id = pre_is[j].plabel_id;
                // Key: name_id (24 bits) << 40 | tag_id (20 bits) << 20 | plabel_id (20 bits)
                uint64_t gk = ((uint64_t)name_id << 40) | ((uint64_t)(r.tag_id & 0xFFFFF) << 20) | (plabel_id & 0xFFFFF);
                auto& g = groups[gk];
                g.sum += r.value;
                g.cnt++;
            }
        }
    };

    vector<thread> threads;
    for (int i = 0; i < num_threads; i++) threads.emplace_back(worker, i);
    for (auto& t : threads) t.join();

    munmap((void*)mapped, file_size);

    // Merge
    unordered_map<uint64_t, GroupVal> merged;
    merged.reserve(100000);
    for (auto& td : tdata) {
        for (auto& [k, v] : td.groups) {
            auto& m = merged[k];
            m.sum += v.sum;
            m.cnt += v.cnt;
        }
    }

    // Collect results
    struct Result { string name, tag, plabel; double total; uint64_t cnt; };
    vector<Result> results;
    results.reserve(merged.size());
    for (auto& [k, v] : merged) {
        uint32_t name_id = (uint32_t)(k >> 40);
        uint32_t tag_id = (uint32_t)((k >> 20) & 0xFFFFF);
        uint32_t plabel_id = (uint32_t)(k & 0xFFFFF);
        results.push_back({name_dict.get(name_id), tag_dict.get(tag_id), plabel_dict.get(plabel_id), v.sum, v.cnt});
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.total > b.total;
    });
    if (results.size() > 200) results.resize(200);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q6.csv").c_str(), "w");
        fprintf(f, "name,stmt,tag,plabel,total_value,cnt\n");
        for (auto& r : results) {
            auto csv_esc = [](const string& s) -> string {
                if (s.find(',') == string::npos && s.find('"') == string::npos) return s;
                string e = "\"";
                for (char c : s) { if (c == '"') e += '"'; e += c; }
                e += '"';
                return e;
            };
            fprintf(f, "%s,IS,%s,%s,%.2f,%lu\n",
                csv_esc(r.name).c_str(), r.tag.c_str(), csv_esc(r.plabel).c_str(), r.total, r.cnt);
        }
        fclose(f);
    }
    return 0;
}
