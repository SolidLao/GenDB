// Q6 iter_2: Two-level dense index for pre_IS (counting sort + binary search)
// Key optimizations over iter_1:
//   1. Replace unordered_map pre_idx with counting-sort-based dense range index
//      - adsh_start[adsh_id] + adsh_count[adsh_id] (fits in L2 cache: 86K * 8B = 688KB)
//   2. Binary search within each adsh's pre_IS entries (sorted by tv = tag<<20|ver)
//   3. Counting sort to build index in O(n) instead of O(n log n) sort + hash map build
//   4. Increased thread-local groups reserve for fewer rehashes
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

    // Two-level pre_IS index:
    // Level 1: adsh_start[adsh_id], adsh_count[adsh_id] → range in flat sorted pre_is array
    // Level 2: binary search within range for tv = (tag_id<<20 | ver_id)
    // This fits in L2 cache (86K * 8 bytes = 688KB) and avoids hash map overhead.
    struct PreISEntry { uint64_t tv; uint32_t plabel_id; };  // tv = (tag_id<<20)|ver_id

    // Step 1: Count pre_IS rows per adsh_id
    vector<uint32_t> adsh_count(max_adsh, 0);
    uint64_t total_pre_is = 0;
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
            if (r.adsh_id < max_adsh) {
                adsh_count[r.adsh_id]++;
                total_pre_is++;
            }
        }
        munmap((void*)m, st.st_size);
    }

    // Step 2: Prefix sums to build start positions
    vector<uint32_t> adsh_start(max_adsh + 1, 0);
    for (uint32_t i = 0; i < max_adsh; i++) adsh_start[i+1] = adsh_start[i] + adsh_count[i];

    // Step 3: Fill flat array using counting sort
    vector<PreISEntry> pre_is(total_pre_is);
    {
        vector<uint32_t> pos(adsh_start.begin(), adsh_start.begin() + max_adsh); // copy starts

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
            if (r.adsh_id < max_adsh) {
                uint32_t p = pos[r.adsh_id]++;
                pre_is[p] = {((uint64_t)r.tag_id << 20) | r.version_id, r.plabel_id};
            }
        }
        munmap((void*)m, st.st_size);
    }

    // Step 4: Sort within each adsh bucket by tv (for binary search)
    // Parallel bucket sort for speed
    {
        int nt = 16;
        vector<thread> sort_threads;
        sort_threads.reserve(nt);
        for (int tid = 0; tid < nt; tid++) {
            sort_threads.emplace_back([&, tid]() {
                uint32_t start_a = (uint32_t)((uint64_t)tid * max_adsh / nt);
                uint32_t end_a = (uint32_t)((uint64_t)(tid+1) * max_adsh / nt);
                for (uint32_t a = start_a; a < end_a; a++) {
                    if (adsh_count[a] > 1) {
                        auto* begin = pre_is.data() + adsh_start[a];
                        auto* end = begin + adsh_count[a];
                        sort(begin, end, [](const PreISEntry& x, const PreISEntry& y) {
                            return x.tv < y.tv;
                        });
                    }
                }
            });
        }
        for (auto& t : sort_threads) t.join();
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
    for (auto& td : tdata) td.groups.reserve(65536);  // generous reserve

    auto worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end_idx = (uint64_t)(tid+1) * total_rows / num_threads;
        auto& groups = tdata[tid].groups;

        for (uint64_t i = start; i < end_idx; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= (uint32_t)max_adsh) continue;
            if (adsh_fy[r.adsh_id] != 2023) continue;

            uint32_t adsh = r.adsh_id;
            uint32_t cnt = adsh_count[adsh];
            if (cnt == 0) continue;

            // Two-level lookup: dense array → binary search within bucket
            uint64_t tv = ((uint64_t)r.tag_id << 20) | r.version_id;
            uint32_t lo = adsh_start[adsh];
            uint32_t hi = lo + cnt;

            // Binary search for first entry with tv
            uint32_t lo2 = lo, hi2 = hi;
            while (lo2 < hi2) {
                uint32_t mid = (lo2 + hi2) >> 1;
                if (pre_is[mid].tv < tv) lo2 = mid + 1;
                else hi2 = mid;
            }
            if (lo2 >= hi || pre_is[lo2].tv != tv) continue;

            uint32_t name_id = adsh_name[adsh];
            // Emit for all matching entries (same tv, possibly different plabel_id)
            for (uint32_t j = lo2; j < hi && pre_is[j].tv == tv; j++) {
                uint32_t plabel_id = pre_is[j].plabel_id;
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
    merged.reserve(200000);
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
