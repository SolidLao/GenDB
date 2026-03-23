// Q3 iter_1: Parallel scan of num_usd with thread-local aggregation
// Key optimizations:
//   1. mmap for large binary files
//   2. 32 threads, each with local cik_sum and name_cik_sum maps
//   3. Merge at end
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

    VarDict name_dict;
    name_dict.load(gendb_dir + "/name.dict");

    // Load sub fy=2022 → two maps:
    // adsh_id → cik (for avg)
    // adsh_id → {cik, name_id}
    struct SubInfo { uint32_t cik, name_id; };
    // Store as two parallel arrays indexed by adsh_id for O(1) lookup
    uint32_t max_adsh_id = 0;
    vector<SubRow> sub_rows;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        sub_rows.resize(count); fread(sub_rows.data(), sizeof(SubRow), count, f); fclose(f);
        for (auto& r : sub_rows) if (r.adsh_id > max_adsh_id) max_adsh_id = r.adsh_id;
    }
    max_adsh_id++;

    // Dense arrays indexed by adsh_id (max ~86K)
    vector<int32_t> adsh_to_fy(max_adsh_id, -1);
    vector<uint32_t> adsh_to_cik(max_adsh_id, 0);
    vector<uint32_t> adsh_to_name(max_adsh_id, 0);
    for (auto& r : sub_rows) {
        adsh_to_fy[r.adsh_id] = r.fy;
        adsh_to_cik[r.adsh_id] = r.cik;
        adsh_to_name[r.adsh_id] = r.name_id;
    }
    sub_rows.clear();

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

    // Parallel scan: thread-local aggregation
    int num_threads = 32;
    struct ThreadData {
        unordered_map<uint32_t, double> cik_sum;    // cik → sum
        unordered_map<uint64_t, double> name_cik_sum; // (name_id<<32|cik) → sum
    };
    vector<ThreadData> tdata(num_threads);
    for (auto& td : tdata) { td.cik_sum.reserve(4096); td.name_cik_sum.reserve(4096); }

    auto worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end = (uint64_t)(tid+1) * total_rows / num_threads;
        auto& td = tdata[tid];

        for (uint64_t i = start; i < end; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= max_adsh_id) continue;
            if (adsh_to_fy[r.adsh_id] != 2022) continue;

            uint32_t cik = adsh_to_cik[r.adsh_id];
            uint32_t name_id = adsh_to_name[r.adsh_id];

            td.cik_sum[cik] += r.value;
            uint64_t key = ((uint64_t)name_id << 32) | cik;
            td.name_cik_sum[key] += r.value;
        }
    };

    vector<thread> threads;
    for (int i = 0; i < num_threads; i++) threads.emplace_back(worker, i);
    for (auto& t : threads) t.join();

    munmap((void*)mapped, file_size);

    // Merge
    unordered_map<uint32_t, double> cik_sum;
    unordered_map<uint64_t, double> name_cik_sum;
    cik_sum.reserve(100000);
    name_cik_sum.reserve(200000);

    for (auto& td : tdata) {
        for (auto& [k, v] : td.cik_sum) cik_sum[k] += v;
        for (auto& [k, v] : td.name_cik_sum) name_cik_sum[k] += v;
    }

    // Compute avg of cik sums
    long double total_sum = 0;
    for (auto& [k, v] : cik_sum) total_sum += v;
    double avg_sub_total = cik_sum.empty() ? 0.0 : (double)(total_sum / cik_sum.size());

    // Filter and collect results
    struct Result { string name; uint32_t cik; double total; };
    vector<Result> results;
    results.reserve(10000);
    for (auto& [key, total] : name_cik_sum) {
        if (total <= avg_sub_total) continue;
        uint32_t name_id = (uint32_t)(key >> 32);
        uint32_t cik = (uint32_t)(key & 0xFFFFFFFF);
        results.push_back({name_dict.get(name_id), cik, total});
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.total > b.total;
    });
    if (results.size() > 100) results.resize(100);

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
