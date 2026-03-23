// Q4 iter_4: Use pre-built flat hash map files to avoid expensive data loading.
// tag_fmap.bin: (tag_id<<20|version_id) → tlabel_id for non-abstract tags (~96MB)
// pre_eq_fmap.bin: pack3(adsh,tag,ver) → EQ count (~96MB)
// These load in ~10ms from page cache vs 170ms+92ms=262ms to build from source files.
#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_set>
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

// Flat map slot (read-only from mmap)
struct FlatSlot { uint64_t key; uint32_t val; };  // 12 bytes with pack(1)
static const uint64_t FMAP_EMPTY = UINT64_MAX;

inline uint32_t flat_lookup(const FlatSlot* slots, uint64_t cap, uint64_t key) {
    uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
    uint64_t idx = h & (cap - 1);
    while (slots[idx].key != FMAP_EMPTY && slots[idx].key != key) idx = (idx + 1) & (cap - 1);
    return (slots[idx].key == key) ? slots[idx].val : UINT32_MAX;
}

inline uint64_t pack3(uint32_t a, uint32_t t, uint32_t v) {
    return ((uint64_t)a << 40) | ((uint64_t)(t & 0xFFFFF) << 20) | (v & 0xFFFFF);
}

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];
    auto t0 = hrc::now();

    VarDict tlabel_dict;
    tlabel_dict.load(gendb_dir + "/tlabel.dict");
    fprintf(stderr, "[phase] tlabel: %.1f ms\n", ms_since(t0));

    // Load sub: dense array adsh_id → {sic, cik}
    uint32_t max_adsh = 0;
    vector<int32_t> adsh_sic;
    vector<uint32_t> adsh_cik;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<SubRow> sub(count); fread(sub.data(), sizeof(SubRow), count, f); fclose(f);
        for (auto& r : sub) if (r.adsh_id > max_adsh) max_adsh = r.adsh_id;
        max_adsh++;
        adsh_sic.assign(max_adsh, -1);
        adsh_cik.assign(max_adsh, 0);
        for (auto& r : sub) {
            if (r.sic >= 4000 && r.sic <= 4999) { adsh_sic[r.adsh_id] = r.sic; adsh_cik[r.adsh_id] = r.cik; }
        }
    }
    fprintf(stderr, "[phase] sub: %.1f ms\n", ms_since(t0));

    // Load tag_fmap.bin (flat map: tv → tlabel_id for non-abstract tags)
    uint64_t tag_cap = 0;
    const FlatSlot* tag_slots = nullptr;
    size_t tag_fmap_size = 0;
    void* tag_fmap_ptr = nullptr;
    {
        int ffd = open((gendb_dir + "/tag_fmap.bin").c_str(), O_RDONLY);
        if (ffd < 0) { fprintf(stderr, "ERROR: tag_fmap.bin not found\n"); return 1; }
        struct stat st; fstat(ffd, &st);
        tag_fmap_size = st.st_size;
        tag_fmap_ptr = mmap(nullptr, tag_fmap_size, PROT_READ, MAP_PRIVATE, ffd, 0);
        madvise(tag_fmap_ptr, tag_fmap_size, MADV_WILLNEED);
        close(ffd);
        memcpy(&tag_cap, tag_fmap_ptr, 8);
        tag_slots = reinterpret_cast<const FlatSlot*>((const char*)tag_fmap_ptr + 8);
    }
    fprintf(stderr, "[phase] tag_fmap: %.1f ms (cap=%lu)\n", ms_since(t0), (unsigned long)tag_cap);

    // Load pre_eq_fmap.bin (flat map: pack3(adsh,tag,ver) → EQ count)
    uint64_t eq_cap = 0;
    const FlatSlot* eq_slots = nullptr;
    size_t eq_fmap_size = 0;
    void* eq_fmap_ptr = nullptr;
    {
        int ffd = open((gendb_dir + "/pre_eq_fmap.bin").c_str(), O_RDONLY);
        if (ffd < 0) { fprintf(stderr, "ERROR: pre_eq_fmap.bin not found\n"); return 1; }
        struct stat st; fstat(ffd, &st);
        eq_fmap_size = st.st_size;
        eq_fmap_ptr = mmap(nullptr, eq_fmap_size, PROT_READ, MAP_PRIVATE, ffd, 0);
        madvise(eq_fmap_ptr, eq_fmap_size, MADV_WILLNEED);
        close(ffd);
        memcpy(&eq_cap, eq_fmap_ptr, 8);
        eq_slots = reinterpret_cast<const FlatSlot*>((const char*)eq_fmap_ptr + 8);
    }
    fprintf(stderr, "[phase] eq_fmap: %.1f ms (cap=%lu)\n", ms_since(t0), (unsigned long)eq_cap);

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
    fprintf(stderr, "[phase] num_usd mmap: %.1f ms\n", ms_since(t0));

    // Parallel scan: 32 threads
    struct Row4 { int32_t sic; uint32_t tlabel_id, cik; double value; uint32_t pre_cnt; };
    int num_threads = 32;
    vector<vector<Row4>> per_thread(num_threads);

    auto worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end_idx = (uint64_t)(tid+1) * total_rows / num_threads;
        auto& out = per_thread[tid];
        out.reserve(200000);

        for (uint64_t i = start; i < end_idx; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= max_adsh) continue;
            int32_t sic = adsh_sic[r.adsh_id];
            if (sic < 0) continue;

            // Flat map tag lookup (non-abstract)
            uint64_t tv = ((uint64_t)r.tag_id << 20) | r.version_id;
            uint32_t tlabel_id = flat_lookup(tag_slots, tag_cap, tv);
            if (tlabel_id == UINT32_MAX) continue;

            // Flat map EQ count lookup
            uint64_t pk = pack3(r.adsh_id, r.tag_id, r.version_id);
            uint32_t pc = flat_lookup(eq_slots, eq_cap, pk);
            if (pc == 0 || pc == UINT32_MAX) continue;

            uint32_t cik = adsh_cik[r.adsh_id];
            out.push_back({sic, tlabel_id, cik, r.value, pc});
        }
    };

    {
        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) threads.emplace_back(worker, i);
        for (auto& t : threads) t.join();
    }
    munmap((void*)mapped, file_size);
    munmap(tag_fmap_ptr, tag_fmap_size);
    munmap(eq_fmap_ptr, eq_fmap_size);
    fprintf(stderr, "[phase] num scan: %.1f ms\n", ms_since(t0));

    // Merge: aggregate by (sic, tlabel_id)
    struct GroupData {
        unordered_set<uint32_t> cik_set;
        double sum = 0;
        uint64_t cnt = 0;
    };
    unordered_map<uint64_t, GroupData> group_map;
    group_map.reserve(100000);

    for (auto& vec : per_thread) {
        for (auto& r : vec) {
            uint64_t gk = ((uint64_t)(uint32_t)(r.sic + 10000) << 32) | r.tlabel_id;
            auto& g = group_map[gk];
            g.cik_set.insert(r.cik);
            g.sum += r.value * r.pre_cnt;
            g.cnt += r.pre_cnt;
        }
    }
    fprintf(stderr, "[phase] merge: %.1f ms (groups=%zu)\n", ms_since(t0), group_map.size());

    // Collect results with HAVING COUNT(DISTINCT cik) >= 2
    struct Result { int32_t sic; string tlabel; double total, avg; uint64_t num_cos; };
    vector<Result> results;
    for (auto& [gk, g] : group_map) {
        if (g.cik_set.size() < 2) continue;
        int32_t sic = (int32_t)(gk >> 32) - 10000;
        uint32_t tlabel_id = (uint32_t)(gk & 0xFFFFFFFF);
        double avg = g.cnt > 0 ? g.sum / g.cnt : 0;
        results.push_back({sic, tlabel_dict.get(tlabel_id), g.sum, avg, g.cik_set.size()});
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.total > b.total;
    });
    if (results.size() > 500) results.resize(500);
    fprintf(stderr, "[phase] sort: %.1f ms\n", ms_since(t0));

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q4.csv").c_str(), "w");
        fprintf(f, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
        for (auto& r : results) {
            bool need_quote = r.tlabel.find(',') != string::npos || r.tlabel.find('"') != string::npos;
            if (need_quote) {
                string esc = r.tlabel; size_t pos = 0;
                while ((pos = esc.find('"', pos)) != string::npos) { esc.insert(pos, "\""); pos += 2; }
                fprintf(f, "%d,\"%s\",EQ,%lu,%.2f,%.2f\n", r.sic, esc.c_str(), r.num_cos, r.total, r.avg);
            } else {
                fprintf(f, "%d,%s,EQ,%lu,%.2f,%.2f\n", r.sic, r.tlabel.c_str(), r.num_cos, r.total, r.avg);
            }
        }
        fclose(f);
    }
    return 0;
}
