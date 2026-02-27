// Q4 iter_2: Fix JOIN multiplicity with count map instead of existence set
// Key fix: pre_EQ uses FlatHashMap64 (count map) so each (adsh,tag,ver) triple
// contributes count times to sum/cnt when that triple appears multiple times in pre.
#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
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
struct TagRow { uint32_t tag_id, version_id, tlabel_id; uint8_t custom, abstract_flag, crdr, iord; };
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

// Flat open-addressing COUNT map for uint64_t keys → uint32_t count
struct FlatCountMap64 {
    struct Slot { uint64_t key; uint32_t count; };
    vector<Slot> slots;
    size_t mask;
    static constexpr uint64_t EMPTY = UINT64_MAX;

    FlatCountMap64(size_t cap_hint) {
        size_t cap = 1;
        while (cap < cap_hint * 2) cap <<= 1;
        slots.assign(cap, {EMPTY, 0});
        mask = cap - 1;
    }

    inline void increment(uint64_t key) {
        uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        size_t idx = h & mask;
        while (slots[idx].key != EMPTY && slots[idx].key != key) idx = (idx + 1) & mask;
        if (slots[idx].key == EMPTY) slots[idx].key = key;
        slots[idx].count++;
    }

    inline uint32_t get(uint64_t key) const {
        uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        size_t idx = h & mask;
        while (slots[idx].key != EMPTY && slots[idx].key != key) idx = (idx + 1) & mask;
        return (slots[idx].key == key) ? slots[idx].count : 0;
    }
};

inline uint64_t pack3(uint32_t a, uint32_t t, uint32_t v) {
    return ((uint64_t)a << 40) | ((uint64_t)(t & 0xFFFFF) << 20) | (v & 0xFFFFF);
}

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];

    VarDict tlabel_dict;
    tlabel_dict.load(gendb_dir + "/tlabel.dict");

    // Load sub: dense array adsh_id → {sic, cik} for sic 4000-4999
    uint32_t max_adsh = 0;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<SubRow> sub(count); fread(sub.data(), sizeof(SubRow), count, f); fclose(f);
        for (auto& r : sub) if (r.adsh_id > max_adsh) max_adsh = r.adsh_id;
        max_adsh++;
    }
    vector<int32_t> adsh_sic(max_adsh, -1);
    vector<uint32_t> adsh_cik(max_adsh, 0);
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<SubRow> sub(count); fread(sub.data(), sizeof(SubRow), count, f); fclose(f);
        for (auto& r : sub) {
            if (r.sic >= 4000 && r.sic <= 4999) {
                adsh_sic[r.adsh_id] = r.sic;
                adsh_cik[r.adsh_id] = r.cik;
            }
        }
    }

    // Load tag: flat hash map (tag_id<<20|ver_id) → tlabel_id for abstract=0
    unordered_map<uint64_t, uint32_t> tag_map;
    {
        FILE* f = fopen((gendb_dir + "/tag.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<TagRow> tags(count); fread(tags.data(), sizeof(TagRow), count, f); fclose(f);
        tag_map.reserve(count);
        for (auto& r : tags) if (r.abstract_flag == 0) {
            uint64_t k = ((uint64_t)r.tag_id << 20) | r.version_id;
            tag_map[k] = r.tlabel_id;
        }
    }

    // Load pre_EQ: build flat COUNT MAP of packed (adsh,tag,ver) for stmt='EQ'
    // Count how many pre rows have stmt='EQ' for each (adsh,tag,ver) triple.
    // This correctly handles the case where the same triple appears 2+ times.
    FlatCountMap64 pre_eq_count(2000000);
    {
        int fd = open((gendb_dir + "/pre.bin").c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        const char* m = (const char*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise((void*)m, st.st_size, MADV_SEQUENTIAL);
        close(fd);

        uint64_t count; memcpy(&count, m, 8);
        const PreRow* rows = reinterpret_cast<const PreRow*>(m + 8);
        for (uint64_t i = 0; i < count; i++) {
            if (rows[i].stmt[0]=='E' && rows[i].stmt[1]=='Q') {
                pre_eq_count.increment(pack3(rows[i].adsh_id, rows[i].tag_id, rows[i].version_id));
            }
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

    // Parallel scan: 32 threads
    // Row4 now carries pre_cnt to handle multiplicity
    struct Row4 { int32_t sic; uint32_t tlabel_id, cik; double value; uint32_t pre_cnt; };
    int num_threads = 32;
    vector<vector<Row4>> per_thread(num_threads);

    auto worker = [&](int tid) {
        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end_idx = (uint64_t)(tid+1) * total_rows / num_threads;
        auto& out = per_thread[tid];
        out.reserve(100000);

        for (uint64_t i = start; i < end_idx; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= (uint32_t)max_adsh) continue;
            int32_t sic = adsh_sic[r.adsh_id];
            if (sic < 0) continue;

            // Join tag (abstract=0)
            uint64_t tv = ((uint64_t)r.tag_id << 20) | r.version_id;
            auto tit = tag_map.find(tv);
            if (tit == tag_map.end()) continue;

            // Join pre (stmt='EQ') - get count for multiplicity
            uint32_t pc = pre_eq_count.get(pack3(r.adsh_id, r.tag_id, r.version_id));
            if (pc == 0) continue;

            uint32_t cik = adsh_cik[r.adsh_id];
            out.push_back({sic, tit->second, cik, r.value, pc});
        }
    };

    vector<thread> threads;
    for (int i = 0; i < num_threads; i++) threads.emplace_back(worker, i);
    for (auto& t : threads) t.join();

    munmap((void*)mapped, file_size);

    // Merge: aggregate by (sic, tlabel_id)
    // NOTE: cik_set counts distinct companies (regardless of pre_cnt)
    //       sum/cnt are multiplied by pre_cnt for correct JOIN semantics
    struct GroupData {
        unordered_set<uint32_t> cik_set;
        double sum = 0;
        uint64_t cnt = 0;
    };
    unordered_map<uint64_t, GroupData> group_map;
    group_map.reserve(50000);

    for (auto& vec : per_thread) {
        for (auto& r : vec) {
            uint64_t gk = ((uint64_t)(uint32_t)(r.sic + 10000) << 32) | r.tlabel_id;
            auto& g = group_map[gk];
            g.cik_set.insert(r.cik);         // distinct per company (not per join row)
            g.sum += r.value * r.pre_cnt;    // multiply by join multiplicity
            g.cnt += r.pre_cnt;              // count = number of join rows
        }
    }

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
