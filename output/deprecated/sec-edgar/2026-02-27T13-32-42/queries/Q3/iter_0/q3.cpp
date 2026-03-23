#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <cstdio>
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
        strings.resize(count);
        for (uint32_t i = 0; i < count; i++) strings[i] = data_buf.data() + offsets[i];
    }
    const char* get(uint32_t id) const { return (id < count) ? strings[id] : ""; }
};

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];

    VarDict name_dict;
    name_dict.load(gendb_dir + "/name.dict");

    // Load sub fy=2022: adsh_id -> {cik, name_id}
    struct SubInfo { uint32_t cik, name_id; };
    unordered_map<uint32_t, SubInfo> sub_map;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<SubRow> sub(count); fread(sub.data(), sizeof(SubRow), count, f); fclose(f);
        sub_map.reserve(count);
        for (auto& r : sub) if (r.fy == 2022) sub_map[r.adsh_id] = {r.cik, r.name_id};
    }

    // Load num_usd and compute:
    // 1) sum per cik (for avg computation)
    // 2) sum per (name_id, cik)
    // Use long double for higher precision to match ground truth
    unordered_map<uint32_t, long double> cik_sum; // for avg
    unordered_map<uint64_t, long double> name_cik_sum; // (name_id<<32|cik) -> sum

    {
        FILE* f = fopen((gendb_dir + "/num_usd.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);

        const size_t BATCH = 1024 * 1024;
        vector<NumRow> batch(BATCH);

        uint64_t remaining = count;
        while (remaining > 0) {
            size_t to_read = min(remaining, (uint64_t)BATCH);
            fread(batch.data(), sizeof(NumRow), to_read, f);

            for (size_t i = 0; i < to_read; i++) {
                auto& r = batch[i];
                auto sit = sub_map.find(r.adsh_id);
                if (sit == sub_map.end()) continue;

                uint32_t cik = sit->second.cik;
                uint32_t name_id = sit->second.name_id;

                cik_sum[cik] += (long double)r.value;
                uint64_t key = ((uint64_t)name_id << 32) | cik;
                name_cik_sum[key] += (long double)r.value;
            }
            remaining -= to_read;
        }
        fclose(f);
    }

    // Compute avg of cik sums
    long double total_sum = 0;
    for (auto& [cik, s] : cik_sum) total_sum += s;
    long double avg_sub_total = (cik_sum.empty()) ? 0.0L : total_sum / cik_sum.size();

    // Filter name_cik_sum > avg, collect results
    struct Result { string name; uint32_t cik; long double total; };
    vector<Result> results;
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
            auto csv_esc = [](const string& s) -> string {
                if (s.find(',') == string::npos && s.find('"') == string::npos) return s;
                string e = "\"";
                for (char c : s) { if (c == '"') e += '"'; e += c; }
                e += '"';
                return e;
            };
            fprintf(f, "%s,%u,%.2Lf\n", csv_esc(r.name).c_str(), r.cik, r.total);
        }
        fclose(f);
    }

    return 0;
}
