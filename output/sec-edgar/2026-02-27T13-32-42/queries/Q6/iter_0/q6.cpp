#include <iostream>
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

    // Load sub fy=2023: adsh_id -> name_id
    unordered_map<uint32_t, uint32_t> sub_map;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<SubRow> sub(count); fread(sub.data(), sizeof(SubRow), count, f); fclose(f);
        sub_map.reserve(count);
        for (auto& r : sub) if (r.fy == 2023) sub_map[r.adsh_id] = r.name_id;
    }

    // Load pre stmt='IS': for each (adsh_id, tag_id, version_id), collect (plabel_id -> count)
    // The SQL JOIN: num joins pre on (adsh,tag,ver), so each pre row contributes once
    // Group result by (name, tag, plabel), summing value * count_of_pre_matches
    // We need: adsh -> tv_key -> list of (plabel_id, count)
    unordered_map<uint32_t, unordered_map<uint64_t, unordered_map<uint32_t, uint32_t>>> pre_is_index;
    // adsh -> (tv_key -> (plabel_id -> count))
    {
        FILE* f = fopen((gendb_dir + "/pre.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        const size_t BATCH = 512*1024;
        vector<PreRow> batch(BATCH);
        uint64_t remaining = count;
        while (remaining > 0) {
            size_t to_read = min(remaining, (uint64_t)BATCH);
            fread(batch.data(), sizeof(PreRow), to_read, f);
            for (size_t i = 0; i < to_read; i++) {
                auto& r = batch[i];
                if (r.stmt[0]!='I' || r.stmt[1]!='S') continue;
                uint64_t tv = ((uint64_t)r.tag_id << 32) | r.version_id;
                pre_is_index[r.adsh_id][tv][r.plabel_id]++;
            }
            remaining -= to_read;
        }
        fclose(f);
    }

    // Scan num_usd, join sub+pre
    // Group by (name_id, tag_id, plabel_id): sum(value), count
    struct GroupData { double sum = 0; uint64_t cnt = 0; };
    unordered_map<uint64_t, unordered_map<uint32_t, GroupData>> group_map; // (name_id<<32|tag_id) -> (plabel_id -> data)

    {
        FILE* f = fopen((gendb_dir + "/num_usd.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        const size_t BATCH = 1024*1024;
        vector<NumRow> batch(BATCH);
        uint64_t remaining = count;

        while (remaining > 0) {
            size_t to_read = min(remaining, (uint64_t)BATCH);
            fread(batch.data(), sizeof(NumRow), to_read, f);

            for (size_t i = 0; i < to_read; i++) {
                auto& r = batch[i];
                auto sit = sub_map.find(r.adsh_id);
                if (sit == sub_map.end()) continue;

                auto adsh_it = pre_is_index.find(r.adsh_id);
                if (adsh_it == pre_is_index.end()) continue;

                uint64_t tv = ((uint64_t)r.tag_id << 32) | r.version_id;
                auto tv_it = adsh_it->second.find(tv);
                if (tv_it == adsh_it->second.end()) continue;

                uint32_t name_id = sit->second;
                uint64_t gkey = ((uint64_t)name_id << 32) | r.tag_id;

                // For each plabel, add value * count
                for (auto& [plabel_id, cnt] : tv_it->second) {
                    auto& g = group_map[gkey][plabel_id];
                    g.sum += r.value * cnt;
                    g.cnt += cnt;
                }
            }
            remaining -= to_read;
        }
        fclose(f);
    }

    // Collect results
    struct Result { string name, tag, plabel; double total; uint64_t cnt; };
    vector<Result> results;
    for (auto& [gkey, plabel_map] : group_map) {
        uint32_t name_id = (uint32_t)(gkey >> 32);
        uint32_t tag_id = (uint32_t)(gkey & 0xFFFFFFFF);
        for (auto& [plabel_id, g] : plabel_map) {
            results.push_back({name_dict.get(name_id), tag_dict.get(tag_id), plabel_dict.get(plabel_id), g.sum, g.cnt});
        }
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
