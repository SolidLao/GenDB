#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <unordered_map>
#include <cstdio>
#include "timing_utils.h"

using namespace std;

#pragma pack(push, 1)
struct SubRow {
    uint32_t adsh_id, cik, name_id;
    int32_t sic, fy;
    uint32_t period, filed;
    uint8_t wksi;
    char form[11], fp[3], afs[6], countryba[3], countryinc[4], fye[5];
};
struct NumRow {
    uint32_t adsh_id, tag_id, version_id, ddate;
    double value;
};
#pragma pack(pop)

struct VarDict {
    uint32_t count = 0;
    vector<const char*> strings;
    vector<char> data_buf;
    void load(const string& path) {
        FILE* f = fopen(path.c_str(), "rb");
        fread(&count, 4, 1, f);
        vector<uint32_t> offsets(count + 1);
        fread(offsets.data(), 4, count + 1, f);
        uint32_t ds = offsets[count];
        data_buf.resize(ds);
        fread(data_buf.data(), 1, ds, f);
        fclose(f);
        strings.resize(count);
        for (uint32_t i = 0; i < count; i++) strings[i] = data_buf.data() + offsets[i];
    }
    const char* get(uint32_t id) const { return (id < count) ? strings[id] : ""; }
};

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];

    // Load dicts
    VarDict name_dict, tag_dict;
    name_dict.load(gendb_dir + "/name.dict");
    tag_dict.load(gendb_dir + "/tag.dict");

    // Load sub, build hash: adsh_id -> name_id (filter fy=2022)
    unordered_map<uint32_t, uint32_t> sub_fy2022; // adsh_id -> name_id
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<SubRow> sub(count);
        fread(sub.data(), sizeof(SubRow), count, f); fclose(f);
        sub_fy2022.reserve(count);
        for (auto& r : sub) if (r.fy == 2022) sub_fy2022[r.adsh_id] = r.name_id;
    }

    // Load num_pure, compute max_value per (adsh_id, tag_id)
    vector<NumRow> pure;
    {
        FILE* f = fopen((gendb_dir + "/num_pure.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        pure.resize(count);
        fread(pure.data(), sizeof(NumRow), count, f); fclose(f);
    }

    // Build (adsh_id, tag_id) -> max_value from ALL pure rows
    unordered_map<uint64_t, double> max_map;
    max_map.reserve(pure.size() / 2);
    for (auto& r : pure) {
        uint64_t key = ((uint64_t)r.adsh_id << 32) | r.tag_id;
        auto it = max_map.find(key);
        if (it == max_map.end()) max_map[key] = r.value;
        else if (r.value > it->second) it->second = r.value;
    }

    // Find rows where value == max_value AND adsh in fy=2022 sub
    struct Result {
        string name, tag;
        double value;
    };
    vector<Result> results;
    results.reserve(1000);

    for (auto& r : pure) {
        auto sit = sub_fy2022.find(r.adsh_id);
        if (sit == sub_fy2022.end()) continue;
        uint64_t key = ((uint64_t)r.adsh_id << 32) | r.tag_id;
        auto mit = max_map.find(key);
        if (mit == max_map.end() || r.value != mit->second) continue;
        results.push_back({name_dict.get(sit->second), tag_dict.get(r.tag_id), r.value});
    }

    // Sort by value DESC, name ASC, tag ASC; limit 100
    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        if (a.value != b.value) return a.value > b.value;
        if (a.name != b.name) return a.name < b.name;
        return a.tag < b.tag;
    });
    if (results.size() > 100) results.resize(100);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q2.csv").c_str(), "w");
        fprintf(f, "name,tag,value\n");
        for (auto& r : results) {
            // CSV-escape name if it contains comma or quote
            auto csv_esc = [](const string& s) -> string {
                if (s.find(',') == string::npos && s.find('"') == string::npos) return s;
                string e = "\"";
                for (char c : s) { if (c == '"') e += '"'; e += c; }
                e += '"';
                return e;
            };
            fprintf(f, "%s,%s,%.2f\n", csv_esc(r.name).c_str(), r.tag.c_str(), r.value);
        }
        fclose(f);
    }

    return 0;
}
