#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <cstdio>
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

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];

    VarDict tlabel_dict;
    tlabel_dict.load(gendb_dir + "/tlabel.dict");

    // Load sub: adsh_id -> {sic, cik} for sic 4000-4999
    struct SubInfo { int32_t sic; uint32_t cik; };
    unordered_map<uint32_t, SubInfo> sub_map;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<SubRow> sub(count); fread(sub.data(), sizeof(SubRow), count, f); fclose(f);
        sub_map.reserve(count);
        for (auto& r : sub) if (r.sic >= 4000 && r.sic <= 4999) sub_map[r.adsh_id] = {r.sic, r.cik};
    }

    // Load tag: (tag_id, version_id) -> tlabel_id for abstract=0
    unordered_map<uint64_t, uint32_t> tag_map; // (tag_id<<32|version_id) -> tlabel_id
    {
        FILE* f = fopen((gendb_dir + "/tag.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        vector<TagRow> tags(count); fread(tags.data(), sizeof(TagRow), count, f); fclose(f);
        tag_map.reserve(count);
        for (auto& r : tags) if (r.abstract_flag == 0) {
            uint64_t key = ((uint64_t)r.tag_id << 32) | r.version_id;
            tag_map[key] = r.tlabel_id;
        }
    }

    // Load pre: build count map of (adsh_id, tag_id, version_id) for stmt='EQ'
    // The SQL JOIN semantics: each pre row that matches contributes once
    // So we need counts, not just existence
    unordered_map<uint32_t, unordered_map<uint64_t, uint32_t>> pre_eq_count; // adsh_id -> (tv_key -> count)
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
                if (r.stmt[0]!='E' || r.stmt[1]!='Q') continue; // stmt='EQ'
                uint64_t tv_key = ((uint64_t)r.tag_id << 32) | r.version_id;
                pre_eq_count[r.adsh_id][tv_key]++;
            }
            remaining -= to_read;
        }
        fclose(f);
    }

    // Scan num_usd, join with sub, tag, pre
    // Group by (sic, tlabel_id): cnt_distinct_cik, sum_value, count
    struct GroupData {
        unordered_set<uint32_t> cik_set;
        double sum_value = 0;
        uint64_t cnt = 0;
    };
    unordered_map<uint64_t, GroupData> group_map; // key = (sic+10000)<<32 | tlabel_id

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

                // Join sub (sic 4000-4999)
                auto sit = sub_map.find(r.adsh_id);
                if (sit == sub_map.end()) continue;

                // Join tag (abstract=0)
                uint64_t tv = ((uint64_t)r.tag_id << 32) | r.version_id;
                auto tit = tag_map.find(tv);
                if (tit == tag_map.end()) continue;

                // Join pre (stmt='EQ') - with multiplicity
                auto pit = pre_eq_count.find(r.adsh_id);
                if (pit == pre_eq_count.end()) continue;
                auto cit = pit->second.find(tv);
                if (cit == pit->second.end()) continue;

                uint32_t match_count = cit->second; // number of matching pre rows

                // Aggregate - multiply by match_count for JOIN semantics
                int32_t sic = sit->second.sic;
                uint32_t cik = sit->second.cik;
                uint32_t tlabel_id = tit->second;

                uint64_t gkey = ((uint64_t)(uint32_t)(sic + 10000) << 32) | tlabel_id;
                auto& g = group_map[gkey];
                g.cik_set.insert(cik);
                g.sum_value += r.value * match_count;
                g.cnt += match_count;
            }
            remaining -= to_read;
        }
        fclose(f);
    }

    // Collect results with HAVING COUNT(DISTINCT cik) >= 2
    struct Result { int32_t sic; string tlabel; double total_value, avg_value; uint64_t num_companies; };
    vector<Result> results;
    for (auto& [gkey, g] : group_map) {
        if (g.cik_set.size() < 2) continue;
        int32_t sic = (int32_t)(gkey >> 32) - 10000;
        uint32_t tlabel_id = (uint32_t)(gkey & 0xFFFFFFFF);
        double avg = (g.cnt > 0) ? g.sum_value / g.cnt : 0;
        results.push_back({sic, tlabel_dict.get(tlabel_id), g.sum_value, avg, g.cik_set.size()});
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.total_value > b.total_value;
    });
    if (results.size() > 500) results.resize(500);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q4.csv").c_str(), "w");
        fprintf(f, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
        for (auto& r : results) {
            auto csv_esc = [](const string& s) -> string {
                if (s.find(',') == string::npos && s.find('"') == string::npos) return s;
                string e = "\"";
                for (char c : s) { if (c == '"') e += '"'; e += c; }
                e += '"';
                return e;
            };
            fprintf(f, "%d,%s,EQ,%lu,%.2f,%.2f\n", r.sic, csv_esc(r.tlabel).c_str(), r.num_companies, r.total_value, r.avg_value);
        }
        fclose(f);
    }

    return 0;
}
