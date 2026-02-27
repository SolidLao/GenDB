#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <cstdio>
#include "timing_utils.h"

using namespace std;

#pragma pack(push, 1)
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

    VarDict tag_dict, version_dict;
    tag_dict.load(gendb_dir + "/tag.dict");
    version_dict.load(gendb_dir + "/version.dict");

    // Build hash set of (adsh_id, tag_id, version_id) from ALL pre rows
    struct TVAKey { uint32_t adsh, tag, ver; };
    struct TVAHash {
        size_t operator()(const TVAKey& k) const {
            // FNV-like hash
            uint64_t h = 14695981039346656037ULL;
            h ^= k.adsh; h *= 1099511628211ULL;
            h ^= k.tag;  h *= 1099511628211ULL;
            h ^= k.ver;  h *= 1099511628211ULL;
            return h;
        }
    };
    struct TVAEq { bool operator()(const TVAKey& a, const TVAKey& b) const { return a.adsh==b.adsh && a.tag==b.tag && a.ver==b.ver; } };

    unordered_set<TVAKey, TVAHash, TVAEq> pre_set;
    pre_set.reserve(10000000); // pre has 9.6M rows

    {
        FILE* f = fopen((gendb_dir + "/pre.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        const size_t BATCH = 1024*1024;
        vector<PreRow> batch(BATCH);
        uint64_t remaining = count;
        while (remaining > 0) {
            size_t to_read = min(remaining, (uint64_t)BATCH);
            fread(batch.data(), sizeof(PreRow), to_read, f);
            for (size_t i = 0; i < to_read; i++) {
                auto& r = batch[i];
                pre_set.insert({r.adsh_id, r.tag_id, r.version_id});
            }
            remaining -= to_read;
        }
        fclose(f);
    }

    // Scan num_usd with ddate in [20230101, 20231231]
    // Anti-join: keep where (adsh, tag, ver) NOT in pre_set
    // Group by (tag_id, version_id): cnt, sum(value)
    struct GroupData { uint64_t cnt = 0; double sum = 0; };
    unordered_map<uint64_t, GroupData> group_map; // (tag_id<<32|version_id) -> data

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
                if (r.ddate < 20230101 || r.ddate > 20231231) continue;
                if (pre_set.count({r.adsh_id, r.tag_id, r.version_id})) continue;
                uint64_t gkey = ((uint64_t)r.tag_id << 32) | r.version_id;
                auto& g = group_map[gkey];
                g.cnt++;
                g.sum += r.value;
            }
            remaining -= to_read;
        }
        fclose(f);
    }

    // Filter HAVING cnt > 10, sort by cnt DESC, limit 100
    struct Result { string tag, version; uint64_t cnt; double total; };
    vector<Result> results;
    for (auto& [gkey, g] : group_map) {
        if (g.cnt <= 10) continue;
        uint32_t tag_id = (uint32_t)(gkey >> 32);
        uint32_t ver_id = (uint32_t)(gkey & 0xFFFFFFFF);
        results.push_back({tag_dict.get(tag_id), version_dict.get(ver_id), g.cnt, g.sum});
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.cnt > b.cnt;
    });
    if (results.size() > 100) results.resize(100);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q24.csv").c_str(), "w");
        fprintf(f, "tag,version,cnt,total\n");
        for (auto& r : results) {
            fprintf(f, "%s,%s,%lu,%.2f\n", r.tag.c_str(), r.version.c_str(), r.cnt, r.total);
        }
        fclose(f);
    }

    return 0;
}
