// Q24 iter_2: Packed uint64 hash set for fast anti-join + parallel num scan
// Key insight: pack (adsh_id, tag_id, ver_id) into uint64 for O(1) hash lookup
// adsh_id <= 100K (17 bits), tag_id <= 200K (18 bits), ver_id <= 84K (17 bits) → 52 bits total

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <unordered_set>

static const int NTHREADS = 64;

// Pack (adsh_id, tag_id, ver_id) into uint64
// Layout: [adsh_id:17 | tag_id:18 | ver_id:17] (52 bits used)
static inline uint64_t pack_key(uint32_t a, uint32_t b, uint32_t c) {
    return ((uint64_t)(a & 0x1FFFF) << 35) | ((uint64_t)(b & 0x3FFFF) << 17) | (c & 0x1FFFF);
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<NumRec> num;
    BinCol<PreRec> pre;
    if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    Dict tag_dict, ver_dict;
    tag_dict.load((gendb_dir + "/dict_tag.bin").c_str());
    ver_dict.load((gendb_dir + "/dict_ver.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    if (uom_usd == UINT32_MAX) uom_usd = 999;

    // Build hash set of packed pre keys
    std::unordered_set<uint64_t> pre_set;
    pre_set.reserve((size_t)(pre.count * 1.5));  // pre-allocate to avoid rehashing
    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        pre_set.insert(pack_key(p.adsh_id, p.tag_id, p.ver_id));
    }

    // Thread-local aggregation: key=(tag_id<<32|ver_id) → {cnt, sum}
    struct ThreadAgg {
        std::unordered_map<uint64_t, std::pair<uint64_t,double>> map;
        ThreadAgg() { map.reserve(512); }
    };
    std::vector<ThreadAgg> taggs(NTHREADS);

    uint64_t chunk = (num.count + NTHREADS - 1) / NTHREADS;
    std::vector<std::thread> threads;
    threads.reserve(NTHREADS);

    for (int t = 0; t < NTHREADS; t++) {
        uint64_t start = (uint64_t)t * chunk;
        uint64_t end = std::min(start + chunk, num.count);
        if (start >= num.count) break;
        threads.emplace_back([&, t, start, end] {
            auto& agg = taggs[t].map;
            for (uint64_t i = start; i < end; i++) {
                const NumRec& r = num.data[i];
                if (r.uom_id != uom_usd || !r.has_value) continue;
                if (r.ddate < 20230101 || r.ddate > 20231231) continue;

                // Anti-join: check if (adsh,tag,ver) NOT in pre_set
                uint64_t k = pack_key(r.adsh_id, r.tag_id, r.ver_id);
                if (pre_set.count(k)) continue;  // found → skip (inner join side)

                uint64_t agg_key = ((uint64_t)r.tag_id << 32) | r.ver_id;
                auto& cv = agg[agg_key];
                cv.first++;
                cv.second += r.value;
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge
    auto& base = taggs[0].map;
    for (int t = 1; t < NTHREADS; t++) {
        for (auto& [k, cv] : taggs[t].map) {
            auto& b = base[k];
            b.first += cv.first;
            b.second += cv.second;
        }
    }

    // Filter HAVING cnt > 10, sort by cnt DESC, limit 100
    struct Row {
        uint32_t tag_id, ver_id;
        uint64_t cnt;
        double total;
    };
    std::vector<Row> results;
    for (auto& [key, cv] : base) {
        if (cv.first > 10) {
            uint32_t tag_id = (uint32_t)(key >> 32);
            uint32_t ver_id = (uint32_t)(key & 0xFFFFFFFF);
            results.push_back({tag_id, ver_id, cv.first, cv.second});
        }
    }
    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.cnt > b.cnt;
    });
    if (results.size() > 100) results.resize(100);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q24.csv").c_str(), "w");
        fprintf(f, "tag,version,cnt,total\n");
        for (const auto& r : results) {
            write_csv_str(f, tag_dict, r.tag_id);
            fputc(',', f);
            write_csv_str(f, ver_dict, r.ver_id);
            fprintf(f, ",%lu,%.2f\n", r.cnt, r.total);
        }
        fclose(f);
    }

    return 0;
}
