// Q3 iter_1: Use int64 (cents) arithmetic for exact sums matching DuckDB
// USD values should all be multiples of $0.01, so int64*100 gives exact sums

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <algorithm>
#include <vector>

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<SubRec> sub;
    BinCol<NumRec> num;
    if (!sub.load((gendb_dir + "/sub.bin").c_str())) return 1;
    if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;

    Dict name_dict;
    name_dict.load((gendb_dir + "/dict_name.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    if (uom_usd == UINT32_MAX) uom_usd = 999;

    // Build sub hash map: adsh_id → {cik, name_id} for fy=2022
    struct SubInfo { uint32_t cik, name_id; };
    std::unordered_map<uint32_t, SubInfo> sub_map;
    sub_map.reserve(sub.count);
    for (uint64_t i = 0; i < sub.count; i++) {
        if (sub.data[i].fy == 2022) {
            sub_map[sub.data[i].adsh_id] = {sub.data[i].cik, sub.data[i].name_id};
        }
    }

    // Accumulate using int64 (value * 100 = cents) for exact integer arithmetic
    // For USD values, most are multiples of $0.01
    std::unordered_map<uint32_t, int64_t> per_cik;
    per_cik.reserve(50000);
    std::unordered_map<uint64_t, int64_t> per_name_cik;
    per_name_cik.reserve(100000);

    for (uint64_t i = 0; i < num.count; i++) {
        const NumRec& r = num.data[i];
        if (r.uom_id != uom_usd || !r.has_value) continue;
        auto sit = sub_map.find(r.adsh_id);
        if (sit == sub_map.end()) continue;

        uint32_t cik = sit->second.cik;
        uint32_t name_id = sit->second.name_id;

        int64_t cents = llround(r.value * 100.0);
        per_cik[cik] += cents;
        uint64_t key2 = ((uint64_t)name_id << 32) | cik;
        per_name_cik[key2] += cents;
    }

    // Compute threshold = AVG of per-cik sums (in cents)
    double total_sum_cents = 0.0;
    for (auto& [k, v] : per_cik) total_sum_cents += (double)v;
    double threshold_cents = (per_cik.empty()) ? 0.0 : total_sum_cents / per_cik.size();

    struct Row {
        uint32_t name_id, cik;
        double total_value;
    };
    std::vector<Row> results;
    results.reserve(1000);

    for (auto& [key, sum_cents] : per_name_cik) {
        double sum = (double)sum_cents / 100.0;
        double sum_c = (double)sum_cents;
        if (sum_c > threshold_cents) {
            uint32_t name_id = (uint32_t)(key >> 32);
            uint32_t cik = (uint32_t)(key & 0xFFFFFFFF);
            results.push_back({name_id, cik, sum});
        }
    }

    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.total_value > b.total_value;
    });
    if (results.size() > 100) results.resize(100);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q3.csv").c_str(), "w");
        fprintf(f, "name,cik,total_value\n");
        for (const auto& r : results) {
            write_csv_str(f, name_dict, r.name_id);
            fprintf(f, ",%u,%.2f\n", r.cik, r.total_value);
        }
        fclose(f);
    }

    return 0;
}
