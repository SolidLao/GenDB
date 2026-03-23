// Q3 iter_2: Parallel 64-thread double summation to match DuckDB's parallelism
// DuckDB uses 64 threads on this machine, producing slightly different FP sums
// This version replicates DuckDB's parallel hash-aggregate behavior

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>

static const int NTHREADS = 64;

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

    // Build flat sub array: adsh_id → {cik, name_id, valid} for fy=2022
    // adsh_ids are 1..N (N=sub.count), very cache-friendly flat array
    const uint32_t MAX_ADSH = (uint32_t)sub.count + 2;
    struct SubInfo { uint32_t cik, name_id; };
    std::vector<SubInfo> sub_arr(MAX_ADSH, {0, 0});
    std::vector<bool> sub_valid(MAX_ADSH, false);
    for (uint64_t i = 0; i < sub.count; i++) {
        const SubRec& s = sub.data[i];
        if (s.fy == 2022 && s.adsh_id < MAX_ADSH) {
            sub_arr[s.adsh_id] = {s.cik, s.name_id};
            sub_valid[s.adsh_id] = true;
        }
    }

    // Thread-local aggregation maps
    struct ThreadMaps {
        std::unordered_map<uint32_t, double> per_cik;
        std::unordered_map<uint64_t, double> per_name_cik;
        ThreadMaps() { per_cik.reserve(8192); per_name_cik.reserve(16384); }
    };
    std::vector<ThreadMaps> tmaps(NTHREADS);

    // Parallel num scan - plain double summation (matches DuckDB's per-thread accumulation)
    uint64_t chunk = (num.count + NTHREADS - 1) / NTHREADS;
    std::vector<std::thread> threads;
    threads.reserve(NTHREADS);

    for (int t = 0; t < NTHREADS; t++) {
        uint64_t start = (uint64_t)t * chunk;
        uint64_t end = std::min(start + chunk, num.count);
        if (start >= num.count) break;
        threads.emplace_back([&, t, start, end] {
            auto& pc = tmaps[t].per_cik;
            auto& pnc = tmaps[t].per_name_cik;
            for (uint64_t i = start; i < end; i++) {
                const NumRec& r = num.data[i];
                if (r.uom_id != uom_usd || !r.has_value) continue;
                if (r.adsh_id >= MAX_ADSH || !sub_valid[r.adsh_id]) continue;
                uint32_t cik = sub_arr[r.adsh_id].cik;
                uint32_t name_id = sub_arr[r.adsh_id].name_id;
                pc[cik] += r.value;
                uint64_t key2 = ((uint64_t)name_id << 32) | cik;
                pnc[key2] += r.value;
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge thread-local maps (sequential merge matches DuckDB's thread result collection)
    auto& per_cik = tmaps[0].per_cik;
    auto& per_name_cik = tmaps[0].per_name_cik;
    for (int t = 1; t < NTHREADS; t++) {
        for (auto& [k, v] : tmaps[t].per_cik) per_cik[k] += v;
        for (auto& [k, v] : tmaps[t].per_name_cik) per_name_cik[k] += v;
    }

    // Compute threshold = AVG of per-cik sums
    double total_sum = 0.0;
    for (auto& [k, v] : per_cik) total_sum += v;
    double threshold = per_cik.empty() ? 0.0 : total_sum / (double)per_cik.size();

    struct Row {
        uint32_t name_id, cik;
        double total_value;
    };
    std::vector<Row> results;
    results.reserve(2000);

    for (auto& [key, sum] : per_name_cik) {
        if (sum > threshold) {
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
