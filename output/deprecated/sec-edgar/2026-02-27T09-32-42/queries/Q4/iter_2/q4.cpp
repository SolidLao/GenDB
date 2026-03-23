// Q4 iter_2: Use packed uint64 keys + flat vector for CIK counting
// Avoid expensive per-group unordered_set<uint32_t> with sort-merge approach

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>
#include <unordered_map>

static const int NTHREADS = 64;

static inline uint64_t pack_key3(uint32_t a, uint32_t b, uint32_t c) {
    return ((uint64_t)(a & 0x1FFFF) << 35) | ((uint64_t)(b & 0x3FFFF) << 17) | (c & 0x1FFFF);
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<SubRec> sub;
    BinCol<NumRec> num;
    BinCol<TagRec> tag;
    BinCol<PreRec> pre;
    if (!sub.load((gendb_dir + "/sub.bin").c_str())) return 1;
    if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;
    if (!tag.load((gendb_dir + "/tag.bin").c_str())) return 1;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    Dict tlabel_dict;
    tlabel_dict.load((gendb_dir + "/dict_tlabel.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    if (uom_usd == UINT32_MAX) uom_usd = 999;
    uint32_t stmt_eq = metadata_get_id(meta_path, "stmt_ids", "EQ");
    if (stmt_eq == UINT32_MAX) stmt_eq = 4;

    // Build flat sub array: adsh_id → {sic, cik} for sic 4000-4999
    const uint32_t MAX_ADSH = (uint32_t)sub.count + 2;
    struct SubInfo { int32_t sic; uint32_t cik; };
    std::vector<SubInfo> sub_arr(MAX_ADSH, {-1, 0});
    std::vector<bool> sub_valid(MAX_ADSH, false);
    for (uint64_t i = 0; i < sub.count; i++) {
        const SubRec& s = sub.data[i];
        if (s.sic >= 4000 && s.sic <= 4999 && s.adsh_id < MAX_ADSH) {
            sub_arr[s.adsh_id] = {s.sic, s.cik};
            sub_valid[s.adsh_id] = true;
        }
    }

    // Build tag map: packed(tag_id, ver_id) → tlabel_id (abstract=0)
    std::unordered_map<uint64_t, uint32_t> tag_map;
    tag_map.reserve(tag.count);
    for (uint64_t i = 0; i < tag.count; i++) {
        const TagRec& t = tag.data[i];
        if (t.abstract == 0) {
            uint64_t k = ((uint64_t)t.tag_id << 32) | t.ver_id;
            tag_map[k] = t.tlabel_id;
        }
    }

    // Build pre map: packed_key3 → count (stmt='EQ')
    std::unordered_map<uint64_t, uint32_t> pre_map;
    pre_map.reserve(1000000);
    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        if (p.stmt_id == stmt_eq) {
            uint64_t k = pack_key3(p.adsh_id, p.tag_id, p.ver_id);
            pre_map[k]++;
        }
    }

    // Thread-local: collect (agg_key, sum, count, cik) tuples
    // agg_key = (sic << 32) | tlabel_id
    struct Tuple { uint64_t agg_key; double sum; uint64_t count; uint32_t cik; };
    struct ThreadData {
        std::vector<Tuple> tuples;
        ThreadData() { tuples.reserve(100000); }
    };
    std::vector<ThreadData> tdata(NTHREADS);

    uint64_t chunk = (num.count + NTHREADS - 1) / NTHREADS;
    std::vector<std::thread> threads;
    threads.reserve(NTHREADS);

    for (int t = 0; t < NTHREADS; t++) {
        uint64_t start = (uint64_t)t * chunk;
        uint64_t end = std::min(start + chunk, num.count);
        if (start >= num.count) break;
        threads.emplace_back([&, t, start, end] {
            auto& tuples = tdata[t].tuples;
            for (uint64_t i = start; i < end; i++) {
                const NumRec& r = num.data[i];
                if (r.uom_id != uom_usd || !r.has_value) continue;
                if (r.adsh_id >= MAX_ADSH || !sub_valid[r.adsh_id]) continue;

                // tag lookup
                uint64_t tk = ((uint64_t)r.tag_id << 32) | r.ver_id;
                auto tit = tag_map.find(tk);
                if (tit == tag_map.end()) continue;
                uint32_t tlabel_id = tit->second;

                // pre lookup
                uint64_t pk = pack_key3(r.adsh_id, r.tag_id, r.ver_id);
                auto pit = pre_map.find(pk);
                if (pit == pre_map.end()) continue;
                uint32_t pre_count = pit->second;

                int32_t sic = sub_arr[r.adsh_id].sic;
                uint32_t cik = sub_arr[r.adsh_id].cik;
                uint64_t agg_key = ((uint64_t)(uint32_t)sic << 32) | tlabel_id;

                tuples.push_back({agg_key, r.value * pre_count, pre_count, cik});
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge all tuples into a single vector
    size_t total_tuples = 0;
    for (auto& td : tdata) total_tuples += td.tuples.size();
    std::vector<Tuple> all_tuples;
    all_tuples.reserve(total_tuples);
    for (auto& td : tdata) {
        for (auto& t : td.tuples) all_tuples.push_back(t);
    }

    // Sort by (agg_key, cik) for group-by aggregation
    std::sort(all_tuples.begin(), all_tuples.end(), [](const Tuple& a, const Tuple& b) {
        if (a.agg_key != b.agg_key) return a.agg_key < b.agg_key;
        return a.cik < b.cik;
    });

    // Group-by aggregation with distinct CIK counting
    struct Row {
        int32_t sic;
        uint32_t tlabel_id;
        uint32_t num_companies;
        double total_value;
        double avg_value;
    };
    std::vector<Row> results;
    results.reserve(1000);

    uint64_t i = 0;
    while (i < all_tuples.size()) {
        uint64_t agg_key = all_tuples[i].agg_key;
        double total_sum = 0.0;
        uint64_t total_cnt = 0;
        uint32_t distinct_ciks = 0;
        uint32_t last_cik = UINT32_MAX;

        while (i < all_tuples.size() && all_tuples[i].agg_key == agg_key) {
            if (all_tuples[i].cik != last_cik) {
                distinct_ciks++;
                last_cik = all_tuples[i].cik;
            }
            total_sum += all_tuples[i].sum;
            total_cnt += all_tuples[i].count;
            i++;
        }

        if (distinct_ciks >= 2) {
            int32_t sic = (int32_t)(agg_key >> 32);
            uint32_t tlabel_id = (uint32_t)(agg_key & 0xFFFFFFFF);
            double avg_val = (total_cnt > 0) ? (total_sum / total_cnt) : 0.0;
            results.push_back({sic, tlabel_id, distinct_ciks, total_sum, avg_val});
        }
    }

    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.total_value > b.total_value;
    });
    if (results.size() > 500) results.resize(500);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q4.csv").c_str(), "w");
        fprintf(f, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
        for (const auto& r : results) {
            fprintf(f, "%d,", r.sic);
            write_csv_str(f, tlabel_dict, r.tlabel_id);
            fprintf(f, ",EQ,%u,%.2f,%.6f\n", r.num_companies, r.total_value, r.avg_value);
        }
        fclose(f);
    }

    return 0;
}
