// Q4 iter_1: Parallel num scan with pre-built lookup tables
// 4-way join: num+sub(sic 4000-4999)+tag(abstract=0)+pre(stmt='EQ')

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <unordered_set>

static const int NTHREADS = 64;

struct Key3 {
    uint32_t a, b, c;
    bool operator==(const Key3& o) const { return a==o.a && b==o.b && c==o.c; }
};
struct Key3Hash {
    size_t operator()(const Key3& k) const {
        uint64_t h = (uint64_t)k.a * 2654435761ULL ^ (uint64_t)k.b * 805459861ULL ^ (uint64_t)k.c * 3242174819ULL;
        return (size_t)(h ^ (h >> 33));
    }
};

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

    // Build flat sub array: adsh_id → {sic, cik}
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

    // Build tag map: (tag_id<<20|ver_id) → tlabel_id (abstract=0 only)
    // Use uint64 key: (tag_id, ver_id)
    std::unordered_map<uint64_t, uint32_t> tag_map;
    tag_map.reserve(tag.count);
    for (uint64_t i = 0; i < tag.count; i++) {
        const TagRec& t = tag.data[i];
        if (t.abstract == 0) {
            uint64_t k = ((uint64_t)t.tag_id << 32) | t.ver_id;
            tag_map[k] = t.tlabel_id;
        }
    }

    // Build pre map: Key3{adsh,tag,ver} → count (stmt='EQ')
    std::unordered_map<Key3, uint32_t, Key3Hash> pre_map;
    pre_map.reserve(1000000);
    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        if (p.stmt_id == stmt_eq) {
            Key3 k{p.adsh_id, p.tag_id, p.ver_id};
            pre_map[k]++;
        }
    }

    // Thread-local aggregation
    // Key: (sic16 | tlabel_id<<16) - actually use uint64: (sic<<32)|tlabel_id
    struct AggVal { double sum; uint64_t count; std::unordered_set<uint32_t>* cik_set; };
    struct ThreadAgg {
        std::unordered_map<uint64_t, std::pair<double,uint64_t>> map; // key→{sum,count}
        std::unordered_map<uint64_t, std::unordered_set<uint32_t>> cik_map; // key→cik_set
        ThreadAgg() { map.reserve(65536); cik_map.reserve(65536); }
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
            auto& agg = taggs[t];
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
                Key3 pk{r.adsh_id, r.tag_id, r.ver_id};
                auto pit = pre_map.find(pk);
                if (pit == pre_map.end()) continue;
                uint32_t pre_count = pit->second;

                int32_t sic = sub_arr[r.adsh_id].sic;
                uint32_t cik = sub_arr[r.adsh_id].cik;

                // aggregate key: (sic, tlabel_id) - stmt is always EQ
                uint64_t agg_key = ((uint64_t)(uint32_t)sic << 32) | tlabel_id;
                auto& sv = agg.map[agg_key];
                sv.first += r.value * pre_count;
                sv.second += pre_count;
                agg.cik_map[agg_key].insert(cik);
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge thread-local results
    auto& base_map = taggs[0].map;
    auto& base_cik = taggs[0].cik_map;
    for (int t = 1; t < NTHREADS; t++) {
        for (auto& [k, sv] : taggs[t].map) {
            base_map[k].first += sv.first;
            base_map[k].second += sv.second;
        }
        for (auto& [k, cs] : taggs[t].cik_map) {
            auto& bc = base_cik[k];
            bc.insert(cs.begin(), cs.end());
        }
    }

    // Collect results with HAVING >= 2 distinct ciks
    struct Row {
        int32_t sic;
        uint32_t tlabel_id;
        uint32_t num_companies;
        double total_value;
        double avg_value;
    };
    std::vector<Row> results;
    results.reserve(2000);
    for (auto& [k, sv] : base_map) {
        int32_t sic = (int32_t)(k >> 32);
        uint32_t tlabel_id = (uint32_t)(k & 0xFFFFFFFF);
        auto cit = base_cik.find(k);
        if (cit == base_cik.end()) continue;
        uint32_t nc = (uint32_t)cit->second.size();
        if (nc < 2) continue;
        double avg_val = (sv.second > 0) ? (sv.first / sv.second) : 0.0;
        results.push_back({sic, tlabel_id, nc, sv.first, avg_val});
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
