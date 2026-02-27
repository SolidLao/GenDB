// Q24 iter_3: Per-adsh sorted lookup + TBB parallel sort + parallel pre collection
// Key improvements over iter_1:
// 1. Parallel build of pre packed keys (64 threads)
// 2. TBB parallel sort of packed uint64 keys (76.8MB vs 115MB struct)
// 3. Per-adsh lo/hi offsets → binary search in tiny per-adsh range (~112 entries)
//    This gives L1-cache-friendly binary search vs L3-scale global binary search

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
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

    const uint32_t MAX_ADSH = 90000;

    // Build packed pre key array in parallel (all pre records, no filter)
    struct ThreadPre {
        std::vector<uint64_t> keys;
        ThreadPre() { keys.reserve(200000); }
    };
    std::vector<ThreadPre> tpre(NTHREADS);
    {
        uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, pre.count);
            if (start >= pre.count) break;
            threads.emplace_back([&, t, start, end] {
                auto& keys = tpre[t].keys;
                for (uint64_t i = start; i < end; i++) {
                    const PreRec& p = pre.data[i];
                    keys.push_back(pack_key3(p.adsh_id, p.tag_id, p.ver_id));
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Concatenate + TBB sort + unique
    size_t total_pre = 0;
    for (auto& tp : tpre) total_pre += tp.keys.size();
    std::vector<uint64_t> pre_packed;
    pre_packed.reserve(total_pre);
    for (auto& tp : tpre) for (auto k : tp.keys) pre_packed.push_back(k);
    tpre.clear();

    __gnu_parallel::sort(pre_packed.begin(), pre_packed.end());
    pre_packed.erase(std::unique(pre_packed.begin(), pre_packed.end()), pre_packed.end());
    const uint32_t pre_packed_size = (uint32_t)pre_packed.size();

    // Build per-adsh lo/hi offsets into sorted pre_packed
    // adsh_id is encoded in upper 17 bits of pack_key3 output
    std::vector<uint32_t> adsh_lo(MAX_ADSH, UINT32_MAX);
    std::vector<uint32_t> adsh_hi(MAX_ADSH, 0);
    for (uint32_t i = 0; i < pre_packed_size; i++) {
        uint32_t adsh = (uint32_t)(pre_packed[i] >> 35);
        if (adsh < MAX_ADSH) {
            if (adsh_lo[adsh] == UINT32_MAX) adsh_lo[adsh] = i;
            adsh_hi[adsh] = i + 1;
        }
    }

    // Thread-local aggregation
    struct ThreadAgg {
        std::unordered_map<uint64_t, std::pair<uint64_t,double>> map;
        ThreadAgg() { map.reserve(1024); }
    };
    std::vector<ThreadAgg> taggs(NTHREADS);
    {
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
                    if (r.adsh_id >= MAX_ADSH) continue;

                    // Per-adsh binary search (tiny range → L1 cache friendly)
                    uint32_t lo = adsh_lo[r.adsh_id];
                    if (lo != UINT32_MAX) {
                        uint32_t hi = adsh_hi[r.adsh_id];
                        uint64_t probe = pack_key3(r.adsh_id, r.tag_id, r.ver_id);
                        // Binary search in small per-adsh range
                        const uint64_t* base = pre_packed.data() + lo;
                        uint32_t len = hi - lo;
                        // Manual binary search for speed
                        uint32_t left = 0, right = len;
                        while (left < right) {
                            uint32_t mid = (left + right) >> 1;
                            if (base[mid] < probe) left = mid + 1;
                            else right = mid;
                        }
                        if (left < len && base[left] == probe) continue; // found → anti-join skip
                    }
                    // Not in pre → include this record

                    uint64_t key = ((uint64_t)r.tag_id << 32) | r.ver_id;
                    auto& cv = agg[key];
                    cv.first++;
                    cv.second += r.value;
                }
            });
        }
        for (auto& t : threads) t.join();
    }

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
    struct Row { uint32_t tag_id, ver_id; uint64_t cnt; double total; };
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
