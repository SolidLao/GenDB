// Q6 iter_4: Packed 16-byte tuples (uint64 key + double) for sort
// Key improvements over iter_3:
// 1. Pack (name_id, tag_id, plabel_id) into uint64 → struct is 16 bytes (was 20)
// 2. 20% less data moved during sort → faster sort + better cache usage
// 3. After sort, decode fields only for output (not during sort)
// 4. Add GENDB_PHASE profiling to identify bottleneck

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>
#include <cstdio>
#include <cstring>

static const int NTHREADS = 64;

static inline uint64_t pack_key3(uint32_t a, uint32_t b, uint32_t c) {
    return ((uint64_t)(a & 0x1FFFF) << 35) | ((uint64_t)(b & 0x3FFFF) << 17) | (c & 0x1FFFF);
}

// Pack (name_id, tag_id, plabel_id) into 63 bits of uint64
// Use 21 bits each: name_id(bits 62:42), tag_id(bits 41:21), plabel_id(bits 20:0)
// Max values: name_id=9647(14b), tag_id=198312(18b), plabel_id=698148(20b) — all < 2097151
static inline uint64_t pack_agg_key(uint32_t name_id, uint32_t tag_id, uint32_t plabel_id) {
    return ((uint64_t)(name_id & 0x1FFFFF) << 42) |
           ((uint64_t)(tag_id  & 0x1FFFFF) << 21) |
           (uint64_t)(plabel_id & 0x1FFFFF);
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<SubRec> sub;
    BinCol<NumRec> num;
    BinCol<PreRec> pre;
    {
        GENDB_PHASE("load_bins");
        if (!sub.load((gendb_dir + "/sub.bin").c_str())) return 1;
        if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;
        if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;
    }

    Dict name_dict, tag_dict, plabel_dict;
    name_dict.load((gendb_dir + "/dict_name.bin").c_str());
    tag_dict.load((gendb_dir + "/dict_tag.bin").c_str());
    plabel_dict.load((gendb_dir + "/dict_plabel.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    if (uom_usd == UINT32_MAX) uom_usd = 999;
    uint32_t stmt_is = metadata_get_id(meta_path, "stmt_ids", "IS");
    if (stmt_is == UINT32_MAX) stmt_is = 2;

    // Build flat sub array: adsh_id → name_id for fy=2023
    const uint32_t MAX_ADSH = (uint32_t)sub.count + 2;
    std::vector<uint32_t> sub_name(MAX_ADSH, UINT32_MAX);
    for (uint64_t i = 0; i < sub.count; i++) {
        const SubRec& s = sub.data[i];
        if (s.fy == 2023 && s.adsh_id < MAX_ADSH) {
            sub_name[s.adsh_id] = s.name_id;
        }
    }

    // Build sorted pre IS array with packed key only (uint64 per entry instead of 12/16 bytes)
    // Store {uint64_t key, uint32_t plabel_id, uint32_t _pad} = 16 bytes
    struct PreIS { uint64_t key; uint32_t plabel_id; uint32_t _pad; };
    {
        GENDB_PHASE("pre_collect_sort");
        struct ThreadPreIS {
            std::vector<PreIS> entries;
            ThreadPreIS() { entries.reserve(50000); }
        };
        std::vector<ThreadPreIS> tpre(NTHREADS);
        {
            uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
            std::vector<std::thread> threads;
            threads.reserve(NTHREADS);
            for (int t = 0; t < NTHREADS; t++) {
                uint64_t start = (uint64_t)t * chunk;
                uint64_t end = std::min(start + chunk, pre.count);
                if (start >= pre.count) break;
                threads.emplace_back([&, t, start, end] {
                    auto& entries = tpre[t].entries;
                    for (uint64_t i = start; i < end; i++) {
                        const PreRec& p = pre.data[i];
                        if (p.stmt_id == stmt_is) {
                            uint64_t k = pack_key3(p.adsh_id, p.tag_id, p.ver_id);
                            entries.push_back({k, p.plabel_id, 0});
                        }
                    }
                });
            }
            for (auto& t : threads) t.join();
        }

        // Concatenate
        size_t total_pre_is = 0;
        for (auto& tp : tpre) total_pre_is += tp.entries.size();

        // Use a static vector to avoid repeated allocation
        static std::vector<PreIS> pre_is_storage;
        std::vector<PreIS>& pre_is = pre_is_storage;
        pre_is.resize(total_pre_is);
        size_t off = 0;
        for (auto& tp : tpre) {
            memcpy(pre_is.data() + off, tp.entries.data(), tp.entries.size() * sizeof(PreIS));
            off += tp.entries.size();
        }
        tpre.clear();

        // Parallel sort
        __gnu_parallel::sort(pre_is.begin(), pre_is.end(),
            [](const PreIS& a, const PreIS& b) {
                if (a.key != b.key) return a.key < b.key;
                return a.plabel_id < b.plabel_id;
            });
        const uint32_t pre_is_size = (uint32_t)pre_is.size();

        // Build per-adsh lo/hi offsets
        std::vector<uint32_t> adsh_lo(MAX_ADSH, UINT32_MAX);
        std::vector<uint32_t> adsh_hi(MAX_ADSH, 0);
        for (uint32_t i = 0; i < pre_is_size; i++) {
            uint32_t adsh = (uint32_t)(pre_is[i].key >> 35);
            if (adsh < MAX_ADSH) {
                if (adsh_lo[adsh] == UINT32_MAX) adsh_lo[adsh] = i;
                adsh_hi[adsh] = i + 1;
            }
        }

        // Thread-local tuple collection: (uint64 packed_key, double value) = 16 bytes
        // Packed key: name_id(18b) | tag_id(18b) | plabel_id(18b) = 54 bits
        struct SortTuple { uint64_t packed_key; double value; };
        struct ThreadData {
            std::vector<SortTuple> tuples;
            ThreadData() { tuples.reserve(80000); }
        };
        std::vector<ThreadData> tdata(NTHREADS);
        {
            GENDB_PHASE("num_scan");
            uint64_t chunk = (num.count + NTHREADS - 1) / NTHREADS;
            std::vector<std::thread> threads2;
            threads2.reserve(NTHREADS);
            for (int t = 0; t < NTHREADS; t++) {
                uint64_t start = (uint64_t)t * chunk;
                uint64_t end = std::min(start + chunk, num.count);
                if (start >= num.count) break;
                threads2.emplace_back([&, t, start, end] {
                    auto& tuples = tdata[t].tuples;
                    for (uint64_t i = start; i < end; i++) {
                        const NumRec& r = num.data[i];
                        if (r.uom_id != uom_usd || !r.has_value) continue;
                        if (r.adsh_id >= MAX_ADSH) continue;
                        uint32_t name_id = sub_name[r.adsh_id];
                        if (name_id == UINT32_MAX) continue;

                        // Per-adsh range lookup
                        uint32_t lo = adsh_lo[r.adsh_id];
                        if (lo == UINT32_MAX) continue;

                        uint32_t hi = adsh_hi[r.adsh_id];
                        uint64_t probe = pack_key3(r.adsh_id, r.tag_id, r.ver_id);

                        // Binary search within small per-adsh range
                        const PreIS* base = pre_is.data() + lo;
                        uint32_t len = hi - lo;
                        uint32_t left = 0, right = len;
                        while (left < right) {
                            uint32_t mid = (left + right) >> 1;
                            if (base[mid].key < probe) left = mid + 1;
                            else right = mid;
                        }

                        // Iterate matching entries (same key, different plabels)
                        while (left < len && base[left].key == probe) {
                            uint64_t agg_key = pack_agg_key(name_id, r.tag_id, base[left].plabel_id);
                            tuples.push_back({agg_key, r.value});
                            ++left;
                        }
                    }
                });
            }
            for (auto& t : threads2) t.join();
        }

        // Concatenate all tuples
        size_t total_tuples = 0;
        for (auto& td : tdata) total_tuples += td.tuples.size();
        std::vector<SortTuple> all_tuples;
        all_tuples.reserve(total_tuples);
        for (auto& td : tdata) for (auto& t : td.tuples) all_tuples.push_back(t);
        tdata.clear();

        // Parallel sort by packed key (16 bytes per element, faster than 20-byte struct)
        {
            GENDB_PHASE("tuple_sort");
            __gnu_parallel::sort(all_tuples.begin(), all_tuples.end(),
                [](const SortTuple& a, const SortTuple& b) {
                    return a.packed_key < b.packed_key;
                });
        }

        // Linear aggregation
        struct Row { uint64_t packed_key; double total; uint64_t cnt; };
        std::vector<Row> results;
        results.reserve(2000);

        uint64_t idx = 0;
        while (idx < all_tuples.size()) {
            uint64_t k = all_tuples[idx].packed_key;
            double sum = 0.0;
            uint64_t cnt = 0;
            while (idx < all_tuples.size() && all_tuples[idx].packed_key == k) {
                sum += all_tuples[idx].value;
                cnt++;
                idx++;
            }
            results.push_back({k, sum, cnt});
        }

        std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
            return a.total > b.total;
        });
        if (results.size() > 200) results.resize(200);

        {
            GENDB_PHASE("output");
            FILE* f = fopen((results_dir + "/Q6.csv").c_str(), "w");
            fprintf(f, "name,stmt,tag,plabel,total_value,cnt\n");
            for (const auto& r : results) {
                // Decode packed key (21 bits each)
                uint32_t name_id   = (uint32_t)((r.packed_key >> 42) & 0x1FFFFF);
                uint32_t tag_id    = (uint32_t)((r.packed_key >> 21) & 0x1FFFFF);
                uint32_t plabel_id = (uint32_t)(r.packed_key & 0x1FFFFF);
                write_csv_str(f, name_dict, name_id);
                fprintf(f, ",IS,");
                write_csv_str(f, tag_dict, tag_id);
                fputc(',', f);
                write_csv_str(f, plabel_dict, plabel_id);
                fprintf(f, ",%.2f,%lu\n", r.total, r.cnt);
            }
            fclose(f);
        }
    }

    return 0;
}
