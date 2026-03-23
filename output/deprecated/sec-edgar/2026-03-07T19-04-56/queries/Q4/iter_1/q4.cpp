// Q4: 4-table join with aggregation, HAVING, ORDER BY, LIMIT
// Optimizations: two-pass pre EQ sizing (~51K vs 1.24M), small AggMap init (4096 vs 100K),
// vector-based CIK tracking (deferred sort+unique vs per-insert unordered_set)

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"

// ============================================================
// Hash functions matching index builder
// ============================================================
static inline uint64_t hashKey2(uint32_t a, uint32_t b) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}

static inline uint64_t hashKey3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= (uint64_t)c * 3266489917ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}

// ============================================================
// Pre EQ hash map: open-addressing (sub_fk, tag_code, version_code) → count
// Two-pass build: count qualifying, then right-size (~51K entries = ~1.6MB)
// ============================================================
struct PreEqHashMap {
    struct Slot {
        uint32_t sub_fk;
        uint32_t tag_code;
        uint32_t version_code;
        uint32_t count;
        // empty sentinel: sub_fk == UINT32_MAX
    };
    Slot* table;
    size_t mask;

    PreEqHashMap() : table(nullptr), mask(0) {}
    ~PreEqHashMap() { delete[] table; }

    void allocate(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) cap <<= 1;
        mask = cap - 1;
        table = new Slot[cap];
        memset(table, 0xFF, cap * sizeof(Slot));
    }

    void insert(uint32_t sfk, uint32_t tc, uint32_t vc) {
        uint64_t pos = hashKey3(sfk, tc, vc) & mask;
        while (true) {
            auto& s = table[pos];
            if (s.sub_fk == UINT32_MAX) {
                s = {sfk, tc, vc, 1};
                return;
            }
            if (s.sub_fk == sfk && s.tag_code == tc && s.version_code == vc) {
                s.count++;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }

    uint32_t lookup(uint32_t sfk, uint32_t tc, uint32_t vc) const {
        uint64_t pos = hashKey3(sfk, tc, vc) & mask;
        while (true) {
            auto& s = table[pos];
            if (s.sub_fk == UINT32_MAX) return 0;
            if (s.sub_fk == sfk && s.tag_code == tc && s.version_code == vc) return s.count;
            pos = (pos + 1) & mask;
        }
    }
};

// ============================================================
// Tag PK index slot
// ============================================================
struct TagPKSlot {
    uint32_t tag_code;
    uint32_t version_code;
    uint32_t row_idx;
};

// ============================================================
// Aggregation key: (sic, tag_row) packed into uint64_t
// ============================================================
static inline uint64_t makeAggKey(int16_t sic, uint32_t tag_row) {
    return ((uint64_t)(uint16_t)sic << 32) | (uint64_t)tag_row;
}
static inline int16_t aggKeySic(uint64_t k) { return (int16_t)(uint16_t)(k >> 32); }
static inline uint32_t aggKeyTagRow(uint64_t k) { return (uint32_t)(k & 0xFFFFFFFFULL); }

// Thread-local aggregation entry — vector CIK (deferred sort+unique)
struct AggEntry {
    double sum_value;
    uint64_t count_value;
    std::vector<int32_t> cik_buf;
};

// Open-addressing aggregation map with dynamic growth
struct AggMap {
    struct Slot {
        uint64_t key;
        uint32_t entry_idx;
        bool occupied;
    };
    std::vector<Slot> table;
    size_t mask;
    size_t count;
    std::vector<AggEntry> entries;

    AggMap() : mask(0), count(0) {}

    void init(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
        count = 0;
        for (auto& s : table) s.occupied = false;
        entries.reserve(expected);
    }

    void grow() {
        size_t new_cap = (mask + 1) * 2;
        std::vector<Slot> new_table(new_cap);
        size_t new_mask = new_cap - 1;
        for (auto& s : new_table) s.occupied = false;
        for (auto& s : table) {
            if (!s.occupied) continue;
            size_t pos = (s.key * 0x9E3779B97F4A7C15ULL) & new_mask;
            while (new_table[pos].occupied) pos = (pos + 1) & new_mask;
            new_table[pos] = s;
        }
        table = std::move(new_table);
        mask = new_mask;
    }

    AggEntry& find_or_insert(uint64_t key) {
        size_t pos = (key * 0x9E3779B97F4A7C15ULL) & mask;
        while (true) {
            auto& s = table[pos];
            if (!s.occupied) {
                // Check load factor before inserting
                if (count * 4 >= (mask + 1) * 3) {
                    grow();
                    return find_or_insert(key);
                }
                s.key = key;
                s.occupied = true;
                s.entry_idx = (uint32_t)entries.size();
                entries.emplace_back();
                auto& e = entries.back();
                e.sum_value = 0.0;
                e.count_value = 0;
                count++;
                return e;
            }
            if (s.key == key) return entries[s.entry_idx];
            pos = (pos + 1) & mask;
        }
    }
};

// Varlen string reader (uint64_t offsets)
static inline std::string readVarlenString(const uint64_t* offsets, const char* data, uint32_t row) {
    uint64_t start = offsets[row];
    uint64_t end = offsets[row + 1];
    if (end <= start) return "";
    return std::string(data + start, end - start);
}

// CSV-escape a string
static void writeCSVString(FILE* f, const std::string& s) {
    bool need_quote = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { need_quote = true; break; }
    }
    if (need_quote) {
        fputc('"', f);
        for (char c : s) {
            if (c == '"') fputc('"', f);
            fputc(c, f);
        }
        fputc('"', f);
    } else {
        fwrite(s.data(), 1, s.size(), f);
    }
}

// Dictionary lookup helper
static int findDictCode(const uint64_t* offsets, size_t num_offsets, const char* data,
                        const char* target, size_t target_len) {
    size_t n = num_offsets - 1;
    for (size_t i = 0; i < n; i++) {
        uint64_t s = offsets[i], e = offsets[i + 1];
        if (e - s == target_len && memcmp(data + s, target, target_len) == 0)
            return (int)i;
    }
    return -1;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    std::string gendb = argv[1];
    std::string results = argv[2];

    GENDB_PHASE("total");

    // ============================================================
    // Phase 1: Pre-filter sub table — build sic_ok bitset (10.8KB)
    // ============================================================
    gendb::MmapColumn<int16_t> sub_sic(gendb + "/sub/sic.bin");
    gendb::MmapColumn<int32_t> sub_cik(gendb + "/sub/cik.bin");
    const size_t SUB_ROWS = sub_sic.count;

    std::vector<uint8_t> sic_ok(SUB_ROWS, 0);
    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < SUB_ROWS; i++) {
            int16_t s = sub_sic[i];
            if (s >= 4000 && s <= 4999) sic_ok[i] = 1;
        }
    }

    // ============================================================
    // Phase 2: Build pre EQ hashset — TWO-PASS right-sizing
    // Pass 1: count qualifying rows, Pass 2: insert
    // Result: ~51K entries in ~1.6MB (fits L3) vs 1.24M/40MB before
    // ============================================================
    PreEqHashMap preEqSet;
    {
        GENDB_PHASE("build_joins");

        // Find EQ code
        gendb::MmapColumn<uint64_t> stmt_dict_off(gendb + "/pre/stmt_dict_offsets.bin");
        gendb::MmapColumn<char> stmt_dict_data(gendb + "/pre/stmt_dict_data.bin");
        int eq_code = findDictCode(stmt_dict_off.data, stmt_dict_off.count,
                                   stmt_dict_data.data, "EQ", 2);

        // Get EQ range via stmt_offsets
        gendb::MmapColumn<char> stmt_off_raw(gendb + "/pre/stmt_offsets.bin");
        const uint32_t* num_entries_ptr = (const uint32_t*)stmt_off_raw.data;
        uint32_t num_off_entries = *num_entries_ptr;
        struct OffEntry { uint64_t start; uint64_t end; };
        const OffEntry* off_entries = (const OffEntry*)(num_entries_ptr + 1);

        uint64_t eq_start = 0, eq_end = 0;
        if (eq_code >= 0 && (uint32_t)eq_code < num_off_entries) {
            eq_start = off_entries[eq_code].start;
            eq_end = off_entries[eq_code].end;
        }

        gendb::MmapColumn<uint32_t> pre_sub_fk(gendb + "/pre/sub_fk.bin");
        gendb::MmapColumn<uint32_t> pre_tag_code(gendb + "/pre/tag_code.bin");
        gendb::MmapColumn<uint32_t> pre_version_code(gendb + "/pre/version_code.bin");

        // PASS 1: Count qualifying entries where sic_ok[sub_fk]
        size_t qualifying_count = 0;
        for (uint64_t i = eq_start; i < eq_end; i++) {
            uint32_t sfk = pre_sub_fk[i];
            if (sfk < SUB_ROWS && sic_ok[sfk]) qualifying_count++;
        }

        // Right-size: allocate to 2× qualifying count
        preEqSet.allocate(qualifying_count > 0 ? qualifying_count : 16);

        // PASS 2: Insert qualifying entries
        for (uint64_t i = eq_start; i < eq_end; i++) {
            uint32_t sfk = pre_sub_fk[i];
            if (sfk < SUB_ROWS && sic_ok[sfk]) {
                preEqSet.insert(sfk, pre_tag_code[i], pre_version_code[i]);
            }
        }
    }

    // ============================================================
    // Load tag data (abstract + PK index)
    // ============================================================
    gendb::MmapColumn<int8_t> tag_abstract(gendb + "/tag/abstract.bin");
    gendb::MmapColumn<char> tag_pk_raw(gendb + "/indexes/tag_pk_index.idx");
    const uint64_t* tag_pk_header = (const uint64_t*)tag_pk_raw.data;
    uint64_t tag_pk_size = tag_pk_header[0];
    const TagPKSlot* tag_pk_slots = (const TagPKSlot*)(tag_pk_header + 1);
    uint64_t tag_pk_mask = tag_pk_size - 1;

    // ============================================================
    // Phase 3: Get USD range
    // ============================================================
    uint64_t usd_start = 0, usd_end = 0;
    {
        gendb::MmapColumn<uint64_t> uom_dict_off(gendb + "/num/uom_dict_offsets.bin");
        gendb::MmapColumn<char> uom_dict_data(gendb + "/num/uom_dict_data.bin");
        int usd_code = findDictCode(uom_dict_off.data, uom_dict_off.count,
                                    uom_dict_data.data, "USD", 3);

        gendb::MmapColumn<char> uom_off_raw(gendb + "/num/uom_offsets.bin");
        const uint32_t* uo_num = (const uint32_t*)uom_off_raw.data;
        struct OffEntry { uint64_t start; uint64_t end; };
        const OffEntry* uo_entries = (const OffEntry*)(uo_num + 1);
        if (usd_code >= 0 && (uint32_t)usd_code < *uo_num) {
            usd_start = uo_entries[usd_code].start;
            usd_end = uo_entries[usd_code].end;
        }
    }

    // mmap num columns — sequential within USD range
    gendb::MmapColumn<double> num_value(gendb + "/num/value.bin");
    gendb::MmapColumn<uint32_t> num_sub_fk(gendb + "/num/sub_fk.bin");
    gendb::MmapColumn<uint32_t> num_tag_code(gendb + "/num/tag_code.bin");
    gendb::MmapColumn<uint32_t> num_version_code(gendb + "/num/version_code.bin");

    num_sub_fk.advise_sequential();
    num_tag_code.advise_sequential();
    num_version_code.advise_sequential();
    num_value.advise_sequential();

    // Thread-local aggregation maps — small init (4096 vs 100K)
    int nthreads = omp_get_max_threads();
    std::vector<AggMap> local_maps(nthreads);
    for (auto& m : local_maps) m.init(4096);

    // ============================================================
    // Phase 4: Parallel morsel-driven scan over USD rows
    // ============================================================
    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            AggMap& lmap = local_maps[tid];

            const double* val_ptr = num_value.data;
            const uint32_t* sfk_ptr = num_sub_fk.data;
            const uint32_t* tc_ptr = num_tag_code.data;
            const uint32_t* vc_ptr = num_version_code.data;
            const uint8_t* sic_ok_ptr = sic_ok.data();
            const int16_t* sic_ptr = sub_sic.data;
            const int32_t* cik_ptr = sub_cik.data;
            const int8_t* abstract_ptr = tag_abstract.data;

            #pragma omp for schedule(dynamic, 100000)
            for (uint64_t i = usd_start; i < usd_end; i++) {
                // Check sub filter first (cheapest — array lookup)
                uint32_t sfk = sfk_ptr[i];
                if (sfk >= SUB_ROWS || !sic_ok_ptr[sfk]) continue;

                double v = val_ptr[i];
                if (std::isnan(v)) continue;

                uint32_t tc = tc_ptr[i];
                uint32_t vc = vc_ptr[i];

                // Pre EQ semi-join (small hash table ~1.6MB, fits L3)
                uint32_t pre_count = preEqSet.lookup(sfk, tc, vc);
                if (pre_count == 0) continue;

                // Tag PK lookup (bounded probe)
                uint64_t h = hashKey2(tc, vc) & tag_pk_mask;
                uint32_t tag_row = UINT32_MAX;
                for (int probe = 0; probe < 64; probe++) {
                    const auto& slot = tag_pk_slots[h];
                    if (slot.row_idx == UINT32_MAX) break;
                    if (slot.tag_code == tc && slot.version_code == vc) {
                        tag_row = slot.row_idx;
                        break;
                    }
                    h = (h + 1) & tag_pk_mask;
                }
                if (tag_row == UINT32_MAX) continue;
                if (abstract_ptr[tag_row] != 0) continue;

                // Aggregate
                int16_t sic = sic_ptr[sfk];
                uint64_t key = makeAggKey(sic, tag_row);
                AggEntry& e = lmap.find_or_insert(key);
                e.sum_value += v * pre_count;
                e.count_value += pre_count;
                e.cik_buf.push_back(cik_ptr[sfk]);
            }
        }
    }

    // ============================================================
    // Phase 5: Merge + re-aggregate by tlabel + HAVING + sort + output
    // ============================================================
    {
        GENDB_PHASE("merge_aggregate");

        gendb::MmapColumn<uint64_t> tlabel_offsets(gendb + "/tag/tlabel_offsets.bin");
        gendb::MmapColumn<char> tlabel_data(gendb + "/tag/tlabel_data.bin");

        // Re-aggregate directly into (sic, tlabel) final groups
        struct FinalAgg {
            double sum_value;
            uint64_t count_value;
            std::vector<int32_t> cik_buf;
        };
        std::unordered_map<std::string, FinalAgg> final_map;
        {
            size_t total_groups = 0;
            for (auto& m : local_maps) total_groups += m.count;
            final_map.reserve(total_groups);
        }

        for (auto& lm : local_maps) {
            for (auto& slot : lm.table) {
                if (!slot.occupied) continue;
                auto& le = lm.entries[slot.entry_idx];
                int16_t sic = aggKeySic(slot.key);
                uint32_t tr = aggKeyTagRow(slot.key);
                std::string tlabel = readVarlenString(tlabel_offsets.data, tlabel_data.data, tr);

                char buf[8];
                int len = snprintf(buf, sizeof(buf), "%d|", (int)sic);
                std::string key(buf, len);
                key += tlabel;

                auto& fa = final_map[key];
                fa.sum_value += le.sum_value;
                fa.count_value += le.count_value;
                if (fa.cik_buf.empty()) {
                    fa.cik_buf = std::move(le.cik_buf);
                } else {
                    fa.cik_buf.insert(fa.cik_buf.end(), le.cik_buf.begin(), le.cik_buf.end());
                }
            }
        }

        // Build output rows with HAVING and deferred sort+unique for CIK
        struct OutputRow {
            int16_t sic;
            std::string tlabel;
            int32_t num_companies;
            double total_value;
            double avg_value;
        };

        std::vector<OutputRow> rows;
        rows.reserve(final_map.size());

        for (auto& [key, fa] : final_map) {
            std::sort(fa.cik_buf.begin(), fa.cik_buf.end());
            fa.cik_buf.erase(std::unique(fa.cik_buf.begin(), fa.cik_buf.end()), fa.cik_buf.end());
            int32_t nc = (int32_t)fa.cik_buf.size();
            if (nc < 2) continue;

            size_t sep = key.find('|');
            int16_t sic = (int16_t)atoi(key.substr(0, sep).c_str());
            std::string tlabel = key.substr(sep + 1);
            double avg = fa.sum_value / (double)fa.count_value;
            rows.push_back({sic, std::move(tlabel), nc, fa.sum_value, avg});
        }

        // Top-500 by total_value DESC
        size_t limit = std::min((size_t)500, rows.size());
        std::partial_sort(rows.begin(), rows.begin() + limit, rows.end(),
            [](const OutputRow& a, const OutputRow& b) {
                return a.total_value > b.total_value;
            });

        // Write CSV output
        {
            GENDB_PHASE("output");
            std::string outpath = results + "/Q4.csv";
            FILE* f = fopen(outpath.c_str(), "w");
            fprintf(f, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
            for (size_t i = 0; i < limit; i++) {
                auto& r = rows[i];
                fprintf(f, "%d,", (int)r.sic);
                writeCSVString(f, r.tlabel);
                fprintf(f, ",EQ,%d,%.2f,%.2f\n", r.num_companies, r.total_value, r.avg_value);
            }
            fclose(f);
        }
    }

    return 0;
}
