#include "mmap_utils.h"
#include "timing_utils.h"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <omp.h>
#include <sys/stat.h>
#include <sys/types.h>

using gendb::MmapColumn;

namespace {

constexpr uint32_t kRowidNull = std::numeric_limits<uint32_t>::max();
constexpr int32_t kSlotNull = -1;
constexpr int16_t kFiscalYear = 2022;
constexpr size_t kTopK = 100;
constexpr int64_t kMorselSize = 32768;

struct DictView {
    MmapColumn<uint64_t> offsets;
    MmapColumn<char> data;

    void open(const std::string& offsets_path, const std::string& data_path) {
        offsets.open(offsets_path);
        data.open(data_path);
    }

    size_t size() const {
        return offsets.size() == 0 ? 0 : offsets.size() - 1;
    }

    std::string_view view(uint32_t code) const {
        if (static_cast<size_t>(code) + 1 >= offsets.size()) {
            return {};
        }
        const uint64_t begin = offsets[code];
        const uint64_t end = offsets[code + 1];
        return std::string_view(data.data + begin, static_cast<size_t>(end - begin));
    }
};

template <typename CodeT>
CodeT resolve_code(const DictView& dict, std::string_view target) {
    for (uint32_t code = 0; code < dict.size(); ++code) {
        if (dict.view(code) == target) {
            return static_cast<CodeT>(code);
        }
    }
    return std::numeric_limits<CodeT>::max();
}

template <typename T>
bool find_postings_group(const MmapColumn<T>& values, T needle, size_t& group_idx) {
    const T* begin = values.data;
    const T* end = values.data + values.size();
    const T* it = std::lower_bound(begin, end, needle);
    if (it == end || *it != needle) {
        return false;
    }
    group_idx = static_cast<size_t>(it - begin);
    return true;
}

void ensure_dir(const std::string& path) {
    if (::mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        throw std::runtime_error("mkdir failed for " + path);
    }
}

void write_csv_escaped(FILE* out, std::string_view value) {
    bool needs_quotes = false;
    for (char ch : value) {
        if (ch == ',' || ch == '"' || ch == '\n' || ch == '\r') {
            needs_quotes = true;
            break;
        }
    }
    if (!needs_quotes) {
        if (!value.empty()) {
            std::fwrite(value.data(), 1, value.size(), out);
        }
        return;
    }
    std::fputc('"', out);
    for (char ch : value) {
        if (ch == '"') {
            std::fputc('"', out);
        }
        std::fputc(ch, out);
    }
    std::fputc('"', out);
}

struct GroupState {
    uint32_t tag;
    double max_value;
    uint32_t tie_count;
};

struct Bucket {
    std::vector<GroupState> groups;

    Bucket() {
        groups.reserve(4);
    }
};

struct ThreadState {
    std::vector<int32_t> adsh_slot;
    std::vector<uint32_t> touched_adsh;
    std::vector<Bucket> buckets;

    void init(size_t adsh_domain) {
        adsh_slot.assign(adsh_domain, kSlotNull);
        touched_adsh.clear();
        buckets.clear();
        touched_adsh.reserve(2048);
        buckets.reserve(2048);
    }
};

inline void update_local_groups(std::vector<GroupState>& groups, uint32_t tag, double value) {
    for (GroupState& state : groups) {
        if (state.tag != tag) {
            continue;
        }
        if (value > state.max_value) {
            state.max_value = value;
            state.tie_count = 1;
        } else if (value == state.max_value) {
            ++state.tie_count;
        }
        return;
    }
    groups.push_back(GroupState{tag, value, 1});
}

inline void merge_group_state(std::vector<GroupState>& groups, const GroupState& incoming) {
    for (GroupState& state : groups) {
        if (state.tag != incoming.tag) {
            continue;
        }
        if (incoming.max_value > state.max_value) {
            state.max_value = incoming.max_value;
            state.tie_count = incoming.tie_count;
        } else if (incoming.max_value == state.max_value) {
            state.tie_count += incoming.tie_count;
        }
        return;
    }
    groups.push_back(incoming);
}

struct ResultRow {
    uint32_t name_code;
    uint32_t tag_code;
    double value;
};

struct RankComparator {
    int compare(const ResultRow& lhs, const ResultRow& rhs) const {
        if (lhs.value > rhs.value) {
            return -1;
        }
        if (lhs.value < rhs.value) {
            return 1;
        }
        if (lhs.name_code < rhs.name_code) {
            return -1;
        }
        if (lhs.name_code > rhs.name_code) {
            return 1;
        }
        if (lhs.tag_code < rhs.tag_code) {
            return -1;
        }
        if (lhs.tag_code > rhs.tag_code) {
            return 1;
        }
        return 0;
    }

    bool better(const ResultRow& lhs, const ResultRow& rhs) const {
        return compare(lhs, rhs) < 0;
    }

    bool worse(const ResultRow& lhs, const ResultRow& rhs) const {
        return compare(lhs, rhs) > 0;
    }
};

struct TopKBuffer {
    explicit TopKBuffer(const RankComparator& comparator) : cmp(comparator) {
        rows.reserve(kTopK);
    }

    void consider(const ResultRow& row) {
        if (rows.size() < kTopK) {
            rows.push_back(row);
            worst_dirty = true;
            return;
        }
        if (worst_dirty) {
            recompute_worst();
        }
        if (cmp.better(row, rows[worst_idx])) {
            rows[worst_idx] = row;
            worst_dirty = true;
        }
    }

    void finalize() {
        std::sort(rows.begin(), rows.end(), [&](const ResultRow& lhs, const ResultRow& rhs) {
            return cmp.better(lhs, rhs);
        });
        if (rows.size() > kTopK) {
            rows.resize(kTopK);
        }
    }

    std::vector<ResultRow> rows;

private:
    void recompute_worst() {
        worst_idx = 0;
        for (size_t idx = 1; idx < rows.size(); ++idx) {
            if (cmp.worse(rows[idx], rows[worst_idx])) {
                worst_idx = idx;
            }
        }
        worst_dirty = false;
    }

    const RankComparator& cmp;
    size_t worst_idx = 0;
    bool worst_dirty = false;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        GENDB_PHASE("total");

        const int configured_threads = std::max(1, std::min(16, omp_get_max_threads()));
        omp_set_num_threads(configured_threads);

        MmapColumn<uint32_t> num_adsh;
        MmapColumn<uint32_t> num_tag;
        MmapColumn<double> num_value;
        MmapColumn<uint32_t> sub_adsh;
        MmapColumn<uint32_t> sub_name;

        MmapColumn<uint16_t> num_uom_postings_values;
        MmapColumn<uint64_t> num_uom_postings_offsets;
        MmapColumn<uint32_t> num_uom_postings_rowids;

        MmapColumn<int16_t> sub_fy_postings_values;
        MmapColumn<uint64_t> sub_fy_postings_offsets;
        MmapColumn<uint32_t> sub_fy_postings_rowids;
        MmapColumn<uint32_t> sub_adsh_dense_lookup;

        DictView num_uom_dict;
        DictView sub_name_dict;
        DictView global_tag_dict;

        {
            GENDB_PHASE("data_loading");

            num_adsh.open(gendb_dir + "/num/adsh.bin");
            num_tag.open(gendb_dir + "/num/tag.bin");
            num_value.open(gendb_dir + "/num/value.bin");
            sub_adsh.open(gendb_dir + "/sub/adsh.bin");
            sub_name.open(gendb_dir + "/sub/name.bin");

            num_uom_postings_values.open(gendb_dir + "/indexes/num/num_uom_postings.values.bin");
            num_uom_postings_offsets.open(gendb_dir + "/indexes/num/num_uom_postings.offsets.bin");
            num_uom_postings_rowids.open(gendb_dir + "/indexes/num/num_uom_postings.rowids.bin");

            sub_fy_postings_values.open(gendb_dir + "/indexes/sub/sub_fy_postings.values.bin");
            sub_fy_postings_offsets.open(gendb_dir + "/indexes/sub/sub_fy_postings.offsets.bin");
            sub_fy_postings_rowids.open(gendb_dir + "/indexes/sub/sub_fy_postings.rowids.bin");
            sub_adsh_dense_lookup.open(gendb_dir + "/indexes/sub/sub_adsh_dense_lookup.bin");

            num_uom_dict.open(gendb_dir + "/dicts/num_uom.offsets.bin", gendb_dir + "/dicts/num_uom.data.bin");
            sub_name_dict.open(gendb_dir + "/dicts/sub_name.offsets.bin", gendb_dir + "/dicts/sub_name.data.bin");
            global_tag_dict.open(gendb_dir + "/dicts/global_tag.offsets.bin", gendb_dir + "/dicts/global_tag.data.bin");

            num_adsh.prefetch();
            num_tag.prefetch();
            num_value.prefetch();
            sub_adsh.prefetch();
            sub_name.prefetch();
            num_uom_postings_rowids.prefetch();
            sub_fy_postings_rowids.prefetch();
        }

        const uint32_t adsh_domain = static_cast<uint32_t>(sub_adsh_dense_lookup.size());
        if (adsh_domain == 0) {
            throw std::runtime_error("sub_adsh_dense_lookup.bin is empty");
        }

        const uint32_t* pure_rowids = nullptr;
        size_t pure_row_count = 0;
        std::vector<uint32_t> fy2022_sub_rowid_by_adsh;

        {
            GENDB_PHASE("dim_filter");

            const uint16_t pure_code = resolve_code<uint16_t>(num_uom_dict, "pure");
            if (pure_code == std::numeric_limits<uint16_t>::max()) {
                throw std::runtime_error("Failed to resolve num.uom code for 'pure'");
            }

            size_t pure_group_idx = 0;
            if (find_postings_group(num_uom_postings_values, pure_code, pure_group_idx)) {
                const uint64_t pure_begin = num_uom_postings_offsets[pure_group_idx];
                const uint64_t pure_end = num_uom_postings_offsets[pure_group_idx + 1];
                pure_rowids = num_uom_postings_rowids.data + pure_begin;
                pure_row_count = static_cast<size_t>(pure_end - pure_begin);
            }

            fy2022_sub_rowid_by_adsh.assign(adsh_domain, kRowidNull);

            size_t fy_group_idx = 0;
            if (find_postings_group(sub_fy_postings_values, kFiscalYear, fy_group_idx)) {
                const uint64_t fy_begin = sub_fy_postings_offsets[fy_group_idx];
                const uint64_t fy_end = sub_fy_postings_offsets[fy_group_idx + 1];
                for (uint64_t idx = fy_begin; idx < fy_end; ++idx) {
                    const uint32_t sub_rowid = sub_fy_postings_rowids[idx];
                    const uint32_t adsh_code = sub_adsh[sub_rowid];
                    if (adsh_code < adsh_domain) {
                        fy2022_sub_rowid_by_adsh[adsh_code] = sub_rowid;
                    }
                }
            }
        }

        std::vector<uint32_t> global_touched_adsh;
        std::vector<Bucket> global_buckets;

        {
            GENDB_PHASE("build_joins");

            const int thread_count = configured_threads;
            std::vector<ThreadState> thread_states(static_cast<size_t>(thread_count));

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                ThreadState& local = thread_states[static_cast<size_t>(tid)];
                local.init(adsh_domain);

                #pragma omp for schedule(static)
                for (int64_t morsel_begin = 0; morsel_begin < static_cast<int64_t>(pure_row_count); morsel_begin += kMorselSize) {
                    const int64_t morsel_end = std::min<int64_t>(morsel_begin + kMorselSize, static_cast<int64_t>(pure_row_count));
                    for (int64_t idx = morsel_begin; idx < morsel_end; ++idx) {
                        const uint32_t rowid = pure_rowids[idx];
                        const double value = num_value[rowid];
                        if (std::isnan(value)) {
                            continue;
                        }

                        const uint32_t adsh_code = num_adsh[rowid];
                        if (adsh_code >= fy2022_sub_rowid_by_adsh.size()) {
                            continue;
                        }
                        if (fy2022_sub_rowid_by_adsh[adsh_code] == kRowidNull) {
                            continue;
                        }

                        int32_t& bucket_slot = local.adsh_slot[adsh_code];
                        if (bucket_slot == kSlotNull) {
                            bucket_slot = static_cast<int32_t>(local.buckets.size());
                            local.touched_adsh.push_back(adsh_code);
                            local.buckets.emplace_back();
                        }

                        const uint32_t tag_code = num_tag[rowid];
                        update_local_groups(local.buckets[static_cast<size_t>(bucket_slot)].groups, tag_code, value);
                    }
                }
            }

            std::vector<int32_t> global_adsh_slot(adsh_domain, kSlotNull);
            global_touched_adsh.reserve(4096);
            global_buckets.reserve(4096);

            for (ThreadState& state : thread_states) {
                for (uint32_t adsh_code : state.touched_adsh) {
                    const int32_t local_slot = state.adsh_slot[adsh_code];
                    if (local_slot == kSlotNull) {
                        continue;
                    }

                    int32_t& global_slot = global_adsh_slot[adsh_code];
                    if (global_slot == kSlotNull) {
                        global_slot = static_cast<int32_t>(global_buckets.size());
                        global_touched_adsh.push_back(adsh_code);
                        global_buckets.emplace_back();
                    }

                    std::vector<GroupState>& global_groups = global_buckets[static_cast<size_t>(global_slot)].groups;
                    const std::vector<GroupState>& local_groups = state.buckets[static_cast<size_t>(local_slot)].groups;
                    for (const GroupState& local_group : local_groups) {
                        merge_group_state(global_groups, local_group);
                    }
                }
            }
        }

        std::vector<ResultRow> final_rows;
        {
            GENDB_PHASE("main_scan");

            const RankComparator rank_cmp;
            TopKBuffer topk(rank_cmp);

            for (size_t bucket_idx = 0; bucket_idx < global_touched_adsh.size(); ++bucket_idx) {
                const uint32_t adsh_code = global_touched_adsh[bucket_idx];
                const uint32_t sub_rowid = fy2022_sub_rowid_by_adsh[adsh_code];
                if (sub_rowid == kRowidNull) {
                    continue;
                }

                const uint32_t name_code = sub_name[sub_rowid];
                const Bucket& bucket = global_buckets[bucket_idx];
                for (const GroupState& group : bucket.groups) {
                    const ResultRow row{name_code, group.tag, group.max_value};
                    for (uint32_t count = 0; count < group.tie_count; ++count) {
                        topk.consider(row);
                    }
                }
            }

            topk.finalize();
            final_rows = std::move(topk.rows);
        }

        {
            GENDB_PHASE("output");

            ensure_dir(results_dir);
            FILE* out = std::fopen((results_dir + "/Q2.csv").c_str(), "w");
            if (out == nullptr) {
                throw std::runtime_error("Failed to open output CSV");
            }

            std::fprintf(out, "name,tag,value\n");
            for (const ResultRow& row : final_rows) {
                write_csv_escaped(out, sub_name_dict.view(row.name_code));
                std::fputc(',', out);
                write_csv_escaped(out, global_tag_dict.view(row.tag_code));
                std::fprintf(out, ",%.2f\n", row.value);
            }
            std::fclose(out);
        }
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "Error: %s\n", ex.what());
        return 1;
    }

    return 0;
}
