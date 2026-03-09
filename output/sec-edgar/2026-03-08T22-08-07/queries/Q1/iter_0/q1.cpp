#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr size_t kExpectedRows = 9600799;
constexpr int kMaxGroups = 32;
constexpr int kMorselRows = 100000;

template <typename T>
void require_count(const char* name, const gendb::MmapColumn<T>& col, size_t expected) {
    if (col.size() != expected) {
        throw std::runtime_error(std::string("unexpected element count for ") + name);
    }
}

inline uint32_t pack_group_key(uint16_t stmt_code, uint16_t rfile_code) {
    return (static_cast<uint32_t>(stmt_code) << 16) | static_cast<uint32_t>(rfile_code);
}

inline uint16_t unpack_stmt_code(uint32_t group_key) {
    return static_cast<uint16_t>(group_key >> 16);
}

inline uint16_t unpack_rfile_code(uint32_t group_key) {
    return static_cast<uint16_t>(group_key & 0xFFFFu);
}

struct GroupState {
    uint32_t key = 0;
    uint64_t cnt = 0;
    int64_t sum_line = 0;
    uint64_t num_filings = 0;
};

struct alignas(64) ThreadState {
    std::array<GroupState, kMaxGroups> groups{};
    int group_count = 0;
    std::vector<uint64_t> distinct_keys;

    GroupState& lookup_or_insert(uint32_t key) {
        for (int i = 0; i < group_count; ++i) {
            if (groups[static_cast<size_t>(i)].key == key) {
                return groups[static_cast<size_t>(i)];
            }
        }
        if (group_count >= kMaxGroups) {
            throw std::runtime_error("group capacity exceeded");
        }
        GroupState& slot = groups[static_cast<size_t>(group_count++)];
        slot = GroupState{};
        slot.key = key;
        return slot;
    }

    const GroupState* find(uint32_t key) const {
        for (int i = 0; i < group_count; ++i) {
            if (groups[static_cast<size_t>(i)].key == key) {
                return &groups[static_cast<size_t>(i)];
            }
        }
        return nullptr;
    }
};

struct StringDict {
    gendb::MmapColumn<uint64_t> offsets;
    gendb::MmapColumn<char> data;

    void open(const std::string& base_path) {
        offsets.open(base_path + ".offsets.bin");
        data.open(base_path + ".data.bin");
        if (offsets.size() == 0) {
            throw std::runtime_error("empty dictionary offsets: " + base_path);
        }
    }

    std::string_view decode(uint32_t code) const {
        const size_t idx = static_cast<size_t>(code);
        if (idx + 1 >= offsets.size()) {
            throw std::runtime_error("dictionary code out of range");
        }
        const uint64_t begin = offsets[idx];
        const uint64_t end = offsets[idx + 1];
        return std::string_view(data.data + begin, static_cast<size_t>(end - begin));
    }
};

struct OutputRow {
    uint32_t key = 0;
    uint64_t cnt = 0;
    uint64_t num_filings = 0;
    int64_t sum_line = 0;
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        GENDB_PHASE("total");

        gendb::MmapColumn<uint16_t> pre_stmt;
        gendb::MmapColumn<uint16_t> pre_rfile;
        gendb::MmapColumn<uint32_t> pre_adsh;
        gendb::MmapColumn<int32_t> pre_line;
        StringDict stmt_dict;
        StringDict rfile_dict;

        {
            GENDB_PHASE("data_loading");

            const std::string pre_dir = gendb_dir + "/pre";
            const std::string dict_dir = gendb_dir + "/dicts";

            pre_stmt.open(pre_dir + "/stmt.bin");
            pre_rfile.open(pre_dir + "/rfile.bin");
            pre_adsh.open(pre_dir + "/adsh.bin");
            pre_line.open(pre_dir + "/line.bin");
            stmt_dict.open(dict_dir + "/pre_stmt");
            rfile_dict.open(dict_dir + "/pre_rfile");

            require_count("pre.stmt", pre_stmt, kExpectedRows);
            require_count("pre.rfile", pre_rfile, kExpectedRows);
            require_count("pre.adsh", pre_adsh, kExpectedRows);
            require_count("pre.line", pre_line, kExpectedRows);

            gendb::mmap_prefetch_all(pre_stmt, pre_rfile, pre_adsh, pre_line);
        }

        {
            GENDB_PHASE("dim_filter");
        }

        const size_t n_rows = pre_stmt.size();
        const int thread_count = std::max(1, std::min(32, omp_get_num_procs()));
        std::vector<ThreadState> thread_states(static_cast<size_t>(thread_count));

        {
            GENDB_PHASE("main_scan");

            const uint16_t* __restrict stmt = pre_stmt.data;
            const uint16_t* __restrict rfile = pre_rfile.data;
            const uint32_t* __restrict adsh = pre_adsh.data;
            const int32_t* __restrict line = pre_line.data;
            const size_t reserve_per_thread = (n_rows + static_cast<size_t>(thread_count) - 1) /
                                              static_cast<size_t>(thread_count);

#pragma omp parallel num_threads(thread_count)
            {
                ThreadState& local = thread_states[static_cast<size_t>(omp_get_thread_num())];
                local.distinct_keys.reserve(reserve_per_thread);

#pragma omp for schedule(dynamic, kMorselRows)
                for (int64_t row = 0; row < static_cast<int64_t>(n_rows); ++row) {
                    const uint16_t stmt_code = stmt[row];
                    if (stmt_code == 0) {
                        continue;
                    }

                    const uint32_t group_key = pack_group_key(stmt_code, rfile[row]);
                    GroupState& agg = local.lookup_or_insert(group_key);
                    ++agg.cnt;
                    agg.sum_line += static_cast<int64_t>(line[row]);
                    local.distinct_keys.push_back((static_cast<uint64_t>(group_key) << 32) |
                                                  static_cast<uint64_t>(adsh[row]));
                }

                std::sort(local.distinct_keys.begin(), local.distinct_keys.end());
                local.distinct_keys.erase(
                    std::unique(local.distinct_keys.begin(), local.distinct_keys.end()),
                    local.distinct_keys.end());
            }
        }

        ThreadState global_state;
        std::vector<uint64_t> global_distinct_keys;

        {
            GENDB_PHASE("build_joins");

            size_t total_distinct_keys = 0;
            for (const ThreadState& local : thread_states) {
                total_distinct_keys += local.distinct_keys.size();
                for (int i = 0; i < local.group_count; ++i) {
                    const GroupState& src = local.groups[static_cast<size_t>(i)];
                    GroupState& dst = global_state.lookup_or_insert(src.key);
                    dst.cnt += src.cnt;
                    dst.sum_line += src.sum_line;
                }
            }

            global_distinct_keys.reserve(total_distinct_keys);
            for (ThreadState& local : thread_states) {
                global_distinct_keys.insert(global_distinct_keys.end(),
                                            local.distinct_keys.begin(),
                                            local.distinct_keys.end());
                std::vector<uint64_t>().swap(local.distinct_keys);
            }

            std::sort(global_distinct_keys.begin(), global_distinct_keys.end());
            global_distinct_keys.erase(
                std::unique(global_distinct_keys.begin(), global_distinct_keys.end()),
                global_distinct_keys.end());

            for (uint64_t packed : global_distinct_keys) {
                const uint32_t group_key = static_cast<uint32_t>(packed >> 32);
                GroupState& dst = global_state.lookup_or_insert(group_key);
                ++dst.num_filings;
            }
        }

        {
            GENDB_PHASE("output");

            std::vector<OutputRow> rows;
            rows.reserve(static_cast<size_t>(global_state.group_count));
            for (int i = 0; i < global_state.group_count; ++i) {
                const GroupState& state = global_state.groups[static_cast<size_t>(i)];
                if (state.cnt == 0) {
                    continue;
                }
                rows.push_back(OutputRow{state.key, state.cnt, state.num_filings, state.sum_line});
            }

            std::sort(rows.begin(), rows.end(), [](const OutputRow& a, const OutputRow& b) {
                if (a.cnt != b.cnt) {
                    return a.cnt > b.cnt;
                }
                if (a.key != b.key) {
                    return a.key < b.key;
                }
                return a.num_filings < b.num_filings;
            });

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q1.csv";
            std::FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file");
            }

            std::fprintf(out, "stmt,rfile,cnt,num_filings,avg_line_num\n");
            for (const OutputRow& row : rows) {
                const std::string_view stmt_sv = stmt_dict.decode(unpack_stmt_code(row.key));
                const std::string_view rfile_sv = rfile_dict.decode(unpack_rfile_code(row.key));
                const double avg_line = static_cast<double>(row.sum_line) / static_cast<double>(row.cnt);
                std::fprintf(out,
                             "%.*s,%.*s,%llu,%llu,%.2f\n",
                             static_cast<int>(stmt_sv.size()),
                             stmt_sv.data(),
                             static_cast<int>(rfile_sv.size()),
                             rfile_sv.data(),
                             static_cast<unsigned long long>(row.cnt),
                             static_cast<unsigned long long>(row.num_filings),
                             avg_line);
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Q1 failed: %s\n", e.what());
        return 1;
    }
}
