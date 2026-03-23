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
constexpr uint32_t kStmtDomain = 9;
constexpr uint32_t kRfileDomain = 3;
constexpr uint32_t kGroupCount = kStmtDomain * kRfileDomain;
constexpr uint32_t kAdshDomain = 86136;

template <typename T>
void require_count(const char* name, const gendb::MmapColumn<T>& col, size_t expected) {
    if (col.size() != expected) {
        throw std::runtime_error(std::string("unexpected element count for ") + name);
    }
}

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

    uint32_t cardinality() const {
        return offsets.empty() ? 0u : static_cast<uint32_t>(offsets.size() - 1);
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

inline uint32_t group_index(uint16_t stmt_code, uint16_t rfile_code) {
    return static_cast<uint32_t>(stmt_code) * kRfileDomain + static_cast<uint32_t>(rfile_code);
}

inline uint16_t stmt_from_group(uint32_t group_idx) {
    return static_cast<uint16_t>(group_idx / kRfileDomain);
}

inline uint16_t rfile_from_group(uint32_t group_idx) {
    return static_cast<uint16_t>(group_idx % kRfileDomain);
}

struct alignas(64) ThreadState {
    std::array<uint64_t, kGroupCount> cnt{};
    std::array<int64_t, kGroupCount> sum_line{};
    std::array<uint32_t, kAdshDomain> adsh_group_mask{};
};

struct OutputRow {
    uint32_t group_idx = 0;
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
        gendb::MmapColumn<uint64_t> global_adsh_offsets;
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
            global_adsh_offsets.open(dict_dir + "/global_adsh.offsets.bin");

            require_count("pre.stmt", pre_stmt, kExpectedRows);
            require_count("pre.rfile", pre_rfile, kExpectedRows);
            require_count("pre.adsh", pre_adsh, kExpectedRows);
            require_count("pre.line", pre_line, kExpectedRows);

            if (stmt_dict.cardinality() != kStmtDomain) {
                throw std::runtime_error("unexpected pre.stmt dictionary cardinality");
            }
            if (rfile_dict.cardinality() != kRfileDomain) {
                throw std::runtime_error("unexpected pre.rfile dictionary cardinality");
            }
            if (global_adsh_offsets.size() != static_cast<size_t>(kAdshDomain) + 1) {
                throw std::runtime_error("unexpected global_adsh dictionary cardinality");
            }

            gendb::mmap_prefetch_all(pre_stmt, pre_rfile, pre_adsh, pre_line);
        }

        {
            GENDB_PHASE("dim_filter");
        }

        const size_t n_rows = pre_stmt.size();
        const int thread_count = std::max(1, omp_get_num_procs());
        std::vector<ThreadState> thread_states(static_cast<size_t>(thread_count));

        {
            GENDB_PHASE("main_scan");

            const uint16_t* __restrict stmt = pre_stmt.data;
            const uint16_t* __restrict rfile = pre_rfile.data;
            const uint32_t* __restrict adsh = pre_adsh.data;
            const int32_t* __restrict line = pre_line.data;

#pragma omp parallel num_threads(thread_count)
            {
                const int tid = omp_get_thread_num();
                const size_t start = (n_rows * static_cast<size_t>(tid)) / static_cast<size_t>(thread_count);
                const size_t end = (n_rows * static_cast<size_t>(tid + 1)) / static_cast<size_t>(thread_count);
                ThreadState& local = thread_states[static_cast<size_t>(tid)];
                uint64_t* __restrict local_cnt = local.cnt.data();
                int64_t* __restrict local_sum_line = local.sum_line.data();
                uint32_t* __restrict local_masks = local.adsh_group_mask.data();

                for (size_t row = start; row < end; ++row) {
                    const uint16_t stmt_code = stmt[row];
                    if (stmt_code == 0) {
                        continue;
                    }

                    const uint32_t group_idx = group_index(stmt_code, rfile[row]);
                    ++local_cnt[group_idx];
                    local_sum_line[group_idx] += static_cast<int64_t>(line[row]);
                    local_masks[adsh[row]] |= (1u << group_idx);
                }
            }
        }

        std::array<uint64_t, kGroupCount> cnt{};
        std::array<int64_t, kGroupCount> sum_line{};
        std::array<uint64_t, kGroupCount> num_filings{};
        std::vector<uint32_t> merged_adsh_masks(kAdshDomain, 0u);

        {
            GENDB_PHASE("build_joins");

            for (const ThreadState& local : thread_states) {
                for (uint32_t group_idx = 0; group_idx < kGroupCount; ++group_idx) {
                    cnt[group_idx] += local.cnt[group_idx];
                    sum_line[group_idx] += local.sum_line[group_idx];
                }

                const uint32_t* __restrict src_masks = local.adsh_group_mask.data();
                uint32_t* __restrict dst_masks = merged_adsh_masks.data();
                for (uint32_t adsh_code = 0; adsh_code < kAdshDomain; ++adsh_code) {
                    dst_masks[adsh_code] |= src_masks[adsh_code];
                }
            }

            for (uint32_t adsh_code = 0; adsh_code < kAdshDomain; ++adsh_code) {
                uint32_t mask = merged_adsh_masks[adsh_code];
                while (mask != 0) {
                    const uint32_t bit = static_cast<uint32_t>(__builtin_ctz(mask));
                    ++num_filings[bit];
                    mask &= (mask - 1);
                }
            }
        }

        {
            GENDB_PHASE("output");

            std::vector<OutputRow> rows;
            rows.reserve(kGroupCount);
            for (uint32_t group_idx = 0; group_idx < kGroupCount; ++group_idx) {
                if (cnt[group_idx] == 0) {
                    continue;
                }
                rows.push_back(OutputRow{group_idx, cnt[group_idx], num_filings[group_idx], sum_line[group_idx]});
            }

            std::sort(rows.begin(), rows.end(), [](const OutputRow& left, const OutputRow& right) {
                if (left.cnt != right.cnt) {
                    return left.cnt > right.cnt;
                }
                return left.group_idx < right.group_idx;
            });

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q1.csv";
            std::FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file");
            }

            std::fprintf(out, "stmt,rfile,cnt,num_filings,avg_line_num\n");
            for (const OutputRow& row : rows) {
                const uint16_t stmt_code = stmt_from_group(row.group_idx);
                const uint16_t rfile_code = rfile_from_group(row.group_idx);
                const std::string_view stmt_sv = stmt_dict.decode(stmt_code);
                const std::string_view rfile_sv = rfile_dict.decode(rfile_code);
                const double avg_line_num = static_cast<double>(row.sum_line) / static_cast<double>(row.cnt);

                std::fprintf(out,
                             "%.*s,%.*s,%llu,%llu,%.2f\n",
                             static_cast<int>(stmt_sv.size()),
                             stmt_sv.data(),
                             static_cast<int>(rfile_sv.size()),
                             rfile_sv.data(),
                             static_cast<unsigned long long>(row.cnt),
                             static_cast<unsigned long long>(row.num_filings),
                             avg_line_num);
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Q1 failed: %s\n", e.what());
        return 1;
    }
}
