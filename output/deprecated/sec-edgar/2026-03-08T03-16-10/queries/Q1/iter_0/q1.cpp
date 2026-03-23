#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr uint32_t kStmtCardinality = 9;
constexpr uint32_t kRfileCardinality = 2;
constexpr uint32_t kGroupCount = kStmtCardinality * kRfileCardinality;
constexpr size_t kMorselSize = 100000;

struct DictData {
    gendb::MmapColumn<uint64_t> offsets;
    gendb::MmapColumn<char> data;

    std::string_view at(size_t idx) const {
        return std::string_view(data.data + offsets[idx], offsets[idx + 1] - offsets[idx]);
    }

    size_t size() const {
        return offsets.size() > 0 ? offsets.size() - 1 : 0;
    }
};

struct alignas(64) ThreadAgg {
    std::array<uint64_t, kGroupCount> cnt{};
    std::array<int64_t, kGroupCount> sum_line{};
    std::vector<uint64_t> distinct_words;

    explicit ThreadAgg(size_t words_per_group)
        : distinct_words(kGroupCount * words_per_group, 0) {}
};

struct ResultRow {
    uint16_t stmt_id;
    uint16_t rfile_id;
    uint64_t cnt;
    uint64_t num_filings;
    double avg_line_num;
};

DictData load_dict(const std::string& offsets_path, const std::string& data_path) {
    DictData dict;
    dict.offsets.open(offsets_path);
    dict.data.open(data_path);
    return dict;
}

void ensure_directory(const std::string& path) {
    if (::mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        throw std::runtime_error("failed to create directory: " + path);
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    try {
        const std::string gendb_dir = argv[1];
        const std::string results_dir = argv[2];
        const std::string pre_dir = gendb_dir + "/pre";
        const std::string shared_dir = gendb_dir + "/shared";

        GENDB_PHASE("total");

        DictData stmt_dict;
        DictData rfile_dict;
        gendb::MmapColumn<uint16_t> stmt_col;
        gendb::MmapColumn<uint16_t> rfile_col;
        gendb::MmapColumn<uint32_t> adsh_col;
        gendb::MmapColumn<int32_t> line_col;
        size_t adsh_cardinality = 0;
        std::array<uint8_t, kStmtCardinality> stmt_is_valid{};

        {
            GENDB_PHASE("data_loading");
            stmt_dict = load_dict(pre_dir + "/dict_stmt.offsets.bin", pre_dir + "/dict_stmt.data.bin");
            rfile_dict = load_dict(pre_dir + "/dict_rfile.offsets.bin", pre_dir + "/dict_rfile.data.bin");
            gendb::MmapColumn<uint64_t> adsh_offsets(shared_dir + "/adsh.offsets.bin");
            adsh_cardinality = adsh_offsets.size() > 0 ? adsh_offsets.size() - 1 : 0;

            stmt_col.open(pre_dir + "/stmt.bin");
            rfile_col.open(pre_dir + "/rfile.bin");
            adsh_col.open(pre_dir + "/adsh.bin");
            line_col.open(pre_dir + "/line.bin");

            if (stmt_col.size() != rfile_col.size() || stmt_col.size() != adsh_col.size() ||
                stmt_col.size() != line_col.size()) {
                throw std::runtime_error("column size mismatch in pre table");
            }
            if (stmt_dict.size() != kStmtCardinality) {
                throw std::runtime_error("unexpected stmt dictionary cardinality");
            }
            if (rfile_dict.size() != kRfileCardinality) {
                throw std::runtime_error("unexpected rfile dictionary cardinality");
            }
            for (size_t i = 0; i < kStmtCardinality; ++i) {
                stmt_is_valid[i] = !stmt_dict.at(i).empty();
            }
        }

        {
            GENDB_PHASE("dim_filter");
        }

        {
            GENDB_PHASE("build_joins");
        }

        const size_t row_count = stmt_col.size();
        const size_t words_per_group = (adsh_cardinality + 63) / 64;
        const size_t morsel_count = (row_count + kMorselSize - 1) / kMorselSize;

        const int max_threads = std::max(1, omp_get_max_threads());
        std::vector<ThreadAgg> thread_aggs;
        thread_aggs.reserve(max_threads);
        for (int i = 0; i < max_threads; ++i) {
            thread_aggs.emplace_back(words_per_group);
        }

        const uint16_t* stmt = stmt_col.data;
        const uint16_t* rfile = rfile_col.data;
        const uint32_t* adsh = adsh_col.data;
        const int32_t* line = line_col.data;

        gendb::mmap_prefetch_all(stmt_col, rfile_col, adsh_col, line_col);

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel
            {
                ThreadAgg& local = thread_aggs[omp_get_thread_num()];
                uint64_t* local_bits = local.distinct_words.data();

                #pragma omp for schedule(dynamic, 1)
                for (size_t morsel = 0; morsel < morsel_count; ++morsel) {
                    const size_t begin = morsel * kMorselSize;
                    const size_t end = std::min(begin + kMorselSize, row_count);

                    for (size_t i = begin; i < end; ++i) {
                        const uint16_t stmt_id = stmt[i];
                        const uint16_t rfile_id = rfile[i];
                        if (stmt_id >= kStmtCardinality || rfile_id >= kRfileCardinality) {
                            continue;
                        }
                        if (!stmt_is_valid[stmt_id]) {
                            continue;
                        }

                        const uint32_t group_id = stmt_id * kRfileCardinality + rfile_id;
                        local.cnt[group_id] += 1;
                        local.sum_line[group_id] += static_cast<int64_t>(line[i]);

                        const uint32_t adsh_id = adsh[i];
                        const size_t word_index = adsh_id >> 6;
                        if (word_index < words_per_group) {
                            local_bits[group_id * words_per_group + word_index] |=
                                (uint64_t{1} << (adsh_id & 63));
                        }
                    }
                }
            }
        }

        std::array<uint64_t, kGroupCount> total_cnt{};
        std::array<int64_t, kGroupCount> total_sum_line{};
        std::vector<uint64_t> merged_bits(kGroupCount * words_per_group, 0);

        for (const ThreadAgg& local : thread_aggs) {
            for (size_t group_id = 0; group_id < kGroupCount; ++group_id) {
                total_cnt[group_id] += local.cnt[group_id];
                total_sum_line[group_id] += local.sum_line[group_id];
            }
            for (size_t idx = 0; idx < merged_bits.size(); ++idx) {
                merged_bits[idx] |= local.distinct_words[idx];
            }
        }

        std::vector<ResultRow> results;
        results.reserve(kGroupCount);
        for (uint16_t stmt_id = 0; stmt_id < kStmtCardinality; ++stmt_id) {
            for (uint16_t rfile_id = 0; rfile_id < kRfileCardinality; ++rfile_id) {
                const uint32_t group_id = stmt_id * kRfileCardinality + rfile_id;
                const uint64_t cnt = total_cnt[group_id];
                if (cnt == 0) {
                    continue;
                }

                uint64_t num_filings = 0;
                const size_t base = group_id * words_per_group;
                for (size_t w = 0; w < words_per_group; ++w) {
                    num_filings += static_cast<uint64_t>(__builtin_popcountll(merged_bits[base + w]));
                }

                results.push_back(ResultRow{
                    stmt_id,
                    rfile_id,
                    cnt,
                    num_filings,
                    static_cast<double>(total_sum_line[group_id]) / static_cast<double>(cnt),
                });
            }
        }

        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            return a.cnt > b.cnt;
        });

        {
            GENDB_PHASE("output");
            ensure_directory(results_dir);
            const std::string out_path = results_dir + "/Q1.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("failed to open output file: " + out_path);
            }

            std::fprintf(out, "stmt,rfile,cnt,num_filings,avg_line_num\n");
            for (const ResultRow& row : results) {
                const std::string_view stmt_sv = stmt_dict.at(row.stmt_id);
                const std::string_view rfile_sv = rfile_dict.at(row.rfile_id);
                std::fprintf(
                    out,
                    "%.*s,%.*s,%llu,%llu,%.2f\n",
                    static_cast<int>(stmt_sv.size()), stmt_sv.data(),
                    static_cast<int>(rfile_sv.size()), rfile_sv.data(),
                    static_cast<unsigned long long>(row.cnt),
                    static_cast<unsigned long long>(row.num_filings),
                    row.avg_line_num
                );
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
