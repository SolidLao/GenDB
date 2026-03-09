#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <utility>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr uint16_t kEmptySlot = std::numeric_limits<uint16_t>::max();
constexpr size_t kInitialMapCapacity = 32;
constexpr size_t kMorselSize = 100000;

struct DictData {
    std::vector<std::string> values;

    std::string_view at(size_t idx) const {
        return values[idx];
    }

    size_t size() const {
        return values.size();
    }
};

struct GroupState {
    uint16_t key = 0;
    uint64_t cnt = 0;
    int64_t line_sum = 0;
    std::vector<uint64_t> distinct_bits;
};

struct ThreadAgg {
    std::vector<uint16_t> slots;
    std::vector<uint16_t> slot_group_idx;
    std::vector<GroupState> groups;

    explicit ThreadAgg(size_t words_per_group) {
        slots.assign(kInitialMapCapacity, kEmptySlot);
        slot_group_idx.assign(kInitialMapCapacity, 0);
        groups.reserve(16);
        words_per_group_ = words_per_group;
    }

    size_t find_or_insert(uint16_t key) {
        if ((groups.size() + 1) * 10 >= slots.size() * 7) {
            rehash(slots.size() * 2);
        }
        size_t mask = slots.size() - 1;
        size_t pos = hash_key(key) & mask;
        while (true) {
            uint16_t cur = slots[pos];
            if (cur == kEmptySlot) {
                const uint16_t gidx = static_cast<uint16_t>(groups.size());
                slots[pos] = key;
                slot_group_idx[pos] = gidx;

                GroupState g;
                g.key = key;
                g.distinct_bits.assign(words_per_group_, 0);
                groups.push_back(std::move(g));
                return static_cast<size_t>(gidx);
            }
            if (cur == key) {
                return static_cast<size_t>(slot_group_idx[pos]);
            }
            pos = (pos + 1) & mask;
        }
    }

private:
    static inline uint64_t hash_key(uint16_t key) {
        uint64_t x = key;
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
    }

    void rehash(size_t new_cap) {
        size_t cap = 1;
        while (cap < new_cap) {
            cap <<= 1;
        }
        std::vector<uint16_t> new_slots(cap, kEmptySlot);
        std::vector<uint16_t> new_slot_group_idx(cap, 0);
        const size_t mask = cap - 1;
        for (uint16_t gidx = 0; gidx < groups.size(); ++gidx) {
            const uint16_t key = groups[gidx].key;
            size_t pos = hash_key(key) & mask;
            while (new_slots[pos] != kEmptySlot) {
                pos = (pos + 1) & mask;
            }
            new_slots[pos] = key;
            new_slot_group_idx[pos] = gidx;
        }
        slots.swap(new_slots);
        slot_group_idx.swap(new_slot_group_idx);
    }

    size_t words_per_group_ = 0;
};

struct FinalAgg {
    uint16_t key = 0;
    uint64_t cnt = 0;
    int64_t line_sum = 0;
    std::vector<uint64_t> distinct_bits;
};

struct ResultRow {
    uint8_t stmt;
    uint8_t rfile;
    uint64_t cnt;
    uint64_t num_filings;
    double avg_line_num;
};

DictData load_binary_dict(const std::string& path) {
    FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) {
        throw std::runtime_error("failed to open dict: " + path);
    }

    DictData dict;
    uint32_t n = 0;
    if (std::fread(&n, sizeof(uint32_t), 1, f) != 1) {
        std::fclose(f);
        throw std::runtime_error("failed to read dict header: " + path);
    }

    dict.values.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        if (std::fread(&len, sizeof(uint32_t), 1, f) != 1) {
            std::fclose(f);
            throw std::runtime_error("failed to read dict entry len: " + path);
        }
        std::string s;
        s.resize(len);
        if (len > 0 && std::fread(&s[0], 1, len, f) != len) {
            std::fclose(f);
            throw std::runtime_error("failed to read dict entry bytes: " + path);
        }
        dict.values.push_back(std::move(s));
    }

    std::fclose(f);
    return dict;
}

void ensure_directory(const std::string& path) {
    if (::mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        throw std::runtime_error("failed to create directory: " + path);
    }
}

inline uint64_t popcount_words(const std::vector<uint64_t>& bits) {
    uint64_t total = 0;
    for (uint64_t w : bits) {
        total += static_cast<uint64_t>(__builtin_popcountll(w));
    }
    return total;
}

inline uint16_t make_key(uint8_t stmt, uint8_t rfile) {
    return static_cast<uint16_t>((static_cast<uint16_t>(stmt) << 8) | static_cast<uint16_t>(rfile));
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
        const std::string dict_dir = gendb_dir + "/dicts";

        GENDB_PHASE("total");

        DictData stmt_dict;
        DictData rfile_dict;
        gendb::MmapColumn<uint8_t> stmt_col;
        gendb::MmapColumn<uint8_t> rfile_col;
        gendb::MmapColumn<uint32_t> adsh_col;
        gendb::MmapColumn<int16_t> line_col;
        std::array<uint8_t, 256> stmt_not_null{};
        size_t adsh_cardinality = 0;

        {
            GENDB_PHASE("data_loading");
            stmt_dict = load_binary_dict(dict_dir + "/stmt.dict");
            rfile_dict = load_binary_dict(dict_dir + "/rfile.dict");

            stmt_col.open(pre_dir + "/stmt.bin");
            rfile_col.open(pre_dir + "/rfile.bin");
            adsh_col.open(pre_dir + "/adsh.bin");
            line_col.open(pre_dir + "/line.bin");

            const size_t rows = stmt_col.size();
            if (rfile_col.size() != rows || adsh_col.size() != rows || line_col.size() != rows) {
                throw std::runtime_error("pre column size mismatch");
            }

            for (size_t i = 0; i < stmt_dict.size() && i < stmt_not_null.size(); ++i) {
                stmt_not_null[i] = stmt_dict.at(i).empty() ? 0 : 1;
            }

            gendb::MmapColumn<uint32_t> adsh_dict_scan(dict_dir + "/adsh.dict");
            if (adsh_dict_scan.size() == 0) {
                throw std::runtime_error("invalid adsh.dict");
            }
            adsh_cardinality = static_cast<size_t>(adsh_dict_scan[0]);
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

        int thread_count = omp_get_max_threads();
        if (thread_count < 1) {
            thread_count = 1;
        }
        if (thread_count > 64) {
            thread_count = 64;
        }

        std::vector<ThreadAgg> partials;
        partials.reserve(static_cast<size_t>(thread_count));
        for (int t = 0; t < thread_count; ++t) {
            partials.emplace_back(words_per_group);
        }

        const uint8_t* stmt = stmt_col.data;
        const uint8_t* rfile = rfile_col.data;
        const uint32_t* adsh = adsh_col.data;
        const int16_t* line = line_col.data;

        gendb::mmap_prefetch_all(stmt_col, rfile_col, adsh_col, line_col);

        {
            GENDB_PHASE("main_scan");

            #pragma omp parallel num_threads(thread_count)
            {
                ThreadAgg& local = partials[static_cast<size_t>(omp_get_thread_num())];

                #pragma omp for schedule(dynamic, 1)
                for (size_t morsel = 0; morsel < morsel_count; ++morsel) {
                    const size_t begin = morsel * kMorselSize;
                    const size_t end = std::min(begin + kMorselSize, row_count);

                    for (size_t i = begin; i < end; ++i) {
                        const uint8_t stmt_code = stmt[i];
                        if (!stmt_not_null[stmt_code]) {
                            continue;
                        }

                        const uint16_t key = make_key(stmt_code, rfile[i]);
                        const size_t gidx = local.find_or_insert(key);
                        GroupState& g = local.groups[gidx];
                        g.cnt += 1;
                        g.line_sum += static_cast<int64_t>(line[i]);

                        const uint32_t adsh_code = adsh[i];
                        const size_t word_idx = adsh_code >> 6;
                        if (word_idx < words_per_group) {
                            g.distinct_bits[word_idx] |= (uint64_t{1} << (adsh_code & 63));
                        }
                    }
                }
            }
        }

        std::vector<FinalAgg> merged;
        merged.reserve(32);
        std::array<uint16_t, 65536> key_to_merged{};
        key_to_merged.fill(kEmptySlot);

        for (const ThreadAgg& part : partials) {
            for (const GroupState& g : part.groups) {
                uint16_t mg = key_to_merged[g.key];
                if (mg == kEmptySlot) {
                    mg = static_cast<uint16_t>(merged.size());
                    key_to_merged[g.key] = mg;
                    FinalAgg f;
                    f.key = g.key;
                    f.cnt = g.cnt;
                    f.line_sum = g.line_sum;
                    f.distinct_bits = g.distinct_bits;
                    merged.push_back(std::move(f));
                } else {
                    FinalAgg& f = merged[mg];
                    f.cnt += g.cnt;
                    f.line_sum += g.line_sum;
                    for (size_t w = 0; w < words_per_group; ++w) {
                        f.distinct_bits[w] |= g.distinct_bits[w];
                    }
                }
            }
        }

        std::vector<ResultRow> rows;
        rows.reserve(merged.size());
        for (const FinalAgg& g : merged) {
            if (g.cnt == 0) {
                continue;
            }
            const uint8_t stmt_code = static_cast<uint8_t>(g.key >> 8);
            const uint8_t rfile_code = static_cast<uint8_t>(g.key & 0xFFu);
            rows.push_back(ResultRow{
                stmt_code,
                rfile_code,
                g.cnt,
                popcount_words(g.distinct_bits),
                static_cast<double>(g.line_sum) / static_cast<double>(g.cnt),
            });
        }

        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.cnt != b.cnt) {
                return a.cnt > b.cnt;
            }
            if (a.stmt != b.stmt) {
                return a.stmt < b.stmt;
            }
            return a.rfile < b.rfile;
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
            for (const ResultRow& r : rows) {
                const std::string_view stmt_sv = stmt_dict.at(r.stmt);
                const std::string_view rfile_sv = rfile_dict.at(r.rfile);
                std::fprintf(
                    out,
                    "%.*s,%.*s,%llu,%llu,%.2f\n",
                    static_cast<int>(stmt_sv.size()), stmt_sv.data(),
                    static_cast<int>(rfile_sv.size()), rfile_sv.data(),
                    static_cast<unsigned long long>(r.cnt),
                    static_cast<unsigned long long>(r.num_filings),
                    r.avg_line_num
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
