#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int32_t kShipdateCutoff = 10471;

struct Agg {
    int64_t sum_qty_100 = 0;
    int64_t sum_base_price_100 = 0;
    int64_t sum_disc_price_1e4 = 0;
    int64_t sum_charge_1e6 = 0;
    int64_t sum_disc_100 = 0;
    int64_t count_order = 0;
};

struct OutputRow {
    uint32_t rf_code = 0;
    uint32_t ls_code = 0;
    std::string rf;
    std::string ls;
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double avg_qty = 0.0;
    double avg_price = 0.0;
    double avg_disc = 0.0;
    int64_t count_order = 0;
};

struct RawMmap {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    RawMmap() = default;

    explicit RawMmap(const std::string& path) { open(path); }

    void open(const std::string& path) {
        close();
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Failed to open file: " + path);
        }

        struct stat st;
        if (::fstat(fd, &st) < 0) {
            ::close(fd);
            fd = -1;
            throw std::runtime_error("Failed to stat file: " + path);
        }

        size = static_cast<size_t>(st.st_size);
        if (size == 0) {
            data = nullptr;
            return;
        }

        data = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            ::close(fd);
            fd = -1;
            data = nullptr;
            throw std::runtime_error("Failed to mmap file: " + path);
        }

        if (size > (1u << 20)) {
            ::madvise(data, size, MADV_SEQUENTIAL);
        }
    }

    void close() {
        if (data && size > 0) {
            ::munmap(data, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
        data = nullptr;
        size = 0;
        fd = -1;
    }

    ~RawMmap() { close(); }

    RawMmap(const RawMmap&) = delete;
    RawMmap& operator=(const RawMmap&) = delete;

    RawMmap(RawMmap&& o) noexcept : data(o.data), size(o.size), fd(o.fd) {
        o.data = nullptr;
        o.size = 0;
        o.fd = -1;
    }

    RawMmap& operator=(RawMmap&& o) noexcept {
        if (this != &o) {
            close();
            data = o.data;
            size = o.size;
            fd = o.fd;
            o.data = nullptr;
            o.size = 0;
            o.fd = -1;
        }
        return *this;
    }
};

std::vector<std::string> load_dict_binary(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open dictionary: " + path);
    }

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) {
        throw std::runtime_error("Failed to read dictionary header: " + path);
    }

    std::vector<std::string> dict;
    dict.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) {
            throw std::runtime_error("Failed to read dictionary length: " + path);
        }

        std::string s;
        s.resize(len);
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) {
                throw std::runtime_error("Failed to read dictionary payload: " + path);
            }
        }
        dict.push_back(std::move(s));
    }

    return dict;
}

inline size_t group_index(uint32_t rf_code, uint32_t ls_code, size_t ls_cardinality) {
    return static_cast<size_t>(rf_code) * ls_cardinality + static_cast<size_t>(ls_code);
}

}  // namespace

int main(int argc, char** argv) {
    GENDB_PHASE("total");

    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        std::filesystem::create_directories(results_dir);

        std::vector<std::string> returnflag_dict;
        std::vector<std::string> linestatus_dict;

        gendb::MmapColumn<uint32_t> col_returnflag;
        gendb::MmapColumn<uint32_t> col_linestatus;
        gendb::MmapColumn<uint16_t> col_quantity_scaled;
        gendb::MmapColumn<uint32_t> col_extendedprice_scaled;
        gendb::MmapColumn<uint16_t> col_discount_scaled;
        gendb::MmapColumn<uint16_t> col_tax_scaled;

        RawMmap shipdate_postings_offsets;
        gendb::MmapColumn<uint32_t> shipdate_postings_row_ids;

        {
            GENDB_PHASE("data_loading");
            const std::string li_dir = gendb_dir + "/lineitem/";
            const std::string cv_dir = gendb_dir + "/column_versions/";

            returnflag_dict = load_dict_binary(li_dir + "l_returnflag.dict");
            linestatus_dict = load_dict_binary(li_dir + "l_linestatus.dict");

            col_returnflag.open(li_dir + "l_returnflag.bin");
            col_linestatus.open(li_dir + "l_linestatus.bin");
            col_quantity_scaled.open(cv_dir + "lineitem.l_quantity.int16_scaled_by_100/codes.bin");
            col_extendedprice_scaled.open(cv_dir + "lineitem.l_extendedprice.int32_scaled_by_100/codes.bin");
            col_discount_scaled.open(cv_dir + "lineitem.l_discount.int16_scaled_by_100/codes.bin");
            col_tax_scaled.open(cv_dir + "lineitem.l_tax.int16_scaled_by_100/codes.bin");

            shipdate_postings_row_ids.open(cv_dir + "lineitem.l_shipdate.postings_by_day/row_ids.bin");
            shipdate_postings_offsets.open(cv_dir + "lineitem.l_shipdate.postings_by_day/offsets.bin");
        }

        const size_t n = col_returnflag.size();
        if (col_linestatus.size() != n || col_quantity_scaled.size() != n ||
            col_extendedprice_scaled.size() != n || col_discount_scaled.size() != n ||
            col_tax_scaled.size() != n || shipdate_postings_row_ids.size() != n) {
            throw std::runtime_error("Column size mismatch in lineitem files");
        }

        if (shipdate_postings_offsets.size < (2 * sizeof(int32_t) + sizeof(uint64_t))) {
            throw std::runtime_error("Invalid offsets.bin size for shipdate postings");
        }

        const uint8_t* offsets_bytes = static_cast<const uint8_t*>(shipdate_postings_offsets.data);
        int32_t min_day = 0;
        int32_t max_day = 0;
        std::memcpy(&min_day, offsets_bytes, sizeof(int32_t));
        std::memcpy(&max_day, offsets_bytes + sizeof(int32_t), sizeof(int32_t));

        if (max_day < min_day) {
            throw std::runtime_error("Invalid min_day/max_day in shipdate postings offsets");
        }

        const size_t offsets_count = (shipdate_postings_offsets.size - 2 * sizeof(int32_t)) / sizeof(uint64_t);
        const size_t expected_min_offsets = static_cast<size_t>(static_cast<int64_t>(max_day) - static_cast<int64_t>(min_day)) + 2;
        if (offsets_count < expected_min_offsets - 1) {
            throw std::runtime_error("offsets.bin has too few offsets for day domain");
        }

        const uint64_t* offsets = reinterpret_cast<const uint64_t*>(offsets_bytes + 2 * sizeof(int32_t));
        if (offsets[0] != 0 || offsets[offsets_count - 1] != n) {
            throw std::runtime_error("Invalid postings offsets boundaries");
        }

        std::vector<uint64_t> reject_mask_words;
        {
            GENDB_PHASE("dim_filter");
            reject_mask_words.assign((n + 63) >> 6, 0ull);

            if (max_day > kShipdateCutoff) {
                const int64_t start_day = std::max<int64_t>(static_cast<int64_t>(kShipdateCutoff) + 1, static_cast<int64_t>(min_day));
                const int64_t end_day = static_cast<int64_t>(max_day);

                for (int64_t day = start_day; day <= end_day; ++day) {
                    const size_t idx = static_cast<size_t>(day - static_cast<int64_t>(min_day));
                    if (idx + 1 >= offsets_count) {
                        throw std::runtime_error("Day index out of range in postings offsets");
                    }
                    const uint64_t begin = offsets[idx];
                    const uint64_t end = offsets[idx + 1];
                    if (end < begin || end > n) {
                        throw std::runtime_error("Invalid postings range in row_ids");
                    }

                    const uint32_t* rows = shipdate_postings_row_ids.data + begin;
                    for (uint64_t p = begin; p < end; ++p) {
                        const uint32_t row = rows[p - begin];
                        reject_mask_words[row >> 6] |= (1ull << (row & 63));
                    }
                }
            }
        }

        const size_t rf_card = returnflag_dict.size();
        const size_t ls_card = linestatus_dict.size();
        const size_t groups = rf_card * ls_card;
        if (groups == 0) {
            throw std::runtime_error("Empty dictionary cardinality for grouping keys");
        }

        const unsigned hw_threads = std::thread::hardware_concurrency();
        const int nthreads = static_cast<int>(std::max<unsigned>(1u, std::min<unsigned>(hw_threads == 0 ? 1u : hw_threads, 32u)));

        std::vector<std::vector<Agg>> thread_aggs(static_cast<size_t>(nthreads), std::vector<Agg>(groups));

        {
            GENDB_PHASE("main_scan");

            const uint32_t* rf = col_returnflag.data;
            const uint32_t* ls = col_linestatus.data;
            const uint16_t* qty_100 = col_quantity_scaled.data;
            const uint32_t* price_100 = col_extendedprice_scaled.data;
            const uint16_t* disc_100 = col_discount_scaled.data;
            const uint16_t* tax_100 = col_tax_scaled.data;
            const uint64_t* reject_words = reject_mask_words.data();

#pragma omp parallel num_threads(nthreads)
            {
                std::vector<Agg>& local = thread_aggs[static_cast<size_t>(omp_get_thread_num())];

#pragma omp for schedule(static)
                for (int64_t i = 0; i < static_cast<int64_t>(n); ++i) {
                    const uint64_t bit = 1ull << (static_cast<uint64_t>(i) & 63ull);
                    if (reject_words[static_cast<size_t>(i) >> 6] & bit) {
                        continue;
                    }

                    const uint32_t rf_code = rf[i];
                    const uint32_t ls_code = ls[i];
                    if (rf_code >= rf_card || ls_code >= ls_card) {
                        continue;
                    }

                    Agg& a = local[group_index(rf_code, ls_code, ls_card)];
                    const int64_t q100 = static_cast<int64_t>(qty_100[i]);
                    const int64_t p100 = static_cast<int64_t>(price_100[i]);
                    const int64_t d100 = static_cast<int64_t>(disc_100[i]);
                    const int64_t t100 = static_cast<int64_t>(tax_100[i]);

                    const int64_t one_minus_disc_100 = 100 - d100;
                    const int64_t one_plus_tax_100 = 100 + t100;

                    a.sum_qty_100 += q100;
                    a.sum_base_price_100 += p100;
                    a.sum_disc_price_1e4 += p100 * one_minus_disc_100;
                    a.sum_charge_1e6 += p100 * one_minus_disc_100 * one_plus_tax_100;
                    a.sum_disc_100 += d100;
                    a.count_order += 1;
                }
            }
        }

        std::vector<Agg> global(groups);
        {
            GENDB_PHASE("build_joins");
            for (int t = 0; t < nthreads; ++t) {
                const std::vector<Agg>& local = thread_aggs[static_cast<size_t>(t)];
                for (size_t g = 0; g < groups; ++g) {
                    global[g].sum_qty_100 += local[g].sum_qty_100;
                    global[g].sum_base_price_100 += local[g].sum_base_price_100;
                    global[g].sum_disc_price_1e4 += local[g].sum_disc_price_1e4;
                    global[g].sum_charge_1e6 += local[g].sum_charge_1e6;
                    global[g].sum_disc_100 += local[g].sum_disc_100;
                    global[g].count_order += local[g].count_order;
                }
            }
        }

        {
            GENDB_PHASE("output");

            std::vector<OutputRow> rows;
            rows.reserve(groups);

            for (size_t rf_code = 0; rf_code < rf_card; ++rf_code) {
                for (size_t ls_code = 0; ls_code < ls_card; ++ls_code) {
                    const Agg& a = global[group_index(static_cast<uint32_t>(rf_code), static_cast<uint32_t>(ls_code), ls_card)];
                    if (a.count_order == 0) {
                        continue;
                    }

                    const double cnt = static_cast<double>(a.count_order);
                    OutputRow row;
                    row.rf_code = static_cast<uint32_t>(rf_code);
                    row.ls_code = static_cast<uint32_t>(ls_code);
                    row.rf = returnflag_dict[rf_code];
                    row.ls = linestatus_dict[ls_code];
                    row.sum_qty = static_cast<double>(a.sum_qty_100) / 100.0;
                    row.sum_base_price = static_cast<double>(a.sum_base_price_100) / 100.0;
                    row.sum_disc_price = static_cast<double>(a.sum_disc_price_1e4) / 10000.0;
                    row.sum_charge = static_cast<double>(a.sum_charge_1e6) / 1000000.0;
                    row.avg_qty = static_cast<double>(a.sum_qty_100) / (cnt * 100.0);
                    row.avg_price = static_cast<double>(a.sum_base_price_100) / (cnt * 100.0);
                    row.avg_disc = static_cast<double>(a.sum_disc_100) / (cnt * 100.0);
                    row.count_order = a.count_order;
                    rows.push_back(std::move(row));
                }
            }

            std::sort(rows.begin(), rows.end(), [](const OutputRow& a, const OutputRow& b) {
                if (a.rf != b.rf) {
                    return a.rf < b.rf;
                }
                return a.ls < b.ls;
            });

            if (rows.size() > 10) {
                rows.resize(10);
            }

            const std::string out_path = results_dir + "/Q1.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("Failed to open output file: " + out_path);
            }

            std::fprintf(out,
                         "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                         "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

            for (const auto& r : rows) {
                std::fprintf(out,
                             "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%lld\n",
                             r.rf.c_str(),
                             r.ls.c_str(),
                             r.sum_qty,
                             r.sum_base_price,
                             r.sum_disc_price,
                             r.sum_charge,
                             r.avg_qty,
                             r.avg_price,
                             r.avg_disc,
                             static_cast<long long>(r.count_order));
            }
            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
