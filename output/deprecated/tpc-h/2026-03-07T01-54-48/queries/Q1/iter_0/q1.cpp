#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr int32_t kShipdateCutoff = 10471;  // 1998-09-02

struct Agg {
    int64_t sum_qty_100 = 0;         // quantity * 100
    int64_t sum_base_price_100 = 0;  // price * 100
    int64_t sum_disc_price_1e4 = 0;  // sum(price_100 * (100 - disc_100))
    int64_t sum_charge_1e6 = 0;      // sum(price_100 * (100 - disc_100) * (100 + tax_100))
    int64_t sum_disc_100 = 0;        // discount * 100
    int64_t count_order = 0;
};

struct ZoneMap {
    uint32_t block_size = 0;
    uint64_t n = 0;
    uint64_t blocks = 0;
    std::vector<int32_t> mins;
    std::vector<int32_t> maxs;
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

ZoneMap load_shipdate_zonemap(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open zonemap: " + path);
    }

    ZoneMap zm;
    in.read(reinterpret_cast<char*>(&zm.block_size), sizeof(zm.block_size));
    in.read(reinterpret_cast<char*>(&zm.n), sizeof(zm.n));
    in.read(reinterpret_cast<char*>(&zm.blocks), sizeof(zm.blocks));
    if (!in) {
        throw std::runtime_error("Failed to read zonemap header: " + path);
    }

    zm.mins.resize(static_cast<size_t>(zm.blocks));
    zm.maxs.resize(static_cast<size_t>(zm.blocks));

    if (zm.blocks > 0) {
        in.read(reinterpret_cast<char*>(zm.mins.data()), static_cast<std::streamsize>(zm.blocks * sizeof(int32_t)));
        in.read(reinterpret_cast<char*>(zm.maxs.data()), static_cast<std::streamsize>(zm.blocks * sizeof(int32_t)));
        if (!in) {
            throw std::runtime_error("Failed to read zonemap arrays: " + path);
        }
    }

    return zm;
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
        gendb::MmapColumn<double> col_quantity;
        gendb::MmapColumn<double> col_extendedprice;
        gendb::MmapColumn<double> col_discount;
        gendb::MmapColumn<double> col_tax;
        gendb::MmapColumn<int32_t> col_shipdate;

        ZoneMap zonemap;
        std::vector<uint8_t> scan_block;

        {
            GENDB_PHASE("data_loading");
            const std::string li_dir = gendb_dir + "/lineitem/";

            returnflag_dict = load_dict_binary(li_dir + "l_returnflag.dict");
            linestatus_dict = load_dict_binary(li_dir + "l_linestatus.dict");

            col_returnflag.open(li_dir + "l_returnflag.bin");
            col_linestatus.open(li_dir + "l_linestatus.bin");
            col_quantity.open(li_dir + "l_quantity.bin");
            col_extendedprice.open(li_dir + "l_extendedprice.bin");
            col_discount.open(li_dir + "l_discount.bin");
            col_tax.open(li_dir + "l_tax.bin");
            col_shipdate.open(li_dir + "l_shipdate.bin");

            zonemap = load_shipdate_zonemap(li_dir + "lineitem_shipdate_zonemap.idx");
        }

        const size_t n = col_shipdate.size();
        if (col_returnflag.size() != n || col_linestatus.size() != n || col_quantity.size() != n ||
            col_extendedprice.size() != n || col_discount.size() != n || col_tax.size() != n) {
            throw std::runtime_error("Column size mismatch in lineitem");
        }
        if (zonemap.n != n) {
            throw std::runtime_error("Zonemap n does not match lineitem row count");
        }
        if (zonemap.block_size == 0) {
            throw std::runtime_error("Invalid zonemap block_size=0");
        }

        {
            GENDB_PHASE("dim_filter");
            scan_block.assign(static_cast<size_t>(zonemap.blocks), 0);
            for (size_t b = 0; b < static_cast<size_t>(zonemap.blocks); ++b) {
                if (zonemap.mins[b] <= kShipdateCutoff) {
                    scan_block[b] = 1;
                }
            }
        }

        const size_t rf_card = returnflag_dict.size();
        const size_t ls_card = linestatus_dict.size();
        const size_t groups = rf_card * ls_card;
        if (groups == 0) {
            throw std::runtime_error("Empty dictionary cardinality for grouping keys");
        }

        const int max_threads = omp_get_max_threads();
        const int nthreads = std::min(64, std::max(1, max_threads));
        std::vector<std::vector<Agg>> thread_aggs(static_cast<size_t>(nthreads), std::vector<Agg>(groups));

        {
            GENDB_PHASE("main_scan");

#pragma omp parallel num_threads(nthreads)
            {
                const int tid = omp_get_thread_num();
                std::vector<Agg>& local = thread_aggs[static_cast<size_t>(tid)];

#pragma omp for schedule(dynamic, 1)
                for (int64_t b = 0; b < static_cast<int64_t>(zonemap.blocks); ++b) {
                    if (!scan_block[static_cast<size_t>(b)]) {
                        continue;
                    }

                    const size_t start = static_cast<size_t>(b) * static_cast<size_t>(zonemap.block_size);
                    const size_t end = std::min(n, start + static_cast<size_t>(zonemap.block_size));

                    const int32_t* ship = col_shipdate.data + start;
                    const uint32_t* rf = col_returnflag.data + start;
                    const uint32_t* ls = col_linestatus.data + start;
                    const double* qty = col_quantity.data + start;
                    const double* base = col_extendedprice.data + start;
                    const double* disc = col_discount.data + start;
                    const double* tax = col_tax.data + start;

                    for (size_t i = 0, len = end - start; i < len; ++i) {
                        if (ship[i] > kShipdateCutoff) {
                            continue;
                        }

                        const uint32_t rf_code = rf[i];
                        const uint32_t ls_code = ls[i];
                        if (rf_code >= rf_card || ls_code >= ls_card) {
                            continue;
                        }

                        Agg& a = local[group_index(rf_code, ls_code, ls_card)];
                        const int64_t qty_100 = static_cast<int64_t>(qty[i] * 100.0 + 0.5);
                        const int64_t price_100 = static_cast<int64_t>(base[i] * 100.0 + 0.5);
                        const int64_t disc_100 = static_cast<int64_t>(disc[i] * 100.0 + 0.5);
                        const int64_t tax_100 = static_cast<int64_t>(tax[i] * 100.0 + 0.5);
                        const int64_t one_minus_disc_100 = 100 - disc_100;
                        const int64_t one_plus_tax_100 = 100 + tax_100;

                        a.sum_qty_100 += qty_100;
                        a.sum_base_price_100 += price_100;
                        a.sum_disc_price_1e4 += price_100 * one_minus_disc_100;
                        a.sum_charge_1e6 += price_100 * one_minus_disc_100 * one_plus_tax_100;
                        a.sum_disc_100 += disc_100;
                        a.count_order += 1;
                    }
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

            for (size_t rf = 0; rf < rf_card; ++rf) {
                for (size_t ls = 0; ls < ls_card; ++ls) {
                    const Agg& a = global[group_index(static_cast<uint32_t>(rf), static_cast<uint32_t>(ls), ls_card)];
                    if (a.count_order == 0) {
                        continue;
                    }
                    const double cnt = static_cast<double>(a.count_order);
                    OutputRow row;
                    row.rf_code = static_cast<uint32_t>(rf);
                    row.ls_code = static_cast<uint32_t>(ls);
                    row.rf = returnflag_dict[rf];
                    row.ls = linestatus_dict[ls];
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
                if (a.rf != b.rf) return a.rf < b.rf;
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
                             r.rf.c_str(), r.ls.c_str(),
                             r.sum_qty, r.sum_base_price, r.sum_disc_price, r.sum_charge,
                             r.avg_qty, r.avg_price, r.avg_disc,
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
