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

struct ComboKeyMap {
    std::vector<uint32_t> rf_code;
    std::vector<uint32_t> ls_code;
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

ComboKeyMap load_combo_keymap(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open combo keymap: " + path);
    }

    uint32_t k = 0;
    in.read(reinterpret_cast<char*>(&k), sizeof(k));
    if (!in) {
        throw std::runtime_error("Failed to read combo keymap header: " + path);
    }

    ComboKeyMap m;
    m.rf_code.resize(k);
    m.ls_code.resize(k);
    for (uint32_t i = 0; i < k; ++i) {
        in.read(reinterpret_cast<char*>(&m.rf_code[i]), sizeof(uint32_t));
        in.read(reinterpret_cast<char*>(&m.ls_code[i]), sizeof(uint32_t));
        if (!in) {
            throw std::runtime_error("Failed to read combo keymap payload: " + path);
        }
    }
    return m;
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

        gendb::MmapColumn<uint8_t> col_combo;
        gendb::MmapColumn<uint16_t> col_quantity_scaled;
        gendb::MmapColumn<uint32_t> col_extendedprice_scaled;
        gendb::MmapColumn<uint16_t> col_discount_scaled;
        gendb::MmapColumn<uint16_t> col_tax_scaled;
        gendb::MmapColumn<int32_t> col_shipdate;

        ComboKeyMap combo_keymap;
        ZoneMap zonemap;
        std::vector<uint8_t> scan_block;

        {
            GENDB_PHASE("data_loading");
            const std::string li_dir = gendb_dir + "/lineitem/";

            returnflag_dict = load_dict_binary(li_dir + "l_returnflag.dict");
            linestatus_dict = load_dict_binary(li_dir + "l_linestatus.dict");
            combo_keymap =
                load_combo_keymap(gendb_dir + "/column_versions/lineitem.rf_ls.combo_u8/keymap.bin");

            col_combo.open(gendb_dir + "/column_versions/lineitem.rf_ls.combo_u8/codes.bin");
            col_quantity_scaled.open(gendb_dir + "/column_versions/lineitem.l_quantity.int16_scaled_by_100/codes.bin");
            col_extendedprice_scaled.open(gendb_dir + "/column_versions/lineitem.l_extendedprice.int32_scaled_by_100/codes.bin");
            col_discount_scaled.open(gendb_dir + "/column_versions/lineitem.l_discount.int16_scaled_by_100/codes.bin");
            col_tax_scaled.open(gendb_dir + "/column_versions/lineitem.l_tax.int16_scaled_by_100/codes.bin");
            col_shipdate.open(li_dir + "l_shipdate.bin");

            zonemap = load_shipdate_zonemap(li_dir + "lineitem_shipdate_zonemap.idx");
        }

        const size_t n = col_shipdate.size();
        if (col_combo.size() != n || col_quantity_scaled.size() != n ||
            col_extendedprice_scaled.size() != n || col_discount_scaled.size() != n || col_tax_scaled.size() != n) {
            throw std::runtime_error("Column size mismatch in lineitem");
        }
        if (zonemap.n != n) {
            throw std::runtime_error("Zonemap n does not match lineitem row count");
        }
        if (zonemap.block_size == 0) {
            throw std::runtime_error("Invalid zonemap block_size=0");
        }

        const size_t groups = combo_keymap.rf_code.size();
        if (groups == 0) {
            throw std::runtime_error("Combo keymap is empty");
        }
        if (groups > 256) {
            throw std::runtime_error("Combo keymap cardinality exceeds uint8 domain");
        }
        for (size_t i = 0; i < groups; ++i) {
            if (combo_keymap.rf_code[i] >= returnflag_dict.size() ||
                combo_keymap.ls_code[i] >= linestatus_dict.size()) {
                throw std::runtime_error("Combo keymap references out-of-range dictionary code");
            }
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

        const int nthreads = std::max(1, omp_get_max_threads());
        std::vector<std::vector<Agg>> thread_aggs(static_cast<size_t>(nthreads), std::vector<Agg>(groups));

        {
            GENDB_PHASE("main_scan");

#pragma omp parallel num_threads(nthreads)
            {
                const int tid = omp_get_thread_num();
                std::vector<Agg>& local = thread_aggs[static_cast<size_t>(tid)];

#pragma omp for schedule(static)
                for (int64_t b = 0; b < static_cast<int64_t>(zonemap.blocks); ++b) {
                    if (!scan_block[static_cast<size_t>(b)]) {
                        continue;
                    }

                    const size_t start = static_cast<size_t>(b) * static_cast<size_t>(zonemap.block_size);
                    const size_t end = std::min(n, start + static_cast<size_t>(zonemap.block_size));

                    const int32_t* ship = col_shipdate.data + start;
                    const uint8_t* combo = col_combo.data + start;
                    const uint16_t* qty_100 = col_quantity_scaled.data + start;
                    const uint32_t* price_100 = col_extendedprice_scaled.data + start;
                    const uint16_t* disc_100 = col_discount_scaled.data + start;
                    const uint16_t* tax_100 = col_tax_scaled.data + start;
                    const bool whole_block_passes = zonemap.maxs[static_cast<size_t>(b)] <= kShipdateCutoff;

                    if (whole_block_passes) {
                        for (size_t i = 0, len = end - start; i < len; ++i) {
                            const size_t key = static_cast<size_t>(combo[i]);
                            if (key >= groups) {
                                throw std::runtime_error("Encountered combo code outside keymap domain");
                            }
                            Agg& a = local[key];
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
                    } else {
                        for (size_t i = 0, len = end - start; i < len; ++i) {
                            if (ship[i] > kShipdateCutoff) {
                                continue;
                            }
                            const size_t key = static_cast<size_t>(combo[i]);
                            if (key >= groups) {
                                throw std::runtime_error("Encountered combo code outside keymap domain");
                            }
                            Agg& a = local[key];
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

            for (size_t combo = 0; combo < groups; ++combo) {
                const Agg& a = global[combo];
                if (a.count_order == 0) {
                    continue;
                }
                const uint32_t rf_code = combo_keymap.rf_code[combo];
                const uint32_t ls_code = combo_keymap.ls_code[combo];
                const double cnt = static_cast<double>(a.count_order);
                OutputRow row;
                row.rf_code = rf_code;
                row.ls_code = ls_code;
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
