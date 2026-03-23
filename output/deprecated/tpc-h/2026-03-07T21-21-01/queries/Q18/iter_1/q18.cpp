#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr double kQtyThreshold = 300.0;
constexpr size_t kTopK = 100;
constexpr uint64_t kDenseEmpty = std::numeric_limits<uint64_t>::max();

struct OutputRow {
    uint64_t customer_row;
    int32_t custkey;
    int32_t orderkey;
    int32_t orderdate;
    double totalprice;
    double sum_qty;
};

inline bool output_row_less(const OutputRow& a, const OutputRow& b) {
    if (a.totalprice != b.totalprice) {
        return a.totalprice > b.totalprice;
    }
    if (a.orderdate != b.orderdate) {
        return a.orderdate < b.orderdate;
    }
    if (a.orderkey != b.orderkey) {
        return a.orderkey < b.orderkey;
    }
    return a.custkey < b.custkey;
}

inline std::string_view string_at(
    const uint64_t* offsets,
    const char* data,
    uint64_t row_id
) {
    const uint64_t begin = offsets[row_id];
    const uint64_t end = offsets[row_id + 1];
    return std::string_view(data + begin, static_cast<size_t>(end - begin));
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    gendb::init_date_tables();

    try {
        GENDB_PHASE("total");

        gendb::MmapColumn<int32_t> l_orderkey;
        gendb::MmapColumn<double> l_quantity;
        gendb::MmapColumn<int32_t> o_custkey;
        gendb::MmapColumn<int32_t> o_orderdate;
        gendb::MmapColumn<double> o_totalprice;
        gendb::MmapColumn<int32_t> c_custkey;
        gendb::MmapColumn<uint64_t> c_name_offsets;
        gendb::MmapColumn<char> c_name_data;
        gendb::MmapColumn<uint64_t> lineitem_posting_offsets;
        gendb::MmapColumn<uint64_t> lineitem_posting_rowids;
        gendb::MmapColumn<uint64_t> orders_pk_dense;
        gendb::MmapColumn<uint64_t> customer_pk_dense;

        const int max_threads = std::min(64, omp_get_max_threads());

        {
            GENDB_PHASE("data_loading");
            std::filesystem::create_directories(results_dir);

            l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
            l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
            o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
            o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
            o_totalprice.open(gendb_dir + "/orders/o_totalprice.bin");
            c_custkey.open(gendb_dir + "/customer/c_custkey.bin");
            c_name_offsets.open(gendb_dir + "/customer/c_name.offsets.bin");
            c_name_data.open(gendb_dir + "/customer/c_name.data.bin");
            lineitem_posting_offsets.open(
                gendb_dir + "/lineitem/indexes/lineitem_order_postings.offsets.bin"
            );
            lineitem_posting_rowids.open(
                gendb_dir + "/lineitem/indexes/lineitem_order_postings.rowids.bin"
            );
            orders_pk_dense.open(gendb_dir + "/orders/indexes/orders_pk_dense.bin");
            customer_pk_dense.open(gendb_dir + "/customer/indexes/customer_pk_dense.bin");

            o_custkey.advise_random();
            o_orderdate.advise_random();
            o_totalprice.advise_random();
            c_custkey.advise_random();
            c_name_offsets.advise_random();
            c_name_data.advise_random();
            lineitem_posting_offsets.advise_random();
            lineitem_posting_rowids.advise_random();
            orders_pk_dense.advise_random();
            customer_pk_dense.advise_random();

            gendb::mmap_prefetch_all(
                l_orderkey,
                l_quantity,
                o_custkey,
                o_orderdate,
                o_totalprice,
                c_custkey,
                c_name_offsets,
                c_name_data,
                lineitem_posting_offsets,
                lineitem_posting_rowids,
                orders_pk_dense,
                customer_pk_dense
            );
        }

        const size_t lineitem_rows = l_orderkey.size();
        const size_t order_domain = orders_pk_dense.size();
        const size_t customer_domain = customer_pk_dense.size();

        std::vector<int32_t> qualifying_orderkeys;

        {
            GENDB_PHASE("dim_filter");

            std::vector<size_t> boundaries(static_cast<size_t>(max_threads) + 1, lineitem_rows);
            boundaries[0] = 0;
            boundaries[static_cast<size_t>(max_threads)] = lineitem_rows;
            for (int t = 1; t < max_threads; ++t) {
                size_t boundary = (lineitem_rows * static_cast<size_t>(t)) / static_cast<size_t>(max_threads);
                while (boundary < lineitem_rows &&
                       boundary > 0 &&
                       l_orderkey[boundary] == l_orderkey[boundary - 1]) {
                    ++boundary;
                }
                boundaries[static_cast<size_t>(t)] = boundary;
            }

            std::vector<std::vector<int32_t>> local_keys(static_cast<size_t>(max_threads));

            #pragma omp parallel num_threads(max_threads)
            {
                const int tid = omp_get_thread_num();
                const size_t begin = boundaries[static_cast<size_t>(tid)];
                const size_t end = boundaries[static_cast<size_t>(tid + 1)];
                auto& out = local_keys[static_cast<size_t>(tid)];

                if (begin < end) {
                    out.reserve((end - begin) / 96 + 8);

                    int32_t current_orderkey = l_orderkey[begin];
                    double current_sum = 0.0;
                    for (size_t row = begin; row < end; ++row) {
                        const int32_t orderkey = l_orderkey[row];
                        if (orderkey != current_orderkey) {
                            if (current_sum > kQtyThreshold) {
                                out.push_back(current_orderkey);
                            }
                            current_orderkey = orderkey;
                            current_sum = 0.0;
                        }
                        current_sum += l_quantity[row];
                    }
                    if (current_sum > kQtyThreshold) {
                        out.push_back(current_orderkey);
                    }
                }
            }

            size_t total_qualifying = 0;
            for (const auto& local : local_keys) {
                total_qualifying += local.size();
            }

            qualifying_orderkeys.reserve(total_qualifying);
            for (auto& local : local_keys) {
                qualifying_orderkeys.insert(
                    qualifying_orderkeys.end(),
                    local.begin(),
                    local.end()
                );
            }
        }

        std::vector<OutputRow> rows(qualifying_orderkeys.size());
        std::vector<uint8_t> valid(qualifying_orderkeys.size(), 0);

        {
            GENDB_PHASE("build_joins");

            #pragma omp parallel for schedule(static) num_threads(max_threads)
            for (size_t i = 0; i < qualifying_orderkeys.size(); ++i) {
                const int32_t orderkey = qualifying_orderkeys[i];
                if (orderkey < 0) {
                    continue;
                }

                const size_t order_slot = static_cast<size_t>(orderkey);
                if (order_slot >= order_domain || order_slot + 1 >= lineitem_posting_offsets.size()) {
                    continue;
                }

                const uint64_t order_row = orders_pk_dense[order_slot];
                if (order_row == kDenseEmpty) {
                    continue;
                }

                const int32_t custkey = o_custkey[order_row];
                if (custkey < 0) {
                    continue;
                }

                const size_t cust_slot = static_cast<size_t>(custkey);
                if (cust_slot >= customer_domain) {
                    continue;
                }

                const uint64_t customer_row = customer_pk_dense[cust_slot];
                if (customer_row == kDenseEmpty) {
                    continue;
                }

                const uint64_t posting_begin = lineitem_posting_offsets[order_slot];
                const uint64_t posting_end = lineitem_posting_offsets[order_slot + 1];
                double sum_qty = 0.0;
                for (uint64_t pos = posting_begin; pos < posting_end; ++pos) {
                    sum_qty += l_quantity[lineitem_posting_rowids[pos]];
                }

                rows[i] = OutputRow{
                    customer_row,
                    c_custkey[customer_row],
                    orderkey,
                    o_orderdate[order_row],
                    o_totalprice[order_row],
                    sum_qty
                };
                valid[i] = 1;
            }
        }

        {
            GENDB_PHASE("main_scan");

            size_t write_idx = 0;
            for (size_t i = 0; i < rows.size(); ++i) {
                if (valid[i] != 0) {
                    rows[write_idx++] = rows[i];
                }
            }
            rows.resize(write_idx);

            if (rows.size() > kTopK) {
                std::partial_sort(rows.begin(), rows.begin() + kTopK, rows.end(), output_row_less);
                rows.resize(kTopK);
            } else {
                std::sort(rows.begin(), rows.end(), output_row_less);
            }
        }

        {
            GENDB_PHASE("output");

            std::ofstream out(results_dir + "/Q18.csv");
            out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";
            out.setf(std::ios::fixed);
            out.precision(2);

            char date_buf[16];
            for (const OutputRow& row : rows) {
                gendb::epoch_days_to_date_str(row.orderdate, date_buf);
                const std::string_view customer_name =
                    string_at(c_name_offsets.data, c_name_data.data, row.customer_row);

                out.write(customer_name.data(), static_cast<std::streamsize>(customer_name.size()));
                out << ','
                    << row.custkey << ','
                    << row.orderkey << ','
                    << date_buf << ','
                    << row.totalprice << ','
                    << row.sum_qty << '\n';
            }
        }
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "Q18 failed: %s\n", ex.what());
        return 1;
    }

    return 0;
}
