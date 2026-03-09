#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

namespace {

constexpr double kQtyThreshold = 300.0;
constexpr size_t kTopK = 100;
constexpr uint64_t kDenseEmpty = std::numeric_limits<uint64_t>::max();

struct QualifyingOrder {
    int32_t orderkey;
    double sum_qty;
};

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

inline int plan_thread_count() {
    const unsigned hw = std::thread::hardware_concurrency();
    const int hw_threads = hw == 0 ? omp_get_max_threads() : static_cast<int>(hw);
    return std::max(1, std::min(16, std::min(hw_threads, omp_get_max_threads())));
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
        gendb::MmapColumn<uint64_t> orders_pk_dense;
        gendb::MmapColumn<uint64_t> customer_pk_dense;

        const int max_threads = plan_thread_count();

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
            orders_pk_dense.open(gendb_dir + "/orders/indexes/orders_pk_dense.bin");
            customer_pk_dense.open(gendb_dir + "/customer/indexes/customer_pk_dense.bin");

            l_orderkey.advise_sequential();
            l_quantity.advise_sequential();
            o_custkey.advise_random();
            o_orderdate.advise_random();
            o_totalprice.advise_random();
            c_custkey.advise_random();
            c_name_offsets.advise_random();
            c_name_data.advise_random();
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
                orders_pk_dense,
                customer_pk_dense
            );
        }

        const size_t lineitem_rows = l_orderkey.size();
        const size_t order_domain = orders_pk_dense.size();
        const size_t customer_domain = customer_pk_dense.size();

        std::vector<QualifyingOrder> qualifying_orders;

        {
            GENDB_PHASE("dim_filter");

            std::vector<size_t> boundaries(static_cast<size_t>(max_threads) + 1, lineitem_rows);
            boundaries[0] = 0;
            boundaries[static_cast<size_t>(max_threads)] = lineitem_rows;
            for (int t = 1; t < max_threads; ++t) {
                size_t boundary =
                    (lineitem_rows * static_cast<size_t>(t)) / static_cast<size_t>(max_threads);
                while (boundary < lineitem_rows &&
                       boundary > 0 &&
                       l_orderkey[boundary] == l_orderkey[boundary - 1]) {
                    ++boundary;
                }
                boundaries[static_cast<size_t>(t)] = boundary;
            }

            std::vector<std::vector<QualifyingOrder>> local_out(static_cast<size_t>(max_threads));

            #pragma omp parallel num_threads(max_threads)
            {
                const int tid = omp_get_thread_num();
                const size_t begin = boundaries[static_cast<size_t>(tid)];
                const size_t end = boundaries[static_cast<size_t>(tid + 1)];
                auto& out = local_out[static_cast<size_t>(tid)];

                if (begin < end) {
                    out.reserve((end - begin) / 65536 + 8);

                    int32_t current_orderkey = l_orderkey[begin];
                    double current_sum = l_quantity[begin];
                    for (size_t row = begin + 1; row < end; ++row) {
                        const int32_t orderkey = l_orderkey[row];
                        if (orderkey != current_orderkey) {
                            if (current_sum > kQtyThreshold) {
                                out.push_back(QualifyingOrder{current_orderkey, current_sum});
                            }
                            current_orderkey = orderkey;
                            current_sum = l_quantity[row];
                        } else {
                            current_sum += l_quantity[row];
                        }
                    }
                    if (current_sum > kQtyThreshold) {
                        out.push_back(QualifyingOrder{current_orderkey, current_sum});
                    }
                }
            }

            size_t total_qualifying = 0;
            for (const auto& local : local_out) {
                total_qualifying += local.size();
            }

            qualifying_orders.reserve(total_qualifying);
            for (auto& local : local_out) {
                qualifying_orders.insert(qualifying_orders.end(), local.begin(), local.end());
            }
        }

        std::vector<OutputRow> rows(qualifying_orders.size());
        std::vector<uint8_t> valid(qualifying_orders.size(), 0);

        {
            GENDB_PHASE("build_joins");

            #pragma omp parallel for schedule(static) num_threads(max_threads)
            for (size_t i = 0; i < qualifying_orders.size(); ++i) {
                const QualifyingOrder qual = qualifying_orders[i];
                if (qual.orderkey < 0) {
                    continue;
                }

                const size_t order_slot = static_cast<size_t>(qual.orderkey);
                if (order_slot >= order_domain) {
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

                rows[i] = OutputRow{
                    customer_row,
                    c_custkey[customer_row],
                    qual.orderkey,
                    o_orderdate[order_row],
                    o_totalprice[order_row],
                    qual.sum_qty
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
