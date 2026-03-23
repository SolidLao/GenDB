#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#include <fcntl.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "date_utils.h"
#include "timing_utils.h"

struct MmapRegion {
    void* ptr = nullptr;
    size_t size = 0;
    int fd = -1;

    void open_readonly(const std::string& path, int advise) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::perror(path.c_str());
            std::exit(1);
        }

        struct stat st {};
        if (fstat(fd, &st) != 0) {
            std::perror(path.c_str());
            std::exit(1);
        }

        size = static_cast<size_t>(st.st_size);
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
            std::perror(path.c_str());
            std::exit(1);
        }
        madvise(ptr, size, advise);
    }

    template <typename T>
    const T* as() const {
        return reinterpret_cast<const T*>(ptr);
    }

    size_t count_bytes(size_t elem_size) const {
        return size / elem_size;
    }

    ~MmapRegion() {
        if (ptr != nullptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }
};

struct OutputRow {
    uint64_t customer_row = std::numeric_limits<uint64_t>::max();
    int32_t orderkey = 0;
    int32_t orderdate = 0;
    double totalprice = 0.0;
    double sum_qty = 0.0;
    bool valid = false;
};

static inline std::string load_string_at(
    const uint64_t* offsets,
    const char* data,
    uint64_t row_id
) {
    const uint64_t start = offsets[row_id];
    const uint64_t end = offsets[row_id + 1];
    return std::string(data + start, data + end);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    gendb::init_date_tables();

    {
        GENDB_PHASE("total");

        MmapRegion l_orderkey_map;
        MmapRegion l_quantity_map;
        MmapRegion o_custkey_map;
        MmapRegion o_orderdate_map;
        MmapRegion o_totalprice_map;
        MmapRegion c_custkey_map;
        MmapRegion c_name_offsets_map;
        MmapRegion c_name_data_map;
        MmapRegion lineitem_posting_offsets_map;
        MmapRegion lineitem_posting_rowids_map;
        MmapRegion orders_pk_dense_map;
        MmapRegion customer_pk_dense_map;

        const int max_threads = std::min(64, omp_get_max_threads());

        {
            GENDB_PHASE("data_loading");
            std::filesystem::create_directories(results_dir);

            l_orderkey_map.open_readonly(gendb_dir + "/lineitem/l_orderkey.bin", MADV_SEQUENTIAL);
            l_quantity_map.open_readonly(gendb_dir + "/lineitem/l_quantity.bin", MADV_RANDOM);
            o_custkey_map.open_readonly(gendb_dir + "/orders/o_custkey.bin", MADV_RANDOM);
            o_orderdate_map.open_readonly(gendb_dir + "/orders/o_orderdate.bin", MADV_RANDOM);
            o_totalprice_map.open_readonly(gendb_dir + "/orders/o_totalprice.bin", MADV_RANDOM);
            c_custkey_map.open_readonly(gendb_dir + "/customer/c_custkey.bin", MADV_RANDOM);
            c_name_offsets_map.open_readonly(gendb_dir + "/customer/c_name.offsets.bin", MADV_RANDOM);
            c_name_data_map.open_readonly(gendb_dir + "/customer/c_name.data.bin", MADV_RANDOM);
            lineitem_posting_offsets_map.open_readonly(
                gendb_dir + "/lineitem/indexes/lineitem_order_postings.offsets.bin",
                MADV_RANDOM
            );
            lineitem_posting_rowids_map.open_readonly(
                gendb_dir + "/lineitem/indexes/lineitem_order_postings.rowids.bin",
                MADV_RANDOM
            );
            orders_pk_dense_map.open_readonly(
                gendb_dir + "/orders/indexes/orders_pk_dense.bin",
                MADV_RANDOM
            );
            customer_pk_dense_map.open_readonly(
                gendb_dir + "/customer/indexes/customer_pk_dense.bin",
                MADV_RANDOM
            );
        }

        const int32_t* l_orderkey = l_orderkey_map.as<int32_t>();
        const double* l_quantity = l_quantity_map.as<double>();
        const int32_t* o_custkey = o_custkey_map.as<int32_t>();
        const int32_t* o_orderdate = o_orderdate_map.as<int32_t>();
        const double* o_totalprice = o_totalprice_map.as<double>();
        const int32_t* c_custkey = c_custkey_map.as<int32_t>();
        const uint64_t* c_name_offsets = c_name_offsets_map.as<uint64_t>();
        const char* c_name_data = c_name_data_map.as<char>();
        const uint64_t* lineitem_posting_offsets = lineitem_posting_offsets_map.as<uint64_t>();
        const uint64_t* lineitem_posting_rowids = lineitem_posting_rowids_map.as<uint64_t>();
        const uint64_t* orders_pk_dense = orders_pk_dense_map.as<uint64_t>();
        const uint64_t* customer_pk_dense = customer_pk_dense_map.as<uint64_t>();

        const size_t lineitem_rows = l_orderkey_map.count_bytes(sizeof(int32_t));
        const size_t order_domain = orders_pk_dense_map.count_bytes(sizeof(uint64_t));
        const uint64_t dense_empty = std::numeric_limits<uint64_t>::max();

        std::vector<double> order_sum_by_key;
        std::vector<int32_t> qualifying_orderkeys;

        {
            GENDB_PHASE("dim_filter");

            order_sum_by_key.assign(order_domain, 0.0);

            #pragma omp parallel for schedule(static) num_threads(max_threads)
            for (size_t row = 0; row < lineitem_rows; ++row) {
                const int32_t orderkey = l_orderkey[row];
                #pragma omp atomic update
                order_sum_by_key[static_cast<size_t>(orderkey)] += l_quantity[row];
            }

            std::vector<std::vector<int32_t>> thread_qualifying(static_cast<size_t>(max_threads));
            for (auto& local : thread_qualifying) {
                local.reserve(32);
            }

            #pragma omp parallel num_threads(max_threads)
            {
                const int tid = omp_get_thread_num();
                auto& local = thread_qualifying[static_cast<size_t>(tid)];

                #pragma omp for schedule(static)
                for (size_t key = 0; key < order_domain; ++key) {
                    if (order_sum_by_key[key] > 300.0) {
                        local.push_back(static_cast<int32_t>(key));
                    }
                }
            }

            size_t qualifying_count = 0;
            for (const auto& local : thread_qualifying) {
                qualifying_count += local.size();
            }
            qualifying_orderkeys.reserve(qualifying_count);
            for (auto& local : thread_qualifying) {
                qualifying_orderkeys.insert(
                    qualifying_orderkeys.end(),
                    local.begin(),
                    local.end()
                );
            }
        }

        std::vector<OutputRow> rows(qualifying_orderkeys.size());

        {
            GENDB_PHASE("build_joins");

            #pragma omp parallel for schedule(dynamic, 32) num_threads(max_threads)
            for (size_t i = 0; i < qualifying_orderkeys.size(); ++i) {
                const int32_t orderkey = qualifying_orderkeys[i];
                const size_t order_slot = static_cast<size_t>(orderkey);
                if (order_slot >= order_domain) {
                    continue;
                }

                const uint64_t order_row = orders_pk_dense[order_slot];
                if (order_row == dense_empty) {
                    continue;
                }

                const int32_t custkey = o_custkey[order_row];
                const size_t cust_slot = static_cast<size_t>(custkey);
                const uint64_t customer_row = customer_pk_dense[cust_slot];
                if (customer_row == dense_empty) {
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
                    orderkey,
                    o_orderdate[order_row],
                    o_totalprice[order_row],
                    sum_qty,
                    true
                };
            }
        }

        {
            GENDB_PHASE("main_scan");

            size_t write_idx = 0;
            for (size_t i = 0; i < rows.size(); ++i) {
                if (rows[i].valid) {
                    rows[write_idx++] = rows[i];
                }
            }
            rows.resize(write_idx);

            std::sort(rows.begin(), rows.end(), [](const OutputRow& a, const OutputRow& b) {
                if (a.totalprice != b.totalprice) {
                    return a.totalprice > b.totalprice;
                }
                if (a.orderdate != b.orderdate) {
                    return a.orderdate < b.orderdate;
                }
                return a.orderkey < b.orderkey;
            });

            if (rows.size() > 100) {
                rows.resize(100);
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
                const int32_t custkey = c_custkey[row.customer_row];
                const std::string customer_name = load_string_at(
                    c_name_offsets,
                    c_name_data,
                    row.customer_row
                );

                out << customer_name << ','
                    << custkey << ','
                    << row.orderkey << ','
                    << date_buf << ','
                    << row.totalprice << ','
                    << row.sum_qty << '\n';
            }
        }
    }

    return 0;
}
