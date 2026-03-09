#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <queue>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "timing_utils.h"

namespace {

struct MmapFile {
    void* ptr = nullptr;
    size_t size_bytes = 0;
    int fd = -1;

    bool open_read(const std::string& path, int advise) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::perror(path.c_str());
            return false;
        }

        struct stat st {};
        if (::fstat(fd, &st) != 0) {
            std::perror(path.c_str());
            ::close(fd);
            fd = -1;
            return false;
        }

        size_bytes = static_cast<size_t>(st.st_size);
        ptr = ::mmap(nullptr, size_bytes, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
            std::perror(path.c_str());
            ptr = nullptr;
            ::close(fd);
            fd = -1;
            size_bytes = 0;
            return false;
        }

        ::madvise(ptr, size_bytes, advise);
        return true;
    }

    template <typename T>
    const T* as() const {
        return reinterpret_cast<const T*>(ptr);
    }

    template <typename T>
    size_t count() const {
        return size_bytes / sizeof(T);
    }

    ~MmapFile() {
        if (ptr != nullptr) {
            ::munmap(ptr, size_bytes);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

struct QualifiedGroup {
    int32_t orderkey;
    uint32_t row_start;
    uint32_t row_count;
    int64_t sum_qty;
};

struct OrderCandidate {
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;
    int64_t sum_qty;
};

struct ResultRow {
    std::string_view c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;
    int64_t sum_qty;
};

static inline bool better_order(const OrderCandidate& left, const OrderCandidate& right) {
    if (left.o_totalprice != right.o_totalprice) {
        return left.o_totalprice > right.o_totalprice;
    }
    return left.o_orderdate < right.o_orderdate;
}

static inline bool better_order(const ResultRow& left, const ResultRow& right) {
    if (left.o_totalprice != right.o_totalprice) {
        return left.o_totalprice > right.o_totalprice;
    }
    return left.o_orderdate < right.o_orderdate;
}

struct BetterCandidate {
    bool operator()(const OrderCandidate& left, const OrderCandidate& right) const {
        return better_order(left, right);
    }
};

static inline std::array<char, 10> format_date_yyyy_mm_dd(int32_t days_since_epoch) {
    int z = days_since_epoch + 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = static_cast<unsigned>(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int y = static_cast<int>(yoe) + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned d = doy - (153 * mp + 2) / 5 + 1;
    unsigned m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);

    std::array<char, 10> out{};
    out[0] = static_cast<char>('0' + (y / 1000) % 10);
    out[1] = static_cast<char>('0' + (y / 100) % 10);
    out[2] = static_cast<char>('0' + (y / 10) % 10);
    out[3] = static_cast<char>('0' + y % 10);
    out[4] = '-';
    out[5] = static_cast<char>('0' + (m / 10) % 10);
    out[6] = static_cast<char>('0' + m % 10);
    out[7] = '-';
    out[8] = static_cast<char>('0' + (d / 10) % 10);
    out[9] = static_cast<char>('0' + d % 10);
    return out;
}

static inline void write_scaled_2(std::ostream& out, int64_t value) {
    if (value < 0) {
        out.put('-');
        value = -value;
    }
    out << (value / 100) << '.' << std::setw(2) << std::setfill('0') << (value % 100) << std::setfill(' ');
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    constexpr int64_t kHavingThreshold = 30000;
    constexpr size_t kLimit = 100;
    constexpr size_t kMorselSize = 1u << 18;
    constexpr uint32_t kMissingRow = std::numeric_limits<uint32_t>::max();

    { GENDB_PHASE("total");

    MmapFile li_group_keys_file;
    MmapFile li_row_starts_file;
    MmapFile li_row_counts_file;
    MmapFile li_sum_qty_file;
    MmapFile orders_dense_file;
    MmapFile orders_custkey_file;
    MmapFile orders_orderdate_file;
    MmapFile orders_totalprice_file;
    MmapFile customer_dense_file;
    MmapFile customer_name_offsets_file;
    MmapFile customer_name_data_file;

    { GENDB_PHASE("data_loading");
        const bool ok =
            li_group_keys_file.open_read(gendb_dir + "/lineitem/indexes/lineitem_orderkey_groups.keys.bin", MADV_SEQUENTIAL) &&
            li_row_starts_file.open_read(gendb_dir + "/lineitem/indexes/lineitem_orderkey_groups.row_starts.bin", MADV_RANDOM) &&
            li_row_counts_file.open_read(gendb_dir + "/lineitem/indexes/lineitem_orderkey_groups.row_counts.bin", MADV_RANDOM) &&
            li_sum_qty_file.open_read(gendb_dir + "/lineitem/indexes/lineitem_orderkey_groups.sum_quantity.bin", MADV_SEQUENTIAL) &&
            orders_dense_file.open_read(gendb_dir + "/orders/indexes/orders_pk_dense.bin", MADV_RANDOM) &&
            orders_custkey_file.open_read(gendb_dir + "/orders/o_custkey.bin", MADV_RANDOM) &&
            orders_orderdate_file.open_read(gendb_dir + "/orders/o_orderdate.bin", MADV_RANDOM) &&
            orders_totalprice_file.open_read(gendb_dir + "/orders/o_totalprice.bin", MADV_RANDOM) &&
            customer_dense_file.open_read(gendb_dir + "/customer/indexes/customer_pk_dense.bin", MADV_RANDOM) &&
            customer_name_offsets_file.open_read(gendb_dir + "/customer/c_name.offsets.bin", MADV_RANDOM) &&
            customer_name_data_file.open_read(gendb_dir + "/customer/c_name.data.bin", MADV_RANDOM);
        if (!ok) {
            return 1;
        }
    }

    const int32_t* li_group_keys = li_group_keys_file.as<int32_t>();
    const uint32_t* li_row_starts = li_row_starts_file.as<uint32_t>();
    const uint32_t* li_row_counts = li_row_counts_file.as<uint32_t>();
    const int64_t* li_sum_qty = li_sum_qty_file.as<int64_t>();
    const uint32_t* orders_pk_dense = orders_dense_file.as<uint32_t>();
    const int32_t* orders_custkey = orders_custkey_file.as<int32_t>();
    const int32_t* orders_orderdate = orders_orderdate_file.as<int32_t>();
    const int64_t* orders_totalprice = orders_totalprice_file.as<int64_t>();
    const uint32_t* customer_pk_dense = customer_dense_file.as<uint32_t>();
    const uint64_t* customer_name_offsets = customer_name_offsets_file.as<uint64_t>();
    const char* customer_name_data = customer_name_data_file.as<char>();

    const size_t group_count = li_sum_qty_file.count<int64_t>();
    const int scan_threads = std::max(1, std::min(32, omp_get_num_procs()));

    std::vector<QualifiedGroup> qualifying_groups;

    { GENDB_PHASE("dim_filter");
        const size_t morsel_count = (group_count + kMorselSize - 1) / kMorselSize;
        std::vector<std::vector<QualifiedGroup>> per_thread(scan_threads);
        for (auto& bucket : per_thread) {
            bucket.reserve(32);
        }

        #pragma omp parallel num_threads(scan_threads)
        {
            std::vector<QualifiedGroup>& local = per_thread[omp_get_thread_num()];

            #pragma omp for schedule(dynamic, 1)
            for (size_t morsel = 0; morsel < morsel_count; ++morsel) {
                const size_t begin = morsel * kMorselSize;
                const size_t end = std::min(begin + kMorselSize, group_count);
                for (size_t idx = begin; idx < end; ++idx) {
                    const int64_t sum_qty = li_sum_qty[idx];
                    if (sum_qty > kHavingThreshold) {
                        local.push_back(QualifiedGroup{
                            li_group_keys[idx],
                            li_row_starts[idx],
                            li_row_counts[idx],
                            sum_qty,
                        });
                    }
                }
            }
        }

        size_t qualifying_count = 0;
        for (const auto& bucket : per_thread) {
            qualifying_count += bucket.size();
        }

        qualifying_groups.reserve(qualifying_count);
        for (auto& bucket : per_thread) {
            qualifying_groups.insert(qualifying_groups.end(), bucket.begin(), bucket.end());
        }
    }

    std::vector<OrderCandidate> order_candidates;

    { GENDB_PHASE("build_joins");
        std::vector<std::vector<OrderCandidate>> per_thread(scan_threads);
        for (auto& bucket : per_thread) {
            bucket.reserve((qualifying_groups.size() + static_cast<size_t>(scan_threads) - 1) / static_cast<size_t>(scan_threads));
        }

        #pragma omp parallel for schedule(dynamic, 32) num_threads(scan_threads)
        for (size_t idx = 0; idx < qualifying_groups.size(); ++idx) {
            const QualifiedGroup& group = qualifying_groups[idx];
            const int32_t orderkey = group.orderkey;
            if (orderkey < 0) {
                continue;
            }

            const uint32_t order_row = orders_pk_dense[static_cast<size_t>(orderkey)];
            if (order_row == kMissingRow) {
                continue;
            }

            per_thread[omp_get_thread_num()].push_back(OrderCandidate{
                orders_custkey[order_row],
                orderkey,
                orders_orderdate[order_row],
                orders_totalprice[order_row],
                group.sum_qty,
            });
        }

        size_t candidate_count = 0;
        for (const auto& bucket : per_thread) {
            candidate_count += bucket.size();
        }

        order_candidates.reserve(candidate_count);
        for (auto& bucket : per_thread) {
            order_candidates.insert(order_candidates.end(), bucket.begin(), bucket.end());
        }
    }

    std::vector<OrderCandidate> top_orders;

    { GENDB_PHASE("main_scan");
        std::priority_queue<OrderCandidate, std::vector<OrderCandidate>, BetterCandidate> topk_heap;
        for (const OrderCandidate& candidate : order_candidates) {
            if (topk_heap.size() < kLimit) {
                topk_heap.push(candidate);
            } else if (better_order(candidate, topk_heap.top())) {
                topk_heap.pop();
                topk_heap.push(candidate);
            }
        }

        top_orders.reserve(topk_heap.size());
        while (!topk_heap.empty()) {
            top_orders.push_back(topk_heap.top());
            topk_heap.pop();
        }

        std::sort(top_orders.begin(), top_orders.end(), [](const OrderCandidate& left, const OrderCandidate& right) {
            return better_order(left, right);
        });
    }

    std::vector<ResultRow> results;
    results.reserve(top_orders.size());

    for (const OrderCandidate& candidate : top_orders) {
        const int32_t custkey = candidate.c_custkey;
        if (custkey < 0) {
            continue;
        }

        const uint32_t customer_row = customer_pk_dense[static_cast<size_t>(custkey)];
        if (customer_row == kMissingRow) {
            continue;
        }

        const uint64_t begin = customer_name_offsets[customer_row];
        const uint64_t end = customer_name_offsets[customer_row + 1];
        results.push_back(ResultRow{
            std::string_view(customer_name_data + begin, static_cast<size_t>(end - begin)),
            custkey,
            candidate.o_orderkey,
            candidate.o_orderdate,
            candidate.o_totalprice,
            candidate.sum_qty,
        });
    }

    { GENDB_PHASE("output");
        std::ofstream out(results_dir + "/Q18.csv", std::ios::out | std::ios::trunc);
        if (!out) {
            std::fprintf(stderr, "Failed to open output file\n");
            return 1;
        }

        out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";
        for (const ResultRow& row : results) {
            const std::array<char, 10> date = format_date_yyyy_mm_dd(row.o_orderdate);
            out.write(row.c_name.data(), static_cast<std::streamsize>(row.c_name.size()));
            out << ',' << row.c_custkey << ',' << row.o_orderkey << ',';
            out.write(date.data(), static_cast<std::streamsize>(date.size()));
            out << ',';
            write_scaled_2(out, row.o_totalprice);
            out << ',';
            write_scaled_2(out, row.sum_qty);
            out << '\n';
        }
    }

    }

    return 0;
}
