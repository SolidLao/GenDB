#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

template <typename T>
std::vector<T> read_vector(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("failed to open " + path.string());
    }
    uint64_t bytes = fs::file_size(path);
    if (bytes % sizeof(T) != 0) {
        throw std::runtime_error("bad file size for " + path.string());
    }
    std::vector<T> data(bytes / sizeof(T));
    if (!data.empty()) {
        in.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(bytes));
    }
    return data;
}

std::vector<char> read_bytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("failed to open " + path.string());
    }
    uint64_t bytes = fs::file_size(path);
    std::vector<char> data(bytes);
    if (!data.empty()) {
        in.read(data.data(), static_cast<std::streamsize>(bytes));
    }
    return data;
}

template <typename T>
void write_vector(const fs::path& path, const std::vector<T>& data) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("failed to open " + path.string());
    }
    if (!data.empty()) {
        out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size() * sizeof(T)));
    }
}

void write_bytes(const fs::path& path, const std::vector<char>& data) {
    std::ofstream out(path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("failed to open " + path.string());
    }
    if (!data.empty()) {
        out.write(data.data(), static_cast<std::streamsize>(data.size()));
    }
}

template <typename Fn>
void parallel_run(std::vector<Fn>& tasks, size_t max_threads) {
    if (tasks.empty()) {
        return;
    }
    std::atomic<size_t> next{0};
    size_t thread_count = std::max<size_t>(1, std::min(max_threads, tasks.size()));
    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (size_t tid = 0; tid < thread_count; ++tid) {
        threads.emplace_back([&]() {
            while (true) {
                size_t idx = next.fetch_add(1, std::memory_order_relaxed);
                if (idx >= tasks.size()) {
                    break;
                }
                tasks[idx]();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

uint64_t next_power_of_two(uint64_t value) {
    uint64_t n = 1;
    while (n < value) {
        n <<= 1;
    }
    return n;
}

void write_dense_pk(const fs::path& index_dir, const std::string& name, const std::vector<int32_t>& keys) {
    int32_t max_key = *std::max_element(keys.begin(), keys.end());
    std::vector<uint32_t> dense(static_cast<size_t>(max_key) + 1, std::numeric_limits<uint32_t>::max());
    for (uint32_t row = 0; row < keys.size(); ++row) {
        dense[static_cast<size_t>(keys[row])] = row;
    }
    write_vector(index_dir / (name + ".bin"), dense);
}

void write_dense_postings(const fs::path& index_dir, const std::string& name, const std::vector<int32_t>& keys) {
    int32_t max_key = *std::max_element(keys.begin(), keys.end());
    std::vector<uint64_t> offsets(static_cast<size_t>(max_key) + 2, 0);
    for (int32_t key : keys) {
        offsets[static_cast<size_t>(key) + 1]++;
    }
    for (size_t i = 1; i < offsets.size(); ++i) {
        offsets[i] += offsets[i - 1];
    }
    std::vector<uint64_t> cursor = offsets;
    std::vector<uint32_t> row_ids(keys.size());
    for (uint32_t row = 0; row < keys.size(); ++row) {
        size_t slot = static_cast<size_t>(keys[row]);
        row_ids[cursor[slot]++] = row;
    }
    write_vector(index_dir / (name + ".offsets.bin"), offsets);
    write_vector(index_dir / (name + ".row_ids.bin"), row_ids);
}

template <typename CodeT>
void write_code_postings(const fs::path& index_dir, const std::string& name, const std::vector<CodeT>& codes) {
    CodeT max_code = *std::max_element(codes.begin(), codes.end());
    std::vector<uint64_t> offsets(static_cast<size_t>(max_code) + 2, 0);
    for (CodeT code : codes) {
        offsets[static_cast<size_t>(code) + 1]++;
    }
    for (size_t i = 1; i < offsets.size(); ++i) {
        offsets[i] += offsets[i - 1];
    }
    std::vector<uint64_t> cursor = offsets;
    std::vector<uint32_t> row_ids(codes.size());
    for (uint32_t row = 0; row < codes.size(); ++row) {
        size_t slot = static_cast<size_t>(codes[row]);
        row_ids[cursor[slot]++] = row;
    }
    write_vector(index_dir / (name + ".offsets.bin"), offsets);
    write_vector(index_dir / (name + ".row_ids.bin"), row_ids);
}

template <typename T>
void write_zone_map(const fs::path& index_dir, const std::string& name, const std::vector<T>& data, uint32_t block_size) {
    size_t blocks = (data.size() + block_size - 1) / block_size;
    std::vector<T> mins(blocks);
    std::vector<T> maxs(blocks);
    for (size_t block = 0; block < blocks; ++block) {
        size_t begin = block * block_size;
        size_t end = std::min(begin + block_size, data.size());
        T min_v = data[begin];
        T max_v = data[begin];
        for (size_t i = begin + 1; i < end; ++i) {
            min_v = std::min(min_v, data[i]);
            max_v = std::max(max_v, data[i]);
        }
        mins[block] = min_v;
        maxs[block] = max_v;
    }
    write_vector(index_dir / (name + ".mins.bin"), mins);
    write_vector(index_dir / (name + ".maxs.bin"), maxs);
}

bool contains_green(std::string_view sv) {
    constexpr std::string_view needle = "green";
    if (sv.size() < needle.size()) {
        return false;
    }
    for (size_t i = 0; i + needle.size() <= sv.size(); ++i) {
        bool match = true;
        for (size_t j = 0; j < needle.size(); ++j) {
            char c = static_cast<char>(std::tolower(static_cast<unsigned char>(sv[i + j])));
            if (c != needle[j]) {
                match = false;
                break;
            }
        }
        if (match) {
            return true;
        }
    }
    return false;
}

void build_region_indexes(const fs::path& base_dir) {
    fs::path table_dir = base_dir / "region";
    fs::path index_dir = table_dir / "indexes";
    fs::create_directories(index_dir);
    auto keys = read_vector<int32_t>(table_dir / "r_regionkey.bin");
    write_dense_pk(index_dir, "region_pk_dense", keys);
}

void build_nation_indexes(const fs::path& base_dir) {
    fs::path table_dir = base_dir / "nation";
    fs::path index_dir = table_dir / "indexes";
    fs::create_directories(index_dir);
    auto keys = read_vector<int32_t>(table_dir / "n_nationkey.bin");
    write_dense_pk(index_dir, "nation_pk_dense", keys);
}

void build_supplier_indexes(const fs::path& base_dir) {
    fs::path table_dir = base_dir / "supplier";
    fs::path index_dir = table_dir / "indexes";
    fs::create_directories(index_dir);
    auto keys = read_vector<int32_t>(table_dir / "s_suppkey.bin");
    write_dense_pk(index_dir, "supplier_pk_dense", keys);
}

void build_part_indexes(const fs::path& base_dir) {
    fs::path table_dir = base_dir / "part";
    fs::path index_dir = table_dir / "indexes";
    fs::create_directories(index_dir);

    auto keys = read_vector<int32_t>(table_dir / "p_partkey.bin");
    write_dense_pk(index_dir, "part_pk_dense", keys);

    auto offsets = read_vector<uint64_t>(table_dir / "p_name.offsets.bin");
    auto bytes = read_bytes(table_dir / "p_name.data.bin");
    int32_t max_key = *std::max_element(keys.begin(), keys.end());
    std::vector<uint8_t> bitset(static_cast<size_t>(max_key) + 1, 0);
    for (size_t row = 0; row < keys.size(); ++row) {
        uint64_t begin = offsets[row];
        uint64_t end = offsets[row + 1];
        if (contains_green(std::string_view(bytes.data() + begin, static_cast<size_t>(end - begin)))) {
            bitset[static_cast<size_t>(keys[row])] = 1;
        }
    }
    write_vector(index_dir / "part_name_green_bitset.bin", bitset);
}

void build_customer_indexes(const fs::path& base_dir) {
    fs::path table_dir = base_dir / "customer";
    fs::path index_dir = table_dir / "indexes";
    fs::create_directories(index_dir);

    auto keys = read_vector<int32_t>(table_dir / "c_custkey.bin");
    write_dense_pk(index_dir, "customer_pk_dense", keys);

    auto codes = read_vector<uint8_t>(table_dir / "c_mktsegment.codes.bin");
    write_code_postings(index_dir, "customer_mktsegment_postings", codes);
}

void build_orders_indexes(const fs::path& base_dir) {
    fs::path table_dir = base_dir / "orders";
    fs::path index_dir = table_dir / "indexes";
    fs::create_directories(index_dir);

    auto orderkeys = read_vector<int32_t>(table_dir / "o_orderkey.bin");
    auto custkeys = read_vector<int32_t>(table_dir / "o_custkey.bin");
    auto orderdates = read_vector<int32_t>(table_dir / "o_orderdate.bin");

    write_dense_pk(index_dir, "orders_pk_dense", orderkeys);
    write_dense_postings(index_dir, "orders_custkey_postings", custkeys);
    write_zone_map(index_dir, "orders_orderdate_zone_map", orderdates, 100000);
}

void build_partsupp_indexes(const fs::path& base_dir) {
    fs::path table_dir = base_dir / "partsupp";
    fs::path index_dir = table_dir / "indexes";
    fs::create_directories(index_dir);

    auto partkeys = read_vector<int32_t>(table_dir / "ps_partkey.bin");
    auto suppkeys = read_vector<int32_t>(table_dir / "ps_suppkey.bin");
    uint64_t slots = next_power_of_two(static_cast<uint64_t>(partkeys.size()) * 2);
    std::vector<uint64_t> keys(slots, 0);
    std::vector<uint32_t> row_ids(slots, std::numeric_limits<uint32_t>::max());
    uint64_t mask = slots - 1;
    for (uint32_t row = 0; row < partkeys.size(); ++row) {
        uint64_t packed = (static_cast<uint64_t>(static_cast<uint32_t>(partkeys[row])) << 32) |
                          static_cast<uint32_t>(suppkeys[row]);
        uint64_t slot = (packed * 11400714819323198485ull) & mask;
        while (keys[slot] != 0) {
            slot = (slot + 1) & mask;
        }
        keys[slot] = packed;
        row_ids[slot] = row;
    }
    write_vector(index_dir / "partsupp_pk_hash.keys.bin", keys);
    write_vector(index_dir / "partsupp_pk_hash.row_ids.bin", row_ids);
}

void build_lineitem_indexes(const fs::path& base_dir) {
    fs::path table_dir = base_dir / "lineitem";
    fs::path index_dir = table_dir / "indexes";
    fs::create_directories(index_dir);

    auto shipdate = read_vector<int32_t>(table_dir / "l_shipdate.bin");
    auto discount = read_vector<int64_t>(table_dir / "l_discount.bin");
    auto quantity = read_vector<int64_t>(table_dir / "l_quantity.bin");
    write_zone_map(index_dir, "lineitem_shipdate_zone_map", shipdate, 100000);
    write_zone_map(index_dir, "lineitem_discount_zone_map", discount, 100000);
    write_zone_map(index_dir, "lineitem_quantity_zone_map", quantity, 100000);

    auto orderkeys = read_vector<int32_t>(table_dir / "l_orderkey.bin");
    std::vector<int32_t> group_keys;
    std::vector<uint32_t> row_starts;
    std::vector<uint32_t> row_counts;
    std::vector<int64_t> sum_quantity;
    group_keys.reserve(15000000);
    row_starts.reserve(15000000);
    row_counts.reserve(15000000);
    sum_quantity.reserve(15000000);
    for (uint32_t row = 0; row < orderkeys.size();) {
        int32_t key = orderkeys[row];
        uint32_t start = row;
        int64_t sum = 0;
        while (row < orderkeys.size() && orderkeys[row] == key) {
            sum += quantity[row];
            ++row;
        }
        group_keys.push_back(key);
        row_starts.push_back(start);
        row_counts.push_back(row - start);
        sum_quantity.push_back(sum);
    }
    write_vector(index_dir / "lineitem_orderkey_groups.keys.bin", group_keys);
    write_vector(index_dir / "lineitem_orderkey_groups.row_starts.bin", row_starts);
    write_vector(index_dir / "lineitem_orderkey_groups.row_counts.bin", row_counts);
    write_vector(index_dir / "lineitem_orderkey_groups.sum_quantity.bin", sum_quantity);
}

int main(int argc, char** argv) {
    try {
        if (argc != 2) {
            std::cerr << "usage: build_indexes <gendb_dir>\n";
            return 1;
        }
        fs::path base_dir = argv[1];
        std::vector<std::function<void()>> tasks;
        tasks.emplace_back([&]() { build_region_indexes(base_dir); });
        tasks.emplace_back([&]() { build_nation_indexes(base_dir); });
        tasks.emplace_back([&]() { build_supplier_indexes(base_dir); });
        tasks.emplace_back([&]() { build_part_indexes(base_dir); });
        tasks.emplace_back([&]() { build_customer_indexes(base_dir); });
        tasks.emplace_back([&]() { build_orders_indexes(base_dir); });
        tasks.emplace_back([&]() { build_partsupp_indexes(base_dir); });
        tasks.emplace_back([&]() { build_lineitem_indexes(base_dir); });
        parallel_run(tasks, 4);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "build_indexes failed: " << ex.what() << '\n';
        return 2;
    }
}
