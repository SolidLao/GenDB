#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <map>
#include <unordered_map>

// Load a binary column file into memory
template<typename T>
std::vector<T> load_column(const std::string& filename, size_t row_count) {
    std::vector<T> data(row_count);
    std::ifstream in(filename, std::ios::binary);
    in.read((char*)data.data(), row_count * sizeof(T));
    return data;
}

// Sorted index: stores (value, row_index) pairs sorted by value
template<typename T>
void build_sorted_index(const std::string& col_filename, const std::string& idx_filename,
                        size_t row_count) {
    std::vector<T> col_data = load_column<T>(col_filename, row_count);

    std::vector<std::pair<T, uint32_t>> index;
    for (uint32_t i = 0; i < row_count; i++) {
        index.push_back({col_data[i], i});
    }

    std::sort(index.begin(), index.end());

    std::ofstream out(idx_filename, std::ios::binary);
    for (const auto& p : index) {
        out.write((const char*)&p.first, sizeof(T));
        out.write((const char*)&p.second, sizeof(uint32_t));
    }
    out.close();
}

// Hash index: stores (hash_bucket -> vector of row indices) for quick lookups
template<typename T>
void build_hash_index(const std::string& col_filename, const std::string& idx_filename,
                      size_t row_count) {
    std::vector<T> col_data = load_column<T>(col_filename, row_count);

    std::unordered_map<T, std::vector<uint32_t>> hash_index;
    for (uint32_t i = 0; i < row_count; i++) {
        hash_index[col_data[i]].push_back(i);
    }

    // Serialize hash index: for each unique value, write value + row indices
    std::ofstream out(idx_filename, std::ios::binary);
    uint32_t num_buckets = hash_index.size();
    out.write((const char*)&num_buckets, sizeof(uint32_t));

    for (const auto& kv : hash_index) {
        T value = kv.first;
        const auto& indices = kv.second;
        uint32_t count = indices.size();

        out.write((const char*)&value, sizeof(T));
        out.write((const char*)&count, sizeof(uint32_t));
        out.write((const char*)indices.data(), count * sizeof(uint32_t));
    }
    out.close();
}

// Zone map: min/max values per block
template<typename T>
void build_zone_map(const std::string& col_filename, const std::string& zmap_filename,
                    size_t row_count, uint32_t block_size = 100000) {
    std::vector<T> col_data = load_column<T>(col_filename, row_count);

    std::ofstream out(zmap_filename, std::ios::binary);
    uint32_t num_blocks = (row_count + block_size - 1) / block_size;
    out.write((const char*)&num_blocks, sizeof(uint32_t));
    out.write((const char*)&block_size, sizeof(uint32_t));

    for (uint32_t block_id = 0; block_id < num_blocks; block_id++) {
        uint32_t start = block_id * block_size;
        uint32_t end = std::min(start + block_size, (uint32_t)row_count);

        T min_val = col_data[start];
        T max_val = col_data[start];

        for (uint32_t i = start; i < end; i++) {
            if (col_data[i] < min_val) min_val = col_data[i];
            if (col_data[i] > max_val) max_val = col_data[i];
        }

        out.write((const char*)&min_val, sizeof(T));
        out.write((const char*)&max_val, sizeof(T));
    }
    out.close();
}

void build_indexes_for_table(const std::string& gendb_dir, const std::string& table_name,
                             size_t row_count, const std::string& sort_col) {
    // Build zone maps for the sort column
    if (!sort_col.empty()) {
        std::cout << "  Building zone map for " << table_name << "." << sort_col << "..." << std::flush;
        auto t0 = std::chrono::high_resolution_clock::now();

        std::string col_file = gendb_dir + "/" + table_name + "." + sort_col + ".col";
        std::string zmap_file = gendb_dir + "/" + table_name + "." + sort_col + ".zmap";

        // Assuming dates and order keys are int32_t
        build_zone_map<int32_t>(col_file, zmap_file, row_count);

        auto t1 = std::chrono::high_resolution_clock::now();
        std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;
    }

    // Build sorted index on sort column (if present)
    if (!sort_col.empty()) {
        std::cout << "  Building sorted index for " << table_name << "." << sort_col << "..." << std::flush;
        auto t0 = std::chrono::high_resolution_clock::now();

        std::string col_file = gendb_dir + "/" + table_name + "." + sort_col + ".col";
        std::string idx_file = gendb_dir + "/" + table_name + "." + sort_col + ".sorted_idx";

        build_sorted_index<int32_t>(col_file, idx_file, row_count);

        auto t1 = std::chrono::high_resolution_clock::now();
        std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;
    }
}

void build_hash_indexes(const std::string& gendb_dir, const std::string& table_name,
                        size_t row_count, const std::vector<std::string>& key_cols) {
    for (const auto& col : key_cols) {
        std::cout << "  Building hash index for " << table_name << "." << col << "..." << std::flush;
        auto t0 = std::chrono::high_resolution_clock::now();

        std::string col_file = gendb_dir + "/" + table_name + "." + col + ".col";
        std::string idx_file = gendb_dir + "/" + table_name + "." + col + ".hash_idx";

        build_hash_index<int32_t>(col_file, idx_file, row_count);

        auto t1 = std::chrono::high_resolution_clock::now();
        std::cout << " done in " << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms" << std::endl;
    }
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    auto start_time = std::chrono::high_resolution_clock::now();

    std::cout << "=== Building Indexes ===" << std::endl;

    // lineitem table: 59986052 rows, sort by l_shipdate, hash indexes on l_orderkey
    std::cout << "Processing lineitem (59986052 rows)..." << std::endl;
    build_indexes_for_table(gendb_dir, "lineitem", 59986052, "l_shipdate");
    build_hash_indexes(gendb_dir, "lineitem", 59986052, {"l_orderkey"});

    // orders table: 15000000 rows, sort by o_orderdate, hash indexes on o_custkey
    std::cout << "Processing orders (15000000 rows)..." << std::endl;
    build_indexes_for_table(gendb_dir, "orders", 15000000, "o_orderdate");
    build_hash_indexes(gendb_dir, "orders", 15000000, {"o_custkey"});

    // customer table: 1500000 rows, no sort, hash indexes on c_custkey and c_mktsegment
    std::cout << "Processing customer (1500000 rows)..." << std::endl;
    build_hash_indexes(gendb_dir, "customer", 1500000, {"c_custkey", "c_mktsegment"});

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "\n=== Index Building Complete ===" << std::endl;
    std::cout << "Total time: " << total_ms << "ms (" << (total_ms / 1000.0) << "s)" << std::endl;
    std::cout << "Output directory: " << gendb_dir << std::endl;

    return 0;
}
