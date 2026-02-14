#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <filesystem>
#include <cstring>
#include <sstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

struct Index {
    std::string name;
    std::string type;  // "sorted", "hash", "zone_map"
    std::vector<std::string> columns;
};

struct TableIndex {
    std::string table_name;
    std::vector<Index> indexes;
};

// Parse metadata file (simple key=value format)
std::map<std::string, std::string> read_metadata(const std::string& metadata_file) {
    std::map<std::string, std::string> meta;
    std::ifstream f(metadata_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == ' ') continue;
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            meta[line.substr(0, eq)] = line.substr(eq + 1);
        }
    }
    return meta;
}

// Build sorted index on a single column
void build_sorted_index(
    const std::string& table_dir,
    const std::string& column_name,
    const std::string& index_name
) {
    std::cout << "  Building sorted index: " << index_name << " on " << column_name << std::endl;

    std::string col_file = table_dir + "/" + column_name + ".bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "  Error: Cannot open " << col_file << std::endl;
        return;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    // Infer type from file size - assume int32_t for now
    int32_t row_count = file_size / sizeof(int32_t);
    if (file_size % sizeof(int32_t) != 0) {
        row_count = file_size / sizeof(double);  // Try double
    }

    std::vector<std::pair<int64_t, int32_t>> pairs;

    // Try reading as int32_t first
    if (file_size % sizeof(int32_t) == 0) {
        std::vector<int32_t> data(row_count);
        read(fd, data.data(), row_count * sizeof(int32_t));

        for (int32_t i = 0; i < row_count; ++i) {
            pairs.emplace_back(data[i], i);
        }
    } else if (file_size % sizeof(double) == 0) {
        row_count = file_size / sizeof(double);
        std::vector<double> data(row_count);
        read(fd, data.data(), row_count * sizeof(double));

        for (int32_t i = 0; i < row_count; ++i) {
            pairs.emplace_back(static_cast<int64_t>(data[i]), i);
        }
    }

    close(fd);

    std::sort(pairs.begin(), pairs.end());

    std::string idx_file = table_dir + "/" + index_name + ".idx";
    std::ofstream idx_out(idx_file, std::ios::binary);
    for (const auto& [val, row_idx] : pairs) {
        idx_out.write((char*)&row_idx, sizeof(int32_t));
    }

    std::cout << "    ✓ Wrote " << pairs.size() << " entries to " << idx_file << std::endl;
}

// Build hash index on a single column
void build_hash_index(
    const std::string& table_dir,
    const std::string& column_name,
    const std::string& index_name
) {
    std::cout << "  Building hash index: " << index_name << " on " << column_name << std::endl;

    std::string col_file = table_dir + "/" + column_name + ".bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "  Error: Cannot open " << col_file << std::endl;
        return;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    int32_t row_count = file_size / sizeof(int32_t);
    if (file_size % sizeof(int32_t) != 0) {
        row_count = file_size / sizeof(double);
    }

    std::unordered_map<int64_t, std::vector<int32_t>> hash_map;

    if (file_size % sizeof(int32_t) == 0) {
        std::vector<int32_t> data(row_count);
        read(fd, data.data(), row_count * sizeof(int32_t));

        for (int32_t i = 0; i < row_count; ++i) {
            hash_map[data[i]].push_back(i);
        }
    } else if (file_size % sizeof(double) == 0) {
        row_count = file_size / sizeof(double);
        std::vector<double> data(row_count);
        read(fd, data.data(), row_count * sizeof(double));

        for (int32_t i = 0; i < row_count; ++i) {
            hash_map[static_cast<int64_t>(data[i])].push_back(i);
        }
    }

    close(fd);

    std::string idx_file = table_dir + "/" + index_name + ".hidx";
    std::ofstream idx_out(idx_file, std::ios::binary);

    int32_t num_buckets = std::max((int32_t)row_count / 10, 1000);
    idx_out.write((char*)&num_buckets, sizeof(int32_t));

    for (const auto& [value, row_indices] : hash_map) {
        idx_out.write((char*)&value, sizeof(int64_t));
        int32_t count = row_indices.size();
        idx_out.write((char*)&count, sizeof(int32_t));
        for (int32_t idx : row_indices) {
            idx_out.write((char*)&idx, sizeof(int32_t));
        }
    }

    std::cout << "    ✓ Wrote hash index with " << hash_map.size() << " unique values" << std::endl;
}

// Build zone map (min/max per block)
void build_zone_map(
    const std::string& table_dir,
    const std::string& column_name,
    const std::string& index_name,
    int32_t block_size = 100000
) {
    std::cout << "  Building zone map: " << index_name << " on " << column_name << std::endl;

    std::string col_file = table_dir + "/" + column_name + ".bin";
    int fd = open(col_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "  Error: Cannot open " << col_file << std::endl;
        return;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    int32_t row_count = file_size / sizeof(int32_t);
    if (file_size % sizeof(int32_t) != 0) {
        row_count = file_size / sizeof(double);
    }

    std::vector<std::pair<int64_t, int64_t>> zone_map;

    if (file_size % sizeof(int32_t) == 0) {
        std::vector<int32_t> data(row_count);
        read(fd, data.data(), row_count * sizeof(int32_t));

        for (int64_t block_start = 0; block_start < row_count; block_start += block_size) {
            int64_t block_end = std::min(block_start + (int64_t)block_size, (int64_t)row_count);
            int32_t min_val = data[block_start];
            int32_t max_val = data[block_start];

            for (int64_t i = block_start + 1; i < block_end; ++i) {
                min_val = std::min(min_val, data[i]);
                max_val = std::max(max_val, data[i]);
            }

            zone_map.emplace_back(min_val, max_val);
        }
    } else if (file_size % sizeof(double) == 0) {
        row_count = file_size / sizeof(double);
        std::vector<double> data(row_count);
        read(fd, data.data(), row_count * sizeof(double));

        for (int64_t block_start = 0; block_start < row_count; block_start += block_size) {
            int64_t block_end = std::min(block_start + (int64_t)block_size, (int64_t)row_count);
            double min_val = data[block_start];
            double max_val = data[block_start];

            for (int64_t i = block_start + 1; i < block_end; ++i) {
                min_val = std::min(min_val, data[i]);
                max_val = std::max(max_val, data[i]);
            }

            zone_map.emplace_back((int64_t)min_val, (int64_t)max_val);
        }
    }

    close(fd);

    std::string zm_file = table_dir + "/" + index_name + ".zmap";
    std::ofstream zm_out(zm_file, std::ios::binary);

    int32_t num_zones = zone_map.size();
    zm_out.write((char*)&num_zones, sizeof(int32_t));

    for (const auto& [min_val, max_val] : zone_map) {
        zm_out.write((char*)&min_val, sizeof(int64_t));
        zm_out.write((char*)&max_val, sizeof(int64_t));
    }

    std::cout << "    ✓ Wrote zone map with " << num_zones << " zones" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    std::cout << "Building indexes for GenDB at " << gendb_dir << std::endl;

    // Define indexes to build for each table
    std::vector<TableIndex> table_indexes = {
        {
            "lineitem",
            {
                {"l_shipdate_sorted", "sorted", {"l_shipdate"}},
                {"l_returnflag_zone", "zone_map", {"l_returnflag"}},
                {"l_orderkey_hash", "hash", {"l_orderkey"}}
            }
        },
        {
            "orders",
            {
                {"o_orderdate_sorted", "sorted", {"o_orderdate"}},
                {"o_orderkey_hash", "hash", {"o_orderkey"}},
                {"o_custkey_hash", "hash", {"o_custkey"}}
            }
        },
        {
            "customer",
            {
                {"c_custkey_hash", "hash", {"c_custkey"}},
                {"c_mktsegment_zone", "zone_map", {"c_mktsegment"}}
            }
        },
        {
            "part",
            {
                {"p_partkey_hash", "hash", {"p_partkey"}}
            }
        },
        {
            "supplier",
            {
                {"s_suppkey_hash", "hash", {"s_suppkey"}}
            }
        },
        {
            "partsupp",
            {
                {"ps_partkey_hash", "hash", {"ps_partkey"}},
                {"ps_suppkey_hash", "hash", {"ps_suppkey"}}
            }
        },
        {
            "nation",
            {
                {"n_nationkey_hash", "hash", {"n_nationkey"}}
            }
        },
        {
            "region",
            {
                {"r_regionkey_hash", "hash", {"r_regionkey"}}
            }
        }
    };

    // Build indexes for each table
    for (const auto& table_idx : table_indexes) {
        std::string table_dir = gendb_dir + "/" + table_idx.table_name;

        if (!std::filesystem::exists(table_dir)) {
            std::cout << "Skipping " << table_idx.table_name << " (directory not found)" << std::endl;
            continue;
        }

        std::cout << "Building indexes for " << table_idx.table_name << std::endl;

        for (const auto& index : table_idx.indexes) {
            const std::string& col = index.columns[0];

            if (index.type == "sorted") {
                build_sorted_index(table_dir, col, index.name);
            } else if (index.type == "hash") {
                build_hash_index(table_dir, col, index.name);
            } else if (index.type == "zone_map") {
                build_zone_map(table_dir, col, index.name);
            }
        }
    }

    std::cout << "\n✓ All indexes built successfully" << std::endl;

    return 0;
}
