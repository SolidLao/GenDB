#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <cstring>

namespace gendb {

// Helper: Split string by delimiter
static std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> result;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        result.push_back(item);
    }
    return result;
}

// Helper: Write binary column file
template<typename T>
static void write_column(const std::string& filepath, const std::vector<T>& data) {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Failed to open file for writing: " + filepath);
    }
    size_t count = data.size();
    ofs.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
    ofs.write(reinterpret_cast<const char*>(data.data()), count * sizeof(T));
    ofs.close();
}

// Helper: Write string column (length-prefixed strings)
static void write_string_column(const std::string& filepath, const std::vector<std::string>& data) {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Failed to open file for writing: " + filepath);
    }
    size_t count = data.size();
    ofs.write(reinterpret_cast<const char*>(&count), sizeof(size_t));

    for (const auto& s : data) {
        size_t len = s.size();
        ofs.write(reinterpret_cast<const char*>(&len), sizeof(size_t));
        ofs.write(s.data(), len);
    }
    ofs.close();
}

// Helper: Build and write zone map for int32 column
static void build_zone_map(const std::string& filepath, const std::vector<int32_t>& data, size_t block_size) {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Failed to open zone map file for writing: " + filepath);
    }

    size_t num_blocks = (data.size() + block_size - 1) / block_size;

    // Write header: block_size and num_blocks
    ofs.write(reinterpret_cast<const char*>(&block_size), sizeof(size_t));
    ofs.write(reinterpret_cast<const char*>(&num_blocks), sizeof(size_t));

    // Write zone map entries (min, max, block_start, block_size for each block)
    for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
        size_t block_start = block_idx * block_size;
        size_t block_end = std::min(block_start + block_size, data.size());
        size_t actual_block_size = block_end - block_start;

        if (actual_block_size == 0) continue;

        // Compute min/max for this block
        int32_t min_val = data[block_start];
        int32_t max_val = data[block_start];
        for (size_t i = block_start + 1; i < block_end; ++i) {
            if (data[i] < min_val) min_val = data[i];
            if (data[i] > max_val) max_val = data[i];
        }

        // Write entry
        ofs.write(reinterpret_cast<const char*>(&min_val), sizeof(int32_t));
        ofs.write(reinterpret_cast<const char*>(&max_val), sizeof(int32_t));
        ofs.write(reinterpret_cast<const char*>(&block_start), sizeof(size_t));
        ofs.write(reinterpret_cast<const char*>(&actual_block_size), sizeof(size_t));
    }

    ofs.close();
}

// Lineitem ingestion
void ingest_lineitem(const std::string& tbl_path, const std::string& output_dir) {
    std::cout << "Ingesting lineitem from " << tbl_path << "..." << std::endl;

    LineitemTable table;
    std::ifstream ifs(tbl_path);
    if (!ifs) {
        throw std::runtime_error("Failed to open file: " + tbl_path);
    }

    std::string line;
    size_t row_count = 0;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 16) continue;  // Skip incomplete rows

        table.l_orderkey.push_back(std::stoi(fields[0]));
        table.l_partkey.push_back(std::stoi(fields[1]));
        table.l_suppkey.push_back(std::stoi(fields[2]));
        table.l_linenumber.push_back(std::stoi(fields[3]));
        table.l_quantity.push_back(std::stod(fields[4]));
        table.l_extendedprice.push_back(std::stod(fields[5]));
        table.l_discount.push_back(std::stod(fields[6]));
        table.l_tax.push_back(std::stod(fields[7]));
        table.l_returnflag.push_back(fields[8].empty() ? 0 : static_cast<uint8_t>(fields[8][0]));
        table.l_linestatus.push_back(fields[9].empty() ? 0 : static_cast<uint8_t>(fields[9][0]));
        table.l_shipdate.push_back(parse_date(fields[10]));
        table.l_commitdate.push_back(parse_date(fields[11]));
        table.l_receiptdate.push_back(parse_date(fields[12]));
        table.l_shipinstruct.push_back(fields[13]);
        table.l_shipmode.push_back(fields[14]);
        table.l_comment.push_back(fields[15]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Loaded " << row_count << " rows..." << std::endl;
        }
    }
    ifs.close();

    std::cout << "  Total rows: " << row_count << std::endl;

    // Sort lineitem by l_shipdate for zone map effectiveness (as per storage design)
    std::cout << "  Sorting by l_shipdate..." << std::endl;
    std::vector<size_t> indices(row_count);
    for (size_t i = 0; i < row_count; ++i) {
        indices[i] = i;
    }
    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        if (table.l_shipdate[a] != table.l_shipdate[b]) {
            return table.l_shipdate[a] < table.l_shipdate[b];
        }
        return table.l_orderkey[a] < table.l_orderkey[b];  // Secondary sort by orderkey
    });

    // Reorder all columns according to sorted indices
    LineitemTable sorted_table;
    sorted_table.l_orderkey.reserve(row_count);
    sorted_table.l_partkey.reserve(row_count);
    sorted_table.l_suppkey.reserve(row_count);
    sorted_table.l_linenumber.reserve(row_count);
    sorted_table.l_quantity.reserve(row_count);
    sorted_table.l_extendedprice.reserve(row_count);
    sorted_table.l_discount.reserve(row_count);
    sorted_table.l_tax.reserve(row_count);
    sorted_table.l_returnflag.reserve(row_count);
    sorted_table.l_linestatus.reserve(row_count);
    sorted_table.l_shipdate.reserve(row_count);
    sorted_table.l_commitdate.reserve(row_count);
    sorted_table.l_receiptdate.reserve(row_count);
    sorted_table.l_shipinstruct.reserve(row_count);
    sorted_table.l_shipmode.reserve(row_count);
    sorted_table.l_comment.reserve(row_count);

    for (size_t idx : indices) {
        sorted_table.l_orderkey.push_back(table.l_orderkey[idx]);
        sorted_table.l_partkey.push_back(table.l_partkey[idx]);
        sorted_table.l_suppkey.push_back(table.l_suppkey[idx]);
        sorted_table.l_linenumber.push_back(table.l_linenumber[idx]);
        sorted_table.l_quantity.push_back(table.l_quantity[idx]);
        sorted_table.l_extendedprice.push_back(table.l_extendedprice[idx]);
        sorted_table.l_discount.push_back(table.l_discount[idx]);
        sorted_table.l_tax.push_back(table.l_tax[idx]);
        sorted_table.l_returnflag.push_back(table.l_returnflag[idx]);
        sorted_table.l_linestatus.push_back(table.l_linestatus[idx]);
        sorted_table.l_shipdate.push_back(table.l_shipdate[idx]);
        sorted_table.l_commitdate.push_back(table.l_commitdate[idx]);
        sorted_table.l_receiptdate.push_back(table.l_receiptdate[idx]);
        sorted_table.l_shipinstruct.push_back(table.l_shipinstruct[idx]);
        sorted_table.l_shipmode.push_back(table.l_shipmode[idx]);
        sorted_table.l_comment.push_back(table.l_comment[idx]);
    }

    // Replace original table with sorted table
    table = std::move(sorted_table);
    std::cout << "  Sorting complete." << std::endl;

    // Build zone map on l_shipdate (64K rows per block as per storage design)
    std::cout << "  Building zone map on l_shipdate..." << std::endl;
    const size_t BLOCK_SIZE = 65536;
    build_zone_map(output_dir + "/lineitem.l_shipdate.zonemap", table.l_shipdate, BLOCK_SIZE);
    std::cout << "  Zone map complete." << std::endl;

    std::cout << "  Writing binary columns..." << std::endl;

    // Write columns in parallel
    std::vector<std::thread> threads;
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_orderkey.bin", table.l_orderkey); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_partkey.bin", table.l_partkey); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_suppkey.bin", table.l_suppkey); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_linenumber.bin", table.l_linenumber); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_quantity.bin", table.l_quantity); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_extendedprice.bin", table.l_extendedprice); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_discount.bin", table.l_discount); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_tax.bin", table.l_tax); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_returnflag.bin", table.l_returnflag); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_linestatus.bin", table.l_linestatus); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_shipdate.bin", table.l_shipdate); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_commitdate.bin", table.l_commitdate); });
    threads.emplace_back([&]() { write_column(output_dir + "/lineitem.l_receiptdate.bin", table.l_receiptdate); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/lineitem.l_shipinstruct.bin", table.l_shipinstruct); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/lineitem.l_shipmode.bin", table.l_shipmode); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/lineitem.l_comment.bin", table.l_comment); });

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "  Lineitem ingestion complete." << std::endl;
}

// Orders ingestion
void ingest_orders(const std::string& tbl_path, const std::string& output_dir) {
    std::cout << "Ingesting orders from " << tbl_path << "..." << std::endl;

    OrdersTable table;
    std::ifstream ifs(tbl_path);
    if (!ifs) {
        throw std::runtime_error("Failed to open file: " + tbl_path);
    }

    std::string line;
    size_t row_count = 0;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 9) continue;

        table.o_orderkey.push_back(std::stoi(fields[0]));
        table.o_custkey.push_back(std::stoi(fields[1]));
        table.o_orderstatus.push_back(fields[2].empty() ? 0 : static_cast<uint8_t>(fields[2][0]));
        table.o_totalprice.push_back(std::stod(fields[3]));
        table.o_orderdate.push_back(parse_date(fields[4]));
        table.o_orderpriority.push_back(fields[5]);
        table.o_clerk.push_back(fields[6]);
        table.o_shippriority.push_back(std::stoi(fields[7]));
        table.o_comment.push_back(fields[8]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Loaded " << row_count << " rows..." << std::endl;
        }
    }
    ifs.close();

    std::cout << "  Total rows: " << row_count << std::endl;

    // Build zone map on o_orderdate (64K rows per block as per storage design)
    std::cout << "  Building zone map on o_orderdate..." << std::endl;
    const size_t BLOCK_SIZE = 65536;
    build_zone_map(output_dir + "/orders.o_orderdate.zonemap", table.o_orderdate, BLOCK_SIZE);
    std::cout << "  Zone map complete." << std::endl;

    std::cout << "  Writing binary columns..." << std::endl;

    std::vector<std::thread> threads;
    threads.emplace_back([&]() { write_column(output_dir + "/orders.o_orderkey.bin", table.o_orderkey); });
    threads.emplace_back([&]() { write_column(output_dir + "/orders.o_custkey.bin", table.o_custkey); });
    threads.emplace_back([&]() { write_column(output_dir + "/orders.o_orderstatus.bin", table.o_orderstatus); });
    threads.emplace_back([&]() { write_column(output_dir + "/orders.o_totalprice.bin", table.o_totalprice); });
    threads.emplace_back([&]() { write_column(output_dir + "/orders.o_orderdate.bin", table.o_orderdate); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/orders.o_orderpriority.bin", table.o_orderpriority); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/orders.o_clerk.bin", table.o_clerk); });
    threads.emplace_back([&]() { write_column(output_dir + "/orders.o_shippriority.bin", table.o_shippriority); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/orders.o_comment.bin", table.o_comment); });

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "  Orders ingestion complete." << std::endl;
}

// Customer ingestion
void ingest_customer(const std::string& tbl_path, const std::string& output_dir) {
    std::cout << "Ingesting customer from " << tbl_path << "..." << std::endl;

    CustomerTable table;
    std::ifstream ifs(tbl_path);
    if (!ifs) {
        throw std::runtime_error("Failed to open file: " + tbl_path);
    }

    std::string line;
    size_t row_count = 0;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 8) continue;

        table.c_custkey.push_back(std::stoi(fields[0]));
        table.c_name.push_back(fields[1]);
        table.c_address.push_back(fields[2]);
        table.c_nationkey.push_back(std::stoi(fields[3]));
        table.c_phone.push_back(fields[4]);
        table.c_acctbal.push_back(std::stod(fields[5]));
        table.c_mktsegment.push_back(fields[6]);
        table.c_comment.push_back(fields[7]);

        row_count++;
    }
    ifs.close();

    std::cout << "  Total rows: " << row_count << std::endl;
    std::cout << "  Writing binary columns..." << std::endl;

    std::vector<std::thread> threads;
    threads.emplace_back([&]() { write_column(output_dir + "/customer.c_custkey.bin", table.c_custkey); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/customer.c_name.bin", table.c_name); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/customer.c_address.bin", table.c_address); });
    threads.emplace_back([&]() { write_column(output_dir + "/customer.c_nationkey.bin", table.c_nationkey); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/customer.c_phone.bin", table.c_phone); });
    threads.emplace_back([&]() { write_column(output_dir + "/customer.c_acctbal.bin", table.c_acctbal); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/customer.c_mktsegment.bin", table.c_mktsegment); });
    threads.emplace_back([&]() { write_string_column(output_dir + "/customer.c_comment.bin", table.c_comment); });

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "  Customer ingestion complete." << std::endl;
}

// Simplified ingestion stubs for unused tables
void ingest_part(const std::string& tbl_path, const std::string& output_dir) {
    std::cout << "Skipping part table (not used in queries)" << std::endl;
}

void ingest_partsupp(const std::string& tbl_path, const std::string& output_dir) {
    std::cout << "Skipping partsupp table (not used in queries)" << std::endl;
}

void ingest_supplier(const std::string& tbl_path, const std::string& output_dir) {
    std::cout << "Skipping supplier table (not used in queries)" << std::endl;
}

void ingest_nation(const std::string& tbl_path, const std::string& output_dir) {
    std::cout << "Skipping nation table (not used in queries)" << std::endl;
}

void ingest_region(const std::string& tbl_path, const std::string& output_dir) {
    std::cout << "Skipping region table (not used in queries)" << std::endl;
}

// mmap functions for query-time column access
MappedColumn<int32_t> mmap_int32_column(const std::string& gendb_dir, const std::string& table, const std::string& column) {
    std::string filepath = gendb_dir + "/" + table + "." + column + ".bin";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Failed to open column file: " + filepath);
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        throw std::runtime_error("Failed to stat column file: " + filepath);
    }

    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        throw std::runtime_error("Failed to mmap column file: " + filepath);
    }

    // Advise sequential access
    madvise(addr, sb.st_size, MADV_SEQUENTIAL);

    MappedColumn<int32_t> result;
    result.mmap_ptr = addr;
    result.mmap_size = sb.st_size;

    // Read size from first 8 bytes
    size_t* size_ptr = reinterpret_cast<size_t*>(addr);
    result.size = *size_ptr;
    result.data = reinterpret_cast<int32_t*>(size_ptr + 1);

    return result;
}

MappedColumn<double> mmap_double_column(const std::string& gendb_dir, const std::string& table, const std::string& column) {
    std::string filepath = gendb_dir + "/" + table + "." + column + ".bin";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Failed to open column file: " + filepath);
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        throw std::runtime_error("Failed to stat column file: " + filepath);
    }

    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        throw std::runtime_error("Failed to mmap column file: " + filepath);
    }

    madvise(addr, sb.st_size, MADV_SEQUENTIAL);

    MappedColumn<double> result;
    result.mmap_ptr = addr;
    result.mmap_size = sb.st_size;

    size_t* size_ptr = reinterpret_cast<size_t*>(addr);
    result.size = *size_ptr;
    result.data = reinterpret_cast<double*>(size_ptr + 1);

    return result;
}

MappedColumn<uint8_t> mmap_uint8_column(const std::string& gendb_dir, const std::string& table, const std::string& column) {
    std::string filepath = gendb_dir + "/" + table + "." + column + ".bin";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Failed to open column file: " + filepath);
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        throw std::runtime_error("Failed to stat column file: " + filepath);
    }

    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        throw std::runtime_error("Failed to mmap column file: " + filepath);
    }

    madvise(addr, sb.st_size, MADV_SEQUENTIAL);

    MappedColumn<uint8_t> result;
    result.mmap_ptr = addr;
    result.mmap_size = sb.st_size;

    size_t* size_ptr = reinterpret_cast<size_t*>(addr);
    result.size = *size_ptr;
    result.data = reinterpret_cast<uint8_t*>(size_ptr + 1);

    return result;
}

std::vector<std::string> mmap_string_column(const std::string& gendb_dir, const std::string& table, const std::string& column) {
    std::string filepath = gendb_dir + "/" + table + "." + column + ".bin";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Failed to open column file: " + filepath);
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        throw std::runtime_error("Failed to stat column file: " + filepath);
    }

    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        throw std::runtime_error("Failed to mmap column file: " + filepath);
    }

    madvise(addr, sb.st_size, MADV_SEQUENTIAL);

    // Read count from first 8 bytes
    size_t* size_ptr = reinterpret_cast<size_t*>(addr);
    size_t count = *size_ptr;

    // Parse variable-length strings
    std::vector<std::string> result;
    result.reserve(count);

    char* data_ptr = reinterpret_cast<char*>(size_ptr + 1);
    char* end_ptr = reinterpret_cast<char*>(addr) + sb.st_size;

    for (size_t i = 0; i < count; ++i) {
        if (data_ptr + sizeof(size_t) > end_ptr) {
            munmap(addr, sb.st_size);
            throw std::runtime_error("Corrupted string column file: " + filepath);
        }

        size_t str_len = *reinterpret_cast<size_t*>(data_ptr);
        data_ptr += sizeof(size_t);

        if (data_ptr + str_len > end_ptr) {
            munmap(addr, sb.st_size);
            throw std::runtime_error("Corrupted string column file: " + filepath);
        }

        result.emplace_back(data_ptr, str_len);
        data_ptr += str_len;
    }

    // Unmap after reading all strings into memory
    munmap(addr, sb.st_size);

    return result;
}

void unmap_column(void* mmap_ptr, size_t mmap_size) {
    if (mmap_ptr && mmap_size > 0) {
        munmap(mmap_ptr, mmap_size);
    }
}

// Load zone map for a column
ZoneMap load_zone_map(const std::string& gendb_dir, const std::string& table, const std::string& column) {
    std::string filepath = gendb_dir + "/" + table + "." + column + ".zonemap";

    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
        // Zone map file doesn't exist - return empty zone map
        ZoneMap zm;
        zm.block_granularity = 0;
        return zm;
    }

    ZoneMap zm;

    // Read header
    size_t num_blocks;
    ifs.read(reinterpret_cast<char*>(&zm.block_granularity), sizeof(size_t));
    ifs.read(reinterpret_cast<char*>(&num_blocks), sizeof(size_t));

    zm.blocks.reserve(num_blocks);

    // Read zone map entries
    for (size_t i = 0; i < num_blocks; ++i) {
        ZoneMapEntry entry;
        ifs.read(reinterpret_cast<char*>(&entry.min_value), sizeof(int32_t));
        ifs.read(reinterpret_cast<char*>(&entry.max_value), sizeof(int32_t));
        ifs.read(reinterpret_cast<char*>(&entry.block_start), sizeof(size_t));
        ifs.read(reinterpret_cast<char*>(&entry.block_size), sizeof(size_t));
        zm.blocks.push_back(entry);
    }

    ifs.close();
    return zm;
}

}  // namespace gendb
