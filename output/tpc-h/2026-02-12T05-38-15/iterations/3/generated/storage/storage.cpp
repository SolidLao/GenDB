#include "storage.h"
#include "../utils/date_utils.h"

#include <fstream>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <iostream>
#include <chrono>
#include <algorithm>

namespace gendb {
namespace storage {

// Helper: parse pipe-delimited line into fields
static std::vector<std::string> parse_line(const std::string& line) {
    std::vector<std::string> fields;
    size_t start = 0;
    size_t pos = 0;
    while ((pos = line.find('|', start)) != std::string::npos) {
        fields.push_back(line.substr(start, pos - start));
        start = pos + 1;
    }
    // Handle trailing pipe (TPC-H format has trailing delimiter)
    return fields;
}

// Helper: mmap a .tbl file for fast ingestion
static const char* mmap_file(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Failed to open file: " + path);
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        throw std::runtime_error("Failed to stat file: " + path);
    }

    file_size = st.st_size;
    const char* data = static_cast<const char*>(
        mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    if (data == MAP_FAILED) {
        throw std::runtime_error("Failed to mmap file: " + path);
    }

    madvise((void*)data, file_size, MADV_SEQUENTIAL);
    return data;
}

// Helper: write column to binary file
template<typename T>
static void write_column(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        throw std::runtime_error("Failed to open file for writing: " + path);
    }

    // Set 1MB write buffer
    char buf[1 << 20];
    setvbuf(f, buf, _IOFBF, sizeof(buf));

    fwrite(data.data(), sizeof(T), data.size(), f);
    fclose(f);
}

// Specialization for std::string columns (write as length-prefixed)
static void write_string_column(const std::string& path, const std::vector<std::string>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        throw std::runtime_error("Failed to open file for writing: " + path);
    }

    char buf[1 << 20];
    setvbuf(f, buf, _IOFBF, sizeof(buf));

    for (const auto& str : data) {
        uint32_t len = str.size();
        fwrite(&len, sizeof(uint32_t), 1, f);
        fwrite(str.data(), 1, len, f);
    }
    fclose(f);
}

// Helper: write metadata file with row count
static void write_metadata(const std::string& path, size_t row_count) {
    std::ofstream out(path);
    out << "{\"row_count\": " << row_count << "}\n";
}

// Helper: write zone map metadata (min/max per block)
struct ZoneMapEntry {
    size_t block_id;
    int32_t min_val;
    int32_t max_val;
    size_t start_row;
    size_t end_row;
};

static void write_zone_map(const std::string& path, const std::vector<ZoneMapEntry>& zone_map) {
    std::ofstream out(path);
    out << "{\"zone_map\": [\n";
    for (size_t i = 0; i < zone_map.size(); ++i) {
        const auto& entry = zone_map[i];
        out << "  {\"block_id\": " << entry.block_id
            << ", \"min\": " << entry.min_val
            << ", \"max\": " << entry.max_val
            << ", \"start_row\": " << entry.start_row
            << ", \"end_row\": " << entry.end_row << "}";
        if (i + 1 < zone_map.size()) out << ",";
        out << "\n";
    }
    out << "]}\n";
}

// Parse double from const char*
static double parse_double(const char* ptr, const char** end) {
    return strtod(ptr, const_cast<char**>(end));
}

// Parse int from const char*
static int32_t parse_int(const char* ptr, const char** end) {
    return static_cast<int32_t>(strtol(ptr, const_cast<char**>(end), 10));
}

// Lineitem ingestion
void ingest_lineitem(const std::string& tbl_file, const std::string& output_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_file, file_size);

    // Estimate row count and pre-allocate
    size_t estimated_rows = file_size / 150; // ~150 bytes per line
    LineitemTable table;
    table.l_orderkey.reserve(estimated_rows);
    table.l_partkey.reserve(estimated_rows);
    table.l_suppkey.reserve(estimated_rows);
    table.l_linenumber.reserve(estimated_rows);
    table.l_quantity.reserve(estimated_rows);
    table.l_extendedprice.reserve(estimated_rows);
    table.l_discount.reserve(estimated_rows);
    table.l_tax.reserve(estimated_rows);
    table.l_returnflag.reserve(estimated_rows);
    table.l_linestatus.reserve(estimated_rows);
    table.l_shipdate.reserve(estimated_rows);
    table.l_commitdate.reserve(estimated_rows);
    table.l_receiptdate.reserve(estimated_rows);
    table.l_shipinstruct.reserve(estimated_rows);
    table.l_shipmode.reserve(estimated_rows);
    table.l_comment.reserve(estimated_rows);

    // Parse line by line
    const char* ptr = data;
    const char* end = data + file_size;
    while (ptr < end) {
        const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end - ptr));
        if (!line_end) line_end = end;

        // Parse fields separated by '|'
        const char* field_start = ptr;
        int field_idx = 0;
        const char* field_end;

        while (field_start < line_end && field_idx < 16) {
            field_end = static_cast<const char*>(memchr(field_start, '|', line_end - field_start));
            if (!field_end) field_end = line_end;

            switch (field_idx) {
                case 0: table.l_orderkey.push_back(parse_int(field_start, &field_end)); break;
                case 1: table.l_partkey.push_back(parse_int(field_start, &field_end)); break;
                case 2: table.l_suppkey.push_back(parse_int(field_start, &field_end)); break;
                case 3: table.l_linenumber.push_back(parse_int(field_start, &field_end)); break;
                case 4: table.l_quantity.push_back(parse_double(field_start, &field_end)); break;
                case 5: table.l_extendedprice.push_back(parse_double(field_start, &field_end)); break;
                case 6: table.l_discount.push_back(parse_double(field_start, &field_end)); break;
                case 7: table.l_tax.push_back(parse_double(field_start, &field_end)); break;
                case 8: table.l_returnflag.push_back(*field_start); break;
                case 9: table.l_linestatus.push_back(*field_start); break;
                case 10: table.l_shipdate.push_back(date_utils::parse_date_fast(field_start)); break;
                case 11: table.l_commitdate.push_back(date_utils::parse_date_fast(field_start)); break;
                case 12: table.l_receiptdate.push_back(date_utils::parse_date_fast(field_start)); break;
                case 13: table.l_shipinstruct.emplace_back(field_start, field_end - field_start); break;
                case 14: table.l_shipmode.emplace_back(field_start, field_end - field_start); break;
                case 15: table.l_comment.emplace_back(field_start, field_end - field_start); break;
            }

            field_start = field_end + 1;
            field_idx++;
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);

    // Sort by l_shipdate for zone map efficiency
    std::vector<size_t> indices(table.size());
    for (size_t i = 0; i < indices.size(); ++i) indices[i] = i;

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.l_shipdate[a] < table.l_shipdate[b];
    });

    // Reorder all columns
    auto reorder = [&indices](auto& col) {
        std::remove_reference_t<decltype(col)> temp(col.size());
        for (size_t i = 0; i < indices.size(); ++i) {
            temp[i] = std::move(col[indices[i]]);
        }
        col = std::move(temp);
    };

    reorder(table.l_orderkey);
    reorder(table.l_partkey);
    reorder(table.l_suppkey);
    reorder(table.l_linenumber);
    reorder(table.l_quantity);
    reorder(table.l_extendedprice);
    reorder(table.l_discount);
    reorder(table.l_tax);
    reorder(table.l_returnflag);
    reorder(table.l_linestatus);
    reorder(table.l_shipdate);
    reorder(table.l_commitdate);
    reorder(table.l_receiptdate);
    reorder(table.l_shipinstruct);
    reorder(table.l_shipmode);
    reorder(table.l_comment);

    // Generate zone maps for l_shipdate, l_discount, l_quantity
    // Block size: 100K rows (as per storage design)
    const size_t BLOCK_SIZE = 100000;
    std::vector<ZoneMapEntry> shipdate_zonemap;
    std::vector<ZoneMapEntry> discount_zonemap;
    std::vector<ZoneMapEntry> quantity_zonemap;

    size_t num_blocks = (table.size() + BLOCK_SIZE - 1) / BLOCK_SIZE;
    for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
        size_t start_row = block_id * BLOCK_SIZE;
        size_t end_row = std::min(start_row + BLOCK_SIZE, table.size());

        // Shipdate zone map (int32_t)
        int32_t min_shipdate = table.l_shipdate[start_row];
        int32_t max_shipdate = table.l_shipdate[start_row];
        for (size_t i = start_row + 1; i < end_row; ++i) {
            min_shipdate = std::min(min_shipdate, table.l_shipdate[i]);
            max_shipdate = std::max(max_shipdate, table.l_shipdate[i]);
        }
        shipdate_zonemap.push_back({block_id, min_shipdate, max_shipdate, start_row, end_row});

        // Discount zone map (store as int32_t with 2 decimal precision)
        int32_t min_discount = static_cast<int32_t>(table.l_discount[start_row] * 100);
        int32_t max_discount = static_cast<int32_t>(table.l_discount[start_row] * 100);
        for (size_t i = start_row + 1; i < end_row; ++i) {
            int32_t disc = static_cast<int32_t>(table.l_discount[i] * 100);
            min_discount = std::min(min_discount, disc);
            max_discount = std::max(max_discount, disc);
        }
        discount_zonemap.push_back({block_id, min_discount, max_discount, start_row, end_row});

        // Quantity zone map (store as int32_t)
        int32_t min_quantity = static_cast<int32_t>(table.l_quantity[start_row]);
        int32_t max_quantity = static_cast<int32_t>(table.l_quantity[start_row]);
        for (size_t i = start_row + 1; i < end_row; ++i) {
            int32_t qty = static_cast<int32_t>(table.l_quantity[i]);
            min_quantity = std::min(min_quantity, qty);
            max_quantity = std::max(max_quantity, qty);
        }
        quantity_zonemap.push_back({block_id, min_quantity, max_quantity, start_row, end_row});
    }

    // Write binary columns
    write_column(output_dir + "/lineitem_l_orderkey.bin", table.l_orderkey);
    write_column(output_dir + "/lineitem_l_partkey.bin", table.l_partkey);
    write_column(output_dir + "/lineitem_l_suppkey.bin", table.l_suppkey);
    write_column(output_dir + "/lineitem_l_linenumber.bin", table.l_linenumber);
    write_column(output_dir + "/lineitem_l_quantity.bin", table.l_quantity);
    write_column(output_dir + "/lineitem_l_extendedprice.bin", table.l_extendedprice);
    write_column(output_dir + "/lineitem_l_discount.bin", table.l_discount);
    write_column(output_dir + "/lineitem_l_tax.bin", table.l_tax);
    write_column(output_dir + "/lineitem_l_returnflag.bin", table.l_returnflag);
    write_column(output_dir + "/lineitem_l_linestatus.bin", table.l_linestatus);
    write_column(output_dir + "/lineitem_l_shipdate.bin", table.l_shipdate);
    write_column(output_dir + "/lineitem_l_commitdate.bin", table.l_commitdate);
    write_column(output_dir + "/lineitem_l_receiptdate.bin", table.l_receiptdate);
    write_string_column(output_dir + "/lineitem_l_shipinstruct.bin", table.l_shipinstruct);
    write_string_column(output_dir + "/lineitem_l_shipmode.bin", table.l_shipmode);
    write_string_column(output_dir + "/lineitem_l_comment.bin", table.l_comment);
    write_metadata(output_dir + "/lineitem_metadata.json", table.size());

    // Write zone maps
    write_zone_map(output_dir + "/lineitem_l_shipdate_zonemap.json", shipdate_zonemap);
    write_zone_map(output_dir + "/lineitem_l_discount_zonemap.json", discount_zonemap);
    write_zone_map(output_dir + "/lineitem_l_quantity_zonemap.json", quantity_zonemap);

    auto elapsed = std::chrono::duration<double>(
        std::chrono::high_resolution_clock::now() - start).count();
    std::cout << "lineitem: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Orders ingestion
void ingest_orders(const std::string& tbl_file, const std::string& output_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_file, file_size);

    size_t estimated_rows = file_size / 100;
    OrdersTable table;
    table.o_orderkey.reserve(estimated_rows);
    table.o_custkey.reserve(estimated_rows);
    table.o_orderstatus.reserve(estimated_rows);
    table.o_totalprice.reserve(estimated_rows);
    table.o_orderdate.reserve(estimated_rows);
    table.o_orderpriority.reserve(estimated_rows);
    table.o_clerk.reserve(estimated_rows);
    table.o_shippriority.reserve(estimated_rows);
    table.o_comment.reserve(estimated_rows);

    const char* ptr = data;
    const char* end = data + file_size;
    while (ptr < end) {
        const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end - ptr));
        if (!line_end) line_end = end;

        const char* field_start = ptr;
        int field_idx = 0;
        const char* field_end;

        while (field_start < line_end && field_idx < 9) {
            field_end = static_cast<const char*>(memchr(field_start, '|', line_end - field_start));
            if (!field_end) field_end = line_end;

            switch (field_idx) {
                case 0: table.o_orderkey.push_back(parse_int(field_start, &field_end)); break;
                case 1: table.o_custkey.push_back(parse_int(field_start, &field_end)); break;
                case 2: table.o_orderstatus.push_back(*field_start); break;
                case 3: table.o_totalprice.push_back(parse_double(field_start, &field_end)); break;
                case 4: table.o_orderdate.push_back(date_utils::parse_date_fast(field_start)); break;
                case 5: table.o_orderpriority.emplace_back(field_start, field_end - field_start); break;
                case 6: table.o_clerk.emplace_back(field_start, field_end - field_start); break;
                case 7: table.o_shippriority.push_back(parse_int(field_start, &field_end)); break;
                case 8: table.o_comment.emplace_back(field_start, field_end - field_start); break;
            }

            field_start = field_end + 1;
            field_idx++;
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);

    write_column(output_dir + "/orders_o_orderkey.bin", table.o_orderkey);
    write_column(output_dir + "/orders_o_custkey.bin", table.o_custkey);
    write_column(output_dir + "/orders_o_orderstatus.bin", table.o_orderstatus);
    write_column(output_dir + "/orders_o_totalprice.bin", table.o_totalprice);
    write_column(output_dir + "/orders_o_orderdate.bin", table.o_orderdate);
    write_string_column(output_dir + "/orders_o_orderpriority.bin", table.o_orderpriority);
    write_string_column(output_dir + "/orders_o_clerk.bin", table.o_clerk);
    write_column(output_dir + "/orders_o_shippriority.bin", table.o_shippriority);
    write_string_column(output_dir + "/orders_o_comment.bin", table.o_comment);
    write_metadata(output_dir + "/orders_metadata.json", table.size());

    auto elapsed = std::chrono::duration<double>(
        std::chrono::high_resolution_clock::now() - start).count();
    std::cout << "orders: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Customer ingestion
void ingest_customer(const std::string& tbl_file, const std::string& output_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_file, file_size);

    size_t estimated_rows = file_size / 150;
    CustomerTable table;
    table.c_custkey.reserve(estimated_rows);
    table.c_name.reserve(estimated_rows);
    table.c_address.reserve(estimated_rows);
    table.c_nationkey.reserve(estimated_rows);
    table.c_phone.reserve(estimated_rows);
    table.c_acctbal.reserve(estimated_rows);
    table.c_mktsegment.reserve(estimated_rows);
    table.c_comment.reserve(estimated_rows);

    // c_mktsegment dictionary: BUILDING=0, AUTOMOBILE=1, HOUSEHOLD=2, MACHINERY=3, FURNITURE=4
    auto encode_mktsegment = [](const char* str, size_t len) -> uint8_t {
        if (len >= 8 && strncmp(str, "BUILDING", 8) == 0) return 0;
        if (len >= 10 && strncmp(str, "AUTOMOBILE", 10) == 0) return 1;
        if (len >= 9 && strncmp(str, "HOUSEHOLD", 9) == 0) return 2;
        if (len >= 9 && strncmp(str, "MACHINERY", 9) == 0) return 3;
        if (len >= 9 && strncmp(str, "FURNITURE", 9) == 0) return 4;
        return 255; // Unknown
    };

    const char* ptr = data;
    const char* end = data + file_size;
    while (ptr < end) {
        const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end - ptr));
        if (!line_end) line_end = end;

        const char* field_start = ptr;
        int field_idx = 0;
        const char* field_end;

        while (field_start < line_end && field_idx < 8) {
            field_end = static_cast<const char*>(memchr(field_start, '|', line_end - field_start));
            if (!field_end) field_end = line_end;

            switch (field_idx) {
                case 0: table.c_custkey.push_back(parse_int(field_start, &field_end)); break;
                case 1: table.c_name.emplace_back(field_start, field_end - field_start); break;
                case 2: table.c_address.emplace_back(field_start, field_end - field_start); break;
                case 3: table.c_nationkey.push_back(parse_int(field_start, &field_end)); break;
                case 4: table.c_phone.emplace_back(field_start, field_end - field_start); break;
                case 5: table.c_acctbal.push_back(parse_double(field_start, &field_end)); break;
                case 6: table.c_mktsegment.push_back(encode_mktsegment(field_start, field_end - field_start)); break;
                case 7: table.c_comment.emplace_back(field_start, field_end - field_start); break;
            }

            field_start = field_end + 1;
            field_idx++;
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);

    write_column(output_dir + "/customer_c_custkey.bin", table.c_custkey);
    write_string_column(output_dir + "/customer_c_name.bin", table.c_name);
    write_string_column(output_dir + "/customer_c_address.bin", table.c_address);
    write_column(output_dir + "/customer_c_nationkey.bin", table.c_nationkey);
    write_string_column(output_dir + "/customer_c_phone.bin", table.c_phone);
    write_column(output_dir + "/customer_c_acctbal.bin", table.c_acctbal);
    write_column(output_dir + "/customer_c_mktsegment.bin", table.c_mktsegment);
    write_string_column(output_dir + "/customer_c_comment.bin", table.c_comment);
    write_metadata(output_dir + "/customer_metadata.json", table.size());

    auto elapsed = std::chrono::duration<double>(
        std::chrono::high_resolution_clock::now() - start).count();
    std::cout << "customer: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Nation ingestion
void ingest_nation(const std::string& tbl_file, const std::string& output_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream file(tbl_file);
    std::string line;
    NationTable table;

    while (std::getline(file, line)) {
        auto fields = parse_line(line);
        if (fields.size() >= 4) {
            table.n_nationkey.push_back(std::stoi(fields[0]));
            table.n_name.push_back(fields[1]);
            table.n_regionkey.push_back(std::stoi(fields[2]));
            table.n_comment.push_back(fields[3]);
        }
    }

    write_column(output_dir + "/nation_n_nationkey.bin", table.n_nationkey);
    write_string_column(output_dir + "/nation_n_name.bin", table.n_name);
    write_column(output_dir + "/nation_n_regionkey.bin", table.n_regionkey);
    write_string_column(output_dir + "/nation_n_comment.bin", table.n_comment);
    write_metadata(output_dir + "/nation_metadata.json", table.size());

    auto elapsed = std::chrono::duration<double>(
        std::chrono::high_resolution_clock::now() - start).count();
    std::cout << "nation: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Region ingestion
void ingest_region(const std::string& tbl_file, const std::string& output_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::ifstream file(tbl_file);
    std::string line;
    RegionTable table;

    while (std::getline(file, line)) {
        auto fields = parse_line(line);
        if (fields.size() >= 3) {
            table.r_regionkey.push_back(std::stoi(fields[0]));
            table.r_name.push_back(fields[1]);
            table.r_comment.push_back(fields[2]);
        }
    }

    write_column(output_dir + "/region_r_regionkey.bin", table.r_regionkey);
    write_string_column(output_dir + "/region_r_name.bin", table.r_name);
    write_string_column(output_dir + "/region_r_comment.bin", table.r_comment);
    write_metadata(output_dir + "/region_metadata.json", table.size());

    auto elapsed = std::chrono::duration<double>(
        std::chrono::high_resolution_clock::now() - start).count();
    std::cout << "region: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Supplier ingestion
void ingest_supplier(const std::string& tbl_file, const std::string& output_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_file, file_size);

    size_t estimated_rows = file_size / 150;
    SupplierTable table;
    table.s_suppkey.reserve(estimated_rows);
    table.s_name.reserve(estimated_rows);
    table.s_address.reserve(estimated_rows);
    table.s_nationkey.reserve(estimated_rows);
    table.s_phone.reserve(estimated_rows);
    table.s_acctbal.reserve(estimated_rows);
    table.s_comment.reserve(estimated_rows);

    const char* ptr = data;
    const char* end = data + file_size;
    while (ptr < end) {
        const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end - ptr));
        if (!line_end) line_end = end;

        const char* field_start = ptr;
        int field_idx = 0;
        const char* field_end;

        while (field_start < line_end && field_idx < 7) {
            field_end = static_cast<const char*>(memchr(field_start, '|', line_end - field_start));
            if (!field_end) field_end = line_end;

            switch (field_idx) {
                case 0: table.s_suppkey.push_back(parse_int(field_start, &field_end)); break;
                case 1: table.s_name.emplace_back(field_start, field_end - field_start); break;
                case 2: table.s_address.emplace_back(field_start, field_end - field_start); break;
                case 3: table.s_nationkey.push_back(parse_int(field_start, &field_end)); break;
                case 4: table.s_phone.emplace_back(field_start, field_end - field_start); break;
                case 5: table.s_acctbal.push_back(parse_double(field_start, &field_end)); break;
                case 6: table.s_comment.emplace_back(field_start, field_end - field_start); break;
            }

            field_start = field_end + 1;
            field_idx++;
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);

    write_column(output_dir + "/supplier_s_suppkey.bin", table.s_suppkey);
    write_string_column(output_dir + "/supplier_s_name.bin", table.s_name);
    write_string_column(output_dir + "/supplier_s_address.bin", table.s_address);
    write_column(output_dir + "/supplier_s_nationkey.bin", table.s_nationkey);
    write_string_column(output_dir + "/supplier_s_phone.bin", table.s_phone);
    write_column(output_dir + "/supplier_s_acctbal.bin", table.s_acctbal);
    write_string_column(output_dir + "/supplier_s_comment.bin", table.s_comment);
    write_metadata(output_dir + "/supplier_metadata.json", table.size());

    auto elapsed = std::chrono::duration<double>(
        std::chrono::high_resolution_clock::now() - start).count();
    std::cout << "supplier: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Part ingestion
void ingest_part(const std::string& tbl_file, const std::string& output_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_file, file_size);

    size_t estimated_rows = file_size / 150;
    PartTable table;
    table.p_partkey.reserve(estimated_rows);
    table.p_name.reserve(estimated_rows);
    table.p_mfgr.reserve(estimated_rows);
    table.p_brand.reserve(estimated_rows);
    table.p_type.reserve(estimated_rows);
    table.p_size.reserve(estimated_rows);
    table.p_container.reserve(estimated_rows);
    table.p_retailprice.reserve(estimated_rows);
    table.p_comment.reserve(estimated_rows);

    const char* ptr = data;
    const char* end = data + file_size;
    while (ptr < end) {
        const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end - ptr));
        if (!line_end) line_end = end;

        const char* field_start = ptr;
        int field_idx = 0;
        const char* field_end;

        while (field_start < line_end && field_idx < 9) {
            field_end = static_cast<const char*>(memchr(field_start, '|', line_end - field_start));
            if (!field_end) field_end = line_end;

            switch (field_idx) {
                case 0: table.p_partkey.push_back(parse_int(field_start, &field_end)); break;
                case 1: table.p_name.emplace_back(field_start, field_end - field_start); break;
                case 2: table.p_mfgr.emplace_back(field_start, field_end - field_start); break;
                case 3: table.p_brand.emplace_back(field_start, field_end - field_start); break;
                case 4: table.p_type.emplace_back(field_start, field_end - field_start); break;
                case 5: table.p_size.push_back(parse_int(field_start, &field_end)); break;
                case 6: table.p_container.emplace_back(field_start, field_end - field_start); break;
                case 7: table.p_retailprice.push_back(parse_double(field_start, &field_end)); break;
                case 8: table.p_comment.emplace_back(field_start, field_end - field_start); break;
            }

            field_start = field_end + 1;
            field_idx++;
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);

    write_column(output_dir + "/part_p_partkey.bin", table.p_partkey);
    write_string_column(output_dir + "/part_p_name.bin", table.p_name);
    write_string_column(output_dir + "/part_p_mfgr.bin", table.p_mfgr);
    write_string_column(output_dir + "/part_p_brand.bin", table.p_brand);
    write_string_column(output_dir + "/part_p_type.bin", table.p_type);
    write_column(output_dir + "/part_p_size.bin", table.p_size);
    write_string_column(output_dir + "/part_p_container.bin", table.p_container);
    write_column(output_dir + "/part_p_retailprice.bin", table.p_retailprice);
    write_string_column(output_dir + "/part_p_comment.bin", table.p_comment);
    write_metadata(output_dir + "/part_metadata.json", table.size());

    auto elapsed = std::chrono::duration<double>(
        std::chrono::high_resolution_clock::now() - start).count();
    std::cout << "part: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Partsupp ingestion
void ingest_partsupp(const std::string& tbl_file, const std::string& output_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_file, file_size);

    size_t estimated_rows = file_size / 150;
    PartsuppTable table;
    table.ps_partkey.reserve(estimated_rows);
    table.ps_suppkey.reserve(estimated_rows);
    table.ps_availqty.reserve(estimated_rows);
    table.ps_supplycost.reserve(estimated_rows);
    table.ps_comment.reserve(estimated_rows);

    const char* ptr = data;
    const char* end = data + file_size;
    while (ptr < end) {
        const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end - ptr));
        if (!line_end) line_end = end;

        const char* field_start = ptr;
        int field_idx = 0;
        const char* field_end;

        while (field_start < line_end && field_idx < 5) {
            field_end = static_cast<const char*>(memchr(field_start, '|', line_end - field_start));
            if (!field_end) field_end = line_end;

            switch (field_idx) {
                case 0: table.ps_partkey.push_back(parse_int(field_start, &field_end)); break;
                case 1: table.ps_suppkey.push_back(parse_int(field_start, &field_end)); break;
                case 2: table.ps_availqty.push_back(parse_int(field_start, &field_end)); break;
                case 3: table.ps_supplycost.push_back(parse_double(field_start, &field_end)); break;
                case 4: table.ps_comment.emplace_back(field_start, field_end - field_start); break;
            }

            field_start = field_end + 1;
            field_idx++;
        }

        ptr = line_end + 1;
    }

    munmap((void*)data, file_size);

    write_column(output_dir + "/partsupp_ps_partkey.bin", table.ps_partkey);
    write_column(output_dir + "/partsupp_ps_suppkey.bin", table.ps_suppkey);
    write_column(output_dir + "/partsupp_ps_availqty.bin", table.ps_availqty);
    write_column(output_dir + "/partsupp_ps_supplycost.bin", table.ps_supplycost);
    write_string_column(output_dir + "/partsupp_ps_comment.bin", table.ps_comment);
    write_metadata(output_dir + "/partsupp_metadata.json", table.size());

    auto elapsed = std::chrono::duration<double>(
        std::chrono::high_resolution_clock::now() - start).count();
    std::cout << "partsupp: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Template specialization for mmap_column
template<typename T>
T* mmap_column(const std::string& file_path, size_t& num_elements) {
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Failed to open column file: " + file_path);
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        throw std::runtime_error("Failed to stat column file: " + file_path);
    }

    num_elements = st.st_size / sizeof(T);
    T* data = static_cast<T*>(mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    if (data == MAP_FAILED) {
        throw std::runtime_error("Failed to mmap column file: " + file_path);
    }

    madvise(data, st.st_size, MADV_WILLNEED);
    return data;
}

// Explicit instantiations
template int32_t* mmap_column<int32_t>(const std::string&, size_t&);
template double* mmap_column<double>(const std::string&, size_t&);
template char* mmap_column<char>(const std::string&, size_t&);
template uint8_t* mmap_column<uint8_t>(const std::string&, size_t&);

// Helper to read row count from metadata
size_t read_row_count(const std::string& metadata_file) {
    std::ifstream in(metadata_file);
    std::string line;
    std::getline(in, line);

    // Parse JSON manually (simple case)
    size_t pos = line.find(":");
    if (pos != std::string::npos) {
        std::string num_str = line.substr(pos + 1);
        num_str = num_str.substr(num_str.find_first_of("0123456789"));
        num_str = num_str.substr(0, num_str.find_first_not_of("0123456789"));
        return std::stoull(num_str);
    }
    return 0;
}

// Helper to read zone map from JSON file
std::vector<ZoneMapBlock> read_zone_map(const std::string& zonemap_file) {
    std::vector<ZoneMapBlock> result;
    std::ifstream in(zonemap_file);
    if (!in.good()) return result;

    std::string line;
    // Skip first line: {"zone_map": [
    std::getline(in, line);

    while (std::getline(in, line)) {
        // Stop at closing bracket
        if (line.find(']') != std::string::npos) break;

        // Parse: {"block_id": N, "min": M, "max": X, "start_row": S, "end_row": E}
        ZoneMapBlock block;
        size_t pos;

        pos = line.find("\"block_id\":");
        if (pos != std::string::npos) {
            block.block_id = std::stoul(line.substr(pos + 11));
        }

        pos = line.find("\"min\":");
        if (pos != std::string::npos) {
            block.min_val = std::stoi(line.substr(pos + 6));
        }

        pos = line.find("\"max\":");
        if (pos != std::string::npos) {
            block.max_val = std::stoi(line.substr(pos + 6));
        }

        pos = line.find("\"start_row\":");
        if (pos != std::string::npos) {
            block.start_row = std::stoul(line.substr(pos + 12));
        }

        pos = line.find("\"end_row\":");
        if (pos != std::string::npos) {
            block.end_row = std::stoul(line.substr(pos + 10));
        }

        result.push_back(block);
    }

    return result;
}

} // namespace storage
} // namespace gendb
