#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstring>
#include <algorithm>
#include <numeric>

namespace gendb {

// Helper to split pipe-delimited line
static std::vector<std::string> split_line(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string field;

    while (std::getline(ss, field, '|')) {
        fields.push_back(field);
    }

    return fields;
}

// Helper to encode string to dictionary code
static uint8_t encode_string(const std::string& value,
                             std::vector<std::string>& dict,
                             std::unordered_map<std::string, uint8_t>& lookup) {
    auto it = lookup.find(value);
    if (it != lookup.end()) {
        return it->second;
    }
    uint8_t code = static_cast<uint8_t>(dict.size());
    dict.push_back(value);
    lookup[value] = code;
    return code;
}

// Helper to build zone map from sorted date column
static void build_zonemap(const std::vector<int32_t>& dates, ZoneMap& zonemap, size_t block_size = 65536) {
    size_t n = dates.size();
    size_t num_blocks = (n + block_size - 1) / block_size;

    zonemap.block_size = block_size;
    zonemap.block_min.resize(num_blocks);
    zonemap.block_max.resize(num_blocks);

    for (size_t block = 0; block < num_blocks; block++) {
        size_t start = block * block_size;
        size_t end = std::min(start + block_size, n);

        int32_t min_val = dates[start];
        int32_t max_val = dates[start];

        for (size_t i = start + 1; i < end; i++) {
            if (dates[i] < min_val) min_val = dates[i];
            if (dates[i] > max_val) max_val = dates[i];
        }

        zonemap.block_min[block] = min_val;
        zonemap.block_max[block] = max_val;
    }
}

void load_lineitem(const std::string& filepath, LineitemTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    std::string line;
    size_t count = 0;

    // Reserve space for efficiency (SF10 has ~60M rows)
    table.l_orderkey.reserve(60000000);
    table.l_partkey.reserve(60000000);
    table.l_suppkey.reserve(60000000);
    table.l_linenumber.reserve(60000000);
    table.l_quantity.reserve(60000000);
    table.l_extendedprice.reserve(60000000);
    table.l_discount.reserve(60000000);
    table.l_tax.reserve(60000000);
    table.l_returnflag_code.reserve(60000000);
    table.l_linestatus_code.reserve(60000000);
    table.l_shipdate.reserve(60000000);
    table.l_commitdate.reserve(60000000);
    table.l_receiptdate.reserve(60000000);
    table.l_shipinstruct.reserve(60000000);
    table.l_shipmode.reserve(60000000);
    table.l_comment.reserve(60000000);

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split_line(line);
        if (fields.size() < 16) continue;

        table.l_orderkey.push_back(std::stoi(fields[0]));
        table.l_partkey.push_back(std::stoi(fields[1]));
        table.l_suppkey.push_back(std::stoi(fields[2]));
        table.l_linenumber.push_back(std::stoi(fields[3]));
        table.l_quantity.push_back(std::stod(fields[4]));
        table.l_extendedprice.push_back(std::stod(fields[5]));
        table.l_discount.push_back(std::stod(fields[6]));
        table.l_tax.push_back(std::stod(fields[7]));

        // Dictionary encode returnflag and linestatus
        uint8_t returnflag_code = encode_string(fields[8], table.l_returnflag_dict, table.l_returnflag_lookup);
        uint8_t linestatus_code = encode_string(fields[9], table.l_linestatus_dict, table.l_linestatus_lookup);
        table.l_returnflag_code.push_back(returnflag_code);
        table.l_linestatus_code.push_back(linestatus_code);

        table.l_shipdate.push_back(parse_date(fields[10]));
        table.l_commitdate.push_back(parse_date(fields[11]));
        table.l_receiptdate.push_back(parse_date(fields[12]));
        table.l_shipinstruct.push_back(fields[13]);
        table.l_shipmode.push_back(fields[14]);
        table.l_comment.push_back(fields[15]);

        count++;
        if (count % 1000000 == 0) {
            std::cerr << "Loaded " << count << " lineitem rows..." << std::flush;
        }
    }

    std::cerr << "\nSorting lineitem by l_shipdate..." << std::flush;

    // Sort by l_shipdate and reorder all columns
    size_t n = table.l_orderkey.size();
    std::vector<size_t> indices(n);
    std::iota(indices.begin(), indices.end(), 0);

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.l_shipdate[a] < table.l_shipdate[b];
    });

    // Reorder all columns based on sorted indices
    auto reorder_int32 = [&](std::vector<int32_t>& vec) {
        std::vector<int32_t> temp(n);
        for (size_t i = 0; i < n; i++) temp[i] = vec[indices[i]];
        vec = std::move(temp);
    };
    auto reorder_uint8 = [&](std::vector<uint8_t>& vec) {
        std::vector<uint8_t> temp(n);
        for (size_t i = 0; i < n; i++) temp[i] = vec[indices[i]];
        vec = std::move(temp);
    };
    auto reorder_double = [&](std::vector<double>& vec) {
        std::vector<double> temp(n);
        for (size_t i = 0; i < n; i++) temp[i] = vec[indices[i]];
        vec = std::move(temp);
    };
    auto reorder_string = [&](std::vector<std::string>& vec) {
        std::vector<std::string> temp(n);
        for (size_t i = 0; i < n; i++) temp[i] = vec[indices[i]];
        vec = std::move(temp);
    };

    reorder_int32(table.l_orderkey);
    reorder_int32(table.l_partkey);
    reorder_int32(table.l_suppkey);
    reorder_int32(table.l_linenumber);
    reorder_double(table.l_quantity);
    reorder_double(table.l_extendedprice);
    reorder_double(table.l_discount);
    reorder_double(table.l_tax);
    reorder_uint8(table.l_returnflag_code);
    reorder_uint8(table.l_linestatus_code);
    reorder_int32(table.l_shipdate);
    reorder_int32(table.l_commitdate);
    reorder_int32(table.l_receiptdate);
    reorder_string(table.l_shipinstruct);
    reorder_string(table.l_shipmode);
    reorder_string(table.l_comment);

    std::cerr << " Done.\nBuilding zone maps..." << std::flush;

    // Build zone map for l_shipdate
    build_zonemap(table.l_shipdate, table.shipdate_zonemap);

    std::cerr << " Done.\n";
}

void load_orders(const std::string& filepath, OrdersTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    std::string line;
    size_t count = 0;

    // Reserve space (SF10 has ~15M orders)
    table.o_orderkey.reserve(15000000);
    table.o_custkey.reserve(15000000);
    table.o_orderstatus.reserve(15000000);
    table.o_totalprice.reserve(15000000);
    table.o_orderdate.reserve(15000000);
    table.o_orderpriority.reserve(15000000);
    table.o_clerk.reserve(15000000);
    table.o_shippriority.reserve(15000000);
    table.o_comment.reserve(15000000);

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split_line(line);
        if (fields.size() < 9) continue;

        table.o_orderkey.push_back(std::stoi(fields[0]));
        table.o_custkey.push_back(std::stoi(fields[1]));
        table.o_orderstatus.push_back(fields[2]);
        table.o_totalprice.push_back(std::stod(fields[3]));
        table.o_orderdate.push_back(parse_date(fields[4]));
        table.o_orderpriority.push_back(fields[5]);
        table.o_clerk.push_back(fields[6]);
        table.o_shippriority.push_back(std::stoi(fields[7]));
        table.o_comment.push_back(fields[8]);

        count++;
        if (count % 1000000 == 0) {
            std::cerr << "Loaded " << count << " orders rows..." << std::flush;
        }
    }

    std::cerr << "\nSorting orders by o_orderdate..." << std::flush;

    // Sort by o_orderdate and reorder all columns
    size_t n = table.o_orderkey.size();
    std::vector<size_t> indices(n);
    std::iota(indices.begin(), indices.end(), 0);

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.o_orderdate[a] < table.o_orderdate[b];
    });

    // Reorder all columns
    auto reorder_int32 = [&](std::vector<int32_t>& vec) {
        std::vector<int32_t> temp(n);
        for (size_t i = 0; i < n; i++) temp[i] = vec[indices[i]];
        vec = std::move(temp);
    };
    auto reorder_double = [&](std::vector<double>& vec) {
        std::vector<double> temp(n);
        for (size_t i = 0; i < n; i++) temp[i] = vec[indices[i]];
        vec = std::move(temp);
    };
    auto reorder_string = [&](std::vector<std::string>& vec) {
        std::vector<std::string> temp(n);
        for (size_t i = 0; i < n; i++) temp[i] = vec[indices[i]];
        vec = std::move(temp);
    };

    reorder_int32(table.o_orderkey);
    reorder_int32(table.o_custkey);
    reorder_string(table.o_orderstatus);
    reorder_double(table.o_totalprice);
    reorder_int32(table.o_orderdate);
    reorder_string(table.o_orderpriority);
    reorder_string(table.o_clerk);
    reorder_int32(table.o_shippriority);
    reorder_string(table.o_comment);

    std::cerr << " Done.\nBuilding zone maps..." << std::flush;

    // Build zone map for o_orderdate
    build_zonemap(table.o_orderdate, table.orderdate_zonemap);

    std::cerr << " Done.\n";
}

void load_customer(const std::string& filepath, CustomerTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    std::string line;
    size_t count = 0;

    // Reserve space (SF10 has ~1.5M customers)
    table.c_custkey.reserve(1500000);
    table.c_name.reserve(1500000);
    table.c_address.reserve(1500000);
    table.c_nationkey.reserve(1500000);
    table.c_phone.reserve(1500000);
    table.c_acctbal.reserve(1500000);
    table.c_mktsegment_code.reserve(1500000);
    table.c_comment.reserve(1500000);

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split_line(line);
        if (fields.size() < 8) continue;

        table.c_custkey.push_back(std::stoi(fields[0]));
        table.c_name.push_back(fields[1]);
        table.c_address.push_back(fields[2]);
        table.c_nationkey.push_back(std::stoi(fields[3]));
        table.c_phone.push_back(fields[4]);
        table.c_acctbal.push_back(std::stod(fields[5]));

        // Dictionary encode c_mktsegment
        uint8_t mktsegment_code = encode_string(fields[6], table.c_mktsegment_dict, table.c_mktsegment_lookup);
        table.c_mktsegment_code.push_back(mktsegment_code);

        table.c_comment.push_back(fields[7]);

        count++;
        if (count % 100000 == 0) {
            std::cerr << "Loaded " << count << " customer rows..." << std::flush;
        }
    }

    std::cerr << std::endl;
}

} // namespace gendb
