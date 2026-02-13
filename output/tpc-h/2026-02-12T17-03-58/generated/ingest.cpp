#include "storage/storage.h"
#include "utils/date_utils.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <sys/stat.h>
#include <cstring>

// Helper: Parse a single field from pipe-delimited line
std::string parseField(const std::string& line, size_t& pos) {
    size_t pipe_pos = line.find('|', pos);
    if (pipe_pos == std::string::npos) {
        pipe_pos = line.length();
    }
    std::string field = line.substr(pos, pipe_pos - pos);
    pos = pipe_pos + 1;
    return field;
}

// Helper: Parse int64_t scaled decimal (DECIMAL(15,2) -> int64_t * 100)
int64_t parseDecimal(const std::string& str) {
    if (str.empty()) return 0;
    size_t dot_pos = str.find('.');
    if (dot_pos == std::string::npos) {
        // No decimal point, multiply by 100
        return std::stoll(str) * 100;
    }
    // Parse before and after decimal
    int64_t before = 0;
    if (dot_pos > 0) {
        before = std::stoll(str.substr(0, dot_pos));
    }
    std::string after_str = str.substr(dot_pos + 1);
    // Pad or truncate to 2 digits
    while (after_str.length() < 2) after_str += '0';
    if (after_str.length() > 2) after_str = after_str.substr(0, 2);
    int64_t after = std::stoll(after_str);
    return before * 100 + (before < 0 ? -after : after);
}

// Ingest lineitem table
void ingestLineitem(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    std::string input_file = data_dir + "/lineitem.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    TableMetadata metadata;
    metadata.table_name = "lineitem";
    metadata.block_size = 65536;

    // Define columns
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<int64_t> l_quantity;
    std::vector<int64_t> l_extendedprice;
    std::vector<int64_t> l_discount;
    std::vector<int64_t> l_tax;
    std::vector<uint8_t> l_returnflag;
    std::vector<uint8_t> l_linestatus;
    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<uint8_t> l_shipinstruct;
    std::vector<uint8_t> l_shipmode;
    std::vector<std::string> l_comment;

    Dictionary dict_returnflag, dict_linestatus, dict_shipinstruct, dict_shipmode;

    std::string line;
    uint64_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        size_t pos = 0;
        l_orderkey.push_back(std::stoi(parseField(line, pos)));
        l_partkey.push_back(std::stoi(parseField(line, pos)));
        l_suppkey.push_back(std::stoi(parseField(line, pos)));
        l_linenumber.push_back(std::stoi(parseField(line, pos)));
        l_quantity.push_back(parseDecimal(parseField(line, pos)));
        l_extendedprice.push_back(parseDecimal(parseField(line, pos)));
        l_discount.push_back(parseDecimal(parseField(line, pos)));
        l_tax.push_back(parseDecimal(parseField(line, pos)));
        l_returnflag.push_back(dict_returnflag.encode(parseField(line, pos)));
        l_linestatus.push_back(dict_linestatus.encode(parseField(line, pos)));
        l_shipdate.push_back(DateUtils::stringToDate(parseField(line, pos)));
        l_commitdate.push_back(DateUtils::stringToDate(parseField(line, pos)));
        l_receiptdate.push_back(DateUtils::stringToDate(parseField(line, pos)));
        l_shipinstruct.push_back(dict_shipinstruct.encode(parseField(line, pos)));
        l_shipmode.push_back(dict_shipmode.encode(parseField(line, pos)));
        l_comment.push_back(parseField(line, pos));

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Parsed " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    metadata.row_count = row_count;
    metadata.num_blocks = (row_count + metadata.block_size - 1) / metadata.block_size;

    std::cout << "  Sorting by l_shipdate..." << std::endl;

    // Create sort indices
    std::vector<size_t> indices(row_count);
    for (size_t i = 0; i < row_count; ++i) indices[i] = i;

    // Sort by l_shipdate, then l_orderkey
    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        if (l_shipdate[a] != l_shipdate[b])
            return l_shipdate[a] < l_shipdate[b];
        return l_orderkey[a] < l_orderkey[b];
    });

    // Apply permutation to all columns
    auto applyPerm = [&](auto& vec) {
        std::vector<typename std::remove_reference<decltype(vec)>::type::value_type> temp(vec.size());
        for (size_t i = 0; i < vec.size(); ++i) {
            temp[i] = vec[indices[i]];
        }
        vec = std::move(temp);
    };

    applyPerm(l_orderkey);
    applyPerm(l_partkey);
    applyPerm(l_suppkey);
    applyPerm(l_linenumber);
    applyPerm(l_quantity);
    applyPerm(l_extendedprice);
    applyPerm(l_discount);
    applyPerm(l_tax);
    applyPerm(l_returnflag);
    applyPerm(l_linestatus);
    applyPerm(l_shipdate);
    applyPerm(l_commitdate);
    applyPerm(l_receiptdate);
    applyPerm(l_shipinstruct);
    applyPerm(l_shipmode);
    applyPerm(l_comment);

    // Build zone maps for l_shipdate, l_quantity, l_discount
    std::vector<ZoneMap> zone_shipdate, zone_quantity, zone_discount;
    for (uint32_t block_id = 0; block_id < metadata.num_blocks; ++block_id) {
        uint64_t start = static_cast<uint64_t>(block_id) * metadata.block_size;
        uint64_t end = std::min(start + metadata.block_size, row_count);

        int64_t min_sd = l_shipdate[start], max_sd = l_shipdate[start];
        int64_t min_qty = l_quantity[start], max_qty = l_quantity[start];
        int64_t min_disc = l_discount[start], max_disc = l_discount[start];

        for (uint64_t i = start + 1; i < end; ++i) {
            min_sd = std::min(min_sd, (int64_t)l_shipdate[i]);
            max_sd = std::max(max_sd, (int64_t)l_shipdate[i]);
            min_qty = std::min(min_qty, l_quantity[i]);
            max_qty = std::max(max_qty, l_quantity[i]);
            min_disc = std::min(min_disc, l_discount[i]);
            max_disc = std::max(max_disc, l_discount[i]);
        }

        zone_shipdate.push_back({min_sd, max_sd});
        zone_quantity.push_back({min_qty, max_qty});
        zone_discount.push_back({min_disc, max_disc});
    }

    metadata.zone_maps["l_shipdate"] = zone_shipdate;
    metadata.zone_maps["l_quantity"] = zone_quantity;
    metadata.zone_maps["l_discount"] = zone_discount;

    metadata.dictionaries["l_returnflag"] = dict_returnflag;
    metadata.dictionaries["l_linestatus"] = dict_linestatus;
    metadata.dictionaries["l_shipinstruct"] = dict_shipinstruct;
    metadata.dictionaries["l_shipmode"] = dict_shipmode;

    // Add column info
    metadata.columns = {
        {"l_orderkey", "int32", false, false},
        {"l_partkey", "int32", false, false},
        {"l_suppkey", "int32", false, false},
        {"l_linenumber", "int32", false, false},
        {"l_quantity", "int64", false, true},
        {"l_extendedprice", "int64", false, false},
        {"l_discount", "int64", false, true},
        {"l_tax", "int64", false, false},
        {"l_returnflag", "uint8", true, false},
        {"l_linestatus", "uint8", true, false},
        {"l_shipdate", "int32", false, true},
        {"l_commitdate", "int32", false, false},
        {"l_receiptdate", "int32", false, false},
        {"l_shipinstruct", "uint8", true, false},
        {"l_shipmode", "uint8", true, false}
    };

    std::cout << "  Writing columns..." << std::endl;

    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_orderkey", l_orderkey);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_partkey", l_partkey);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_suppkey", l_suppkey);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_linenumber", l_linenumber);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_quantity", l_quantity);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_extendedprice", l_extendedprice);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_discount", l_discount);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_tax", l_tax);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_returnflag", l_returnflag);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_linestatus", l_linestatus);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_shipdate", l_shipdate);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_commitdate", l_commitdate);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_receiptdate", l_receiptdate);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_shipinstruct", l_shipinstruct);
    Storage::writeColumnTyped(gendb_dir, "lineitem", "l_shipmode", l_shipmode);

    Storage::writeMetadata(gendb_dir, metadata);

    std::cout << "  Lineitem ingested: " << row_count << " rows" << std::endl;
}

// Ingest orders table
void ingestOrders(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting orders..." << std::endl;

    std::string input_file = data_dir + "/orders.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    TableMetadata metadata;
    metadata.table_name = "orders";
    metadata.block_size = 65536;

    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<uint8_t> o_orderstatus;
    std::vector<int64_t> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<uint8_t> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;

    Dictionary dict_orderstatus, dict_orderpriority;

    std::string line;
    uint64_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        size_t pos = 0;
        o_orderkey.push_back(std::stoi(parseField(line, pos)));
        o_custkey.push_back(std::stoi(parseField(line, pos)));
        o_orderstatus.push_back(dict_orderstatus.encode(parseField(line, pos)));
        o_totalprice.push_back(parseDecimal(parseField(line, pos)));
        o_orderdate.push_back(DateUtils::stringToDate(parseField(line, pos)));
        o_orderpriority.push_back(dict_orderpriority.encode(parseField(line, pos)));
        o_clerk.push_back(parseField(line, pos));
        o_shippriority.push_back(std::stoi(parseField(line, pos)));
        o_comment.push_back(parseField(line, pos));

        row_count++;
        if (row_count % 500000 == 0) {
            std::cout << "  Parsed " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    metadata.row_count = row_count;
    metadata.num_blocks = (row_count + metadata.block_size - 1) / metadata.block_size;

    std::cout << "  Sorting by o_orderdate..." << std::endl;

    std::vector<size_t> indices(row_count);
    for (size_t i = 0; i < row_count; ++i) indices[i] = i;

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        if (o_orderdate[a] != o_orderdate[b])
            return o_orderdate[a] < o_orderdate[b];
        return o_orderkey[a] < o_orderkey[b];
    });

    auto applyPerm = [&](auto& vec) {
        std::vector<typename std::remove_reference<decltype(vec)>::type::value_type> temp(vec.size());
        for (size_t i = 0; i < vec.size(); ++i) {
            temp[i] = vec[indices[i]];
        }
        vec = std::move(temp);
    };

    applyPerm(o_orderkey);
    applyPerm(o_custkey);
    applyPerm(o_orderstatus);
    applyPerm(o_totalprice);
    applyPerm(o_orderdate);
    applyPerm(o_orderpriority);
    applyPerm(o_clerk);
    applyPerm(o_shippriority);
    applyPerm(o_comment);

    // Build zone map for o_orderdate
    std::vector<ZoneMap> zone_orderdate;
    for (uint32_t block_id = 0; block_id < metadata.num_blocks; ++block_id) {
        uint64_t start = static_cast<uint64_t>(block_id) * metadata.block_size;
        uint64_t end = std::min(start + metadata.block_size, row_count);

        int64_t min_od = o_orderdate[start], max_od = o_orderdate[start];
        for (uint64_t i = start + 1; i < end; ++i) {
            min_od = std::min(min_od, (int64_t)o_orderdate[i]);
            max_od = std::max(max_od, (int64_t)o_orderdate[i]);
        }
        zone_orderdate.push_back({min_od, max_od});
    }

    metadata.zone_maps["o_orderdate"] = zone_orderdate;
    metadata.dictionaries["o_orderstatus"] = dict_orderstatus;
    metadata.dictionaries["o_orderpriority"] = dict_orderpriority;

    metadata.columns = {
        {"o_orderkey", "int32", false, false},
        {"o_custkey", "int32", false, false},
        {"o_orderstatus", "uint8", true, false},
        {"o_totalprice", "int64", false, false},
        {"o_orderdate", "int32", false, true},
        {"o_orderpriority", "uint8", true, false},
        {"o_shippriority", "int32", false, false}
    };

    std::cout << "  Writing columns..." << std::endl;

    Storage::writeColumnTyped(gendb_dir, "orders", "o_orderkey", o_orderkey);
    Storage::writeColumnTyped(gendb_dir, "orders", "o_custkey", o_custkey);
    Storage::writeColumnTyped(gendb_dir, "orders", "o_orderstatus", o_orderstatus);
    Storage::writeColumnTyped(gendb_dir, "orders", "o_totalprice", o_totalprice);
    Storage::writeColumnTyped(gendb_dir, "orders", "o_orderdate", o_orderdate);
    Storage::writeColumnTyped(gendb_dir, "orders", "o_orderpriority", o_orderpriority);
    Storage::writeColumnTyped(gendb_dir, "orders", "o_shippriority", o_shippriority);

    Storage::writeMetadata(gendb_dir, metadata);

    std::cout << "  Orders ingested: " << row_count << " rows" << std::endl;
}

// Ingest customer table
void ingestCustomer(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting customer..." << std::endl;

    std::string input_file = data_dir + "/customer.tbl";
    std::ifstream in(input_file);
    if (!in) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    TableMetadata metadata;
    metadata.table_name = "customer";
    metadata.block_size = 32768;

    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<int64_t> c_acctbal;
    std::vector<uint8_t> c_mktsegment;
    std::vector<std::string> c_comment;

    Dictionary dict_mktsegment;

    std::string line;
    uint64_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        size_t pos = 0;
        c_custkey.push_back(std::stoi(parseField(line, pos)));
        c_name.push_back(parseField(line, pos));
        c_address.push_back(parseField(line, pos));
        c_nationkey.push_back(std::stoi(parseField(line, pos)));
        c_phone.push_back(parseField(line, pos));
        c_acctbal.push_back(parseDecimal(parseField(line, pos)));
        c_mktsegment.push_back(dict_mktsegment.encode(parseField(line, pos)));
        c_comment.push_back(parseField(line, pos));

        row_count++;
    }
    in.close();

    metadata.row_count = row_count;
    metadata.num_blocks = (row_count + metadata.block_size - 1) / metadata.block_size;
    metadata.dictionaries["c_mktsegment"] = dict_mktsegment;

    metadata.columns = {
        {"c_custkey", "int32", false, false},
        {"c_nationkey", "int32", false, false},
        {"c_acctbal", "int64", false, false},
        {"c_mktsegment", "uint8", true, false}
    };

    std::cout << "  Writing columns..." << std::endl;

    Storage::writeColumnTyped(gendb_dir, "customer", "c_custkey", c_custkey);
    Storage::writeColumnTyped(gendb_dir, "customer", "c_nationkey", c_nationkey);
    Storage::writeColumnTyped(gendb_dir, "customer", "c_acctbal", c_acctbal);
    Storage::writeColumnTyped(gendb_dir, "customer", "c_mktsegment", c_mktsegment);

    Storage::writeMetadata(gendb_dir, metadata);

    std::cout << "  Customer ingested: " << row_count << " rows" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create gendb directory
    mkdir(gendb_dir.c_str(), 0755);

    auto start_time = std::chrono::high_resolution_clock::now();

    // Ingest tables sequentially (can be parallelized, but kept simple for now)
    ingestCustomer(data_dir, gendb_dir);
    ingestOrders(data_dir, gendb_dir);
    ingestLineitem(data_dir, gendb_dir);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    std::cout << "\nIngestion complete in " << duration.count() << " seconds" << std::endl;

    return 0;
}
