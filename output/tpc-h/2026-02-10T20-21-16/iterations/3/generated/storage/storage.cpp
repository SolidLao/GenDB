#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace gendb {

// Helper: Parse pipe-delimited line
static std::vector<std::string> split_pipe(const std::string& line) {
    std::vector<std::string> fields;
    std::string field;
    std::istringstream ss(line);
    while (std::getline(ss, field, '|')) {
        fields.push_back(field);
    }
    return fields;
}

// Helper: Write binary column to file
template<typename T>
static void write_column_binary(const std::string& path, const std::vector<T>& data) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
    out.close();
}

// Helper: mmap read binary column with zero-copy (returns raw pointer and size info)
template<typename T>
static const T* mmap_column_zerocopy(const std::string& path, size_t& count, void*& mmap_ptr, size_t& mmap_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    struct stat st;
    fstat(fd, &st);
    count = st.st_size / sizeof(T);
    mmap_size = st.st_size;

    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }

    // Advise sequential access for scans
    madvise(ptr, st.st_size, MADV_SEQUENTIAL | MADV_WILLNEED);

    mmap_ptr = ptr;
    return static_cast<const T*>(ptr);
}

// Lineitem ingestion with zone map generation
void ingest_lineitem(const std::string& tbl_file, const std::string& gendb_dir) {
    std::cout << "Ingesting lineitem from " << tbl_file << std::endl;

    // Temporary vectors for ingestion (these will be sorted and written)
    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<double> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<uint8_t> l_returnflag, l_linestatus;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<std::string> l_shipinstruct, l_shipmode, l_comment;

    std::vector<std::string> returnflag_dict, linestatus_dict;
    std::unordered_map<std::string, uint8_t> returnflag_lookup, linestatus_lookup;

    std::ifstream in(tbl_file);
    std::string line;

    // Reserve space for efficiency (SF10 has ~60M rows)
    l_orderkey.reserve(60000000);
    l_partkey.reserve(60000000);
    l_suppkey.reserve(60000000);
    l_linenumber.reserve(60000000);
    l_quantity.reserve(60000000);
    l_extendedprice.reserve(60000000);
    l_discount.reserve(60000000);
    l_tax.reserve(60000000);
    l_returnflag.reserve(60000000);
    l_linestatus.reserve(60000000);
    l_shipdate.reserve(60000000);
    l_commitdate.reserve(60000000);
    l_receiptdate.reserve(60000000);

    size_t row_count = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        auto fields = split_pipe(line);
        if (fields.size() < 16) continue;

        l_orderkey.push_back(std::stoi(fields[0]));
        l_partkey.push_back(std::stoi(fields[1]));
        l_suppkey.push_back(std::stoi(fields[2]));
        l_linenumber.push_back(std::stoi(fields[3]));
        l_quantity.push_back(std::stod(fields[4]));
        l_extendedprice.push_back(std::stod(fields[5]));
        l_discount.push_back(std::stod(fields[6]));
        l_tax.push_back(std::stod(fields[7]));

        // Dictionary encoding for returnflag
        std::string rf = fields[8];
        if (returnflag_lookup.find(rf) == returnflag_lookup.end()) {
            uint8_t code = returnflag_dict.size();
            returnflag_dict.push_back(rf);
            returnflag_lookup[rf] = code;
        }
        l_returnflag.push_back(returnflag_lookup[rf]);

        // Dictionary encoding for linestatus
        std::string ls = fields[9];
        if (linestatus_lookup.find(ls) == linestatus_lookup.end()) {
            uint8_t code = linestatus_dict.size();
            linestatus_dict.push_back(ls);
            linestatus_lookup[ls] = code;
        }
        l_linestatus.push_back(linestatus_lookup[ls]);

        l_shipdate.push_back(parse_date(fields[10]));
        l_commitdate.push_back(parse_date(fields[11]));
        l_receiptdate.push_back(parse_date(fields[12]));
        l_shipinstruct.push_back(fields[13]);
        l_shipmode.push_back(fields[14]);
        l_comment.push_back(fields[15]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Loaded " << row_count / 1000000 << "M rows..." << std::endl;
        }
    }
    in.close();

    std::cout << "  Total rows: " << row_count << std::endl;
    std::cout << "  Sorting by l_shipdate..." << std::endl;

    // Sort by l_shipdate (critical for zone maps)
    std::vector<size_t> indices(row_count);
    for (size_t i = 0; i < row_count; i++) indices[i] = i;

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return l_shipdate[a] < l_shipdate[b];
    });

    // Reorder all columns based on sorted indices
    auto reorder = [&](auto& vec) {
        using T = typename std::decay<decltype(vec[0])>::type;
        std::vector<T> temp(row_count);
        for (size_t i = 0; i < row_count; i++) {
            temp[i] = vec[indices[i]];
        }
        vec = std::move(temp);
    };

    reorder(l_orderkey);
    reorder(l_partkey);
    reorder(l_suppkey);
    reorder(l_linenumber);
    reorder(l_quantity);
    reorder(l_extendedprice);
    reorder(l_discount);
    reorder(l_tax);
    reorder(l_returnflag);
    reorder(l_linestatus);
    reorder(l_shipdate);
    reorder(l_commitdate);
    reorder(l_receiptdate);
    reorder(l_shipinstruct);
    reorder(l_shipmode);
    reorder(l_comment);

    std::cout << "  Computing zone maps..." << std::endl;

    // Compute zone maps on l_shipdate (blocks of 65536 rows)
    const size_t BLOCK_SIZE = 65536;
    std::vector<ZoneMap> shipdate_zones;
    for (size_t block_start = 0; block_start < row_count; block_start += BLOCK_SIZE) {
        size_t block_end = std::min(block_start + BLOCK_SIZE, row_count);
        size_t block_count = block_end - block_start;

        int32_t min_date = l_shipdate[block_start];
        int32_t max_date = l_shipdate[block_start];
        for (size_t i = block_start + 1; i < block_end; i++) {
            min_date = std::min(min_date, l_shipdate[i]);
            max_date = std::max(max_date, l_shipdate[i]);
        }

        ZoneMap zm;
        zm.min_value = min_date;
        zm.max_value = max_date;
        zm.block_id = shipdate_zones.size();
        zm.row_offset = block_start;
        zm.row_count = block_count;
        shipdate_zones.push_back(zm);
    }

    std::cout << "  Generated " << shipdate_zones.size() << " zone map blocks" << std::endl;
    std::cout << "  Writing binary columns..." << std::endl;

    // Write binary column files
    std::string dir = gendb_dir + "/lineitem";
    system(("mkdir -p " + dir).c_str());

    write_column_binary(dir + "/l_orderkey.col", l_orderkey);
    write_column_binary(dir + "/l_partkey.col", l_partkey);
    write_column_binary(dir + "/l_suppkey.col", l_suppkey);
    write_column_binary(dir + "/l_linenumber.col", l_linenumber);
    write_column_binary(dir + "/l_quantity.col", l_quantity);
    write_column_binary(dir + "/l_extendedprice.col", l_extendedprice);
    write_column_binary(dir + "/l_discount.col", l_discount);
    write_column_binary(dir + "/l_tax.col", l_tax);
    write_column_binary(dir + "/l_returnflag.col", l_returnflag);
    write_column_binary(dir + "/l_linestatus.col", l_linestatus);
    write_column_binary(dir + "/l_shipdate.col", l_shipdate);
    write_column_binary(dir + "/l_commitdate.col", l_commitdate);
    write_column_binary(dir + "/l_receiptdate.col", l_receiptdate);

    // Write metadata (row count, dictionaries, and zone maps)
    std::ofstream meta(dir + "/metadata.txt");
    meta << "row_count=" << row_count << "\n";
    meta << "returnflag_dict=";
    for (const auto& s : returnflag_dict) meta << s << ",";
    meta << "\n";
    meta << "linestatus_dict=";
    for (const auto& s : linestatus_dict) meta << s << ",";
    meta << "\n";

    // Write zone maps for shipdate
    meta << "shipdate_zones=" << shipdate_zones.size() << "\n";
    for (const auto& zm : shipdate_zones) {
        meta << "zone:" << zm.block_id << "," << zm.row_offset << "," << zm.row_count
             << "," << zm.min_value << "," << zm.max_value << "\n";
    }

    meta.close();

    std::cout << "  Lineitem ingestion complete!" << std::endl;
}

// Orders ingestion with zone map generation
void ingest_orders(const std::string& tbl_file, const std::string& gendb_dir) {
    std::cout << "Ingesting orders from " << tbl_file << std::endl;

    std::vector<int32_t> o_orderkey, o_custkey, o_orderdate, o_shippriority;
    std::vector<std::string> o_orderstatus, o_orderpriority, o_clerk, o_comment;
    std::vector<double> o_totalprice;

    std::ifstream in(tbl_file);
    std::string line;

    o_orderkey.reserve(15000000);
    o_custkey.reserve(15000000);
    o_orderdate.reserve(15000000);
    o_shippriority.reserve(15000000);

    size_t row_count = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        auto fields = split_pipe(line);
        if (fields.size() < 9) continue;

        o_orderkey.push_back(std::stoi(fields[0]));
        o_custkey.push_back(std::stoi(fields[1]));
        o_orderstatus.push_back(fields[2]);
        o_totalprice.push_back(std::stod(fields[3]));
        o_orderdate.push_back(parse_date(fields[4]));
        o_orderpriority.push_back(fields[5]);
        o_clerk.push_back(fields[6]);
        o_shippriority.push_back(std::stoi(fields[7]));
        o_comment.push_back(fields[8]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Loaded " << row_count / 1000000 << "M rows..." << std::endl;
        }
    }
    in.close();

    std::cout << "  Total rows: " << row_count << std::endl;
    std::cout << "  Computing zone maps on o_orderdate..." << std::endl;

    // Compute zone maps on o_orderdate (blocks of 65536 rows)
    const size_t BLOCK_SIZE = 65536;
    std::vector<ZoneMap> orderdate_zones;
    for (size_t block_start = 0; block_start < row_count; block_start += BLOCK_SIZE) {
        size_t block_end = std::min(block_start + BLOCK_SIZE, row_count);
        size_t block_count = block_end - block_start;

        int32_t min_date = o_orderdate[block_start];
        int32_t max_date = o_orderdate[block_start];
        for (size_t i = block_start + 1; i < block_end; i++) {
            min_date = std::min(min_date, o_orderdate[i]);
            max_date = std::max(max_date, o_orderdate[i]);
        }

        ZoneMap zm;
        zm.min_value = min_date;
        zm.max_value = max_date;
        zm.block_id = orderdate_zones.size();
        zm.row_offset = block_start;
        zm.row_count = block_count;
        orderdate_zones.push_back(zm);
    }

    std::cout << "  Generated " << orderdate_zones.size() << " zone map blocks" << std::endl;
    std::cout << "  Writing binary columns..." << std::endl;

    std::string dir = gendb_dir + "/orders";
    system(("mkdir -p " + dir).c_str());

    write_column_binary(dir + "/o_orderkey.col", o_orderkey);
    write_column_binary(dir + "/o_custkey.col", o_custkey);
    write_column_binary(dir + "/o_orderdate.col", o_orderdate);
    write_column_binary(dir + "/o_shippriority.col", o_shippriority);

    std::ofstream meta(dir + "/metadata.txt");
    meta << "row_count=" << row_count << "\n";

    // Write zone maps for orderdate
    meta << "orderdate_zones=" << orderdate_zones.size() << "\n";
    for (const auto& zm : orderdate_zones) {
        meta << "zone:" << zm.block_id << "," << zm.row_offset << "," << zm.row_count
             << "," << zm.min_value << "," << zm.max_value << "\n";
    }

    meta.close();

    std::cout << "  Orders ingestion complete!" << std::endl;
}

// Customer ingestion
void ingest_customer(const std::string& tbl_file, const std::string& gendb_dir) {
    std::cout << "Ingesting customer from " << tbl_file << std::endl;

    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<uint8_t> c_mktsegment;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;
    std::vector<double> c_acctbal;

    std::vector<std::string> mktsegment_dict;
    std::unordered_map<std::string, uint8_t> mktsegment_lookup;

    std::ifstream in(tbl_file);
    std::string line;

    c_custkey.reserve(1500000);
    c_mktsegment.reserve(1500000);

    size_t row_count = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        auto fields = split_pipe(line);
        if (fields.size() < 8) continue;

        c_custkey.push_back(std::stoi(fields[0]));
        c_name.push_back(fields[1]);
        c_address.push_back(fields[2]);
        c_nationkey.push_back(std::stoi(fields[3]));
        c_phone.push_back(fields[4]);
        c_acctbal.push_back(std::stod(fields[5]));

        // Dictionary encoding for mktsegment
        std::string ms = fields[6];
        if (mktsegment_lookup.find(ms) == mktsegment_lookup.end()) {
            uint8_t code = mktsegment_dict.size();
            mktsegment_dict.push_back(ms);
            mktsegment_lookup[ms] = code;
        }
        c_mktsegment.push_back(mktsegment_lookup[ms]);

        c_comment.push_back(fields[7]);

        row_count++;
    }
    in.close();

    std::cout << "  Total rows: " << row_count << std::endl;
    std::cout << "  Writing binary columns..." << std::endl;

    std::string dir = gendb_dir + "/customer";
    system(("mkdir -p " + dir).c_str());

    write_column_binary(dir + "/c_custkey.col", c_custkey);
    write_column_binary(dir + "/c_mktsegment.col", c_mktsegment);

    std::ofstream meta(dir + "/metadata.txt");
    meta << "row_count=" << row_count << "\n";
    meta << "mktsegment_dict=";
    for (const auto& s : mktsegment_dict) meta << s << ",";
    meta << "\n";
    meta.close();

    std::cout << "  Customer ingestion complete!" << std::endl;
}

// Load lineitem (only specified columns) with zero-copy mmap
void load_lineitem(const std::string& gendb_dir, LineitemTable& table,
                   const std::vector<std::string>& columns_needed) {
    std::string dir = gendb_dir + "/lineitem";

    // Read metadata
    std::ifstream meta(dir + "/metadata.txt");
    std::string line;
    size_t row_count = 0;
    while (std::getline(meta, line)) {
        if (line.find("row_count=") == 0) {
            row_count = std::stoull(line.substr(10));
        } else if (line.find("returnflag_dict=") == 0) {
            std::string dict_str = line.substr(16);
            std::istringstream ss(dict_str);
            std::string token;
            while (std::getline(ss, token, ',')) {
                if (!token.empty()) {
                    uint8_t code = table.returnflag_dict.size();
                    table.returnflag_dict.push_back(token);
                    table.returnflag_lookup[token] = code;
                }
            }
        } else if (line.find("linestatus_dict=") == 0) {
            std::string dict_str = line.substr(16);
            std::istringstream ss(dict_str);
            std::string token;
            while (std::getline(ss, token, ',')) {
                if (!token.empty()) {
                    uint8_t code = table.linestatus_dict.size();
                    table.linestatus_dict.push_back(token);
                    table.linestatus_lookup[token] = code;
                }
            }
        } else if (line.find("zone:") == 0) {
            // Parse zone map: zone:block_id,row_offset,row_count,min_value,max_value
            std::string zone_str = line.substr(5);
            std::istringstream ss(zone_str);
            std::string token;
            std::vector<std::string> parts;
            while (std::getline(ss, token, ',')) {
                parts.push_back(token);
            }
            if (parts.size() == 5) {
                ZoneMap zm;
                zm.block_id = std::stoul(parts[0]);
                zm.row_offset = std::stoull(parts[1]);
                zm.row_count = std::stoul(parts[2]);
                zm.min_value = std::stoi(parts[3]);
                zm.max_value = std::stoi(parts[4]);
                table.shipdate_zones.push_back(zm);
            }
        }
    }
    meta.close();

    table.row_count = row_count;

    // Zero-copy mmap only needed columns
    for (const auto& col : columns_needed) {
        size_t count = 0;
        void* mmap_ptr = nullptr;
        size_t mmap_size = 0;

        if (col == "l_orderkey") {
            table.l_orderkey = mmap_column_zerocopy<int32_t>(dir + "/l_orderkey.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "l_quantity") {
            table.l_quantity = mmap_column_zerocopy<double>(dir + "/l_quantity.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "l_extendedprice") {
            table.l_extendedprice = mmap_column_zerocopy<double>(dir + "/l_extendedprice.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "l_discount") {
            table.l_discount = mmap_column_zerocopy<double>(dir + "/l_discount.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "l_tax") {
            table.l_tax = mmap_column_zerocopy<double>(dir + "/l_tax.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "l_returnflag") {
            table.l_returnflag = mmap_column_zerocopy<uint8_t>(dir + "/l_returnflag.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "l_linestatus") {
            table.l_linestatus = mmap_column_zerocopy<uint8_t>(dir + "/l_linestatus.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "l_shipdate") {
            table.l_shipdate = mmap_column_zerocopy<int32_t>(dir + "/l_shipdate.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        }
    }
}

// Load orders (only specified columns) with zero-copy mmap
void load_orders(const std::string& gendb_dir, OrdersTable& table,
                 const std::vector<std::string>& columns_needed) {
    std::string dir = gendb_dir + "/orders";

    std::ifstream meta(dir + "/metadata.txt");
    std::string line;
    size_t row_count = 0;
    while (std::getline(meta, line)) {
        if (line.find("row_count=") == 0) {
            row_count = std::stoull(line.substr(10));
        } else if (line.find("zone:") == 0) {
            // Parse zone map: zone:block_id,row_offset,row_count,min_value,max_value
            std::string zone_str = line.substr(5);
            std::istringstream ss(zone_str);
            std::string token;
            std::vector<std::string> parts;
            while (std::getline(ss, token, ',')) {
                parts.push_back(token);
            }
            if (parts.size() == 5) {
                ZoneMap zm;
                zm.block_id = std::stoul(parts[0]);
                zm.row_offset = std::stoull(parts[1]);
                zm.row_count = std::stoul(parts[2]);
                zm.min_value = std::stoi(parts[3]);
                zm.max_value = std::stoi(parts[4]);
                table.orderdate_zones.push_back(zm);
            }
        }
    }
    meta.close();

    table.row_count = row_count;

    for (const auto& col : columns_needed) {
        size_t count = 0;
        void* mmap_ptr = nullptr;
        size_t mmap_size = 0;

        if (col == "o_orderkey") {
            table.o_orderkey = mmap_column_zerocopy<int32_t>(dir + "/o_orderkey.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "o_custkey") {
            table.o_custkey = mmap_column_zerocopy<int32_t>(dir + "/o_custkey.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "o_orderdate") {
            table.o_orderdate = mmap_column_zerocopy<int32_t>(dir + "/o_orderdate.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "o_shippriority") {
            table.o_shippriority = mmap_column_zerocopy<int32_t>(dir + "/o_shippriority.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        }
    }
}

// Load customer (only specified columns) with zero-copy mmap
void load_customer(const std::string& gendb_dir, CustomerTable& table,
                   const std::vector<std::string>& columns_needed) {
    std::string dir = gendb_dir + "/customer";

    std::ifstream meta(dir + "/metadata.txt");
    std::string line;
    size_t row_count = 0;
    while (std::getline(meta, line)) {
        if (line.find("row_count=") == 0) {
            row_count = std::stoull(line.substr(10));
        } else if (line.find("mktsegment_dict=") == 0) {
            std::string dict_str = line.substr(16);
            std::istringstream ss(dict_str);
            std::string token;
            while (std::getline(ss, token, ',')) {
                if (!token.empty()) {
                    uint8_t code = table.mktsegment_dict.size();
                    table.mktsegment_dict.push_back(token);
                    table.mktsegment_lookup[token] = code;
                }
            }
        }
    }
    meta.close();

    table.row_count = row_count;

    for (const auto& col : columns_needed) {
        size_t count = 0;
        void* mmap_ptr = nullptr;
        size_t mmap_size = 0;

        if (col == "c_custkey") {
            table.c_custkey = mmap_column_zerocopy<int32_t>(dir + "/c_custkey.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        } else if (col == "c_mktsegment") {
            table.c_mktsegment = mmap_column_zerocopy<uint8_t>(dir + "/c_mktsegment.col", count, mmap_ptr, mmap_size);
            table.mmap_regions.push_back({mmap_ptr, mmap_size});
        }
    }
}

} // namespace gendb
