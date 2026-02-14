#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <charconv>
#include <cassert>

// Manual date parsing: YYYY-MM-DD to days since epoch (1970-01-01)
int32_t parse_date(const char* str, size_t len) {
    if (len < 10) return 0;

    int year = 0, month = 0, day = 0;
    std::from_chars(str, str + 4, year);
    std::from_chars(str + 5, str + 7, month);
    std::from_chars(str + 8, str + 10, day);

    // Days since epoch (1970-01-01)
    static const int days_before_month[13] = {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    int days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;
    days += days_before_month[month];
    if (month > 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
        days++;
    }
    days += day;
    return days;
}

// Dictionary for string encoding
struct Dictionary {
    std::unordered_map<std::string, uint8_t> map;
    std::vector<std::string> values;
    std::mutex mtx;

    uint8_t encode(const std::string& s) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(s);
        if (it != map.end()) return it->second;
        uint8_t code = values.size();
        values.push_back(s);
        map[s] = code;
        return code;
    }
};

// Column data storage
struct ColumnData {
    std::vector<int32_t> int_data;
    std::vector<double> double_data;
    std::vector<uint8_t> uint8_data;
    std::vector<std::string> string_data;
};

// Parse lineitem table
void parse_lineitem(const char* data, size_t size, std::vector<ColumnData>& cols,
                    Dictionary& returnflag_dict, Dictionary& linestatus_dict,
                    Dictionary& shipinstruct_dict, Dictionary& shipmode_dict,
                    size_t start_row, size_t end_row) {
    const char* ptr = data;
    const char* end = data + size;
    size_t row = 0;

    while (ptr < end && row < end_row) {
        const char* line_start = ptr;
        const char* line_end = (const char*)memchr(ptr, '\n', end - ptr);
        if (!line_end) line_end = end;

        if (row >= start_row) {
            const char* field_start = line_start;
            int field = 0;

            for (const char* p = line_start; p <= line_end; p++) {
                if (p == line_end || *p == '|') {
                    size_t field_len = p - field_start;

                    switch (field) {
                        case 0: { // l_orderkey
                            int32_t val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[0].int_data.push_back(val);
                            break;
                        }
                        case 1: { // l_partkey
                            int32_t val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[1].int_data.push_back(val);
                            break;
                        }
                        case 2: { // l_suppkey
                            int32_t val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[2].int_data.push_back(val);
                            break;
                        }
                        case 3: { // l_linenumber
                            int32_t val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[3].int_data.push_back(val);
                            break;
                        }
                        case 4: { // l_quantity
                            double val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[4].double_data.push_back(val);
                            break;
                        }
                        case 5: { // l_extendedprice
                            double val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[5].double_data.push_back(val);
                            break;
                        }
                        case 6: { // l_discount
                            double val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[6].double_data.push_back(val);
                            break;
                        }
                        case 7: { // l_tax
                            double val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[7].double_data.push_back(val);
                            break;
                        }
                        case 8: { // l_returnflag
                            std::string val(field_start, field_len);
                            cols[8].uint8_data.push_back(returnflag_dict.encode(val));
                            break;
                        }
                        case 9: { // l_linestatus
                            std::string val(field_start, field_len);
                            cols[9].uint8_data.push_back(linestatus_dict.encode(val));
                            break;
                        }
                        case 10: { // l_shipdate
                            cols[10].int_data.push_back(parse_date(field_start, field_len));
                            break;
                        }
                        case 11: { // l_commitdate
                            cols[11].int_data.push_back(parse_date(field_start, field_len));
                            break;
                        }
                        case 12: { // l_receiptdate
                            cols[12].int_data.push_back(parse_date(field_start, field_len));
                            break;
                        }
                        case 13: { // l_shipinstruct
                            std::string val(field_start, field_len);
                            cols[13].uint8_data.push_back(shipinstruct_dict.encode(val));
                            break;
                        }
                        case 14: { // l_shipmode
                            std::string val(field_start, field_len);
                            cols[14].uint8_data.push_back(shipmode_dict.encode(val));
                            break;
                        }
                        case 15: { // l_comment
                            cols[15].string_data.emplace_back(field_start, field_len);
                            break;
                        }
                    }
                    field_start = p + 1;
                    field++;
                }
            }
        }

        ptr = line_end + 1;
        row++;
    }
}

// Parse orders table
void parse_orders(const char* data, size_t size, std::vector<ColumnData>& cols,
                  Dictionary& orderstatus_dict, Dictionary& orderpriority_dict,
                  size_t start_row, size_t end_row) {
    const char* ptr = data;
    const char* end = data + size;
    size_t row = 0;

    while (ptr < end && row < end_row) {
        const char* line_start = ptr;
        const char* line_end = (const char*)memchr(ptr, '\n', end - ptr);
        if (!line_end) line_end = end;

        if (row >= start_row) {
            const char* field_start = line_start;
            int field = 0;

            for (const char* p = line_start; p <= line_end; p++) {
                if (p == line_end || *p == '|') {
                    size_t field_len = p - field_start;

                    switch (field) {
                        case 0: { // o_orderkey
                            int32_t val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[0].int_data.push_back(val);
                            break;
                        }
                        case 1: { // o_custkey
                            int32_t val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[1].int_data.push_back(val);
                            break;
                        }
                        case 2: { // o_orderstatus
                            std::string val(field_start, field_len);
                            cols[2].uint8_data.push_back(orderstatus_dict.encode(val));
                            break;
                        }
                        case 3: { // o_totalprice
                            double val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[3].double_data.push_back(val);
                            break;
                        }
                        case 4: { // o_orderdate
                            cols[4].int_data.push_back(parse_date(field_start, field_len));
                            break;
                        }
                        case 5: { // o_orderpriority
                            std::string val(field_start, field_len);
                            cols[5].uint8_data.push_back(orderpriority_dict.encode(val));
                            break;
                        }
                        case 6: { // o_clerk
                            cols[6].string_data.emplace_back(field_start, field_len);
                            break;
                        }
                        case 7: { // o_shippriority
                            int32_t val;
                            std::from_chars(field_start, field_start + field_len, val);
                            cols[7].int_data.push_back(val);
                            break;
                        }
                        case 8: { // o_comment
                            cols[8].string_data.emplace_back(field_start, field_len);
                            break;
                        }
                    }
                    field_start = p + 1;
                    field++;
                }
            }
        }

        ptr = line_end + 1;
        row++;
    }
}

// Parse customer table
void parse_customer(const char* data, size_t size, std::vector<ColumnData>& cols,
                    Dictionary& mktsegment_dict) {
    const char* ptr = data;
    const char* end = data + size;

    while (ptr < end) {
        const char* line_start = ptr;
        const char* line_end = (const char*)memchr(ptr, '\n', end - ptr);
        if (!line_end) line_end = end;

        const char* field_start = line_start;
        int field = 0;

        for (const char* p = line_start; p <= line_end; p++) {
            if (p == line_end || *p == '|') {
                size_t field_len = p - field_start;

                switch (field) {
                    case 0: { // c_custkey
                        int32_t val;
                        std::from_chars(field_start, field_start + field_len, val);
                        cols[0].int_data.push_back(val);
                        break;
                    }
                    case 1: // c_name
                        cols[1].string_data.emplace_back(field_start, field_len);
                        break;
                    case 2: // c_address
                        cols[2].string_data.emplace_back(field_start, field_len);
                        break;
                    case 3: { // c_nationkey
                        int32_t val;
                        std::from_chars(field_start, field_start + field_len, val);
                        cols[3].int_data.push_back(val);
                        break;
                    }
                    case 4: // c_phone
                        cols[4].string_data.emplace_back(field_start, field_len);
                        break;
                    case 5: { // c_acctbal
                        double val;
                        std::from_chars(field_start, field_start + field_len, val);
                        cols[5].double_data.push_back(val);
                        break;
                    }
                    case 6: { // c_mktsegment
                        std::string val(field_start, field_len);
                        cols[6].uint8_data.push_back(mktsegment_dict.encode(val));
                        break;
                    }
                    case 7: // c_comment
                        cols[7].string_data.emplace_back(field_start, field_len);
                        break;
                }
                field_start = p + 1;
                field++;
            }
        }

        ptr = line_end + 1;
    }
}

// Generic simple table parser (partsupp, part, supplier, nation, region)
void parse_simple_table(const char* data, size_t size, std::vector<ColumnData>& cols,
                        const std::vector<std::string>& types) {
    const char* ptr = data;
    const char* end = data + size;

    while (ptr < end) {
        const char* line_start = ptr;
        const char* line_end = (const char*)memchr(ptr, '\n', end - ptr);
        if (!line_end) line_end = end;

        const char* field_start = line_start;
        int field = 0;

        for (const char* p = line_start; p <= line_end; p++) {
            if (p == line_end || *p == '|') {
                size_t field_len = p - field_start;

                if (field < types.size()) {
                    if (types[field] == "int") {
                        int32_t val;
                        std::from_chars(field_start, field_start + field_len, val);
                        cols[field].int_data.push_back(val);
                    } else if (types[field] == "double") {
                        double val;
                        std::from_chars(field_start, field_start + field_len, val);
                        cols[field].double_data.push_back(val);
                    } else {
                        cols[field].string_data.emplace_back(field_start, field_len);
                    }
                }
                field_start = p + 1;
                field++;
            }
        }

        ptr = line_end + 1;
    }
}

// Write binary column file
template<typename T>
void write_column(const std::string& path, const std::vector<T>& data) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
}

// Write string column (length-prefixed)
void write_string_column(const std::string& path, const std::vector<std::string>& data) {
    std::ofstream out(path, std::ios::binary);
    for (const auto& s : data) {
        uint32_t len = s.size();
        out.write(reinterpret_cast<const char*>(&len), sizeof(len));
        out.write(s.data(), len);
    }
}

// Sort by column permutation
template<typename T>
std::vector<size_t> create_sort_permutation(const std::vector<T>& data) {
    std::vector<size_t> perm(data.size());
    for (size_t i = 0; i < perm.size(); i++) perm[i] = i;
    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return data[a] < data[b];
    });
    return perm;
}

// Apply permutation to vector
template<typename T>
std::vector<T> apply_permutation(const std::vector<T>& data, const std::vector<size_t>& perm) {
    std::vector<T> result(data.size());
    for (size_t i = 0; i < perm.size(); i++) {
        result[i] = data[perm[i]];
    }
    return result;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create output directory
    system(("mkdir -p " + gendb_dir).c_str());

    std::cout << "Ingesting lineitem..." << std::endl;
    {
        std::string path = data_dir + "/lineitem.tbl";
        int fd = open(path.c_str(), O_RDONLY);
        struct stat sb;
        fstat(fd, &sb);
        size_t size = sb.st_size;

        char* data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(data, size, MADV_SEQUENTIAL);

        std::vector<ColumnData> cols(16);
        Dictionary returnflag_dict, linestatus_dict, shipinstruct_dict, shipmode_dict;

        // Count rows first
        size_t row_count = 0;
        for (size_t i = 0; i < size; i++) {
            if (data[i] == '\n') row_count++;
        }

        // Reserve space
        for (int i = 0; i < 16; i++) {
            cols[i].int_data.reserve(row_count);
            cols[i].double_data.reserve(row_count);
            cols[i].uint8_data.reserve(row_count);
            cols[i].string_data.reserve(row_count);
        }

        parse_lineitem(data, size, cols, returnflag_dict, linestatus_dict,
                       shipinstruct_dict, shipmode_dict, 0, row_count);

        // Sort by l_shipdate
        std::cout << "Sorting lineitem by l_shipdate..." << std::endl;
        auto perm = create_sort_permutation(cols[10].int_data);

        // Create output directory
        system(("mkdir -p " + gendb_dir + "/lineitem").c_str());

        // Apply permutation and write columns
        write_column(gendb_dir + "/lineitem/l_orderkey.bin", apply_permutation(cols[0].int_data, perm));
        write_column(gendb_dir + "/lineitem/l_partkey.bin", apply_permutation(cols[1].int_data, perm));
        write_column(gendb_dir + "/lineitem/l_suppkey.bin", apply_permutation(cols[2].int_data, perm));
        write_column(gendb_dir + "/lineitem/l_linenumber.bin", apply_permutation(cols[3].int_data, perm));
        write_column(gendb_dir + "/lineitem/l_quantity.bin", apply_permutation(cols[4].double_data, perm));
        write_column(gendb_dir + "/lineitem/l_extendedprice.bin", apply_permutation(cols[5].double_data, perm));
        write_column(gendb_dir + "/lineitem/l_discount.bin", apply_permutation(cols[6].double_data, perm));
        write_column(gendb_dir + "/lineitem/l_tax.bin", apply_permutation(cols[7].double_data, perm));
        write_column(gendb_dir + "/lineitem/l_returnflag.bin", apply_permutation(cols[8].uint8_data, perm));
        write_column(gendb_dir + "/lineitem/l_linestatus.bin", apply_permutation(cols[9].uint8_data, perm));
        write_column(gendb_dir + "/lineitem/l_shipdate.bin", apply_permutation(cols[10].int_data, perm));
        write_column(gendb_dir + "/lineitem/l_commitdate.bin", apply_permutation(cols[11].int_data, perm));
        write_column(gendb_dir + "/lineitem/l_receiptdate.bin", apply_permutation(cols[12].int_data, perm));
        write_column(gendb_dir + "/lineitem/l_shipinstruct.bin", apply_permutation(cols[13].uint8_data, perm));
        write_column(gendb_dir + "/lineitem/l_shipmode.bin", apply_permutation(cols[14].uint8_data, perm));
        write_string_column(gendb_dir + "/lineitem/l_comment.bin", apply_permutation(cols[15].string_data, perm));

        // Write metadata
        std::ofstream meta(gendb_dir + "/lineitem_metadata.json");
        meta << "{\n";
        meta << "  \"row_count\": " << row_count << ",\n";
        meta << "  \"columns\": [\n";
        meta << "    {\"name\": \"l_orderkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"l_partkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"l_suppkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"l_linenumber\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"l_quantity\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"l_extendedprice\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"l_discount\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"l_tax\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"l_returnflag\", \"type\": \"uint8_t\", \"encoding\": \"dictionary\"},\n";
        meta << "    {\"name\": \"l_linestatus\", \"type\": \"uint8_t\", \"encoding\": \"dictionary\"},\n";
        meta << "    {\"name\": \"l_shipdate\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"l_commitdate\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"l_receiptdate\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"l_shipinstruct\", \"type\": \"uint8_t\", \"encoding\": \"dictionary\"},\n";
        meta << "    {\"name\": \"l_shipmode\", \"type\": \"uint8_t\", \"encoding\": \"dictionary\"},\n";
        meta << "    {\"name\": \"l_comment\", \"type\": \"string\"}\n";
        meta << "  ],\n";
        meta << "  \"dictionaries\": {\n";
        meta << "    \"l_returnflag\": [";
        for (size_t i = 0; i < returnflag_dict.values.size(); i++) {
            if (i > 0) meta << ", ";
            meta << "\"" << returnflag_dict.values[i] << "\"";
        }
        meta << "],\n";
        meta << "    \"l_linestatus\": [";
        for (size_t i = 0; i < linestatus_dict.values.size(); i++) {
            if (i > 0) meta << ", ";
            meta << "\"" << linestatus_dict.values[i] << "\"";
        }
        meta << "],\n";
        meta << "    \"l_shipinstruct\": [";
        for (size_t i = 0; i < shipinstruct_dict.values.size(); i++) {
            if (i > 0) meta << ", ";
            meta << "\"" << shipinstruct_dict.values[i] << "\"";
        }
        meta << "],\n";
        meta << "    \"l_shipmode\": [";
        for (size_t i = 0; i < shipmode_dict.values.size(); i++) {
            if (i > 0) meta << ", ";
            meta << "\"" << shipmode_dict.values[i] << "\"";
        }
        meta << "]\n";
        meta << "  }\n";
        meta << "}\n";

        munmap(data, size);
        close(fd);

        // Verify dates
        std::ifstream date_check(gendb_dir + "/lineitem/l_shipdate.bin", std::ios::binary);
        int32_t sample_dates[10];
        date_check.read(reinterpret_cast<char*>(sample_dates), sizeof(sample_dates));
        std::cout << "Sample shipdate values (epoch days): ";
        for (int i = 0; i < 10; i++) std::cout << sample_dates[i] << " ";
        std::cout << std::endl;
        for (int i = 0; i < 10; i++) {
            if (sample_dates[i] < 3000) {
                std::cerr << "ERROR: Date value " << sample_dates[i] << " indicates year-only parsing!" << std::endl;
                return 1;
            }
        }

        std::cout << "Ingested " << row_count << " rows into lineitem" << std::endl;
    }

    std::cout << "Ingesting orders..." << std::endl;
    {
        std::string path = data_dir + "/orders.tbl";
        int fd = open(path.c_str(), O_RDONLY);
        struct stat sb;
        fstat(fd, &sb);
        size_t size = sb.st_size;

        char* data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(data, size, MADV_SEQUENTIAL);

        std::vector<ColumnData> cols(9);
        Dictionary orderstatus_dict, orderpriority_dict;

        size_t row_count = 0;
        for (size_t i = 0; i < size; i++) {
            if (data[i] == '\n') row_count++;
        }

        for (int i = 0; i < 9; i++) {
            cols[i].int_data.reserve(row_count);
            cols[i].double_data.reserve(row_count);
            cols[i].uint8_data.reserve(row_count);
            cols[i].string_data.reserve(row_count);
        }

        parse_orders(data, size, cols, orderstatus_dict, orderpriority_dict, 0, row_count);

        // Sort by o_orderdate
        std::cout << "Sorting orders by o_orderdate..." << std::endl;
        auto perm = create_sort_permutation(cols[4].int_data);

        system(("mkdir -p " + gendb_dir + "/orders").c_str());

        write_column(gendb_dir + "/orders/o_orderkey.bin", apply_permutation(cols[0].int_data, perm));
        write_column(gendb_dir + "/orders/o_custkey.bin", apply_permutation(cols[1].int_data, perm));
        write_column(gendb_dir + "/orders/o_orderstatus.bin", apply_permutation(cols[2].uint8_data, perm));
        write_column(gendb_dir + "/orders/o_totalprice.bin", apply_permutation(cols[3].double_data, perm));
        write_column(gendb_dir + "/orders/o_orderdate.bin", apply_permutation(cols[4].int_data, perm));
        write_column(gendb_dir + "/orders/o_orderpriority.bin", apply_permutation(cols[5].uint8_data, perm));
        write_string_column(gendb_dir + "/orders/o_clerk.bin", apply_permutation(cols[6].string_data, perm));
        write_column(gendb_dir + "/orders/o_shippriority.bin", apply_permutation(cols[7].int_data, perm));
        write_string_column(gendb_dir + "/orders/o_comment.bin", apply_permutation(cols[8].string_data, perm));

        std::ofstream meta(gendb_dir + "/orders_metadata.json");
        meta << "{\n";
        meta << "  \"row_count\": " << row_count << ",\n";
        meta << "  \"columns\": [\n";
        meta << "    {\"name\": \"o_orderkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"o_custkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"o_orderstatus\", \"type\": \"uint8_t\", \"encoding\": \"dictionary\"},\n";
        meta << "    {\"name\": \"o_totalprice\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"o_orderdate\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"o_orderpriority\", \"type\": \"uint8_t\", \"encoding\": \"dictionary\"},\n";
        meta << "    {\"name\": \"o_clerk\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"o_shippriority\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"o_comment\", \"type\": \"string\"}\n";
        meta << "  ],\n";
        meta << "  \"dictionaries\": {\n";
        meta << "    \"o_orderstatus\": [";
        for (size_t i = 0; i < orderstatus_dict.values.size(); i++) {
            if (i > 0) meta << ", ";
            meta << "\"" << orderstatus_dict.values[i] << "\"";
        }
        meta << "],\n";
        meta << "    \"o_orderpriority\": [";
        for (size_t i = 0; i < orderpriority_dict.values.size(); i++) {
            if (i > 0) meta << ", ";
            meta << "\"" << orderpriority_dict.values[i] << "\"";
        }
        meta << "]\n";
        meta << "  }\n";
        meta << "}\n";

        munmap(data, size);
        close(fd);
        std::cout << "Ingested " << row_count << " rows into orders" << std::endl;
    }

    std::cout << "Ingesting customer..." << std::endl;
    {
        std::string path = data_dir + "/customer.tbl";
        int fd = open(path.c_str(), O_RDONLY);
        struct stat sb;
        fstat(fd, &sb);
        size_t size = sb.st_size;

        char* data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(data, size, MADV_SEQUENTIAL);

        std::vector<ColumnData> cols(8);
        Dictionary mktsegment_dict;

        size_t row_count = 0;
        for (size_t i = 0; i < size; i++) {
            if (data[i] == '\n') row_count++;
        }

        for (int i = 0; i < 8; i++) {
            cols[i].int_data.reserve(row_count);
            cols[i].double_data.reserve(row_count);
            cols[i].uint8_data.reserve(row_count);
            cols[i].string_data.reserve(row_count);
        }

        parse_customer(data, size, cols, mktsegment_dict);

        system(("mkdir -p " + gendb_dir + "/customer").c_str());

        write_column(gendb_dir + "/customer/c_custkey.bin", cols[0].int_data);
        write_string_column(gendb_dir + "/customer/c_name.bin", cols[1].string_data);
        write_string_column(gendb_dir + "/customer/c_address.bin", cols[2].string_data);
        write_column(gendb_dir + "/customer/c_nationkey.bin", cols[3].int_data);
        write_string_column(gendb_dir + "/customer/c_phone.bin", cols[4].string_data);
        write_column(gendb_dir + "/customer/c_acctbal.bin", cols[5].double_data);
        write_column(gendb_dir + "/customer/c_mktsegment.bin", cols[6].uint8_data);
        write_string_column(gendb_dir + "/customer/c_comment.bin", cols[7].string_data);

        std::ofstream meta(gendb_dir + "/customer_metadata.json");
        meta << "{\n";
        meta << "  \"row_count\": " << row_count << ",\n";
        meta << "  \"columns\": [\n";
        meta << "    {\"name\": \"c_custkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"c_name\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"c_address\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"c_nationkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"c_phone\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"c_acctbal\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"c_mktsegment\", \"type\": \"uint8_t\", \"encoding\": \"dictionary\"},\n";
        meta << "    {\"name\": \"c_comment\", \"type\": \"string\"}\n";
        meta << "  ],\n";
        meta << "  \"dictionaries\": {\n";
        meta << "    \"c_mktsegment\": [";
        for (size_t i = 0; i < mktsegment_dict.values.size(); i++) {
            if (i > 0) meta << ", ";
            meta << "\"" << mktsegment_dict.values[i] << "\"";
        }
        meta << "]\n";
        meta << "  }\n";
        meta << "}\n";

        munmap(data, size);
        close(fd);
        std::cout << "Ingested " << row_count << " rows into customer" << std::endl;
    }

    std::cout << "Ingesting partsupp..." << std::endl;
    {
        std::string path = data_dir + "/partsupp.tbl";
        int fd = open(path.c_str(), O_RDONLY);
        struct stat sb;
        fstat(fd, &sb);
        size_t size = sb.st_size;

        char* data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(data, size, MADV_SEQUENTIAL);

        std::vector<ColumnData> cols(5);
        std::vector<std::string> types = {"int", "int", "int", "double", "string"};

        size_t row_count = 0;
        for (size_t i = 0; i < size; i++) {
            if (data[i] == '\n') row_count++;
        }

        for (int i = 0; i < 5; i++) {
            cols[i].int_data.reserve(row_count);
            cols[i].double_data.reserve(row_count);
            cols[i].string_data.reserve(row_count);
        }

        parse_simple_table(data, size, cols, types);

        system(("mkdir -p " + gendb_dir + "/partsupp").c_str());

        write_column(gendb_dir + "/partsupp/ps_partkey.bin", cols[0].int_data);
        write_column(gendb_dir + "/partsupp/ps_suppkey.bin", cols[1].int_data);
        write_column(gendb_dir + "/partsupp/ps_availqty.bin", cols[2].int_data);
        write_column(gendb_dir + "/partsupp/ps_supplycost.bin", cols[3].double_data);
        write_string_column(gendb_dir + "/partsupp/ps_comment.bin", cols[4].string_data);

        std::ofstream meta(gendb_dir + "/partsupp_metadata.json");
        meta << "{\n";
        meta << "  \"row_count\": " << row_count << ",\n";
        meta << "  \"columns\": [\n";
        meta << "    {\"name\": \"ps_partkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"ps_suppkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"ps_availqty\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"ps_supplycost\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"ps_comment\", \"type\": \"string\"}\n";
        meta << "  ],\n";
        meta << "  \"dictionaries\": {}\n";
        meta << "}\n";

        munmap(data, size);
        close(fd);
        std::cout << "Ingested " << row_count << " rows into partsupp" << std::endl;
    }

    std::cout << "Ingesting part..." << std::endl;
    {
        std::string path = data_dir + "/part.tbl";
        int fd = open(path.c_str(), O_RDONLY);
        struct stat sb;
        fstat(fd, &sb);
        size_t size = sb.st_size;

        char* data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(data, size, MADV_SEQUENTIAL);

        std::vector<ColumnData> cols(9);
        std::vector<std::string> types = {"int", "string", "string", "string", "string", "int", "string", "double", "string"};

        size_t row_count = 0;
        for (size_t i = 0; i < size; i++) {
            if (data[i] == '\n') row_count++;
        }

        for (int i = 0; i < 9; i++) {
            cols[i].int_data.reserve(row_count);
            cols[i].double_data.reserve(row_count);
            cols[i].string_data.reserve(row_count);
        }

        parse_simple_table(data, size, cols, types);

        system(("mkdir -p " + gendb_dir + "/part").c_str());

        write_column(gendb_dir + "/part/p_partkey.bin", cols[0].int_data);
        write_string_column(gendb_dir + "/part/p_name.bin", cols[1].string_data);
        write_string_column(gendb_dir + "/part/p_mfgr.bin", cols[2].string_data);
        write_string_column(gendb_dir + "/part/p_brand.bin", cols[3].string_data);
        write_string_column(gendb_dir + "/part/p_type.bin", cols[4].string_data);
        write_column(gendb_dir + "/part/p_size.bin", cols[5].int_data);
        write_string_column(gendb_dir + "/part/p_container.bin", cols[6].string_data);
        write_column(gendb_dir + "/part/p_retailprice.bin", cols[7].double_data);
        write_string_column(gendb_dir + "/part/p_comment.bin", cols[8].string_data);

        std::ofstream meta(gendb_dir + "/part_metadata.json");
        meta << "{\n";
        meta << "  \"row_count\": " << row_count << ",\n";
        meta << "  \"columns\": [\n";
        meta << "    {\"name\": \"p_partkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"p_name\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"p_mfgr\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"p_brand\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"p_type\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"p_size\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"p_container\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"p_retailprice\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"p_comment\", \"type\": \"string\"}\n";
        meta << "  ],\n";
        meta << "  \"dictionaries\": {}\n";
        meta << "}\n";

        munmap(data, size);
        close(fd);
        std::cout << "Ingested " << row_count << " rows into part" << std::endl;
    }

    std::cout << "Ingesting supplier..." << std::endl;
    {
        std::string path = data_dir + "/supplier.tbl";
        int fd = open(path.c_str(), O_RDONLY);
        struct stat sb;
        fstat(fd, &sb);
        size_t size = sb.st_size;

        char* data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(data, size, MADV_SEQUENTIAL);

        std::vector<ColumnData> cols(7);
        std::vector<std::string> types = {"int", "string", "string", "int", "string", "double", "string"};

        size_t row_count = 0;
        for (size_t i = 0; i < size; i++) {
            if (data[i] == '\n') row_count++;
        }

        for (int i = 0; i < 7; i++) {
            cols[i].int_data.reserve(row_count);
            cols[i].double_data.reserve(row_count);
            cols[i].string_data.reserve(row_count);
        }

        parse_simple_table(data, size, cols, types);

        system(("mkdir -p " + gendb_dir + "/supplier").c_str());

        write_column(gendb_dir + "/supplier/s_suppkey.bin", cols[0].int_data);
        write_string_column(gendb_dir + "/supplier/s_name.bin", cols[1].string_data);
        write_string_column(gendb_dir + "/supplier/s_address.bin", cols[2].string_data);
        write_column(gendb_dir + "/supplier/s_nationkey.bin", cols[3].int_data);
        write_string_column(gendb_dir + "/supplier/s_phone.bin", cols[4].string_data);
        write_column(gendb_dir + "/supplier/s_acctbal.bin", cols[5].double_data);
        write_string_column(gendb_dir + "/supplier/s_comment.bin", cols[6].string_data);

        std::ofstream meta(gendb_dir + "/supplier_metadata.json");
        meta << "{\n";
        meta << "  \"row_count\": " << row_count << ",\n";
        meta << "  \"columns\": [\n";
        meta << "    {\"name\": \"s_suppkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"s_name\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"s_address\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"s_nationkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"s_phone\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"s_acctbal\", \"type\": \"double\"},\n";
        meta << "    {\"name\": \"s_comment\", \"type\": \"string\"}\n";
        meta << "  ],\n";
        meta << "  \"dictionaries\": {}\n";
        meta << "}\n";

        munmap(data, size);
        close(fd);
        std::cout << "Ingested " << row_count << " rows into supplier" << std::endl;
    }

    std::cout << "Ingesting nation..." << std::endl;
    {
        std::string path = data_dir + "/nation.tbl";
        int fd = open(path.c_str(), O_RDONLY);
        struct stat sb;
        fstat(fd, &sb);
        size_t size = sb.st_size;

        char* data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(data, size, MADV_SEQUENTIAL);

        std::vector<ColumnData> cols(4);
        std::vector<std::string> types = {"int", "string", "int", "string"};

        size_t row_count = 0;
        for (size_t i = 0; i < size; i++) {
            if (data[i] == '\n') row_count++;
        }

        for (int i = 0; i < 4; i++) {
            cols[i].int_data.reserve(row_count);
            cols[i].string_data.reserve(row_count);
        }

        parse_simple_table(data, size, cols, types);

        system(("mkdir -p " + gendb_dir + "/nation").c_str());

        write_column(gendb_dir + "/nation/n_nationkey.bin", cols[0].int_data);
        write_string_column(gendb_dir + "/nation/n_name.bin", cols[1].string_data);
        write_column(gendb_dir + "/nation/n_regionkey.bin", cols[2].int_data);
        write_string_column(gendb_dir + "/nation/n_comment.bin", cols[3].string_data);

        std::ofstream meta(gendb_dir + "/nation_metadata.json");
        meta << "{\n";
        meta << "  \"row_count\": " << row_count << ",\n";
        meta << "  \"columns\": [\n";
        meta << "    {\"name\": \"n_nationkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"n_name\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"n_regionkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"n_comment\", \"type\": \"string\"}\n";
        meta << "  ],\n";
        meta << "  \"dictionaries\": {}\n";
        meta << "}\n";

        munmap(data, size);
        close(fd);
        std::cout << "Ingested " << row_count << " rows into nation" << std::endl;
    }

    std::cout << "Ingesting region..." << std::endl;
    {
        std::string path = data_dir + "/region.tbl";
        int fd = open(path.c_str(), O_RDONLY);
        struct stat sb;
        fstat(fd, &sb);
        size_t size = sb.st_size;

        char* data = (char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(data, size, MADV_SEQUENTIAL);

        std::vector<ColumnData> cols(3);
        std::vector<std::string> types = {"int", "string", "string"};

        size_t row_count = 0;
        for (size_t i = 0; i < size; i++) {
            if (data[i] == '\n') row_count++;
        }

        for (int i = 0; i < 3; i++) {
            cols[i].int_data.reserve(row_count);
            cols[i].string_data.reserve(row_count);
        }

        parse_simple_table(data, size, cols, types);

        system(("mkdir -p " + gendb_dir + "/region").c_str());

        write_column(gendb_dir + "/region/r_regionkey.bin", cols[0].int_data);
        write_string_column(gendb_dir + "/region/r_name.bin", cols[1].string_data);
        write_string_column(gendb_dir + "/region/r_comment.bin", cols[2].string_data);

        std::ofstream meta(gendb_dir + "/region_metadata.json");
        meta << "{\n";
        meta << "  \"row_count\": " << row_count << ",\n";
        meta << "  \"columns\": [\n";
        meta << "    {\"name\": \"r_regionkey\", \"type\": \"int32_t\"},\n";
        meta << "    {\"name\": \"r_name\", \"type\": \"string\"},\n";
        meta << "    {\"name\": \"r_comment\", \"type\": \"string\"}\n";
        meta << "  ],\n";
        meta << "  \"dictionaries\": {}\n";
        meta << "}\n";

        munmap(data, size);
        close(fd);
        std::cout << "Ingested " << row_count << " rows into region" << std::endl;
    }

    std::cout << "Ingestion complete!" << std::endl;
    return 0;
}
