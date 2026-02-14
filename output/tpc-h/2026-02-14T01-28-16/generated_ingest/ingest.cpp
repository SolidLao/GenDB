#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <charconv>
#include <chrono>
#include <filesystem>

namespace fs = std::filesystem;

// Dictionary for low-cardinality string columns
struct Dictionary {
    std::unordered_map<std::string, uint8_t> map;
    std::vector<std::string> values;
    std::mutex mtx;

    uint8_t encode(const std::string& str) {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = map.find(str);
        if (it != map.end()) {
            return it->second;
        }
        uint8_t code = values.size();
        values.push_back(str);
        map[str] = code;
        return code;
    }
};

// Date parsing: YYYY-MM-DD -> days since epoch
int32_t parse_date(const char* str, size_t len) {
    int year = 0, month = 0, day = 0;

    // Parse YYYY
    for (int i = 0; i < 4; i++) {
        year = year * 10 + (str[i] - '0');
    }
    // Parse MM
    for (int i = 5; i < 7; i++) {
        month = month * 10 + (str[i] - '0');
    }
    // Parse DD
    for (int i = 8; i < 10; i++) {
        day = day * 10 + (str[i] - '0');
    }

    // Days since epoch (1970-01-01)
    // Simple calculation: count days from 1970 to target date
    int days = 0;

    // Add days for complete years from 1970 to year-1
    for (int y = 1970; y < year; y++) {
        if ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)) {
            days += 366;  // Leap year
        } else {
            days += 365;
        }
    }

    // Add days for complete months in target year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0));
    if (is_leap) days_in_month[1] = 29;

    for (int m = 1; m < month; m++) {
        days += days_in_month[m - 1];
    }

    // Add remaining days
    days += day - 1;  // day is 1-indexed

    return days;
}

struct LineitemRow {
    int32_t l_orderkey;
    int32_t l_partkey;
    int32_t l_suppkey;
    int32_t l_linenumber;
    double l_quantity;
    double l_extendedprice;
    double l_discount;
    double l_tax;
    uint8_t l_returnflag;
    uint8_t l_linestatus;
    int32_t l_shipdate;
    int32_t l_commitdate;
    int32_t l_receiptdate;
    uint8_t l_shipinstruct;
    uint8_t l_shipmode;
    std::string l_comment;
};

struct OrdersRow {
    int32_t o_orderkey;
    int32_t o_custkey;
    uint8_t o_orderstatus;
    double o_totalprice;
    int32_t o_orderdate;
    uint8_t o_orderpriority;
    std::string o_clerk;
    int32_t o_shippriority;
    std::string o_comment;
};

struct CustomerRow {
    int32_t c_custkey;
    std::string c_name;
    std::string c_address;
    int32_t c_nationkey;
    std::string c_phone;
    double c_acctbal;
    uint8_t c_mktsegment;
    std::string c_comment;
};

// Global dictionaries
Dictionary lineitem_returnflag_dict;
Dictionary lineitem_linestatus_dict;
Dictionary lineitem_shipinstruct_dict;
Dictionary lineitem_shipmode_dict;
Dictionary orders_orderstatus_dict;
Dictionary orders_orderpriority_dict;
Dictionary customer_mktsegment_dict;

// Parse a line from lineitem.tbl
bool parse_lineitem_line(const char* line, size_t len, LineitemRow& row) {
    const char* p = line;
    const char* end = line + len;
    int field = 0;
    const char* field_start = p;

    while (p < end && field < 16) {
        if (*p == '|') {
            size_t field_len = p - field_start;

            switch (field) {
                case 0: std::from_chars(field_start, field_start + field_len, row.l_orderkey); break;
                case 1: std::from_chars(field_start, field_start + field_len, row.l_partkey); break;
                case 2: std::from_chars(field_start, field_start + field_len, row.l_suppkey); break;
                case 3: std::from_chars(field_start, field_start + field_len, row.l_linenumber); break;
                case 4: std::from_chars(field_start, field_start + field_len, row.l_quantity); break;
                case 5: std::from_chars(field_start, field_start + field_len, row.l_extendedprice); break;
                case 6: std::from_chars(field_start, field_start + field_len, row.l_discount); break;
                case 7: std::from_chars(field_start, field_start + field_len, row.l_tax); break;
                case 8: row.l_returnflag = lineitem_returnflag_dict.encode(std::string(field_start, field_len)); break;
                case 9: row.l_linestatus = lineitem_linestatus_dict.encode(std::string(field_start, field_len)); break;
                case 10: row.l_shipdate = parse_date(field_start, field_len); break;
                case 11: row.l_commitdate = parse_date(field_start, field_len); break;
                case 12: row.l_receiptdate = parse_date(field_start, field_len); break;
                case 13: row.l_shipinstruct = lineitem_shipinstruct_dict.encode(std::string(field_start, field_len)); break;
                case 14: row.l_shipmode = lineitem_shipmode_dict.encode(std::string(field_start, field_len)); break;
                case 15: row.l_comment = std::string(field_start, field_len); break;
            }

            field++;
            p++;
            field_start = p;
        } else {
            p++;
        }
    }

    return field == 16;
}

bool parse_orders_line(const char* line, size_t len, OrdersRow& row) {
    const char* p = line;
    const char* end = line + len;
    int field = 0;
    const char* field_start = p;

    while (p < end && field < 9) {
        if (*p == '|') {
            size_t field_len = p - field_start;

            switch (field) {
                case 0: std::from_chars(field_start, field_start + field_len, row.o_orderkey); break;
                case 1: std::from_chars(field_start, field_start + field_len, row.o_custkey); break;
                case 2: row.o_orderstatus = orders_orderstatus_dict.encode(std::string(field_start, field_len)); break;
                case 3: std::from_chars(field_start, field_start + field_len, row.o_totalprice); break;
                case 4: row.o_orderdate = parse_date(field_start, field_len); break;
                case 5: row.o_orderpriority = orders_orderpriority_dict.encode(std::string(field_start, field_len)); break;
                case 6: row.o_clerk = std::string(field_start, field_len); break;
                case 7: std::from_chars(field_start, field_start + field_len, row.o_shippriority); break;
                case 8: row.o_comment = std::string(field_start, field_len); break;
            }

            field++;
            p++;
            field_start = p;
        } else {
            p++;
        }
    }

    return field == 9;
}

bool parse_customer_line(const char* line, size_t len, CustomerRow& row) {
    const char* p = line;
    const char* end = line + len;
    int field = 0;
    const char* field_start = p;

    while (p < end && field < 8) {
        if (*p == '|') {
            size_t field_len = p - field_start;

            switch (field) {
                case 0: std::from_chars(field_start, field_start + field_len, row.c_custkey); break;
                case 1: row.c_name = std::string(field_start, field_len); break;
                case 2: row.c_address = std::string(field_start, field_len); break;
                case 3: std::from_chars(field_start, field_start + field_len, row.c_nationkey); break;
                case 4: row.c_phone = std::string(field_start, field_len); break;
                case 5: std::from_chars(field_start, field_start + field_len, row.c_acctbal); break;
                case 6: row.c_mktsegment = customer_mktsegment_dict.encode(std::string(field_start, field_len)); break;
                case 7: row.c_comment = std::string(field_start, field_len); break;
            }

            field++;
            p++;
            field_start = p;
        } else {
            p++;
        }
    }

    return field == 8;
}

// Write binary column file with buffering
template<typename T>
void write_column_binary(const std::string& filepath, const std::vector<T>& data) {
    std::ofstream out(filepath, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open " << filepath << " for writing\n";
        return;
    }

    constexpr size_t BUFFER_SIZE = 1024 * 1024;  // 1MB buffer
    std::vector<char> buffer;
    buffer.reserve(BUFFER_SIZE);

    const char* data_ptr = reinterpret_cast<const char*>(data.data());
    size_t total_bytes = data.size() * sizeof(T);
    size_t written = 0;

    while (written < total_bytes) {
        size_t to_write = std::min(BUFFER_SIZE, total_bytes - written);
        out.write(data_ptr + written, to_write);
        written += to_write;
    }
}

void write_string_column_binary(const std::string& filepath, const std::vector<std::string>& data) {
    std::ofstream out(filepath, std::ios::binary);
    if (!out) {
        std::cerr << "Failed to open " << filepath << " for writing\n";
        return;
    }

    // Write: [count][offset1][offset2]...[offsetN][str1][str2]...[strN]
    size_t count = data.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(size_t));

    std::vector<size_t> offsets;
    offsets.reserve(count);
    size_t current_offset = 0;

    for (const auto& str : data) {
        offsets.push_back(current_offset);
        current_offset += str.size();
    }

    out.write(reinterpret_cast<const char*>(offsets.data()), offsets.size() * sizeof(size_t));

    for (const auto& str : data) {
        out.write(str.data(), str.size());
    }
}

void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string filepath = data_dir + "/lineitem.tbl";

    // mmap the file
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filepath << "\n";
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* mapped = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filepath << "\n";
        close(fd);
        return;
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    std::cout << "Parsing lineitem.tbl (" << file_size / (1024*1024) << " MB)...\n";

    // Parse all rows
    std::vector<LineitemRow> rows;
    rows.reserve(60000000);

    const char* p = mapped;
    const char* end = mapped + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            LineitemRow row;
            if (parse_lineitem_line(line_start, p - line_start, row)) {
                rows.push_back(std::move(row));
            }
            p++;
            line_start = p;
        } else {
            p++;
        }
    }

    munmap(mapped, file_size);
    close(fd);

    std::cout << "Parsed " << rows.size() << " rows. Sorting by l_shipdate...\n";

    // Sort by l_shipdate using permutation
    std::vector<size_t> perm(rows.size());
    for (size_t i = 0; i < perm.size(); i++) perm[i] = i;

    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return rows[a].l_shipdate < rows[b].l_shipdate;
    });

    std::cout << "Sorting complete. Writing binary columns...\n";

    // Apply permutation and write columns
    std::vector<int32_t> l_orderkey(rows.size());
    std::vector<int32_t> l_partkey(rows.size());
    std::vector<int32_t> l_suppkey(rows.size());
    std::vector<int32_t> l_linenumber(rows.size());
    std::vector<double> l_quantity(rows.size());
    std::vector<double> l_extendedprice(rows.size());
    std::vector<double> l_discount(rows.size());
    std::vector<double> l_tax(rows.size());
    std::vector<uint8_t> l_returnflag(rows.size());
    std::vector<uint8_t> l_linestatus(rows.size());
    std::vector<int32_t> l_shipdate(rows.size());
    std::vector<int32_t> l_commitdate(rows.size());
    std::vector<int32_t> l_receiptdate(rows.size());
    std::vector<uint8_t> l_shipinstruct(rows.size());
    std::vector<uint8_t> l_shipmode(rows.size());
    std::vector<std::string> l_comment(rows.size());

    for (size_t i = 0; i < rows.size(); i++) {
        size_t idx = perm[i];
        l_orderkey[i] = rows[idx].l_orderkey;
        l_partkey[i] = rows[idx].l_partkey;
        l_suppkey[i] = rows[idx].l_suppkey;
        l_linenumber[i] = rows[idx].l_linenumber;
        l_quantity[i] = rows[idx].l_quantity;
        l_extendedprice[i] = rows[idx].l_extendedprice;
        l_discount[i] = rows[idx].l_discount;
        l_tax[i] = rows[idx].l_tax;
        l_returnflag[i] = rows[idx].l_returnflag;
        l_linestatus[i] = rows[idx].l_linestatus;
        l_shipdate[i] = rows[idx].l_shipdate;
        l_commitdate[i] = rows[idx].l_commitdate;
        l_receiptdate[i] = rows[idx].l_receiptdate;
        l_shipinstruct[i] = rows[idx].l_shipinstruct;
        l_shipmode[i] = rows[idx].l_shipmode;
        l_comment[i] = rows[idx].l_comment;
    }

    write_column_binary(gendb_dir + "/lineitem_l_orderkey.bin", l_orderkey);
    write_column_binary(gendb_dir + "/lineitem_l_partkey.bin", l_partkey);
    write_column_binary(gendb_dir + "/lineitem_l_suppkey.bin", l_suppkey);
    write_column_binary(gendb_dir + "/lineitem_l_linenumber.bin", l_linenumber);
    write_column_binary(gendb_dir + "/lineitem_l_quantity.bin", l_quantity);
    write_column_binary(gendb_dir + "/lineitem_l_extendedprice.bin", l_extendedprice);
    write_column_binary(gendb_dir + "/lineitem_l_discount.bin", l_discount);
    write_column_binary(gendb_dir + "/lineitem_l_tax.bin", l_tax);
    write_column_binary(gendb_dir + "/lineitem_l_returnflag.bin", l_returnflag);
    write_column_binary(gendb_dir + "/lineitem_l_linestatus.bin", l_linestatus);
    write_column_binary(gendb_dir + "/lineitem_l_shipdate.bin", l_shipdate);
    write_column_binary(gendb_dir + "/lineitem_l_commitdate.bin", l_commitdate);
    write_column_binary(gendb_dir + "/lineitem_l_receiptdate.bin", l_receiptdate);
    write_column_binary(gendb_dir + "/lineitem_l_shipinstruct.bin", l_shipinstruct);
    write_column_binary(gendb_dir + "/lineitem_l_shipmode.bin", l_shipmode);
    write_string_column_binary(gendb_dir + "/lineitem_l_comment.bin", l_comment);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start).count();

    std::cout << "Lineitem ingestion complete: " << rows.size() << " rows in " << duration << "s\n";
    std::cout << "  l_shipdate range: " << l_shipdate.front() << " to " << l_shipdate.back() << " (days since epoch)\n";
}

void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string filepath = data_dir + "/orders.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filepath << "\n";
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* mapped = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filepath << "\n";
        close(fd);
        return;
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    std::cout << "Parsing orders.tbl...\n";

    std::vector<OrdersRow> rows;
    rows.reserve(15000000);

    const char* p = mapped;
    const char* end = mapped + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            OrdersRow row;
            if (parse_orders_line(line_start, p - line_start, row)) {
                rows.push_back(std::move(row));
            }
            p++;
            line_start = p;
        } else {
            p++;
        }
    }

    munmap(mapped, file_size);
    close(fd);

    std::cout << "Parsed " << rows.size() << " rows. Sorting by o_orderdate...\n";

    std::vector<size_t> perm(rows.size());
    for (size_t i = 0; i < perm.size(); i++) perm[i] = i;

    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return rows[a].o_orderdate < rows[b].o_orderdate;
    });

    std::cout << "Writing binary columns...\n";

    std::vector<int32_t> o_orderkey(rows.size());
    std::vector<int32_t> o_custkey(rows.size());
    std::vector<uint8_t> o_orderstatus(rows.size());
    std::vector<double> o_totalprice(rows.size());
    std::vector<int32_t> o_orderdate(rows.size());
    std::vector<uint8_t> o_orderpriority(rows.size());
    std::vector<std::string> o_clerk(rows.size());
    std::vector<int32_t> o_shippriority(rows.size());
    std::vector<std::string> o_comment(rows.size());

    for (size_t i = 0; i < rows.size(); i++) {
        size_t idx = perm[i];
        o_orderkey[i] = rows[idx].o_orderkey;
        o_custkey[i] = rows[idx].o_custkey;
        o_orderstatus[i] = rows[idx].o_orderstatus;
        o_totalprice[i] = rows[idx].o_totalprice;
        o_orderdate[i] = rows[idx].o_orderdate;
        o_orderpriority[i] = rows[idx].o_orderpriority;
        o_clerk[i] = rows[idx].o_clerk;
        o_shippriority[i] = rows[idx].o_shippriority;
        o_comment[i] = rows[idx].o_comment;
    }

    write_column_binary(gendb_dir + "/orders_o_orderkey.bin", o_orderkey);
    write_column_binary(gendb_dir + "/orders_o_custkey.bin", o_custkey);
    write_column_binary(gendb_dir + "/orders_o_orderstatus.bin", o_orderstatus);
    write_column_binary(gendb_dir + "/orders_o_totalprice.bin", o_totalprice);
    write_column_binary(gendb_dir + "/orders_o_orderdate.bin", o_orderdate);
    write_column_binary(gendb_dir + "/orders_o_orderpriority.bin", o_orderpriority);
    write_string_column_binary(gendb_dir + "/orders_o_clerk.bin", o_clerk);
    write_column_binary(gendb_dir + "/orders_o_shippriority.bin", o_shippriority);
    write_string_column_binary(gendb_dir + "/orders_o_comment.bin", o_comment);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start).count();

    std::cout << "Orders ingestion complete: " << rows.size() << " rows in " << duration << "s\n";
    std::cout << "  o_orderdate range: " << o_orderdate.front() << " to " << o_orderdate.back() << " (days since epoch)\n";
}

void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string filepath = data_dir + "/customer.tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filepath << "\n";
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* mapped = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filepath << "\n";
        close(fd);
        return;
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    std::cout << "Parsing customer.tbl...\n";

    std::vector<CustomerRow> rows;
    rows.reserve(1500000);

    const char* p = mapped;
    const char* end = mapped + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            CustomerRow row;
            if (parse_customer_line(line_start, p - line_start, row)) {
                rows.push_back(std::move(row));
            }
            p++;
            line_start = p;
        } else {
            p++;
        }
    }

    munmap(mapped, file_size);
    close(fd);

    std::cout << "Parsed " << rows.size() << " rows. Sorting by c_custkey...\n";

    std::vector<size_t> perm(rows.size());
    for (size_t i = 0; i < perm.size(); i++) perm[i] = i;

    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return rows[a].c_custkey < rows[b].c_custkey;
    });

    std::cout << "Writing binary columns...\n";

    std::vector<int32_t> c_custkey(rows.size());
    std::vector<std::string> c_name(rows.size());
    std::vector<std::string> c_address(rows.size());
    std::vector<int32_t> c_nationkey(rows.size());
    std::vector<std::string> c_phone(rows.size());
    std::vector<double> c_acctbal(rows.size());
    std::vector<uint8_t> c_mktsegment(rows.size());
    std::vector<std::string> c_comment(rows.size());

    for (size_t i = 0; i < rows.size(); i++) {
        size_t idx = perm[i];
        c_custkey[i] = rows[idx].c_custkey;
        c_name[i] = rows[idx].c_name;
        c_address[i] = rows[idx].c_address;
        c_nationkey[i] = rows[idx].c_nationkey;
        c_phone[i] = rows[idx].c_phone;
        c_acctbal[i] = rows[idx].c_acctbal;
        c_mktsegment[i] = rows[idx].c_mktsegment;
        c_comment[i] = rows[idx].c_comment;
    }

    write_column_binary(gendb_dir + "/customer_c_custkey.bin", c_custkey);
    write_string_column_binary(gendb_dir + "/customer_c_name.bin", c_name);
    write_string_column_binary(gendb_dir + "/customer_c_address.bin", c_address);
    write_column_binary(gendb_dir + "/customer_c_nationkey.bin", c_nationkey);
    write_string_column_binary(gendb_dir + "/customer_c_phone.bin", c_phone);
    write_column_binary(gendb_dir + "/customer_c_acctbal.bin", c_acctbal);
    write_column_binary(gendb_dir + "/customer_c_mktsegment.bin", c_mktsegment);
    write_string_column_binary(gendb_dir + "/customer_c_comment.bin", c_comment);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start).count();

    std::cout << "Customer ingestion complete: " << rows.size() << " rows in " << duration << "s\n";
}

// Simple ingest for small/unused tables (no sorting, no parallelism)
template<typename RowType>
void ingest_simple_table(const std::string& table_name, const std::string& data_dir, const std::string& gendb_dir,
                          std::function<bool(const char*, size_t, RowType&)> parser,
                          std::function<void(const std::vector<RowType>&, const std::string&)> writer,
                          size_t expected_rows) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string filepath = data_dir + "/" + table_name + ".tbl";

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filepath << "\n";
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* mapped = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filepath << "\n";
        close(fd);
        return;
    }

    madvise(mapped, file_size, MADV_SEQUENTIAL);

    std::cout << "Parsing " << table_name << ".tbl...\n";

    std::vector<RowType> rows;
    rows.reserve(expected_rows);

    const char* p = mapped;
    const char* end = mapped + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            RowType row;
            if (parser(line_start, p - line_start, row)) {
                rows.push_back(std::move(row));
            }
            p++;
            line_start = p;
        } else {
            p++;
        }
    }

    munmap(mapped, file_size);
    close(fd);

    std::cout << "Writing " << rows.size() << " rows to binary...\n";
    writer(rows, gendb_dir);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start).count();

    std::cout << table_name << " ingestion complete: " << rows.size() << " rows in " << duration << "ms\n";
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create gendb directory
    fs::create_directories(gendb_dir);

    std::cout << "=== GenDB Data Ingestion ===\n";
    std::cout << "Data directory: " << data_dir << "\n";
    std::cout << "GenDB directory: " << gendb_dir << "\n";
    std::cout << "CPU cores: " << std::thread::hardware_concurrency() << "\n\n";

    auto total_start = std::chrono::high_resolution_clock::now();

    // Ingest hot tables sequentially (they're large and memory-intensive)
    ingest_lineitem(data_dir, gendb_dir);
    ingest_orders(data_dir, gendb_dir);
    ingest_customer(data_dir, gendb_dir);

    // Small tables can be done quickly without optimization
    // (part, partsupp, supplier, nation, region - not accessed in workload)

    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(total_end - total_start).count();

    std::cout << "\n=== Ingestion Complete ===\n";
    std::cout << "Total time: " << total_duration << "s\n";

    // Write metadata
    std::ofstream meta(gendb_dir + "/metadata.json");
    meta << "{\n";
    meta << "  \"format\": \"binary_columnar\",\n";
    meta << "  \"date_encoding\": \"days_since_epoch_1970\",\n";
    meta << "  \"dictionaries\": {\n";
    meta << "    \"lineitem_l_returnflag\": [";
    for (size_t i = 0; i < lineitem_returnflag_dict.values.size(); i++) {
        if (i > 0) meta << ", ";
        meta << "\"" << lineitem_returnflag_dict.values[i] << "\"";
    }
    meta << "],\n";
    meta << "    \"lineitem_l_linestatus\": [";
    for (size_t i = 0; i < lineitem_linestatus_dict.values.size(); i++) {
        if (i > 0) meta << ", ";
        meta << "\"" << lineitem_linestatus_dict.values[i] << "\"";
    }
    meta << "],\n";
    meta << "    \"lineitem_l_shipinstruct\": [";
    for (size_t i = 0; i < lineitem_shipinstruct_dict.values.size(); i++) {
        if (i > 0) meta << ", ";
        meta << "\"" << lineitem_shipinstruct_dict.values[i] << "\"";
    }
    meta << "],\n";
    meta << "    \"lineitem_l_shipmode\": [";
    for (size_t i = 0; i < lineitem_shipmode_dict.values.size(); i++) {
        if (i > 0) meta << ", ";
        meta << "\"" << lineitem_shipmode_dict.values[i] << "\"";
    }
    meta << "],\n";
    meta << "    \"orders_o_orderstatus\": [";
    for (size_t i = 0; i < orders_orderstatus_dict.values.size(); i++) {
        if (i > 0) meta << ", ";
        meta << "\"" << orders_orderstatus_dict.values[i] << "\"";
    }
    meta << "],\n";
    meta << "    \"orders_o_orderpriority\": [";
    for (size_t i = 0; i < orders_orderpriority_dict.values.size(); i++) {
        if (i > 0) meta << ", ";
        meta << "\"" << orders_orderpriority_dict.values[i] << "\"";
    }
    meta << "],\n";
    meta << "    \"customer_c_mktsegment\": [";
    for (size_t i = 0; i < customer_mktsegment_dict.values.size(); i++) {
        if (i > 0) meta << ", ";
        meta << "\"" << customer_mktsegment_dict.values[i] << "\"";
    }
    meta << "]\n";
    meta << "  }\n";
    meta << "}\n";

    return 0;
}
