#include "storage/storage.h"
#include "utils/date_utils.h"
#include "index/index.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <thread>
#include <vector>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <execution>

using namespace storage;
using namespace std;
using namespace std::chrono;

// Parse decimal value scaled by 100
int64_t parse_decimal(const string& s) {
    size_t dot_pos = s.find('.');
    if (dot_pos == string::npos) {
        return stoll(s) * 100;
    }

    string int_part = s.substr(0, dot_pos);
    string frac_part = s.substr(dot_pos + 1);

    // Pad or truncate to 2 decimal places
    if (frac_part.length() < 2) frac_part += string(2 - frac_part.length(), '0');
    else if (frac_part.length() > 2) frac_part = frac_part.substr(0, 2);

    int64_t int_val = (int_part.empty() || int_part == "-") ? 0 : stoll(int_part);
    int64_t frac_val = stoll(frac_part);

    return int_val * 100 + (int_val < 0 ? -frac_val : frac_val);
}

// Trim string
string trim(const string& s) {
    size_t start = 0, end = s.length();
    while (start < end && isspace(s[start])) start++;
    while (end > start && isspace(s[end-1])) end--;
    return s.substr(start, end - start);
}

// Split line by delimiter (pipe-delimited .tbl files)
vector<string> split_line(const char* start, const char* end) {
    vector<string> fields;
    const char* field_start = start;

    for (const char* p = start; p < end; p++) {
        if (*p == '|') {
            fields.emplace_back(field_start, p - field_start);
            field_start = p + 1;
        }
    }

    return fields;
}

// Memory-map file and find newline boundaries for parallel parsing
struct MmapFile {
    void* data;
    size_t size;
    int fd;

    MmapFile(const string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw runtime_error("Cannot open file: " + path);

        struct stat st;
        if (fstat(fd, &st) < 0) {
            close(fd);
            throw runtime_error("Cannot stat file: " + path);
        }

        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            close(fd);
            throw runtime_error("Cannot mmap file: " + path);
        }

        madvise(data, size, MADV_SEQUENTIAL);
    }

    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }

    const char* begin() const { return static_cast<const char*>(data); }
    const char* end() const { return begin() + size; }
};

// Find chunk boundaries at newlines
vector<pair<size_t, size_t>> find_chunks(const MmapFile& file, size_t num_chunks) {
    vector<pair<size_t, size_t>> chunks;
    size_t chunk_size = file.size / num_chunks;

    const char* data = file.begin();
    size_t pos = 0;

    for (size_t i = 0; i < num_chunks; i++) {
        size_t start = pos;
        size_t end = (i == num_chunks - 1) ? file.size : min(pos + chunk_size, file.size);

        // Adjust end to next newline
        if (end < file.size) {
            while (end < file.size && data[end] != '\n') end++;
            if (end < file.size) end++; // Include the newline
        }

        chunks.emplace_back(start, end);
        pos = end;
    }

    return chunks;
}

// LINEITEM parallel ingestion
void ingest_lineitem(const string& data_dir, const string& gendb_dir) {
    auto start_time = high_resolution_clock::now();
    cout << "Ingesting lineitem..." << endl;

    string path = data_dir + "/lineitem.tbl";
    MmapFile file(path);

    // Estimate row count and reserve space
    size_t estimated_rows = file.size / 150;

    // Determine number of threads
    unsigned num_threads = thread::hardware_concurrency();
    if (estimated_rows < 1000000) num_threads = min(num_threads, 16u);

    cout << "  Using " << num_threads << " threads for " << estimated_rows << " estimated rows" << endl;

    // Find chunk boundaries
    auto chunks = find_chunks(file, num_threads);

    // Thread-local storage
    struct ThreadLocal {
        vector<int32_t> l_orderkey;
        vector<int32_t> l_partkey;
        vector<int32_t> l_suppkey;
        vector<int32_t> l_linenumber;
        vector<int64_t> l_quantity;
        vector<int64_t> l_extendedprice;
        vector<int64_t> l_discount;
        vector<int64_t> l_tax;
        vector<string> l_returnflag;
        vector<string> l_linestatus;
        vector<int32_t> l_shipdate;
        vector<int32_t> l_commitdate;
        vector<int32_t> l_receiptdate;
        vector<string> l_shipinstruct;
        vector<string> l_shipmode;
        vector<string> l_comment;
    };

    vector<ThreadLocal> thread_data(num_threads);
    vector<thread> threads;

    // Parse chunks in parallel
    for (size_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            auto& local = thread_data[t];
            size_t chunk_rows = (chunks[t].second - chunks[t].first) / 150;

            local.l_orderkey.reserve(chunk_rows);
            local.l_partkey.reserve(chunk_rows);
            local.l_suppkey.reserve(chunk_rows);
            local.l_linenumber.reserve(chunk_rows);
            local.l_quantity.reserve(chunk_rows);
            local.l_extendedprice.reserve(chunk_rows);
            local.l_discount.reserve(chunk_rows);
            local.l_tax.reserve(chunk_rows);
            local.l_returnflag.reserve(chunk_rows);
            local.l_linestatus.reserve(chunk_rows);
            local.l_shipdate.reserve(chunk_rows);
            local.l_commitdate.reserve(chunk_rows);
            local.l_receiptdate.reserve(chunk_rows);
            local.l_shipinstruct.reserve(chunk_rows);
            local.l_shipmode.reserve(chunk_rows);
            local.l_comment.reserve(chunk_rows);

            const char* chunk_start = file.begin() + chunks[t].first;
            const char* chunk_end = file.begin() + chunks[t].second;
            const char* line_start = chunk_start;

            for (const char* p = chunk_start; p < chunk_end; p++) {
                if (*p == '\n') {
                    auto fields = split_line(line_start, p);
                    if (fields.size() >= 16) {
                        local.l_orderkey.push_back(stoi(fields[0]));
                        local.l_partkey.push_back(stoi(fields[1]));
                        local.l_suppkey.push_back(stoi(fields[2]));
                        local.l_linenumber.push_back(stoi(fields[3]));
                        local.l_quantity.push_back(parse_decimal(fields[4]));
                        local.l_extendedprice.push_back(parse_decimal(fields[5]));
                        local.l_discount.push_back(parse_decimal(fields[6]));
                        local.l_tax.push_back(parse_decimal(fields[7]));
                        local.l_returnflag.push_back(trim(fields[8]));
                        local.l_linestatus.push_back(trim(fields[9]));
                        local.l_shipdate.push_back(date_utils::date_to_days(fields[10]));
                        local.l_commitdate.push_back(date_utils::date_to_days(fields[11]));
                        local.l_receiptdate.push_back(date_utils::date_to_days(fields[12]));
                        local.l_shipinstruct.push_back(trim(fields[13]));
                        local.l_shipmode.push_back(trim(fields[14]));
                        local.l_comment.push_back(trim(fields[15]));
                    }
                    line_start = p + 1;
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    cout << "  Merging thread results..." << endl;

    // Merge thread-local data
    LineitemTable table;
    size_t total_rows = 0;
    for (const auto& local : thread_data) {
        total_rows += local.l_orderkey.size();
    }

    table.reserve(total_rows);

    for (const auto& local : thread_data) {
        table.l_orderkey.insert(table.l_orderkey.end(), local.l_orderkey.begin(), local.l_orderkey.end());
        table.l_partkey.insert(table.l_partkey.end(), local.l_partkey.begin(), local.l_partkey.end());
        table.l_suppkey.insert(table.l_suppkey.end(), local.l_suppkey.begin(), local.l_suppkey.end());
        table.l_linenumber.insert(table.l_linenumber.end(), local.l_linenumber.begin(), local.l_linenumber.end());
        table.l_quantity.insert(table.l_quantity.end(), local.l_quantity.begin(), local.l_quantity.end());
        table.l_extendedprice.insert(table.l_extendedprice.end(), local.l_extendedprice.begin(), local.l_extendedprice.end());
        table.l_discount.insert(table.l_discount.end(), local.l_discount.begin(), local.l_discount.end());
        table.l_tax.insert(table.l_tax.end(), local.l_tax.begin(), local.l_tax.end());
        table.l_shipdate.insert(table.l_shipdate.end(), local.l_shipdate.begin(), local.l_shipdate.end());
        table.l_commitdate.insert(table.l_commitdate.end(), local.l_commitdate.begin(), local.l_commitdate.end());
        table.l_receiptdate.insert(table.l_receiptdate.end(), local.l_receiptdate.begin(), local.l_receiptdate.end());
        table.l_comment.insert(table.l_comment.end(), local.l_comment.begin(), local.l_comment.end());

        // Dictionary-encoded columns - encode during merge
        for (const auto& s : local.l_returnflag) {
            table.l_returnflag.push_back(table.dict_returnflag.encode(s));
        }
        for (const auto& s : local.l_linestatus) {
            table.l_linestatus.push_back(table.dict_linestatus.encode(s));
        }
        for (const auto& s : local.l_shipinstruct) {
            table.l_shipinstruct.push_back(table.dict_shipinstruct.encode(s));
        }
        for (const auto& s : local.l_shipmode) {
            table.l_shipmode.push_back(table.dict_shipmode.encode(s));
        }
    }

    cout << "  Sorting by l_shipdate..." << endl;

    // Sort by l_shipdate using parallel sort
    vector<size_t> indices(table.size());
    for (size_t i = 0; i < indices.size(); i++) indices[i] = i;

    sort(execution::par, indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.l_shipdate[a] < table.l_shipdate[b];
    });

    // Reorder all columns by sorted indices
    auto reorder = [&](auto& vec) {
        auto copy = vec;
        for (size_t i = 0; i < indices.size(); i++) {
            vec[i] = copy[indices[i]];
        }
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

    cout << "  Writing columns..." << endl;

    // Write columns to disk
    write_column_int32(gendb_dir + "/lineitem.l_orderkey", table.l_orderkey);
    write_column_int32(gendb_dir + "/lineitem.l_partkey", table.l_partkey);
    write_column_int32(gendb_dir + "/lineitem.l_suppkey", table.l_suppkey);
    write_column_int32(gendb_dir + "/lineitem.l_linenumber", table.l_linenumber);
    write_column_int64(gendb_dir + "/lineitem.l_quantity", table.l_quantity);
    write_column_int64(gendb_dir + "/lineitem.l_extendedprice", table.l_extendedprice);
    write_column_int64(gendb_dir + "/lineitem.l_discount", table.l_discount);
    write_column_int64(gendb_dir + "/lineitem.l_tax", table.l_tax);
    write_column_uint8(gendb_dir + "/lineitem.l_returnflag", table.l_returnflag);
    write_column_uint8(gendb_dir + "/lineitem.l_linestatus", table.l_linestatus);
    write_column_int32(gendb_dir + "/lineitem.l_shipdate", table.l_shipdate);
    write_column_int32(gendb_dir + "/lineitem.l_commitdate", table.l_commitdate);
    write_column_int32(gendb_dir + "/lineitem.l_receiptdate", table.l_receiptdate);
    write_column_uint8(gendb_dir + "/lineitem.l_shipinstruct", table.l_shipinstruct);
    write_column_uint8(gendb_dir + "/lineitem.l_shipmode", table.l_shipmode);
    write_column_string(gendb_dir + "/lineitem.l_comment", table.l_comment);

    write_dictionary(gendb_dir + "/lineitem.l_returnflag.dict", table.dict_returnflag);
    write_dictionary(gendb_dir + "/lineitem.l_linestatus.dict", table.dict_linestatus);
    write_dictionary(gendb_dir + "/lineitem.l_shipinstruct.dict", table.dict_shipinstruct);
    write_dictionary(gendb_dir + "/lineitem.l_shipmode.dict", table.dict_shipmode);

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();

    cout << "  Completed: " << table.size() << " rows in " << duration << " ms" << endl;
}

// ORDERS parallel ingestion
void ingest_orders(const string& data_dir, const string& gendb_dir) {
    auto start_time = high_resolution_clock::now();
    cout << "Ingesting orders..." << endl;

    string path = data_dir + "/orders.tbl";
    MmapFile file(path);

    unsigned num_threads = min(thread::hardware_concurrency(), 32u);
    auto chunks = find_chunks(file, num_threads);

    struct ThreadLocal {
        vector<int32_t> o_orderkey;
        vector<int32_t> o_custkey;
        vector<string> o_orderstatus;
        vector<int64_t> o_totalprice;
        vector<int32_t> o_orderdate;
        vector<string> o_orderpriority;
        vector<string> o_clerk;
        vector<int32_t> o_shippriority;
        vector<string> o_comment;
    };

    vector<ThreadLocal> thread_data(num_threads);
    vector<thread> threads;

    for (size_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            auto& local = thread_data[t];
            size_t chunk_rows = (chunks[t].second - chunks[t].first) / 120;

            local.o_orderkey.reserve(chunk_rows);
            local.o_custkey.reserve(chunk_rows);
            local.o_orderstatus.reserve(chunk_rows);
            local.o_totalprice.reserve(chunk_rows);
            local.o_orderdate.reserve(chunk_rows);
            local.o_orderpriority.reserve(chunk_rows);
            local.o_clerk.reserve(chunk_rows);
            local.o_shippriority.reserve(chunk_rows);
            local.o_comment.reserve(chunk_rows);

            const char* chunk_start = file.begin() + chunks[t].first;
            const char* chunk_end = file.begin() + chunks[t].second;
            const char* line_start = chunk_start;

            for (const char* p = chunk_start; p < chunk_end; p++) {
                if (*p == '\n') {
                    auto fields = split_line(line_start, p);
                    if (fields.size() >= 9) {
                        local.o_orderkey.push_back(stoi(fields[0]));
                        local.o_custkey.push_back(stoi(fields[1]));
                        local.o_orderstatus.push_back(trim(fields[2]));
                        local.o_totalprice.push_back(parse_decimal(fields[3]));
                        local.o_orderdate.push_back(date_utils::date_to_days(fields[4]));
                        local.o_orderpriority.push_back(trim(fields[5]));
                        local.o_clerk.push_back(trim(fields[6]));
                        local.o_shippriority.push_back(stoi(fields[7]));
                        local.o_comment.push_back(trim(fields[8]));
                    }
                    line_start = p + 1;
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    OrdersTable table;
    size_t total_rows = 0;
    for (const auto& local : thread_data) total_rows += local.o_orderkey.size();
    table.reserve(total_rows);

    for (const auto& local : thread_data) {
        table.o_orderkey.insert(table.o_orderkey.end(), local.o_orderkey.begin(), local.o_orderkey.end());
        table.o_custkey.insert(table.o_custkey.end(), local.o_custkey.begin(), local.o_custkey.end());
        table.o_totalprice.insert(table.o_totalprice.end(), local.o_totalprice.begin(), local.o_totalprice.end());
        table.o_orderdate.insert(table.o_orderdate.end(), local.o_orderdate.begin(), local.o_orderdate.end());
        table.o_orderpriority.insert(table.o_orderpriority.end(), local.o_orderpriority.begin(), local.o_orderpriority.end());
        table.o_clerk.insert(table.o_clerk.end(), local.o_clerk.begin(), local.o_clerk.end());
        table.o_shippriority.insert(table.o_shippriority.end(), local.o_shippriority.begin(), local.o_shippriority.end());
        table.o_comment.insert(table.o_comment.end(), local.o_comment.begin(), local.o_comment.end());

        for (const auto& s : local.o_orderstatus) {
            table.o_orderstatus.push_back(table.dict_orderstatus.encode(s));
        }
    }

    cout << "  Sorting by o_orderdate..." << endl;

    vector<size_t> indices(table.size());
    for (size_t i = 0; i < indices.size(); i++) indices[i] = i;

    sort(execution::par, indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.o_orderdate[a] < table.o_orderdate[b];
    });

    auto reorder = [&](auto& vec) {
        auto copy = vec;
        for (size_t i = 0; i < indices.size(); i++) {
            vec[i] = copy[indices[i]];
        }
    };

    reorder(table.o_orderkey);
    reorder(table.o_custkey);
    reorder(table.o_orderstatus);
    reorder(table.o_totalprice);
    reorder(table.o_orderdate);
    reorder(table.o_orderpriority);
    reorder(table.o_clerk);
    reorder(table.o_shippriority);
    reorder(table.o_comment);

    write_column_int32(gendb_dir + "/orders.o_orderkey", table.o_orderkey);
    write_column_int32(gendb_dir + "/orders.o_custkey", table.o_custkey);
    write_column_uint8(gendb_dir + "/orders.o_orderstatus", table.o_orderstatus);
    write_column_int64(gendb_dir + "/orders.o_totalprice", table.o_totalprice);
    write_column_int32(gendb_dir + "/orders.o_orderdate", table.o_orderdate);
    write_column_string(gendb_dir + "/orders.o_orderpriority", table.o_orderpriority);
    write_column_string(gendb_dir + "/orders.o_clerk", table.o_clerk);
    write_column_int32(gendb_dir + "/orders.o_shippriority", table.o_shippriority);
    write_column_string(gendb_dir + "/orders.o_comment", table.o_comment);
    write_dictionary(gendb_dir + "/orders.o_orderstatus.dict", table.dict_orderstatus);

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();
    cout << "  Completed: " << table.size() << " rows in " << duration << " ms" << endl;
}

// CUSTOMER parallel ingestion
void ingest_customer(const string& data_dir, const string& gendb_dir) {
    auto start_time = high_resolution_clock::now();
    cout << "Ingesting customer..." << endl;

    string path = data_dir + "/customer.tbl";
    MmapFile file(path);

    unsigned num_threads = min(thread::hardware_concurrency(), 16u);
    auto chunks = find_chunks(file, num_threads);

    struct ThreadLocal {
        vector<int32_t> c_custkey;
        vector<string> c_name;
        vector<string> c_address;
        vector<int32_t> c_nationkey;
        vector<string> c_phone;
        vector<int64_t> c_acctbal;
        vector<string> c_mktsegment;
        vector<string> c_comment;
    };

    vector<ThreadLocal> thread_data(num_threads);
    vector<thread> threads;

    for (size_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            auto& local = thread_data[t];

            const char* chunk_start = file.begin() + chunks[t].first;
            const char* chunk_end = file.begin() + chunks[t].second;
            const char* line_start = chunk_start;

            for (const char* p = chunk_start; p < chunk_end; p++) {
                if (*p == '\n') {
                    auto fields = split_line(line_start, p);
                    if (fields.size() >= 8) {
                        local.c_custkey.push_back(stoi(fields[0]));
                        local.c_name.push_back(trim(fields[1]));
                        local.c_address.push_back(trim(fields[2]));
                        local.c_nationkey.push_back(stoi(fields[3]));
                        local.c_phone.push_back(trim(fields[4]));
                        local.c_acctbal.push_back(parse_decimal(fields[5]));
                        local.c_mktsegment.push_back(trim(fields[6]));
                        local.c_comment.push_back(trim(fields[7]));
                    }
                    line_start = p + 1;
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    CustomerTable table;
    size_t total_rows = 0;
    for (const auto& local : thread_data) total_rows += local.c_custkey.size();
    table.reserve(total_rows);

    for (const auto& local : thread_data) {
        table.c_custkey.insert(table.c_custkey.end(), local.c_custkey.begin(), local.c_custkey.end());
        table.c_name.insert(table.c_name.end(), local.c_name.begin(), local.c_name.end());
        table.c_address.insert(table.c_address.end(), local.c_address.begin(), local.c_address.end());
        table.c_nationkey.insert(table.c_nationkey.end(), local.c_nationkey.begin(), local.c_nationkey.end());
        table.c_phone.insert(table.c_phone.end(), local.c_phone.begin(), local.c_phone.end());
        table.c_acctbal.insert(table.c_acctbal.end(), local.c_acctbal.begin(), local.c_acctbal.end());
        table.c_comment.insert(table.c_comment.end(), local.c_comment.begin(), local.c_comment.end());

        for (const auto& s : local.c_mktsegment) {
            table.c_mktsegment.push_back(table.dict_mktsegment.encode(s));
        }
    }

    write_column_int32(gendb_dir + "/customer.c_custkey", table.c_custkey);
    write_column_string(gendb_dir + "/customer.c_name", table.c_name);
    write_column_string(gendb_dir + "/customer.c_address", table.c_address);
    write_column_int32(gendb_dir + "/customer.c_nationkey", table.c_nationkey);
    write_column_string(gendb_dir + "/customer.c_phone", table.c_phone);
    write_column_int64(gendb_dir + "/customer.c_acctbal", table.c_acctbal);
    write_column_uint8(gendb_dir + "/customer.c_mktsegment", table.c_mktsegment);
    write_column_string(gendb_dir + "/customer.c_comment", table.c_comment);
    write_dictionary(gendb_dir + "/customer.c_mktsegment.dict", table.dict_mktsegment);

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();
    cout << "  Completed: " << table.size() << " rows in " << duration << " ms" << endl;
}

// PART parallel ingestion
void ingest_part(const string& data_dir, const string& gendb_dir) {
    auto start_time = high_resolution_clock::now();
    cout << "Ingesting part..." << endl;

    string path = data_dir + "/part.tbl";
    MmapFile file(path);

    unsigned num_threads = min(thread::hardware_concurrency(), 16u);
    auto chunks = find_chunks(file, num_threads);

    struct ThreadLocal {
        vector<int32_t> p_partkey;
        vector<string> p_name;
        vector<string> p_mfgr;
        vector<string> p_brand;
        vector<string> p_type;
        vector<int32_t> p_size;
        vector<string> p_container;
        vector<int64_t> p_retailprice;
        vector<string> p_comment;
    };

    vector<ThreadLocal> thread_data(num_threads);
    vector<thread> threads;

    for (size_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            auto& local = thread_data[t];

            const char* chunk_start = file.begin() + chunks[t].first;
            const char* chunk_end = file.begin() + chunks[t].second;
            const char* line_start = chunk_start;

            for (const char* p = chunk_start; p < chunk_end; p++) {
                if (*p == '\n') {
                    auto fields = split_line(line_start, p);
                    if (fields.size() >= 9) {
                        local.p_partkey.push_back(stoi(fields[0]));
                        local.p_name.push_back(trim(fields[1]));
                        local.p_mfgr.push_back(trim(fields[2]));
                        local.p_brand.push_back(trim(fields[3]));
                        local.p_type.push_back(trim(fields[4]));
                        local.p_size.push_back(stoi(fields[5]));
                        local.p_container.push_back(trim(fields[6]));
                        local.p_retailprice.push_back(parse_decimal(fields[7]));
                        local.p_comment.push_back(trim(fields[8]));
                    }
                    line_start = p + 1;
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    PartTable table;
    size_t total_rows = 0;
    for (const auto& local : thread_data) total_rows += local.p_partkey.size();
    table.reserve(total_rows);

    for (const auto& local : thread_data) {
        table.p_partkey.insert(table.p_partkey.end(), local.p_partkey.begin(), local.p_partkey.end());
        table.p_name.insert(table.p_name.end(), local.p_name.begin(), local.p_name.end());
        table.p_mfgr.insert(table.p_mfgr.end(), local.p_mfgr.begin(), local.p_mfgr.end());
        table.p_type.insert(table.p_type.end(), local.p_type.begin(), local.p_type.end());
        table.p_size.insert(table.p_size.end(), local.p_size.begin(), local.p_size.end());
        table.p_retailprice.insert(table.p_retailprice.end(), local.p_retailprice.begin(), local.p_retailprice.end());
        table.p_comment.insert(table.p_comment.end(), local.p_comment.begin(), local.p_comment.end());

        for (const auto& s : local.p_brand) {
            table.p_brand.push_back(table.dict_brand.encode(s));
        }
        for (const auto& s : local.p_container) {
            table.p_container.push_back(table.dict_container.encode(s));
        }
    }

    write_column_int32(gendb_dir + "/part.p_partkey", table.p_partkey);
    write_column_string(gendb_dir + "/part.p_name", table.p_name);
    write_column_string(gendb_dir + "/part.p_mfgr", table.p_mfgr);
    write_column_uint8(gendb_dir + "/part.p_brand", table.p_brand);
    write_column_string(gendb_dir + "/part.p_type", table.p_type);
    write_column_int32(gendb_dir + "/part.p_size", table.p_size);
    write_column_uint8(gendb_dir + "/part.p_container", table.p_container);
    write_column_int64(gendb_dir + "/part.p_retailprice", table.p_retailprice);
    write_column_string(gendb_dir + "/part.p_comment", table.p_comment);
    write_dictionary(gendb_dir + "/part.p_brand.dict", table.dict_brand);
    write_dictionary(gendb_dir + "/part.p_container.dict", table.dict_container);

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();
    cout << "  Completed: " << table.size() << " rows in " << duration << " ms" << endl;
}

// PARTSUPP parallel ingestion
void ingest_partsupp(const string& data_dir, const string& gendb_dir) {
    auto start_time = high_resolution_clock::now();
    cout << "Ingesting partsupp..." << endl;

    string path = data_dir + "/partsupp.tbl";
    MmapFile file(path);

    unsigned num_threads = min(thread::hardware_concurrency(), 16u);
    auto chunks = find_chunks(file, num_threads);

    struct ThreadLocal {
        vector<int32_t> ps_partkey;
        vector<int32_t> ps_suppkey;
        vector<int32_t> ps_availqty;
        vector<int64_t> ps_supplycost;
        vector<string> ps_comment;
    };

    vector<ThreadLocal> thread_data(num_threads);
    vector<thread> threads;

    for (size_t t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            auto& local = thread_data[t];

            const char* chunk_start = file.begin() + chunks[t].first;
            const char* chunk_end = file.begin() + chunks[t].second;
            const char* line_start = chunk_start;

            for (const char* p = chunk_start; p < chunk_end; p++) {
                if (*p == '\n') {
                    auto fields = split_line(line_start, p);
                    if (fields.size() >= 5) {
                        local.ps_partkey.push_back(stoi(fields[0]));
                        local.ps_suppkey.push_back(stoi(fields[1]));
                        local.ps_availqty.push_back(stoi(fields[2]));
                        local.ps_supplycost.push_back(parse_decimal(fields[3]));
                        local.ps_comment.push_back(trim(fields[4]));
                    }
                    line_start = p + 1;
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    PartsuppTable table;
    size_t total_rows = 0;
    for (const auto& local : thread_data) total_rows += local.ps_partkey.size();
    table.reserve(total_rows);

    for (const auto& local : thread_data) {
        table.ps_partkey.insert(table.ps_partkey.end(), local.ps_partkey.begin(), local.ps_partkey.end());
        table.ps_suppkey.insert(table.ps_suppkey.end(), local.ps_suppkey.begin(), local.ps_suppkey.end());
        table.ps_availqty.insert(table.ps_availqty.end(), local.ps_availqty.begin(), local.ps_availqty.end());
        table.ps_supplycost.insert(table.ps_supplycost.end(), local.ps_supplycost.begin(), local.ps_supplycost.end());
        table.ps_comment.insert(table.ps_comment.end(), local.ps_comment.begin(), local.ps_comment.end());
    }

    write_column_int32(gendb_dir + "/partsupp.ps_partkey", table.ps_partkey);
    write_column_int32(gendb_dir + "/partsupp.ps_suppkey", table.ps_suppkey);
    write_column_int32(gendb_dir + "/partsupp.ps_availqty", table.ps_availqty);
    write_column_int64(gendb_dir + "/partsupp.ps_supplycost", table.ps_supplycost);
    write_column_string(gendb_dir + "/partsupp.ps_comment", table.ps_comment);

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();
    cout << "  Completed: " << table.size() << " rows in " << duration << " ms" << endl;
}

// SUPPLIER single-threaded ingestion (small table)
void ingest_supplier(const string& data_dir, const string& gendb_dir) {
    auto start_time = high_resolution_clock::now();
    cout << "Ingesting supplier..." << endl;

    ifstream file(data_dir + "/supplier.tbl");
    if (!file.is_open()) throw runtime_error("Cannot open supplier.tbl");

    SupplierTable table;
    string line;

    while (getline(file, line)) {
        vector<string> fields;
        stringstream ss(line);
        string field;
        while (getline(ss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() >= 7) {
            table.s_suppkey.push_back(stoi(fields[0]));
            table.s_name.push_back(trim(fields[1]));
            table.s_address.push_back(trim(fields[2]));
            table.s_nationkey.push_back(stoi(fields[3]));
            table.s_phone.push_back(trim(fields[4]));
            table.s_acctbal.push_back(parse_decimal(fields[5]));
            table.s_comment.push_back(trim(fields[6]));
        }
    }

    write_column_int32(gendb_dir + "/supplier.s_suppkey", table.s_suppkey);
    write_column_string(gendb_dir + "/supplier.s_name", table.s_name);
    write_column_string(gendb_dir + "/supplier.s_address", table.s_address);
    write_column_int32(gendb_dir + "/supplier.s_nationkey", table.s_nationkey);
    write_column_string(gendb_dir + "/supplier.s_phone", table.s_phone);
    write_column_int64(gendb_dir + "/supplier.s_acctbal", table.s_acctbal);
    write_column_string(gendb_dir + "/supplier.s_comment", table.s_comment);

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();
    cout << "  Completed: " << table.size() << " rows in " << duration << " ms" << endl;
}

// NATION single-threaded ingestion (tiny table)
void ingest_nation(const string& data_dir, const string& gendb_dir) {
    auto start_time = high_resolution_clock::now();
    cout << "Ingesting nation..." << endl;

    ifstream file(data_dir + "/nation.tbl");
    if (!file.is_open()) throw runtime_error("Cannot open nation.tbl");

    NationTable table;
    string line;

    while (getline(file, line)) {
        vector<string> fields;
        stringstream ss(line);
        string field;
        while (getline(ss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() >= 4) {
            table.n_nationkey.push_back(stoi(fields[0]));
            table.n_name.push_back(trim(fields[1]));
            table.n_regionkey.push_back(stoi(fields[2]));
            table.n_comment.push_back(trim(fields[3]));
        }
    }

    write_column_int32(gendb_dir + "/nation.n_nationkey", table.n_nationkey);
    write_column_string(gendb_dir + "/nation.n_name", table.n_name);
    write_column_int32(gendb_dir + "/nation.n_regionkey", table.n_regionkey);
    write_column_string(gendb_dir + "/nation.n_comment", table.n_comment);

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();
    cout << "  Completed: " << table.size() << " rows in " << duration << " ms" << endl;
}

// REGION single-threaded ingestion (tiny table)
void ingest_region(const string& data_dir, const string& gendb_dir) {
    auto start_time = high_resolution_clock::now();
    cout << "Ingesting region..." << endl;

    ifstream file(data_dir + "/region.tbl");
    if (!file.is_open()) throw runtime_error("Cannot open region.tbl");

    RegionTable table;
    string line;

    while (getline(file, line)) {
        vector<string> fields;
        stringstream ss(line);
        string field;
        while (getline(ss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() >= 3) {
            table.r_regionkey.push_back(stoi(fields[0]));
            table.r_name.push_back(trim(fields[1]));
            table.r_comment.push_back(trim(fields[2]));
        }
    }

    write_column_int32(gendb_dir + "/region.r_regionkey", table.r_regionkey);
    write_column_string(gendb_dir + "/region.r_name", table.r_name);
    write_column_string(gendb_dir + "/region.r_comment", table.r_comment);

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();
    cout << "  Completed: " << table.size() << " rows in " << duration << " ms" << endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << endl;
        return 1;
    }

    string data_dir = argv[1];
    string gendb_dir = argv[2];

    // Create gendb directory if it doesn't exist
    mkdir(gendb_dir.c_str(), 0755);

    auto total_start = high_resolution_clock::now();

    cout << "GenDB Parallel Ingestion" << endl;
    cout << "Data directory: " << data_dir << endl;
    cout << "GenDB directory: " << gendb_dir << endl;
    cout << "Hardware: " << thread::hardware_concurrency() << " cores" << endl;
    cout << endl;

    // Ingest all tables concurrently
    vector<thread> table_threads;

    table_threads.emplace_back([&]() { ingest_lineitem(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_orders(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_customer(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_part(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_partsupp(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_supplier(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_nation(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_region(data_dir, gendb_dir); });

    for (auto& t : table_threads) t.join();

    auto total_end = high_resolution_clock::now();
    auto total_duration = duration_cast<milliseconds>(total_end - total_start).count();

    cout << endl;
    cout << "Total ingestion time: " << total_duration << " ms ("
         << fixed << setprecision(2) << total_duration / 1000.0 << " seconds)" << endl;

    return 0;
}
