// ingest.cpp — GenDB TPC-H SF10 parallel data ingestion
// Compile: g++ -O2 -std=c++17 -Wall -lpthread -o ingest ingest.cpp
#include <algorithm>
#include <atomic>
#include <cassert>
#include <climits>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
static const uint32_t BLOCK_SIZE = 100000;

// ─────────────────────────── ThreadPool ────────────────────────────────────

struct ThreadPool {
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex mtx;
    std::condition_variable cv;
    bool stop = false;

    explicit ThreadPool(int n) {
        for (int i = 0; i < n; i++) {
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lk(mtx);
                        cv.wait(lk, [this] { return !tasks.empty() || stop; });
                        if (stop && tasks.empty()) return;
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    // Submit a batch of tasks and block until ALL complete.
    void submit_and_wait(std::vector<std::function<void()>> fns) {
        if (fns.empty()) return;
        std::atomic<int> remaining((int)fns.size());
        std::mutex wm;
        std::condition_variable wcv;
        for (auto& fn : fns) {
            auto wrapped = [f = std::move(fn), &remaining, &wm, &wcv]() mutable {
                f();
                if (--remaining == 0) {
                    std::lock_guard<std::mutex> lg(wm);
                    wcv.notify_one();
                }
            };
            {
                std::lock_guard<std::mutex> lk(mtx);
                tasks.push(std::move(wrapped));
            }
            cv.notify_one();
        }
        std::unique_lock<std::mutex> lk(wm);
        wcv.wait(lk, [&remaining] { return remaining.load() == 0; });
    }

    ~ThreadPool() {
        { std::lock_guard<std::mutex> lk(mtx); stop = true; }
        cv.notify_all();
        for (auto& w : workers) w.join();
    }
};

// ─────────────────────────── mmap helper ───────────────────────────────────

struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    explicit MmapFile(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st{};
        fstat(fd, &st);
        size = (size_t)st.st_size;
        if (size == 0) { close(fd); return; }
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (data == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
        madvise((void*)data, size, MADV_SEQUENTIAL);
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
    }
    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;
};

// ─────────────────────────── Date conversion ───────────────────────────────

// Howard Hinnant proleptic Gregorian formula. Self-test: date_to_epoch(1970,1,1)==0
inline int32_t date_to_epoch(int y, int m, int d) {
    y -= (m <= 2);
    int era = (y >= 0 ? y : y - 399) / 400;
    int yoe = y - era * 400;
    int doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

// ─────────────────────────── Fast line parser ───────────────────────────────

struct LP {
    const char* p;
    explicit LP(const char* p_) : p(p_) {}

    void skip_field() {
        while (*p != '|' && *p != '\n' && *p != '\0') p++;
        if (*p == '|') p++;
    }
    void skip_line() {
        while (*p != '\n' && *p != '\0') p++;
        if (*p == '\n') p++;
    }

    int32_t i32() {
        bool neg = (*p == '-') ? (p++, true) : false;
        int32_t v = 0;
        while (*p >= '0' && *p <= '9') v = v * 10 + (*p++ - '0');
        if (*p == '|') p++;
        return neg ? -v : v;
    }

    double dbl() {
        bool neg = (*p == '-') ? (p++, true) : false;
        double v = 0.0;
        while (*p >= '0' && *p <= '9') v = v * 10.0 + (*p++ - '0');
        if (*p == '.') {
            p++;
            double f = 0.1;
            while (*p >= '0' && *p <= '9') { v += (*p++ - '0') * f; f *= 0.1; }
        }
        if (*p == '|') p++;
        return neg ? -v : v;
    }

    int32_t date() {
        int y = (p[0]-'0')*1000 + (p[1]-'0')*100 + (p[2]-'0')*10 + (p[3]-'0');
        int m = (p[5]-'0')*10 + (p[6]-'0');
        int d = (p[8]-'0')*10 + (p[9]-'0');
        p += 10;
        if (*p == '|') p++;
        return date_to_epoch(y, m, d);
    }

    // Raw char as int8_t (for CHAR(1) columns like returnflag, linestatus)
    int8_t char1() {
        int8_t c = (int8_t)*p++;
        if (*p == '|') p++;
        return c;
    }

    // Fixed-width string copy (null-padded), maxlen includes null terminator
    void str_fixed(char* buf, int maxlen) {
        int i = 0;
        while (*p != '|' && *p != '\n' && *p != '\0' && i < maxlen - 1)
            buf[i++] = *p++;
        buf[i] = '\0';
        while (*p != '|' && *p != '\n' && *p != '\0') p++; // skip overflow
        if (*p == '|') p++;
    }

    // Dictionary lookup for multi-char string (linear scan over dict array)
    // Returns code 0..dict_size-1
    int8_t dict8(const char dict[][32], int dict_size) {
        const char* start = p;
        while (*p != '|' && *p != '\n' && *p != '\0') p++;
        int len = (int)(p - start);
        if (*p == '|') p++;
        for (int i = 0; i < dict_size; i++) {
            if ((int)strlen(dict[i]) == len && memcmp(start, dict[i], len) == 0)
                return (int8_t)i;
        }
        return -1; // should not happen for valid TPC-H data
    }
};

// ─────────────────────────── Chunk helpers ──────────────────────────────────

// Split file into n line-aligned chunks; last chunk takes remainder
std::vector<std::pair<size_t, size_t>>
chunk_bounds(const char* data, size_t size, int n) {
    std::vector<std::pair<size_t, size_t>> out;
    if (size == 0 || n <= 0) return out;
    size_t chunk_sz = size / (size_t)n;
    size_t pos = 0;
    for (int i = 0; i < n; i++) {
        if (i == n - 1 || pos >= size) {
            out.push_back({pos, size});
            break;
        }
        size_t end = std::min(pos + chunk_sz, size);
        while (end < size && data[end] != '\n') end++;
        if (end < size) end++; // include '\n'
        out.push_back({pos, end});
        pos = end;
    }
    return out;
}

size_t count_lines(const char* data, size_t beg, size_t end) {
    size_t n = 0;
    for (size_t i = beg; i < end; i++) n += (data[i] == '\n');
    return n;
}

// ─────────────────────────── Write helpers ──────────────────────────────────

template<typename T>
void write_col(const std::string& path, const std::vector<T>& col) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    setvbuf(f, nullptr, _IOFBF, 4 * 1024 * 1024);
    fwrite(col.data(), sizeof(T), col.size(), f);
    fclose(f);
}

void write_str_col(const std::string& path, const std::vector<char>& col) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    setvbuf(f, nullptr, _IOFBF, 4 * 1024 * 1024);
    fwrite(col.data(), 1, col.size(), f);
    fclose(f);
}

void write_dict(const std::string& path, const char dict[][32], int sz) {
    FILE* f = fopen(path.c_str(), "w");
    if (!f) throw std::runtime_error("Cannot write dict: " + path);
    for (int i = 0; i < sz; i++) fprintf(f, "%s\n", dict[i]);
    fclose(f);
}

// ─────────────────────────── Dictionaries ───────────────────────────────────

static const char MKTSEGMENT_DICT[][32] = {
    "AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"
};
static const int MKTSEGMENT_DICT_SZ = 5;

// ─────────────────────────── Table ingest functions ─────────────────────────

// LINEITEM: 16 fields, write 10 columns
// fields: orderkey|partkey|suppkey|linenumber|quantity|extendedprice|discount|tax|
//         returnflag|linestatus|shipdate|commitdate|receiptdate|shipinstruct|shipmode|comment
void ingest_lineitem(const std::string& data_dir, const std::string& db_dir, ThreadPool& pool, int nthreads) {
    std::cout << "[lineitem] mmapping..." << std::flush;
    MmapFile mf(data_dir + "/lineitem.tbl");
    auto chunks = chunk_bounds(mf.data, mf.size, nthreads);
    int nc = (int)chunks.size();

    // Phase 1: count rows per chunk
    std::vector<size_t> cnts(nc, 0);
    {
        std::vector<std::function<void()>> tasks;
        for (int i = 0; i < nc; i++) {
            tasks.push_back([&cnts, &mf, &chunks, i] {
                cnts[i] = count_lines(mf.data, chunks[i].first, chunks[i].second);
            });
        }
        pool.submit_and_wait(std::move(tasks));
    }

    // Compute prefix sums → row offsets per chunk
    std::vector<size_t> offsets(nc + 1, 0);
    for (int i = 0; i < nc; i++) offsets[i + 1] = offsets[i] + cnts[i];
    size_t total = offsets[nc];
    std::cout << " rows=" << total << std::flush;

    // Allocate columns
    std::vector<int32_t> col_orderkey(total),   col_partkey(total),  col_suppkey(total);
    std::vector<double>  col_qty(total),         col_price(total),    col_disc(total), col_tax(total);
    std::vector<int8_t>  col_rflag(total),       col_lstatus(total);
    std::vector<int32_t> col_ship(total);

    // Phase 2: parse rows in parallel
    {
        std::vector<std::function<void()>> tasks;
        for (int i = 0; i < nc; i++) {
            tasks.push_back([&, i] {
                size_t row = offsets[i];
                LP lp(mf.data + chunks[i].first);
                size_t end = chunks[i].second;
                while ((size_t)(lp.p - mf.data) < end && *lp.p != '\0') {
                    col_orderkey[row]  = lp.i32();
                    col_partkey[row]   = lp.i32();
                    col_suppkey[row]   = lp.i32();
                    lp.skip_field(); // linenumber
                    col_qty[row]       = lp.dbl();
                    col_price[row]     = lp.dbl();
                    col_disc[row]      = lp.dbl();
                    col_tax[row]       = lp.dbl();
                    col_rflag[row]     = lp.char1();
                    col_lstatus[row]   = lp.char1();
                    col_ship[row]      = lp.date();
                    lp.skip_line(); // skip remaining fields
                    row++;
                }
            });
        }
        pool.submit_and_wait(std::move(tasks));
    }

    std::cout << " writing..." << std::flush;
    std::string tdir = db_dir + "/lineitem";
    fs::create_directories(tdir);

    // Phase 3: write columns in parallel
    {
        std::vector<std::function<void()>> tasks = {
            [&]{ write_col(tdir+"/l_orderkey.bin",      col_orderkey); },
            [&]{ write_col(tdir+"/l_partkey.bin",       col_partkey); },
            [&]{ write_col(tdir+"/l_suppkey.bin",       col_suppkey); },
            [&]{ write_col(tdir+"/l_quantity.bin",      col_qty); },
            [&]{ write_col(tdir+"/l_extendedprice.bin", col_price); },
            [&]{ write_col(tdir+"/l_discount.bin",      col_disc); },
            [&]{ write_col(tdir+"/l_tax.bin",           col_tax); },
            [&]{ write_col(tdir+"/l_returnflag.bin",    col_rflag); },
            [&]{ write_col(tdir+"/l_linestatus.bin",    col_lstatus); },
            [&]{ write_col(tdir+"/l_shipdate.bin",      col_ship); },
        };
        pool.submit_and_wait(std::move(tasks));
    }
    std::cout << " done.\n";
}

// ORDERS: 9 fields, write 5 columns
// fields: orderkey|custkey|orderstatus|totalprice|orderdate|orderpriority|clerk|shippriority|comment
void ingest_orders(const std::string& data_dir, const std::string& db_dir, ThreadPool& pool, int nthreads) {
    std::cout << "[orders] mmapping..." << std::flush;
    MmapFile mf(data_dir + "/orders.tbl");
    auto chunks = chunk_bounds(mf.data, mf.size, nthreads);
    int nc = (int)chunks.size();

    std::vector<size_t> cnts(nc, 0);
    {
        std::vector<std::function<void()>> tasks;
        for (int i = 0; i < nc; i++)
            tasks.push_back([&cnts,&mf,&chunks,i]{ cnts[i]=count_lines(mf.data,chunks[i].first,chunks[i].second); });
        pool.submit_and_wait(std::move(tasks));
    }
    std::vector<size_t> offsets(nc+1,0);
    for (int i=0;i<nc;i++) offsets[i+1]=offsets[i]+cnts[i];
    size_t total = offsets[nc];
    std::cout << " rows=" << total << std::flush;

    std::vector<int32_t> col_orderkey(total), col_custkey(total), col_shippri(total);
    std::vector<double>  col_totalprice(total);
    std::vector<int32_t> col_orderdate(total);

    {
        std::vector<std::function<void()>> tasks;
        for (int i = 0; i < nc; i++) {
            tasks.push_back([&, i] {
                size_t row = offsets[i];
                LP lp(mf.data + chunks[i].first);
                size_t end = chunks[i].second;
                while ((size_t)(lp.p - mf.data) < end && *lp.p != '\0') {
                    col_orderkey[row]    = lp.i32();
                    col_custkey[row]     = lp.i32();
                    lp.skip_field(); // orderstatus
                    col_totalprice[row]  = lp.dbl();
                    col_orderdate[row]   = lp.date();
                    lp.skip_field(); // orderpriority
                    lp.skip_field(); // clerk
                    col_shippri[row]     = lp.i32();
                    lp.skip_line();
                    row++;
                }
            });
        }
        pool.submit_and_wait(std::move(tasks));
    }

    std::cout << " writing..." << std::flush;
    std::string tdir = db_dir + "/orders";
    fs::create_directories(tdir);
    {
        std::vector<std::function<void()>> tasks = {
            [&]{ write_col(tdir+"/o_orderkey.bin",     col_orderkey); },
            [&]{ write_col(tdir+"/o_custkey.bin",      col_custkey); },
            [&]{ write_col(tdir+"/o_totalprice.bin",   col_totalprice); },
            [&]{ write_col(tdir+"/o_orderdate.bin",    col_orderdate); },
            [&]{ write_col(tdir+"/o_shippriority.bin", col_shippri); },
        };
        pool.submit_and_wait(std::move(tasks));
    }
    std::cout << " done.\n";
}

// CUSTOMER: 8 fields, write 4 columns
// fields: custkey|name|address|nationkey|phone|acctbal|mktsegment|comment
void ingest_customer(const std::string& data_dir, const std::string& db_dir, ThreadPool& pool, int nthreads) {
    std::cout << "[customer] mmapping..." << std::flush;
    MmapFile mf(data_dir + "/customer.tbl");
    auto chunks = chunk_bounds(mf.data, mf.size, nthreads);
    int nc = (int)chunks.size();

    std::vector<size_t> cnts(nc,0);
    {
        std::vector<std::function<void()>> tasks;
        for (int i=0;i<nc;i++)
            tasks.push_back([&cnts,&mf,&chunks,i]{ cnts[i]=count_lines(mf.data,chunks[i].first,chunks[i].second); });
        pool.submit_and_wait(std::move(tasks));
    }
    std::vector<size_t> offsets(nc+1,0);
    for (int i=0;i<nc;i++) offsets[i+1]=offsets[i]+cnts[i];
    size_t total = offsets[nc];
    std::cout << " rows=" << total << std::flush;

    std::vector<int32_t> col_custkey(total), col_nationkey(total);
    std::vector<int8_t>  col_mktsg(total);
    std::vector<char>    col_name(total * 26, '\0');

    {
        std::vector<std::function<void()>> tasks;
        for (int i=0;i<nc;i++) {
            tasks.push_back([&,i] {
                size_t row = offsets[i];
                LP lp(mf.data + chunks[i].first);
                size_t end = chunks[i].second;
                while ((size_t)(lp.p - mf.data) < end && *lp.p != '\0') {
                    col_custkey[row]  = lp.i32();
                    lp.str_fixed(&col_name[row*26], 26);
                    lp.skip_field(); // address
                    col_nationkey[row]= lp.i32();
                    lp.skip_field(); // phone
                    lp.skip_field(); // acctbal
                    col_mktsg[row]    = lp.dict8(MKTSEGMENT_DICT, MKTSEGMENT_DICT_SZ);
                    lp.skip_line();
                    row++;
                }
            });
        }
        pool.submit_and_wait(std::move(tasks));
    }

    std::cout << " writing..." << std::flush;
    std::string tdir = db_dir + "/customer";
    fs::create_directories(tdir);
    {
        std::vector<std::function<void()>> tasks = {
            [&]{ write_col(tdir+"/c_custkey.bin",    col_custkey); },
            [&]{ write_str_col(tdir+"/c_name.bin",   col_name); },
            [&]{ write_col(tdir+"/c_nationkey.bin",  col_nationkey); },
            [&]{ write_col(tdir+"/c_mktsegment.bin", col_mktsg); },
        };
        pool.submit_and_wait(std::move(tasks));
    }
    write_dict(tdir+"/c_mktsegment_dict.txt", MKTSEGMENT_DICT, MKTSEGMENT_DICT_SZ);
    std::cout << " done.\n";
}

// PART: 9 fields, write 2 columns
// fields: partkey|name|mfgr|brand|type|size|container|retailprice|comment
void ingest_part(const std::string& data_dir, const std::string& db_dir, ThreadPool& pool, int nthreads) {
    std::cout << "[part] mmapping..." << std::flush;
    MmapFile mf(data_dir + "/part.tbl");
    auto chunks = chunk_bounds(mf.data, mf.size, nthreads);
    int nc = (int)chunks.size();

    std::vector<size_t> cnts(nc,0);
    {
        std::vector<std::function<void()>> tasks;
        for (int i=0;i<nc;i++)
            tasks.push_back([&cnts,&mf,&chunks,i]{ cnts[i]=count_lines(mf.data,chunks[i].first,chunks[i].second); });
        pool.submit_and_wait(std::move(tasks));
    }
    std::vector<size_t> offsets(nc+1,0);
    for (int i=0;i<nc;i++) offsets[i+1]=offsets[i]+cnts[i];
    size_t total = offsets[nc];
    std::cout << " rows=" << total << std::flush;

    std::vector<int32_t> col_partkey(total);
    std::vector<char>    col_name(total * 56, '\0');

    {
        std::vector<std::function<void()>> tasks;
        for (int i=0;i<nc;i++) {
            tasks.push_back([&,i] {
                size_t row = offsets[i];
                LP lp(mf.data + chunks[i].first);
                size_t end = chunks[i].second;
                while ((size_t)(lp.p - mf.data) < end && *lp.p != '\0') {
                    col_partkey[row] = lp.i32();
                    lp.str_fixed(&col_name[row*56], 56);
                    lp.skip_line();
                    row++;
                }
            });
        }
        pool.submit_and_wait(std::move(tasks));
    }

    std::cout << " writing..." << std::flush;
    std::string tdir = db_dir + "/part";
    fs::create_directories(tdir);
    {
        std::vector<std::function<void()>> tasks = {
            [&]{ write_col(tdir+"/p_partkey.bin", col_partkey); },
            [&]{ write_str_col(tdir+"/p_name.bin", col_name); },
        };
        pool.submit_and_wait(std::move(tasks));
    }
    std::cout << " done.\n";
}

// PARTSUPP: 5 fields, write 3 columns
// fields: partkey|suppkey|availqty|supplycost|comment
void ingest_partsupp(const std::string& data_dir, const std::string& db_dir, ThreadPool& pool, int nthreads) {
    std::cout << "[partsupp] mmapping..." << std::flush;
    MmapFile mf(data_dir + "/partsupp.tbl");
    auto chunks = chunk_bounds(mf.data, mf.size, nthreads);
    int nc = (int)chunks.size();

    std::vector<size_t> cnts(nc,0);
    {
        std::vector<std::function<void()>> tasks;
        for (int i=0;i<nc;i++)
            tasks.push_back([&cnts,&mf,&chunks,i]{ cnts[i]=count_lines(mf.data,chunks[i].first,chunks[i].second); });
        pool.submit_and_wait(std::move(tasks));
    }
    std::vector<size_t> offsets(nc+1,0);
    for (int i=0;i<nc;i++) offsets[i+1]=offsets[i]+cnts[i];
    size_t total = offsets[nc];
    std::cout << " rows=" << total << std::flush;

    std::vector<int32_t> col_partkey(total), col_suppkey(total);
    std::vector<double>  col_supplycost(total);

    {
        std::vector<std::function<void()>> tasks;
        for (int i=0;i<nc;i++) {
            tasks.push_back([&,i] {
                size_t row = offsets[i];
                LP lp(mf.data + chunks[i].first);
                size_t end = chunks[i].second;
                while ((size_t)(lp.p - mf.data) < end && *lp.p != '\0') {
                    col_partkey[row]    = lp.i32();
                    col_suppkey[row]    = lp.i32();
                    lp.skip_field(); // availqty
                    col_supplycost[row] = lp.dbl();
                    lp.skip_line();
                    row++;
                }
            });
        }
        pool.submit_and_wait(std::move(tasks));
    }

    std::cout << " writing..." << std::flush;
    std::string tdir = db_dir + "/partsupp";
    fs::create_directories(tdir);
    {
        std::vector<std::function<void()>> tasks = {
            [&]{ write_col(tdir+"/ps_partkey.bin",    col_partkey); },
            [&]{ write_col(tdir+"/ps_suppkey.bin",    col_suppkey); },
            [&]{ write_col(tdir+"/ps_supplycost.bin", col_supplycost); },
        };
        pool.submit_and_wait(std::move(tasks));
    }
    std::cout << " done.\n";
}

// SUPPLIER: 7 fields, write 2 columns
// fields: suppkey|name|address|nationkey|phone|acctbal|comment
void ingest_supplier(const std::string& data_dir, const std::string& db_dir, ThreadPool& pool, int nthreads) {
    std::cout << "[supplier] mmapping..." << std::flush;
    MmapFile mf(data_dir + "/supplier.tbl");
    auto chunks = chunk_bounds(mf.data, mf.size, nthreads);
    int nc = (int)chunks.size();

    std::vector<size_t> cnts(nc,0);
    {
        std::vector<std::function<void()>> tasks;
        for (int i=0;i<nc;i++)
            tasks.push_back([&cnts,&mf,&chunks,i]{ cnts[i]=count_lines(mf.data,chunks[i].first,chunks[i].second); });
        pool.submit_and_wait(std::move(tasks));
    }
    std::vector<size_t> offsets(nc+1,0);
    for (int i=0;i<nc;i++) offsets[i+1]=offsets[i]+cnts[i];
    size_t total = offsets[nc];
    std::cout << " rows=" << total << std::flush;

    std::vector<int32_t> col_suppkey(total), col_nationkey(total);

    {
        std::vector<std::function<void()>> tasks;
        for (int i=0;i<nc;i++) {
            tasks.push_back([&,i] {
                size_t row = offsets[i];
                LP lp(mf.data + chunks[i].first);
                size_t end = chunks[i].second;
                while ((size_t)(lp.p - mf.data) < end && *lp.p != '\0') {
                    col_suppkey[row]   = lp.i32();
                    lp.skip_field(); // name
                    lp.skip_field(); // address
                    col_nationkey[row] = lp.i32();
                    lp.skip_line();
                    row++;
                }
            });
        }
        pool.submit_and_wait(std::move(tasks));
    }

    std::string tdir = db_dir + "/supplier";
    fs::create_directories(tdir);
    write_col(tdir+"/s_suppkey.bin",   col_suppkey);
    write_col(tdir+"/s_nationkey.bin", col_nationkey);
    std::cout << " done.\n";
}

// NATION: 4 fields, write 2 columns
// fields: nationkey|name|regionkey|comment
void ingest_nation(const std::string& data_dir, const std::string& db_dir) {
    std::cout << "[nation] " << std::flush;
    MmapFile mf(data_dir + "/nation.tbl");
    std::vector<int32_t> col_key;
    std::vector<char>    col_name;

    LP lp(mf.data);
    size_t end = mf.size;
    while ((size_t)(lp.p - mf.data) < end && *lp.p != '\0') {
        col_key.push_back(lp.i32());
        char buf[26]={};
        lp.str_fixed(buf, 26);
        for (int i=0;i<26;i++) col_name.push_back(buf[i]);
        lp.skip_line();
    }

    std::string tdir = db_dir + "/nation";
    fs::create_directories(tdir);
    write_col(tdir+"/n_nationkey.bin", col_key);
    write_str_col(tdir+"/n_name.bin",  col_name);
    std::cout << "rows=" << col_key.size() << " done.\n";
}

// REGION: 3 fields, write 2 columns
void ingest_region(const std::string& data_dir, const std::string& db_dir) {
    std::cout << "[region] " << std::flush;
    MmapFile mf(data_dir + "/region.tbl");
    std::vector<int32_t> col_key;
    std::vector<char>    col_name;

    LP lp(mf.data);
    size_t end = mf.size;
    while ((size_t)(lp.p - mf.data) < end && *lp.p != '\0') {
        col_key.push_back(lp.i32());
        char buf[26]={};
        lp.str_fixed(buf, 26);
        for (int i=0;i<26;i++) col_name.push_back(buf[i]);
        lp.skip_line();
    }

    std::string tdir = db_dir + "/region";
    fs::create_directories(tdir);
    write_col(tdir+"/r_regionkey.bin", col_key);
    write_str_col(tdir+"/r_name.bin",  col_name);
    std::cout << "rows=" << col_key.size() << " done.\n";
}

// ─────────────────────────── Post-ingestion verify ──────────────────────────

void verify_dates(const std::string& path, const std::string& label) {
    // mmap and spot-check that date values > 3000 (1978-03-22 epoch ~ 3000)
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "VERIFY WARN: cannot open " << path << "\n"; return; }
    struct stat st{}; fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    if (sz < sizeof(int32_t)) { close(fd); return; }
    auto* col = (const int32_t*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    size_t n = sz / sizeof(int32_t);
    // check first, middle, last
    size_t indices[] = {0, n/2, n-1};
    for (size_t idx : indices) {
        if (col[idx] <= 3000) {
            munmap((void*)col, sz);
            throw std::runtime_error("DATE VERIFY FAILED: " + label + " value=" + std::to_string(col[idx]) + " at row " + std::to_string(idx));
        }
    }
    std::cout << "  [OK] " << label << " dates: first=" << col[0] << " last=" << col[n-1] << "\n";
    munmap((void*)col, sz);
}

void verify_decimals(const std::string& path, const std::string& label) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::cerr << "VERIFY WARN: cannot open " << path << "\n"; return; }
    struct stat st{}; fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    if (sz < sizeof(double)) { close(fd); return; }
    auto* col = (const double*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    size_t n = sz / sizeof(double);
    size_t indices[] = {0, n/2, n-1};
    for (size_t idx : indices) {
        if (col[idx] == 0.0) {
            munmap((void*)col, sz);
            throw std::runtime_error("DECIMAL VERIFY FAILED: " + label + " zero at row " + std::to_string(idx));
        }
    }
    std::cout << "  [OK] " << label << " decimals: first=" << col[0] << " last=" << col[n-1] << "\n";
    munmap((void*)col, sz);
}

// ─────────────────────────── Main ───────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: ingest <data_dir> <db_dir>\n";
        return 1;
    }
    std::string data_dir = argv[1];
    std::string db_dir   = argv[2];

    // Self-test: date_to_epoch(1970,1,1) must be 0
    if (date_to_epoch(1970, 1, 1) != 0) {
        std::cerr << "FATAL: date_to_epoch self-test failed!\n";
        return 1;
    }

    fs::create_directories(db_dir);
    fs::create_directories(db_dir + "/indexes");

    int nthreads = std::max(1, (int)std::thread::hardware_concurrency());
    std::cout << "Using " << nthreads << " threads.\n";
    ThreadPool pool(nthreads);

    auto t0 = std::chrono::steady_clock::now();

    // Large tables sequentially (HDD friendly, each uses all threads)
    ingest_lineitem(data_dir, db_dir, pool, nthreads);
    ingest_orders(data_dir, db_dir, pool, nthreads);
    ingest_partsupp(data_dir, db_dir, pool, nthreads);

    // Medium/small tables (submit in parallel: each takes 1-4 threads)
    {
        std::vector<std::function<void()>> tasks = {
            [&]{ ingest_customer(data_dir, db_dir, pool, std::max(1, nthreads/4)); },
            [&]{ ingest_part    (data_dir, db_dir, pool, std::max(1, nthreads/4)); },
        };
        pool.submit_and_wait(std::move(tasks));
    }
    ingest_supplier(data_dir, db_dir, pool, std::max(1, nthreads/8));
    ingest_nation(data_dir, db_dir);
    ingest_region(data_dir, db_dir);

    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "Ingestion complete in " << elapsed << "s\n";

    // ── Post-ingestion verification ──
    std::cout << "Verifying...\n";
    try {
        verify_dates(db_dir+"/lineitem/l_shipdate.bin",   "lineitem.l_shipdate");
        verify_dates(db_dir+"/orders/o_orderdate.bin",    "orders.o_orderdate");
        verify_decimals(db_dir+"/lineitem/l_extendedprice.bin", "lineitem.l_extendedprice");
        verify_decimals(db_dir+"/lineitem/l_discount.bin",      "lineitem.l_discount");
        verify_decimals(db_dir+"/orders/o_totalprice.bin",      "orders.o_totalprice");
        verify_decimals(db_dir+"/partsupp/ps_supplycost.bin",   "partsupp.ps_supplycost");
    } catch (const std::exception& e) {
        std::cerr << "VERIFICATION FAILED: " << e.what() << "\n";
        return 1;
    }
    std::cout << "All verifications passed.\n";
    return 0;
}
