#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "date_utils.h"

int main() {
    gendb::init_date_tables();
    const char* db = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/sf10.gendb";
    int32_t thr = gendb::date_str_to_epoch_days("1995-03-15");
    printf("date_threshold = %d\n", thr);
    
    // Check a few lineitem rows
    std::string path = std::string(db) + "/lineitem/l_shipdate.bin";
    int fd = open(path.c_str(), O_RDONLY);
    struct stat st; fstat(fd, &st);
    int32_t* sd = (int32_t*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    size_t n = st.st_size / 4;
    size_t pass = 0;
    for (size_t i = 0; i < n; i++) if (sd[i] > thr) pass++;
    printf("lineitem rows passing l_shipdate > thr: %zu / %zu\n", pass, n);
    munmap(sd, st.st_size);
    
    // Check a few orders
    path = std::string(db) + "/orders/o_orderdate.bin";
    fd = open(path.c_str(), O_RDONLY); fstat(fd, &st);
    int32_t* od = (int32_t*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    size_t n2 = st.st_size / 4;
    pass = 0;
    for (size_t i = 0; i < n2; i++) if (od[i] < thr) pass++;
    printf("orders rows passing o_orderdate < thr: %zu / %zu\n", pass, n2);
    munmap(od, st.st_size);
    
    return 0;
}
