// Quick timing breakdowns
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
int main(){
    auto t0 = std::chrono::high_resolution_clock::now();
    // Allocate and zero orders arrays
    static const uint32_t ORD_CAP = 1u<<25;
    int32_t* k = new int32_t[ORD_CAP]();
    int32_t* v = new int32_t[ORD_CAP];
    auto t1 = std::chrono::high_resolution_clock::now();
    printf("orders alloc+zero: %.1fms\n", 
        std::chrono::duration<double,std::milli>(t1-t0).count());
    delete[]k; delete[]v;

    // Direct array approach
    t0 = std::chrono::high_resolution_clock::now();
    static const int ORD_MAX = 60000001;
    int32_t* da = (int32_t*)mmap(nullptr,(size_t)ORD_MAX*4,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);
    madvise(da,(size_t)ORD_MAX*4,MADV_HUGEPAGE);
    t1 = std::chrono::high_resolution_clock::now();
    printf("direct arr mmap: %.1fms\n",
        std::chrono::duration<double,std::milli>(t1-t0).count());
    munmap(da,(size_t)ORD_MAX*4);
}
