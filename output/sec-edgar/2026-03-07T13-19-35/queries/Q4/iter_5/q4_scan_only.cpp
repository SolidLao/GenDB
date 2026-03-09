// Test: what's the cost of the pure scan (no aggregation)?
#include <vector>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include "mmap_utils.h"
using namespace std; using namespace gendb;

struct ZoneMap{int8_t min_uom,max_uom;int32_t min_ddate,max_ddate;};
static_assert(sizeof(ZoneMap)==12);

int main(int argc,char*argv[]){
    const string gdir=argv[1];
    int NT=(int)thread::hardware_concurrency(); if(NT>64)NT=64;
    
    // Load zone maps
    uint32_t n_blocks=0;
    vector<ZoneMap> zm;
    {int fd=open((gdir+"/indexes/num_zone_maps.bin").c_str(),O_RDONLY);read(fd,&n_blocks,4);zm.resize(n_blocks);read(fd,zm.data(),n_blocks*sizeof(ZoneMap));close(fd);}
    
    MmapColumn<int8_t> uom(gdir+"/num/uom_code.bin");
    MmapColumn<int32_t> adsh(gdir+"/num/adsh_code.bin");
    MmapColumn<int32_t> tagver(gdir+"/num/tagver_code.bin");
    MmapColumn<double> val(gdir+"/num/value.bin");
    size_t NUM_N=uom.count;
    mmap_prefetch_all(uom,adsh,tagver,val);
    
    int8_t usd_code=0; // known from diag
    
    // Test 1: scan all rows, count USD rows
    auto t0=chrono::high_resolution_clock::now();
    atomic<uint64_t> usd_count{0};
    atomic<uint32_t> next{0};
    {auto w=[&](int tid){uint64_t c=0;while(true){uint32_t b=next.fetch_add(1,memory_order_relaxed);if(b>=n_blocks)break;if(zm[b].min_uom>usd_code||zm[b].max_uom<usd_code)continue;size_t rs=(size_t)b*100000,re=min(rs+100000,NUM_N),n=re-rs;const int8_t*u=uom.data+rs;for(size_t i=0;i<n;i++)if(u[i]==usd_code)c++;}usd_count.fetch_add(c,memory_order_relaxed);};
    vector<thread>thrs;for(int i=0;i<NT;i++)thrs.emplace_back(w,i);for(auto&t:thrs)t.join();}
    auto t1=chrono::high_resolution_clock::now();
    fprintf(stderr,"scan_uom_only: %.2fms usd_count=%lu\n",chrono::duration<double,milli>(t1-t0).count(),(unsigned long)usd_count.load());
    
    // Test 2: scan with adsh access (random)
    static const size_t SUB_N=86135;
    vector<uint64_t> qb((SUB_N+63)/64,0ULL);
    {MmapColumn<int16_t>ss(gdir+"/sub/sic.bin");for(size_t i=0;i<SUB_N;i++){if(ss.data[i]>=4000&&ss.data[i]<=4999)qb[i>>6]|=1ULL<<(i&63);}}
    
    next.store(0);
    atomic<uint64_t> sic_count{0};
    auto t2=chrono::high_resolution_clock::now();
    {auto w=[&](int tid){uint64_t c=0;while(true){uint32_t b=next.fetch_add(1,memory_order_relaxed);if(b>=n_blocks)break;if(zm[b].min_uom>usd_code||zm[b].max_uom<usd_code)continue;size_t rs=(size_t)b*100000,re=min(rs+100000,NUM_N),n=re-rs;const int8_t*u=uom.data+rs;const int32_t*a=adsh.data+rs,*tv=tagver.data+rs;for(size_t i=0;i<n;i++){if(u[i]!=usd_code)continue;int32_t t=tv[i];if(t<0)continue;uint32_t ac=(uint32_t)a[i];if(ac>=SUB_N)continue;if((qb[ac>>6]>>(ac&63))&1)c++;};}sic_count.fetch_add(c,memory_order_relaxed);};
    vector<thread>thrs;for(int i=0;i<NT;i++)thrs.emplace_back(w,i);for(auto&t:thrs)t.join();}
    auto t3=chrono::high_resolution_clock::now();
    fprintf(stderr,"scan_sic_filter: %.2fms sic_count=%lu\n",chrono::duration<double,milli>(t3-t2).count(),(unsigned long)sic_count.load());
    
    return 0;
}
