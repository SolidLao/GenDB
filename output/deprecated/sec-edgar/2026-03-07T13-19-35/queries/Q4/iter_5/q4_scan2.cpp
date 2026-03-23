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
#include <unordered_map>
#include <cstring>
#include "mmap_utils.h"
using namespace std; using namespace gendb;

struct ZoneMap{int8_t min_uom,max_uom;int32_t min_ddate,max_ddate;};
static_assert(sizeof(ZoneMap)==12);
struct FlatCountMap {
    struct Entry { uint64_t key; uint32_t val; };
    vector<Entry> slots; size_t mask_=0;
    static constexpr uint64_t EMPTY=UINT64_MAX;
    explicit FlatCountMap(size_t n){size_t c=1;while(c<n*2)c<<=1;slots.assign(c,{EMPTY,0});mask_=c-1;}
    static inline uint64_t mix(uint64_t k){k^=k>>33;k*=0xff51afd7ed558ccdULL;k^=k>>33;return k;}
    inline void inc(uint64_t k){size_t i=mix(k)&mask_;while(slots[i].key!=EMPTY&&slots[i].key!=k)i=(i+1)&mask_;if(slots[i].key==EMPTY)slots[i].key=k;slots[i].val++;}
    inline bool probe(uint64_t k,size_t*s,uint32_t*c)const{size_t i=mix(k)&mask_;while(slots[i].key!=EMPTY&&slots[i].key!=k)i=(i+1)&mask_;if(slots[i].key!=k)return false;*s=i;*c=slots[i].val;return true;}
};

int main(int argc,char*argv[]){
    const string gdir=argv[1];
    int NT=(int)thread::hardware_concurrency(); if(NT>64)NT=64;
    int8_t usd_code=0;  // known
    int8_t eq_code=3;
    
    uint32_t n_blocks=0; vector<ZoneMap> zm;
    {int fd=open((gdir+"/indexes/num_zone_maps.bin").c_str(),O_RDONLY);read(fd,&n_blocks,4);zm.resize(n_blocks);read(fd,zm.data(),n_blocks*sizeof(ZoneMap));close(fd);}
    
    MmapColumn<int8_t> uom(gdir+"/num/uom_code.bin"),tab(gdir+"/tag/abstract.bin");
    MmapColumn<int32_t> adsh(gdir+"/num/adsh_code.bin"),tagver(gdir+"/num/tagver_code.bin");
    MmapColumn<double> val(gdir+"/num/value.bin");
    size_t NUM_N=uom.count,TAG_N=tab.count;
    mmap_prefetch_all(uom,adsh,tagver,val);
    tab.prefetch();
    
    static const size_t SUB_N=86135;
    vector<uint64_t> qb((SUB_N+63)/64,0ULL);
    {MmapColumn<int16_t>ss(gdir+"/sub/sic.bin");for(size_t i=0;i<SUB_N;i++){if(ss.data[i]>=4000&&ss.data[i]<=4999)qb[i>>6]|=1ULL<<(i&63);}}
    
    // Build eq_count_map
    FlatCountMap ecm(25000);
    {MmapColumn<int32_t>pa(gdir+"/pre/adsh_code.bin"),pt(gdir+"/pre/tagver_code.bin");
     MmapColumn<int8_t>ps(gdir+"/pre/stmt_code.bin");
     vector<vector<uint64_t>> tk(NT);
     {auto w=[&](int t){size_t s=(size_t)t*pa.count/NT,e=(size_t)(t+1)*pa.count/NT;auto&k=tk[t];for(size_t i=s;i<e;i++){if(ps.data[i]!=eq_code)continue;uint32_t ac=(uint32_t)pa.data[i];if(ac>=SUB_N)continue;if(!((qb[ac>>6]>>(ac&63))&1))continue;uint64_t key=((uint64_t)ac<<32)|(uint32_t)pt.data[i];if(key!=FlatCountMap::EMPTY)k.push_back(key);}};
     vector<thread>th;for(int i=0;i<NT;i++)th.emplace_back(w,i);for(auto&t:th)t.join();}
     for(auto&k:tk)for(uint64_t v:k)ecm.inc(v);}
    
    // Test: scan with tag_abstract + eq_count_map probe, but NO la write
    atomic<uint32_t> next{0};
    atomic<uint64_t> qual{0};
    auto t0=chrono::high_resolution_clock::now();
    {auto w=[&](int tid){uint64_t c=0;
        while(true){uint32_t b=next.fetch_add(1,memory_order_relaxed);if(b>=n_blocks)break;
        if(zm[b].min_uom>usd_code||zm[b].max_uom<usd_code)continue;
        size_t rs=(size_t)b*100000,re=min(rs+100000,NUM_N),n=re-rs;
        const int8_t*u=uom.data+rs;const int32_t*a=adsh.data+rs,*tv=tagver.data+rs;const double*v=val.data+rs;
        for(size_t i=0;i<n;i++){
            if(u[i]!=usd_code)continue;
            int32_t t=tv[i];if(t<0)continue;
            uint32_t ac=(uint32_t)a[i];if(ac>=SUB_N)continue;
            if(!((qb[ac>>6]>>(ac&63))&1))continue;
            if((uint32_t)t>=TAG_N)continue;
            if(tab.data[t]!=0)continue;  // L3 random access
            uint64_t eq_key=((uint64_t)ac<<32)|(uint32_t)t;
            size_t slot=0;uint32_t eq_cnt=0;
            if(!ecm.probe(eq_key,&slot,&eq_cnt))continue;  // L3 random access
            c++;
        }}qual.fetch_add(c,memory_order_relaxed);};
    vector<thread>th;for(int i=0;i<NT;i++)th.emplace_back(w,i);for(auto&t:th)t.join();}
    auto t1=chrono::high_resolution_clock::now();
    fprintf(stderr,"scan_no_write: %.2fms qual=%lu\n",chrono::duration<double,milli>(t1-t0).count(),(unsigned long)qual.load());
    
    // Now test WITH la writes
    uint32_t N_groups=23545;
    vector<uint64_t> s2g_v(65536,UINT32_MAX);
    {// minimal group assignment
     unordered_map<uint64_t,uint32_t>gm;gm.reserve(30000);uint32_t ng=0;
     for(size_t s=0;s<65536;s++){if(ecm.slots[s].key==FlatCountMap::EMPTY)continue;uint64_t k=ecm.slots[s].key;auto[it,ins]=gm.emplace(k,ng);if(ins)ng++;s2g_v[s]=it->second;}N_groups=ng;}
    fprintf(stderr,"actual N_groups=%u\n",N_groups);
    
    struct SumCnt{double sum;int64_t cnt;};
    vector<vector<SumCnt>> tla(NT);
    for(int t=0;t<NT;t++){tla[t].resize(N_groups,{0.0,0});}
    
    next.store(0); qual.store(0);
    auto t2=chrono::high_resolution_clock::now();
    {auto w=[&](int tid){uint64_t c=0;auto&la=tla[tid];
        while(true){uint32_t b=next.fetch_add(1,memory_order_relaxed);if(b>=n_blocks)break;
        if(zm[b].min_uom>usd_code||zm[b].max_uom<usd_code)continue;
        size_t rs=(size_t)b*100000,re=min(rs+100000,NUM_N),n=re-rs;
        const int8_t*u=uom.data+rs;const int32_t*a=adsh.data+rs,*tv=tagver.data+rs;const double*v=val.data+rs;
        for(size_t i=0;i<n;i++){
            if(u[i]!=usd_code)continue;
            int32_t t=tv[i];if(t<0)continue;
            uint32_t ac=(uint32_t)a[i];if(ac>=SUB_N)continue;
            if(!((qb[ac>>6]>>(ac&63))&1))continue;
            if((uint32_t)t>=TAG_N)continue;
            if(tab.data[t]!=0)continue;
            uint64_t eq_key=((uint64_t)ac<<32)|(uint32_t)t;
            size_t slot=0;uint32_t eq_cnt=0;
            if(!ecm.probe(eq_key,&slot,&eq_cnt))continue;
            uint32_t gidx=s2g_v[slot];
            la[gidx].sum+=v[i]*(double)eq_cnt;  // L3 random write
            la[gidx].cnt+=(int64_t)eq_cnt;
            c++;
        }}qual.fetch_add(c,memory_order_relaxed);};
    vector<thread>th;for(int i=0;i<NT;i++)th.emplace_back(w,i);for(auto&t:th)t.join();}
    auto t3=chrono::high_resolution_clock::now();
    fprintf(stderr,"scan_with_la_write: %.2fms qual=%lu\n",chrono::duration<double,milli>(t3-t2).count(),(unsigned long)qual.load());
    
    return 0;
}
