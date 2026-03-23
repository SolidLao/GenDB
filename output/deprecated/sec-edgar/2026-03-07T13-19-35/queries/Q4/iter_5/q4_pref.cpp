// Test prefetching effectiveness for tag_abstract + eq_count_map
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
    inline uint64_t ecm_prefetch_addr(uint64_t k)const{return (uint64_t)&slots[mix(k)&mask_];}
};

static void test_scan(const char* label, int NT, uint32_t n_blocks, const vector<ZoneMap>&zm,
    MmapColumn<int8_t>&uom, MmapColumn<int32_t>&adsh, MmapColumn<int32_t>&tagver, MmapColumn<double>&val,
    MmapColumn<int8_t>&tab, const vector<uint64_t>&qb, const FlatCountMap&ecm, int8_t usd_code) {
    size_t NUM_N=uom.count,TAG_N=tab.count; static const size_t SUB_N=86135;
    atomic<uint32_t> next{0}; atomic<uint64_t> qual{0};
    
    auto t0=chrono::high_resolution_clock::now();
    {auto w=[&](int tid){uint64_t c=0;
        while(true){uint32_t b=next.fetch_add(1,memory_order_relaxed);if(b>=n_blocks)break;
        if(zm[b].min_uom>usd_code||zm[b].max_uom<usd_code)continue;
        size_t rs=(size_t)b*100000,re=min(rs+100000,NUM_N),n=re-rs;
        const int8_t*u=uom.data+rs;const int32_t*a=adsh.data+rs,*tv=tagver.data+rs;const double*v=val.data+rs;
        
        // Two-level: collect SIC-passing rows, then batch-prefetch
        struct Row{int32_t tv2,ac2;double v2;};
        static thread_local Row buf[1024];
        size_t nb=0;
        
        auto process_batch=[&](){
            // prefetch tag_abstract for all rows in buf
            for(size_t j=0;j<nb;j++) if((uint32_t)buf[j].tv2<TAG_N) __builtin_prefetch(&tab.data[buf[j].tv2],0,1);
            // small delay to let prefetches complete
            for(size_t j=0;j<nb;j++){
                int32_t t=buf[j].tv2; uint32_t ac=(uint32_t)buf[j].ac2;
                if((uint32_t)t>=TAG_N)continue;
                if(tab.data[t]!=0)continue;
                uint64_t eq_key=((uint64_t)ac<<32)|(uint32_t)t;
                size_t slot=0;uint32_t eq_cnt=0;
                if(!ecm.probe(eq_key,&slot,&eq_cnt))continue;
                c++;
            }
            nb=0;
        };
        
        for(size_t i=0;i<n;i++){
            if(u[i]!=usd_code)continue;
            int32_t t=tv[i];if(t<0)continue;
            uint32_t ac=(uint32_t)a[i];if(ac>=SUB_N)continue;
            if(!((qb[ac>>6]>>(ac&63))&1))continue;
            buf[nb++]={t,a[i],v[i]};
            if(nb==1024)process_batch();
        }
        if(nb>0)process_batch();
        }qual.fetch_add(c,memory_order_relaxed);};
    vector<thread>th;for(int i=0;i<NT;i++)th.emplace_back(w,i);for(auto&t:th)t.join();}
    auto t1=chrono::high_resolution_clock::now();
    fprintf(stderr,"%s (NT=%d): %.2fms qual=%lu\n",label,NT,chrono::duration<double,milli>(t1-t0).count(),(unsigned long)qual.load());
}

int main(int argc,char*argv[]){
    const string gdir=argv[1];
    int8_t usd_code=0,eq_code=3;
    uint32_t n_blocks=0; vector<ZoneMap> zm;
    {int fd=open((gdir+"/indexes/num_zone_maps.bin").c_str(),O_RDONLY);read(fd,&n_blocks,4);zm.resize(n_blocks);read(fd,zm.data(),n_blocks*sizeof(ZoneMap));close(fd);}
    
    MmapColumn<int8_t> uom(gdir+"/num/uom_code.bin"),tab(gdir+"/tag/abstract.bin");
    MmapColumn<int32_t> adsh(gdir+"/num/adsh_code.bin"),tagver(gdir+"/num/tagver_code.bin");
    MmapColumn<double> val(gdir+"/num/value.bin");
    size_t NUM_N=uom.count; static const size_t SUB_N=86135;
    mmap_prefetch_all(uom,adsh,tagver,val); tab.prefetch();
    
    vector<uint64_t> qb((SUB_N+63)/64,0ULL);
    {MmapColumn<int16_t>ss(gdir+"/sub/sic.bin");for(size_t i=0;i<SUB_N;i++){if(ss.data[i]>=4000&&ss.data[i]<=4999)qb[i>>6]|=1ULL<<(i&63);}}
    
    FlatCountMap ecm(25000);
    {MmapColumn<int32_t>pa(gdir+"/pre/adsh_code.bin"),pt(gdir+"/pre/tagver_code.bin");
     MmapColumn<int8_t>ps(gdir+"/pre/stmt_code.bin");
     int NT=64;
     vector<vector<uint64_t>> tk(NT);
     {auto w=[&](int t){size_t s=(size_t)t*pa.count/NT,e=(size_t)(t+1)*pa.count/NT;auto&k=tk[t];for(size_t i=s;i<e;i++){if(ps.data[i]!=eq_code)continue;uint32_t ac=(uint32_t)pa.data[i];if(ac>=SUB_N)continue;if(!((qb[ac>>6]>>(ac&63))&1))continue;uint64_t key=((uint64_t)ac<<32)|(uint32_t)pt.data[i];if(key!=FlatCountMap::EMPTY)k.push_back(key);}};
     vector<thread>th;for(int i=0;i<NT;i++)th.emplace_back(w,i);for(auto&t:th)t.join();}
     for(auto&k:tk)for(uint64_t v:k)ecm.inc(v);}
    
    // Test with different thread counts
    for(int NT : {64, 32, 16, 8}) {
        atomic<uint32_t> next{0}; atomic<uint64_t> qual{0};
        auto t0=chrono::high_resolution_clock::now();
        {auto w=[&](int tid){uint64_t c=0;
            size_t TAG_N=tab.count;
            while(true){uint32_t b=next.fetch_add(1,memory_order_relaxed);if(b>=n_blocks)break;
            if(zm[b].min_uom>usd_code||zm[b].max_uom<usd_code)continue;
            size_t rs=(size_t)b*100000,re=min(rs+100000,NUM_N),n=re-rs;
            const int8_t*u=uom.data+rs;const int32_t*a=adsh.data+rs,*tv=tagver.data+rs;
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
                c++;
            }}qual.fetch_add(c,memory_order_relaxed);};
        vector<thread>th;for(int i=0;i<NT;i++)th.emplace_back(w,i);for(auto&t:th)t.join();}
        auto t1=chrono::high_resolution_clock::now();
        fprintf(stderr,"no_pref NT=%d: %.2fms qual=%lu\n",NT,chrono::duration<double,milli>(t1-t0).count(),(unsigned long)qual.load());
    }
    
    // Test with batch prefetch
    for(int NT : {64, 32}) {
        test_scan("batch_pref", NT, n_blocks, zm, uom, adsh, tagver, val, tab, qb, ecm, usd_code);
    }
    
    return 0;
}
