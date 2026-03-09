// Diagnostic version - prints timing for sub-phases and row counts
#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include <chrono>
#include "timing_utils.h"
#include "mmap_utils.h"
using namespace std;
using namespace gendb;

struct FlatCountMap {
    struct Entry { uint64_t key; uint32_t val; };
    vector<Entry> slots;
    size_t mask_=0;
    static constexpr uint64_t EMPTY=UINT64_MAX;
    explicit FlatCountMap(size_t cap_hint){
        size_t cap=1; while(cap<cap_hint*2) cap<<=1;
        slots.assign(cap,{EMPTY,0}); mask_=cap-1;
    }
    static inline uint64_t mix(uint64_t k){k^=k>>33;k*=0xff51afd7ed558ccdULL;k^=k>>33;return k;}
    inline void increment(uint64_t key){size_t idx=mix(key)&mask_;while(slots[idx].key!=EMPTY&&slots[idx].key!=key)idx=(idx+1)&mask_;if(slots[idx].key==EMPTY)slots[idx].key=key;slots[idx].val++;}
    inline bool probe(uint64_t key,size_t*slot_out,uint32_t*cnt_out)const{size_t idx=mix(key)&mask_;while(slots[idx].key!=EMPTY&&slots[idx].key!=key)idx=(idx+1)&mask_;if(slots[idx].key!=key)return false;*slot_out=idx;*cnt_out=slots[idx].val;return true;}
};
struct ZoneMap{int8_t min_uom,max_uom;int32_t min_ddate,max_ddate;};
static_assert(sizeof(ZoneMap)==12);
struct SumCnt{double sum;int64_t cnt;};
static int8_t load_dict_code(const string&path,const char*key){FILE*f=fopen(path.c_str(),"rb");if(!f)exit(1);uint8_t N;fread(&N,1,1,f);for(int i=0;i<N;i++){int8_t c;uint8_t sl;fread(&c,1,1,f);fread(&sl,1,1,f);char buf[256]={};fread(buf,1,sl,f);buf[sl]=0;if(!strcmp(buf,key)){fclose(f);return c;}}fclose(f);exit(1);}

int main(int argc,char*argv[]){
    if(argc<2){return 1;}
    const string gdir=argv[1];
    int num_threads=(int)thread::hardware_concurrency();
    if(num_threads>64)num_threads=64;
    
    int8_t usd_code=load_dict_code(gdir+"/indexes/uom_codes.bin","USD");
    int8_t eq_code=load_dict_code(gdir+"/indexes/stmt_codes.bin","EQ");
    fprintf(stderr,"usd_code=%d eq_code=%d num_threads=%d\n",(int)usd_code,(int)eq_code,num_threads);

    static const size_t SUB_N=86135,TAG_N=1070662;
    MmapColumn<int16_t> sub_sic(gdir+"/sub/sic.bin");
    MmapColumn<int32_t> sub_cik_col(gdir+"/sub/cik.bin");
    MmapColumn<int8_t> tag_abstract(gdir+"/tag/abstract.bin");

    static const size_t BITSET_WORDS=(SUB_N+63)/64;
    vector<uint64_t> qual_bits_v(BITSET_WORDS,0ULL);
    vector<uint16_t> adsh_to_cik_idx(SUB_N,UINT16_MAX);
    vector<int32_t> idx_to_cik; idx_to_cik.reserve(4096);
    size_t N_ciks=0,cik_words=0;
    {
        unordered_map<int32_t,uint16_t> cik_to_idx; cik_to_idx.reserve(4096);
        for(size_t i=0;i<SUB_N;i++){int16_t sic=sub_sic.data[i];if(sic<4000||sic>4999)continue;qual_bits_v[i>>6]|=1ULL<<(i&63);int32_t cik=sub_cik_col.data[i];auto[it,ins]=cik_to_idx.emplace(cik,(uint16_t)idx_to_cik.size());if(ins)idx_to_cik.push_back(cik);adsh_to_cik_idx[i]=it->second;}
        N_ciks=idx_to_cik.size(); cik_words=(N_ciks+63)/64;
        fprintf(stderr,"N_ciks=%zu cik_words=%zu\n",N_ciks,cik_words);
    }

    vector<ZoneMap> zone_maps; uint32_t n_blocks=0;
    {int fd=open((gdir+"/indexes/num_zone_maps.bin").c_str(),O_RDONLY);read(fd,&n_blocks,4);zone_maps.resize(n_blocks);read(fd,zone_maps.data(),n_blocks*sizeof(ZoneMap));close(fd);}
    int zm_pass=0;
    for(uint32_t b=0;b<n_blocks;b++) if(!(zone_maps[b].min_uom>usd_code||zone_maps[b].max_uom<usd_code)) zm_pass++;
    fprintf(stderr,"zone_map_pass=%d/%u\n",zm_pass,n_blocks);

    MmapColumn<int8_t> num_uom(gdir+"/num/uom_code.bin");
    MmapColumn<int32_t> num_adsh(gdir+"/num/adsh_code.bin");
    MmapColumn<int32_t> num_tagver(gdir+"/num/tagver_code.bin");
    MmapColumn<double> num_value(gdir+"/num/value.bin");
    const size_t NUM_N=num_uom.count;
    mmap_prefetch_all(num_uom,num_adsh,num_tagver,num_value);

    FlatCountMap eq_count_map(25000);
    static const size_t SLOT_CAP=65536;
    vector<uint32_t> slot_to_group_idx(SLOT_CAP,UINT32_MAX);
    vector<int16_t> group_sic; vector<int32_t> group_tagver;
    uint32_t N_groups=0;
    {
        MmapColumn<int32_t> pa(gdir+"/pre/adsh_code.bin"),pt(gdir+"/pre/tagver_code.bin");
        MmapColumn<int8_t> ps(gdir+"/pre/stmt_code.bin");
        size_t PRE_N=pa.count;
        vector<vector<uint64_t>> tkeys(num_threads);
        {auto pw=[&](int tid){size_t s=(size_t)tid*PRE_N/num_threads,e=(size_t)(tid+1)*PRE_N/num_threads;auto&keys=tkeys[tid];keys.reserve((e-s)/20+16);for(size_t i=s;i<e;i++){if(ps.data[i]!=eq_code)continue;uint32_t ac=(uint32_t)pa.data[i];if(ac>=SUB_N)continue;if(!((qual_bits_v[ac>>6]>>(ac&63))&1))continue;uint64_t key=((uint64_t)ac<<32)|(uint32_t)pt.data[i];if(key!=FlatCountMap::EMPTY)keys.push_back(key);}};
        vector<thread>thrs;for(int i=0;i<num_threads;i++)thrs.emplace_back(pw,i);for(auto&t:thrs)t.join();}
        for(auto&keys:tkeys)for(uint64_t k:keys)eq_count_map.increment(k);
        group_sic.reserve(50000);group_tagver.reserve(50000);
        unordered_map<uint64_t,uint32_t> gki;gki.reserve(50000);
        for(size_t s=0;s<SLOT_CAP;s++){const auto&slot=eq_count_map.slots[s];if(slot.key==FlatCountMap::EMPTY)continue;uint32_t ac=(uint32_t)(slot.key>>32);uint32_t tv=(uint32_t)(slot.key&0xFFFFFFFF);int16_t sic=sub_sic.data[ac];uint64_t gkey=((uint64_t)(uint16_t)sic<<32)|tv;auto[it,ins]=gki.emplace(gkey,N_groups);if(ins){group_sic.push_back(sic);group_tagver.push_back((int32_t)tv);N_groups++;}slot_to_group_idx[s]=it->second;}
    }
    fprintf(stderr,"N_groups=%u\n",N_groups);

    // Check calloc timing
    auto t0=chrono::high_resolution_clock::now();
    vector<SumCnt*> tla(num_threads,nullptr);
    for(int t=0;t<num_threads;t++) tla[t]=(SumCnt*)calloc(N_groups,sizeof(SumCnt));
    auto t1=chrono::high_resolution_clock::now();
    fprintf(stderr,"calloc time=%.2fms\n",chrono::duration<double,milli>(t1-t0).count());

    // Count qualifying rows
    atomic<uint64_t> qual_rows{0};
    atomic<uint32_t> next_block{0};
    const uint64_t*qb=qual_bits_v.data();
    const uint16_t*cia=adsh_to_cik_idx.data();
    const uint32_t*s2g=slot_to_group_idx.data();
    const int8_t*tab=tag_abstract.data;
    uint32_t ng=N_groups;

    auto t2=chrono::high_resolution_clock::now();
    auto sw=[&](int tid){
        uint64_t rows=0;
        SumCnt*la=tla[tid];
        vector<uint32_t> cb; cb.reserve(16384);
        while(true){
            uint32_t b=next_block.fetch_add(1,memory_order_relaxed);
            if(b>=n_blocks)break;
            const ZoneMap&zm=zone_maps[b];
            if(zm.min_uom>usd_code||zm.max_uom<usd_code)continue;
            size_t rs=(size_t)b*100000,re=min(rs+100000,NUM_N),n=re-rs;
            const int8_t*uom=num_uom.data+rs;
            const int32_t*adsh=num_adsh.data+rs,*tagver=num_tagver.data+rs;
            const double*val=num_value.data+rs;
            for(size_t i=0;i<n;i++){
                if(uom[i]!=usd_code)continue;
                int32_t tv=tagver[i]; if(tv<0)continue;
                uint32_t ac=(uint32_t)adsh[i]; if(ac>=SUB_N)continue;
                if(!((qb[ac>>6]>>(ac&63))&1))continue;
                if((uint32_t)tv>=TAG_N)continue;
                if(tab[tv]!=0)continue;
                uint64_t eq_key=((uint64_t)ac<<32)|(uint32_t)tv;
                size_t slot=0; uint32_t eq_cnt=0;
                if(!eq_count_map.probe(eq_key,&slot,&eq_cnt))continue;
                uint32_t gidx=s2g[slot];
                la[gidx].sum+=val[i]*(double)eq_cnt;
                la[gidx].cnt+=(int64_t)eq_cnt;
                uint16_t ci=cia[ac];
                cb.push_back(((uint32_t)gidx<<16)|(uint32_t)ci);
                rows++;
            }
        }
        qual_rows.fetch_add(rows,memory_order_relaxed);
    };
    vector<thread>thrs2;for(int i=0;i<num_threads;i++)thrs2.emplace_back(sw,i);for(auto&t:thrs2)t.join();
    auto t3=chrono::high_resolution_clock::now();
    fprintf(stderr,"scan time=%.2fms qual_rows=%lu\n",chrono::duration<double,milli>(t3-t2).count(),(unsigned long)qual_rows.load());
    for(int t=0;t<num_threads;t++) free(tla[t]);
    return 0;
}
