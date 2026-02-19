// Single-thread Q9 for comparison
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <unordered_map>
#include "date_utils.h"

template<typename T>
static const T* mc(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat st; fstat(fd, &st);
    void* ptr = mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd); count = (size_t)st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

int main(int argc, char* argv[]) {
    gendb::init_date_tables();
    std::string gd = argv[1];
    std::string rd = argc > 2 ? argv[2] : ".";

    std::vector<std::string> nn(25);
    { std::ifstream f(gd+"/nation/n_name_dict.txt"); std::string l; int i=0; while(std::getline(f,l)&&i<25) nn[i++]=l; }

    static int8_t snk[100001]; memset(snk,0,sizeof(snk));
    { size_t n; auto* sk=mc<int32_t>(gd+"/supplier/s_suppkey.bin",n);
      auto* nk=mc<int32_t>(gd+"/supplier/s_nationkey.bin",n);
      for(size_t i=0;i<n;i++) snk[sk[i]]=(int8_t)nk[i]; }

    static uint64_t pbits[(2000001+63)/64]; memset(pbits,0,sizeof(pbits));
    { size_t n,n2; auto* pn=mc<char>(gd+"/part/p_name.bin",n);
      auto* pk=mc<int32_t>(gd+"/part/p_partkey.bin",n2);
      for(size_t i=0;i<n2;i++) if(strstr(pn+i*56,"green")) pbits[pk[i]>>6]|=(1ULL<<(pk[i]&63)); }

    std::unordered_map<uint64_t,double> psmap; psmap.reserve(10000000);
    { size_t n; auto* ppk=mc<int32_t>(gd+"/partsupp/ps_partkey.bin",n);
      auto* psk=mc<int32_t>(gd+"/partsupp/ps_suppkey.bin",n);
      auto* psc=mc<double>(gd+"/partsupp/ps_supplycost.bin",n);
      for(size_t i=0;i<n;i++) psmap[((uint64_t)(uint32_t)ppk[i]<<32)|(uint32_t)psk[i]]=psc[i]; }

    // load orders index
    struct Bucket { int32_t key; uint32_t pos; };
    std::string oidx_path = gd+"/indexes/orders_orderkey_hash.bin";
    int fd2=open(oidx_path.c_str(),O_RDONLY); struct stat st2; fstat(fd2,&st2);
    const char* raw=(const char*)mmap(nullptr,(size_t)st2.st_size,PROT_READ,MAP_PRIVATE,fd2,0); close(fd2);
    uint32_t ocap=*(const uint32_t*)raw;
    const Bucket* obuckets=(const Bucket*)(raw+sizeof(uint32_t));
    auto olookup=[&](int32_t key) -> uint32_t {
        uint32_t idx=(uint32_t)(((uint64_t)(uint32_t)key*0x9E3779B97F4A7C15ULL>>32)&(uint64_t)(ocap-1));
        while(obuckets[idx].key!=INT32_MIN){if(obuckets[idx].key==key)return obuckets[idx].pos;idx=(idx+1)&(ocap-1);}
        return UINT32_MAX;
    };
    size_t no; auto* oodate=mc<int32_t>(gd+"/orders/o_orderdate.bin",no);

    size_t n_li,nd;
    auto* lpk=mc<int32_t>(gd+"/lineitem/l_partkey.bin",n_li);
    auto* lsk=mc<int32_t>(gd+"/lineitem/l_suppkey.bin",n_li);
    auto* lok=mc<int32_t>(gd+"/lineitem/l_orderkey.bin",n_li);
    auto* lep=mc<double>(gd+"/lineitem/l_extendedprice.bin",nd);
    auto* ld =mc<double>(gd+"/lineitem/l_discount.bin",nd);
    auto* lq =mc<double>(gd+"/lineitem/l_quantity.bin",nd);

    // single-thread agg
    double agg[25][8]; memset(agg,0,sizeof(agg));
    for(size_t i=0;i<n_li;i++){
        uint32_t pk=(uint32_t)lpk[i];
        if(!((pbits[pk>>6]>>(pk&63))&1)) continue;
        int32_t sk=lsk[i];
        uint64_t psk=((uint64_t)pk<<32)|(uint32_t)sk;
        auto it=psmap.find(psk); if(it==psmap.end()) continue;
        uint32_t opos=olookup(lok[i]); if(opos==UINT32_MAX) continue;
        int yr=gendb::extract_year(oodate[opos]);
        if(yr<1992||yr>1998) continue;
        double amt=lep[i]*(1.0-ld[i])-it->second*lq[i];
        agg[(int)snk[sk]][yr-1992]+=amt;
    }

    struct Row{const char* nat; int yr; double sp;};
    std::vector<Row> res;
    for(int n=0;n<25;n++) for(int y=0;y<7;y++) if(agg[n][y]!=0) res.push_back({nn[n].c_str(),1992+y,agg[n][y]});
    std::sort(res.begin(),res.end(),[](const Row&a,const Row&b){ int c=strcmp(a.nat,b.nat); return c?c<0:a.yr>b.yr; });

    FILE* f=fopen((rd+"/Q9_single.csv").c_str(),"w");
    fprintf(f,"nation,o_year,sum_profit\n");
    for(auto& r:res) fprintf(f,"%s,%d,%.2f\n",r.nat,r.yr,r.sp);
    fclose(f);
    printf("Done\n");
    return 0;
}
