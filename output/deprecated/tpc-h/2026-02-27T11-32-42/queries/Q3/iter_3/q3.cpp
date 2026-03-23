// Q3 iter_3 — Shipping Priority
// Key optimizations over iter_2:
//   1. Parallel orders scan (64 threads) distributes page faults (34ms→2ms)
//   2. Position-based lineitem partitioning (no binary search except one for cutoff)
//   3. Bitset prefilter + small per-thread RevMaps

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "timing_utils.h"
#include "date_utils.h"
using namespace std;

template<typename T>
static T* mmap_col(const char* dir, const char* name, size_t& count) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    count = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    madvise(p, st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return (T*)p;
}

struct OrderInfo { int32_t orderdate, shippriority; };

// Global qualifying-order hash map: okey → OrderInfo
struct OrderMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    int cap;
    vector<int32_t>   keys;
    vector<OrderInfo> vals;
    void init(int n) {
        cap=1; while(cap<n*2) cap<<=1;
        keys.assign(cap,EMPTY); vals.resize(cap);
    }
    void insert(int32_t k, OrderInfo v) {
        int h=(int)((uint32_t)(k*2654435761u))&(cap-1);
        while(keys[h]!=EMPTY) h=(h+1)&(cap-1);
        keys[h]=k; vals[h]=v;
    }
    int find(int32_t k) const {
        int h=(int)((uint32_t)(k*2654435761u))&(cap-1);
        while(keys[h]!=EMPTY&&keys[h]!=k) h=(h+1)&(cap-1);
        return keys[h]==k?h:-1;
    }
};

// Thread-local revenue accumulator: open addressing, small
struct RevMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    int cap;
    vector<int32_t> keys;
    vector<double>  vals;
    void init(int n) {
        cap=1; while(cap<n*2) cap<<=1;
        keys.assign(cap,EMPTY); vals.assign(cap,0.0);
    }
    void add(int32_t k, double v) {
        int h=(int)((uint32_t)(k*2654435761u))&(cap-1);
        while(keys[h]!=EMPTY&&keys[h]!=k) h=(h+1)&(cap-1);
        if(keys[h]==EMPTY) keys[h]=k;
        vals[h]+=v;
    }
    void for_each(auto fn) const {
        for(int i=0;i<cap;++i) if(keys[i]!=EMPTY) fn(keys[i],vals[i]);
    }
};

int main(int argc, char** argv) {
    if (argc<3){fprintf(stderr,"Usage: q3 <gendb_dir> <results_dir>\n");return 1;}
    const char* gendb_dir=argv[1];
    const char* results_dir=argv[2];
    gendb::init_date_tables();
    double output_ms=0;
    {
        GENDB_PHASE("total");

        // ── Load columns ──────────────────────────────────────────────────────
        size_t Nc,tmp;
        int32_t* cust_custkey=mmap_col<int32_t>(gendb_dir,"cust_custkey.bin",Nc);
        uint8_t* cust_isbld  =mmap_col<uint8_t>(gendb_dir,"cust_isbuilding.bin",tmp);

        size_t No;
        int32_t* ord_okey=mmap_col<int32_t>(gendb_dir,"ord_orderkey.bin",No);
        int32_t* ord_ckey=mmap_col<int32_t>(gendb_dir,"ord_custkey.bin",tmp);
        int32_t* ord_odat=mmap_col<int32_t>(gendb_dir,"ord_orderdate.bin",tmp);
        int32_t* ord_shpr=mmap_col<int32_t>(gendb_dir,"ord_shippriority.bin",tmp);

        size_t Nl;
        int32_t* li_shd =mmap_col<int32_t>(gendb_dir,"li_shipdate.bin",Nl);
        int32_t* li_okey=mmap_col<int32_t>(gendb_dir,"li_orderkey.bin",tmp);
        double*  li_ep  =mmap_col<double> (gendb_dir,"li_extprice.bin",tmp);
        int8_t*  li_disc=mmap_col<int8_t> (gendb_dir,"li_discount.bin",tmp);

        int32_t cut_date=gendb::date_str_to_epoch_days("1995-03-15");

        // ── Step 1: Build is_building array ───────────────────────────────────
        int32_t max_custkey=0;
        for(size_t i=0;i<Nc;++i) if(cust_custkey[i]>max_custkey) max_custkey=cust_custkey[i];
        vector<uint8_t> is_building(max_custkey+1,0);
        for(size_t i=0;i<Nc;++i) if(cust_isbld[i]) is_building[cust_custkey[i]]=1;

        int NT=min((int)thread::hardware_concurrency(),64);

        // ── Step 2: Parallel orders scan → thread-local qualifying vectors ────
        // Distributes 60K page faults across 64 threads: wall time ~2ms vs 34ms
        vector<vector<tuple<int32_t,int32_t,int32_t>>> local_qual(NT); // {okey,odat,shpr}
        {
            vector<thread> thr;
            size_t chunk=(No+NT-1)/NT;
            for(int t=0;t<NT;++t){
                size_t s=t*chunk, e=min(s+chunk,No);
                thr.emplace_back([&,t,s,e](){
                    auto& lq=local_qual[t];
                    for(size_t i=s;i<e;++i){
                        int32_t ck=ord_ckey[i];
                        if(ord_odat[i]<cut_date&&(uint32_t)ck<=(uint32_t)max_custkey&&is_building[ck])
                            lq.push_back({ord_okey[i],ord_odat[i],ord_shpr[i]});
                    }
                });
            }
            for(auto& th:thr) th.join();
        }

        // ── Step 3: Merge + build OrderMap ────────────────────────────────────
        int qual_cnt=0;
        for(auto& lq:local_qual) qual_cnt+=lq.size();

        OrderMap omap; omap.init(qual_cnt+16);
        for(auto& lq:local_qual)
            for(auto& [ok,od,sp]:lq)
                omap.insert(ok,{od,sp});

        // ── Step 4: Build qualifying-orderkey bitset (7.5MB, L3-resident) ─────
        int32_t max_okey=0;
        for(int i=0;i<omap.cap;++i)
            if(omap.keys[i]!=OrderMap::EMPTY&&omap.keys[i]>max_okey)
                max_okey=omap.keys[i];

        size_t bitset_words=(max_okey+64)/64;
        vector<uint64_t> qual_bits(bitset_words,0);
        for(int i=0;i<omap.cap;++i){
            if(omap.keys[i]!=OrderMap::EMPTY){
                int32_t ok=omap.keys[i];
                qual_bits[ok>>6]|=(1ULL<<(ok&63));
            }
        }
        auto bit_test=[&](int32_t ok)->bool{
            if((uint32_t)ok>(uint32_t)max_okey) return false;
            return (qual_bits[ok>>6]>>(ok&63))&1;
        };

        // ── Step 5: Binary search for lineitem cutoff (one search, fast) ──────
        size_t li_start;
        {
            size_t l=0,r=Nl;
            while(l<r){size_t m=(l+r)/2;if(li_shd[m]<=cut_date)l=m+1;else r=m;}
            li_start=l;
        }

        // ── Step 6: Parallel lineitem scan (position-based, no binary search) ─
        size_t li_range=Nl-li_start;
        // Estimate unique okeys per thread: qual_cnt/NT
        int local_cap=max(512, qual_cnt/NT*4);
        vector<RevMap> local_rev(NT);
        for(int t=0;t<NT;++t) local_rev[t].init(local_cap);

        {
            vector<thread> thr;
            for(int t=0;t<NT;++t){
                size_t s=li_start+(size_t)t*li_range/NT;
                size_t e=li_start+(size_t)(t+1)*li_range/NT;
                thr.emplace_back([&,t,s,e](){
                    RevMap& rm=local_rev[t];
                    for(size_t i=s;i<e;++i){
                        int32_t ok=li_okey[i];
                        if(!bit_test(ok)) continue;
                        double contrib=li_ep[i]*(1.0-li_disc[i]*0.01);
                        rm.add(ok,contrib);
                    }
                });
            }
            for(auto& th:thr) th.join();
        }

        // ── Step 7: Merge RevMaps → result rows ───────────────────────────────
        RevMap merged; merged.init(qual_cnt*2+16);
        for(int t=0;t<NT;++t)
            local_rev[t].for_each([&](int32_t k,double v){merged.add(k,v);});

        struct Row{int32_t orderkey,orderdate,shippriority; double revenue;};
        vector<Row> rows; rows.reserve(qual_cnt);
        merged.for_each([&](int32_t ok,double rev){
            int slot=omap.find(ok);
            if(slot>=0) rows.push_back({ok,omap.vals[slot].orderdate,
                                        omap.vals[slot].shippriority,rev});
        });

        sort(rows.begin(),rows.end(),[](const Row& a,const Row& b){
            if(a.revenue!=b.revenue) return a.revenue>b.revenue;
            return a.orderdate<b.orderdate;
        });
        if((int)rows.size()>10) rows.resize(10);

        // ── Output ────────────────────────────────────────────────────────────
        {
            GENDB_PHASE_MS("output",output_ms);
            char outpath[512];
            snprintf(outpath,sizeof(outpath),"%s/Q3.csv",results_dir);
            FILE* out=fopen(outpath,"w");
            fprintf(out,"l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char datebuf[16];
            for(auto& row:rows){
                gendb::epoch_days_to_date_str(row.orderdate,datebuf);
                fprintf(out,"%d,%.4f,%s,%d\n",row.orderkey,row.revenue,datebuf,row.shippriority);
            }
            fclose(out);
        }
    }
    return 0;
}
