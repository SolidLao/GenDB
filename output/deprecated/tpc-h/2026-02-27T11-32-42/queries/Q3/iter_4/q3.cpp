// Q3 iter_4 — Shipping Priority
// Key optimizations:
//   1. Parallel orders scan (64 threads) distributes page faults
//   2. Compact IdxMap (75% load, 16MB) + bitset (7.5MB) + atomic revenue (11MB) < 44MB L3
//   3. TOP-K heap selection (k=10) instead of sort(1.38M) → eliminates 180ms bottleneck
//   4. Position-based lineitem partitioning

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>
#include <algorithm>
#include <atomic>
#include <queue>
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

// Compact 75%-load hash map: okey → int32_t index
struct IdxMap {
    static constexpr int32_t EMPTY = INT32_MIN;
    int cap;
    vector<int32_t> keys, idxs;
    void init(int n) {
        cap = 1;
        while (cap * 3 < n * 4) cap <<= 1;
        keys.assign(cap, EMPTY); idxs.assign(cap, -1);
    }
    void insert(int32_t k, int32_t idx) {
        int h = (int)((uint32_t)(k*2654435761u))&(cap-1);
        while(keys[h]!=EMPTY) h=(h+1)&(cap-1);
        keys[h]=k; idxs[h]=idx;
    }
    int32_t find(int32_t k) const {
        int h = (int)((uint32_t)(k*2654435761u))&(cap-1);
        while(keys[h]!=EMPTY&&keys[h]!=k) h=(h+1)&(cap-1);
        return keys[h]==k?idxs[h]:-1;
    }
};

struct Row { int32_t orderkey, orderdate, shippriority; double revenue; };

// Comparator: revenue DESC, orderdate ASC
struct RowCmp {
    bool operator()(const Row& a, const Row& b) const {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.orderdate < b.orderdate;
    }
};

int main(int argc, char** argv) {
    if (argc<3){fprintf(stderr,"Usage: q3 <gendb_dir> <results_dir>\n");return 1;}
    const char* gendb_dir=argv[1]; const char* results_dir=argv[2];
    gendb::init_date_tables();
    double output_ms=0;
    {
        GENDB_PHASE("total");

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
        int NT=min((int)thread::hardware_concurrency(),64);

        // ── Step 1: Build is_building ─────────────────────────────────────────
        int32_t max_custkey=0;
        for(size_t i=0;i<Nc;++i) if(cust_custkey[i]>max_custkey) max_custkey=cust_custkey[i];
        vector<uint8_t> is_building(max_custkey+1,0);
        for(size_t i=0;i<Nc;++i) if(cust_isbld[i]) is_building[cust_custkey[i]]=1;

        // ── Step 2: Parallel orders scan ──────────────────────────────────────
        struct QualOrd{int32_t okey,odat,shpr;};
        vector<vector<QualOrd>> local_qual(NT);
        {
            vector<thread> thr;
            size_t chunk=(No+NT-1)/NT;
            for(int t=0;t<NT;++t){
                size_t s=t*chunk,e=min(s+chunk,No);
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

        // ── Step 3: Merge + build IdxMap + bitset ─────────────────────────────
        int qual_cnt=0; for(auto& lq:local_qual) qual_cnt+=lq.size();
        vector<QualOrd> qual_orders; qual_orders.reserve(qual_cnt);
        for(auto& lq:local_qual) for(auto& q:lq) qual_orders.push_back(q);

        IdxMap imap; imap.init(qual_cnt+16);
        for(int i=0;i<qual_cnt;++i) imap.insert(qual_orders[i].okey,i);

        int32_t max_okey=0;
        for(int i=0;i<imap.cap;++i)
            if(imap.keys[i]!=IdxMap::EMPTY&&imap.keys[i]>max_okey) max_okey=imap.keys[i];

        size_t bsz=(max_okey+64)/64;
        vector<uint64_t> qual_bits(bsz,0);
        for(int i=0;i<imap.cap;++i){
            if(imap.keys[i]!=IdxMap::EMPTY){int32_t ok=imap.keys[i];qual_bits[ok>>6]|=(1ULL<<(ok&63));}
        }
        auto btest=[&](int32_t ok)->bool{
            if((uint32_t)ok>(uint32_t)max_okey) return false;
            return (qual_bits[ok>>6]>>(ok&63))&1;
        };

        // ── Step 4: Atomic revenue array ──────────────────────────────────────
        vector<atomic<double>> revenues(qual_cnt);
        for(int i=0;i<qual_cnt;++i) revenues[i].store(0.0,memory_order_relaxed);

        // ── Step 5: Binary search for lineitem cutoff ─────────────────────────
        size_t li_start;
        {size_t l=0,r=Nl; while(l<r){size_t m=(l+r)/2;if(li_shd[m]<=cut_date)l=m+1;else r=m;} li_start=l;}

        // ── Step 6: Parallel lineitem scan ────────────────────────────────────
        size_t li_range=Nl-li_start;
        {
            vector<thread> thr;
            for(int t=0;t<NT;++t){
                size_t s=li_start+(size_t)t*li_range/NT;
                size_t e=li_start+(size_t)(t+1)*li_range/NT;
                thr.emplace_back([&,s,e](){
                    for(size_t i=s;i<e;++i){
                        int32_t ok=li_okey[i];
                        if(!btest(ok)) continue;
                        int32_t idx=imap.find(ok);
                        if(idx<0) continue;
                        revenues[idx].fetch_add(li_ep[i]*(1.0-li_disc[i]*0.01),memory_order_relaxed);
                    }
                });
            }
            for(auto& th:thr) th.join();
        }

        // ── Step 7: Top-10 selection using min-heap (O(n) not O(n log n)) ─────
        // min-heap: weakest element on top → easy to check if new element beats it
        auto worse_than = [](const Row& a, const Row& b) {
            // "worse" = would rank lower = smaller revenue, or same revenue but later date
            if (a.revenue != b.revenue) return a.revenue < b.revenue;
            return a.orderdate > b.orderdate;
        };

        vector<Row> heap; heap.reserve(11);
        for(int i=0;i<qual_cnt;++i){
            double rev=revenues[i].load(memory_order_relaxed);
            if(rev<=0.0) continue;
            Row r={qual_orders[i].okey,qual_orders[i].odat,qual_orders[i].shpr,rev};
            if((int)heap.size()<10){
                heap.push_back(r);
                push_heap(heap.begin(),heap.end(),worse_than); // min-heap
            } else if(!worse_than(r,heap[0])){
                pop_heap(heap.begin(),heap.end(),worse_than);
                heap.back()=r;
                push_heap(heap.begin(),heap.end(),worse_than);
            }
        }
        // Sort final heap by revenue DESC, orderdate ASC
        sort(heap.begin(),heap.end(),[](const Row& a,const Row& b){
            if(a.revenue!=b.revenue) return a.revenue>b.revenue;
            return a.orderdate<b.orderdate;
        });

        // ── Output ────────────────────────────────────────────────────────────
        {
            GENDB_PHASE_MS("output",output_ms);
            char outpath[512]; snprintf(outpath,sizeof(outpath),"%s/Q3.csv",results_dir);
            FILE* out=fopen(outpath,"w");
            fprintf(out,"l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char datebuf[16];
            for(auto& row:heap){
                gendb::epoch_days_to_date_str(row.orderdate,datebuf);
                fprintf(out,"%d,%.4f,%s,%d\n",row.orderkey,row.revenue,datebuf,row.shippriority);
            }
            fclose(out);
        }
    }
    return 0;
}
