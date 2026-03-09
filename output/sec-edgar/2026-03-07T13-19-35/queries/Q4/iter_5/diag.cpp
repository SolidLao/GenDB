#include <cstdio>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <atomic>
#include <thread>

struct ZoneMap { int8_t min_uom, max_uom; int32_t min_ddate, max_ddate; };
static_assert(sizeof(ZoneMap) == 12);

static int8_t load_dict_code(const char* path, const char* key) {
    FILE* f = fopen(path, "rb"); if (!f) return -99;
    uint8_t N; fread(&N,1,1,f);
    for (int i=0;i<N;i++) {
        int8_t c; uint8_t sl; fread(&c,1,1,f); fread(&sl,1,1,f);
        char buf[256]={}; fread(buf,1,sl,f); buf[sl]=0;
        if (!strcmp(buf,key)){fclose(f);return c;}
    }
    fclose(f); return -99;
}

int main(int argc, char* argv[]) {
    const char* gdir = argv[1];
    char path[512];
    
    snprintf(path,512,"%s/indexes/uom_codes.bin",gdir);
    int8_t usd_code = load_dict_code(path,"USD");
    snprintf(path,512,"%s/indexes/stmt_codes.bin",gdir);
    int8_t eq_code = load_dict_code(path,"EQ");
    fprintf(stderr,"usd_code=%d eq_code=%d\n",(int)usd_code,(int)eq_code);
    
    // Load zone maps
    snprintf(path,512,"%s/indexes/num_zone_maps.bin",gdir);
    int fd = open(path,O_RDONLY);
    uint32_t n_blocks; read(fd,&n_blocks,sizeof(uint32_t));
    std::vector<ZoneMap> zm(n_blocks);
    read(fd,zm.data(),n_blocks*sizeof(ZoneMap));
    close(fd);
    fprintf(stderr,"n_blocks=%u\n",n_blocks);
    
    // Count passing blocks
    int passing=0;
    for (uint32_t b=0;b<n_blocks;b++) {
        if (!(zm[b].min_uom > usd_code || zm[b].max_uom < usd_code))
            passing++;
    }
    fprintf(stderr,"passing zone map=%d/%u (%.1f%%)\n",passing,n_blocks,100.0*passing/n_blocks);
    
    // Print first 10 zone maps
    for(int i=0;i<10&&i<(int)n_blocks;i++)
        fprintf(stderr,"zm[%d]: min_uom=%d max_uom=%d\n",i,(int)zm[i].min_uom,(int)zm[i].max_uom);
    fprintf(stderr,"zm[last]: min_uom=%d max_uom=%d\n",(int)zm[n_blocks-1].min_uom,(int)zm[n_blocks-1].max_uom);
    
    // Also load sub/sic to count qualifying adsh
    snprintf(path,512,"%s/sub/sic.bin",gdir);
    fd=open(path,O_RDONLY); struct stat st; fstat(fd,&st);
    const int16_t* sic=(const int16_t*)mmap(nullptr,st.st_size,PROT_READ,MAP_PRIVATE,fd,0);
    close(fd);
    size_t sub_n=st.st_size/2;
    int qual_adsh=0;
    for(size_t i=0;i<sub_n;i++) if(sic[i]>=4000&&sic[i]<=4999) qual_adsh++;
    fprintf(stderr,"qualifying adsh codes=%d out of %zu\n",qual_adsh,sub_n);
    return 0;
}
