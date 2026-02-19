// Single-thread test to check if row order matters
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
#include "date_utils.h"

template<typename T>
static const T* mmap_col(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat st; fstat(fd, &st);
    void* ptr = mmap(nullptr, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    count = (size_t)st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

int main(int argc, char* argv[]) {
    gendb::init_date_tables();
    std::string gd = argv[1];

    // Load nation dict
    std::vector<std::string> nn(25);
    { std::ifstream f(gd+"/nation/n_name_dict.txt"); std::string l; int i=0; while(std::getline(f,l)&&i<25) nn[i++]=l; }

    // supp_nk
    static int8_t snk[100001];
    { size_t n; auto* sk=mmap_col<int32_t>(gd+"/supplier/s_suppkey.bin",n);
      auto* nk=mmap_col<int32_t>(gd+"/supplier/s_nationkey.bin",n);
      for(size_t i=0;i<n;i++) snk[sk[i]]=(int8_t)nk[i]; }

    // part bitset
    static uint64_t pbits[(2000001+63)/64];
    memset(pbits,0,sizeof(pbits));
    { size_t n,n2; auto* pn=mmap_col<char>(gd+"/part/p_name.bin",n);
      auto* pk=mmap_col<int32_t>(gd+"/part/p_partkey.bin",n2);
      for(size_t i=0;i<n2;i++) if(strstr(pn+i*56,"green")) pbits[pk[i]>>6]|=(1ULL<<(pk[i]&63)); }

    // partsupp map (simple unordered_map for test)
    #include <unordered_map>
    // ... skip - use different approach
    printf("single thread test\n");
    return 0;
}
