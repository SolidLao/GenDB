CXX = g++
CXXFLAGS = -O3 -march=native -flto -std=c++17 -fopenmp -DGENDB_PROFILE -DGENDB_LIBRARY \
           -I/home/jl4492/GenDB/src/gendb/utils \
           -funroll-loops -ffast-math -ftree-vectorize \
           -falign-functions=32 -falign-loops=32 \
           -fno-math-errno -ffinite-math-only \
           -lpthread

TARGETS = q1 q3 q6 q9 q18

all: $(TARGETS)

q1: q1.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

q3: q3.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

q6: q6.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

q9: q9.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

q18: q18.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f $(TARGETS)

.PHONY: all clean
