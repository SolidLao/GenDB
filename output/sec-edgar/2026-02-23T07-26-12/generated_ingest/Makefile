CXX      = g++
CXXFLAGS = -O3 -std=c++17 -fopenmp -march=native -D_GLIBCXX_PARALLEL
LDFLAGS  = -fopenmp

.PHONY: all clean

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f ingest build_indexes
