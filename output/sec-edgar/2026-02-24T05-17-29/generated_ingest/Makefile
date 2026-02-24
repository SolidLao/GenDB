CXX      = g++
CXXFLAGS = -std=c++17 -O3 -march=native -fopenmp -I/home/jl4492/GenDB/src
LDFLAGS  = -fopenmp

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o ingest ingest.cpp $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o build_indexes build_indexes.cpp $(LDFLAGS)

clean:
	rm -f ingest build_indexes

.PHONY: all clean
