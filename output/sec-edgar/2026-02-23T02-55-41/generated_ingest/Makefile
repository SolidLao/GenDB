CXX      = g++
CXXFLAGS = -O3 -std=c++17 -fopenmp -march=native -Wall
LDFLAGS  =

.PHONY: all clean

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o ingest ingest.cpp $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o build_indexes build_indexes.cpp $(LDFLAGS)

clean:
	rm -f ingest build_indexes
