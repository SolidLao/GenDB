CXX      = g++
CXXFLAGS_INGEST = -O2 -std=c++17 -Wall -lpthread
CXXFLAGS_IDX    = -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp

.PHONY: all clean

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS_INGEST) -o $@ $<

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS_IDX) -o $@ $<

clean:
	rm -f ingest build_indexes
