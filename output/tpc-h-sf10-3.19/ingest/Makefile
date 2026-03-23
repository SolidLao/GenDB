CXX = g++
CXXFLAGS = -std=c++17 -O3 -march=native -fopenmp -Wall
LDFLAGS = -fopenmp -lpthread

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f ingest build_indexes

.PHONY: all clean
