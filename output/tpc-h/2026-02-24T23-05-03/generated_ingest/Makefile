CXX      = g++
CXXFLAGS = -O3 -march=native -std=c++17 -fopenmp -Wall -Wextra
TARGETS  = ingest build_indexes

.PHONY: all clean

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f $(TARGETS)
