CXX ?= g++
CXXFLAGS ?= -O3 -march=native -std=c++17 -Wall -Wextra -Wpedantic -pthread

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f ingest build_indexes

.PHONY: all clean
