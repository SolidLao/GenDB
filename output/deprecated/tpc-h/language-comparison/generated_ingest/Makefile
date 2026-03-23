CXX = g++
CXXFLAGS = -std=c++17 -O3 -march=native -pthread -Wall -Wextra

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f ingest build_indexes

.PHONY: all clean
