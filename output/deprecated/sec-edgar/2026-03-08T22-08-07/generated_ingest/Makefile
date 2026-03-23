CXX := g++
CXXFLAGS := -O3 -std=c++17 -march=native -pthread -Wall -Wextra

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ ingest.cpp

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ build_indexes.cpp

clean:
	rm -f ingest build_indexes

.PHONY: all clean
