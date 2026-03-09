CXX      := g++
CXXFLAGS := -std=c++17 -O3 -march=native -pthread -Wall -Wextra
LDFLAGS  := -pthread

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o ingest ingest.cpp $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o build_indexes build_indexes.cpp $(LDFLAGS)

clean:
	rm -f ingest build_indexes

.PHONY: all clean
