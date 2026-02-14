CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -lpthread

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o ingest ingest.cpp

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o build_indexes build_indexes.cpp

clean:
	rm -f ingest build_indexes

.PHONY: all clean
