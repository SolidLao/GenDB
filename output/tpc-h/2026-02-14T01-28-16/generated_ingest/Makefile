CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread
TARGETS = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o ingest ingest.cpp

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o build_indexes build_indexes.cpp

clean:
	rm -f $(TARGETS)

.PHONY: all clean
