CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread
LDFLAGS = -lpthread

TARGETS = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) ingest.cpp -o ingest $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) build_indexes.cpp -o build_indexes $(LDFLAGS)

clean:
	rm -f $(TARGETS)

.PHONY: all clean
