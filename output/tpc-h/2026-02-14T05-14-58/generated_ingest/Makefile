CXX = g++
CXXFLAGS = -O3 -std=c++17 -Wall -Wextra -march=native
LDFLAGS = -lpthread -lstdc++fs

TARGETS = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS)

.PHONY: all clean
