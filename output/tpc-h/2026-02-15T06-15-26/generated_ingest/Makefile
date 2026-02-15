CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -Wextra -pthread
LDFLAGS = -lpthread

TARGETS = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS) *.o

.PHONY: all clean
