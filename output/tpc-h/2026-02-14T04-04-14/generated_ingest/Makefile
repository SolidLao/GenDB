CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread
LDFLAGS = -lpthread

TARGETS = ingest build_indexes
SRCS = ingest.cpp build_indexes.cpp
OBJS = ingest.o build_indexes.o

all: $(TARGETS)

ingest: ingest.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

build_indexes: build_indexes.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

ingest.o: ingest.cpp
	$(CXX) $(CXXFLAGS) -c $<

build_indexes.o: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -c $<

clean:
	rm -f $(TARGETS) $(OBJS)

.PHONY: all clean
