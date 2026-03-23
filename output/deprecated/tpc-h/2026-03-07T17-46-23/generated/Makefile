CXX = g++
CXXFLAGS = -O3 -march=native -flto -std=c++17 -Wall -lpthread -fopenmp -DGENDB_LIBRARY -I/home/jl4492/GenDB/src/gendb/utils

QUERY_SRCS = queries/q1.cpp queries/q3.cpp queries/q6.cpp queries/q9.cpp queries/q18.cpp

all: main

main: main.cpp $(QUERY_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f main

.PHONY: all clean
