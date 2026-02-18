CXX = g++
CXXFLAGS = -O3 -march=native -flto -std=c++17 -Wall -lpthread -fopenmp -DGENDB_LIBRARY

QUERY_SRCS = queries/q3.cpp queries/q5.cpp queries/q6.cpp queries/q7.cpp queries/q9.cpp queries/q10.cpp queries/q12.cpp queries/q13.cpp queries/q16.cpp queries/q18.cpp queries/q20.cpp queries/q21.cpp queries/q22.cpp

all: main

main: main.cpp $(QUERY_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f main

.PHONY: all clean
