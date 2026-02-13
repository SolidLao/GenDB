CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -lpthread

QUERY_SRCS = queries/q1.cpp queries/q3.cpp queries/q6.cpp

all: main

main: main.cpp $(QUERY_SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f main

.PHONY: all clean
