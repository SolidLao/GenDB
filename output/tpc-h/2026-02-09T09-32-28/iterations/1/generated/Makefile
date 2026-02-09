CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall

SRCS = main.cpp storage/storage.cpp queries/q1.cpp queries/q3.cpp queries/q6.cpp
OBJS = $(SRCS:.cpp=.o)

all: main

main: $(OBJS)
	$(CXX) $(CXXFLAGS) -o main $(OBJS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

run: main
	./main /home/jl4492/GenDB/benchmarks/tpc-h/data/sf1

clean:
	rm -f main $(OBJS)

.PHONY: all run clean
