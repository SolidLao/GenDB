# Makefile for GenDB TPC-H Query Execution

CXX = g++
CXXFLAGS = -O3 -std=c++17 -Wall -Wextra -mavx2 -march=native
TARGET = gendb_tpch
DATA_DIR = /home/jl4492/GenDB/benchmarks/tpc-h/data/sf1

# Source files
SRCS = main.cpp \
       storage/storage.cpp \
       queries/q1.cpp \
       queries/q3.cpp \
       queries/q6.cpp

# Object files
OBJS = $(SRCS:.cpp=.o)

# Default target
all: $(TARGET)

# Link
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS)

# Compile
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Run with default data directory
run: $(TARGET)
	./$(TARGET) $(DATA_DIR)

# Clean
clean:
	rm -f $(TARGET) $(OBJS)

# Phony targets
.PHONY: all run clean
