CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall
TARGET = tpch_queries
DATA_DIR = /home/jl4492/GenDB/benchmarks/tpc-h/data/sf10

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

# Link target
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS)

# Compile source files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Run the program
run: $(TARGET)
	./$(TARGET) $(DATA_DIR)

# Clean build artifacts
clean:
	rm -f $(OBJS) $(TARGET)

# Phony targets
.PHONY: all run clean
