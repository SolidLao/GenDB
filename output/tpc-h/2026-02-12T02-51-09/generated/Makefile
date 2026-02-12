CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread -march=native
LDFLAGS = -lpthread

# Directories
SRC_DIR = .
STORAGE_DIR = storage
QUERIES_DIR = queries

# Source files
STORAGE_SRCS = $(STORAGE_DIR)/storage.cpp
QUERY_SRCS = $(QUERIES_DIR)/q1.cpp $(QUERIES_DIR)/q3.cpp $(QUERIES_DIR)/q6.cpp

# Object files
STORAGE_OBJS = $(STORAGE_SRCS:.cpp=.o)
QUERY_OBJS = $(QUERY_SRCS:.cpp=.o)

# Executables
INGEST_TARGET = ingest
MAIN_TARGET = main

# Default target
all: $(INGEST_TARGET) $(MAIN_TARGET)

# Ingest program
$(INGEST_TARGET): ingest.o $(STORAGE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Main query program
$(MAIN_TARGET): main.o $(STORAGE_OBJS) $(QUERY_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Compile source files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Individual targets
ingest: $(INGEST_TARGET)

main: $(MAIN_TARGET)

# Run targets
run-ingest: $(INGEST_TARGET)
	./$(INGEST_TARGET) $(DATA_DIR) $(GENDB_DIR)

run-main: $(MAIN_TARGET)
	./$(MAIN_TARGET) $(GENDB_DIR)

# Clean
clean:
	rm -f $(INGEST_TARGET) $(MAIN_TARGET) *.o $(STORAGE_DIR)/*.o $(QUERIES_DIR)/*.o

.PHONY: all clean ingest main run-ingest run-main
