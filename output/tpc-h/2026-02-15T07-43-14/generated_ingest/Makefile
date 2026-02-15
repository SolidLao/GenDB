CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -Wextra -pthread
LDFLAGS = -lpthread

# Targets
INGEST = ingest
BUILD_INDEXES = build_indexes

# Source files
INGEST_SRC = ingest.cpp
BUILD_INDEXES_SRC = build_indexes.cpp

# Object files
INGEST_OBJ = $(INGEST_SRC:.cpp=.o)
BUILD_INDEXES_OBJ = $(BUILD_INDEXES_SRC:.cpp=.o)

# Default target
all: $(INGEST) $(BUILD_INDEXES)

# Ingest binary
$(INGEST): $(INGEST_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Build indexes binary
$(BUILD_INDEXES): $(BUILD_INDEXES_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Object file compilation
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

# Clean up
clean:
	rm -f $(INGEST_OBJ) $(BUILD_INDEXES_OBJ) $(INGEST) $(BUILD_INDEXES)

# Phony targets
.PHONY: all clean
