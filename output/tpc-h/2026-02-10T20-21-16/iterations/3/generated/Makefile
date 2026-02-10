CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -mavx2 -lpthread
INCLUDES = -I.

# Source files
STORAGE_SRC = storage/storage.cpp
QUERY_SRC = queries/q1.cpp queries/q3.cpp queries/q6.cpp

INGEST_SRC = ingest.cpp $(STORAGE_SRC)
MAIN_SRC = main.cpp $(STORAGE_SRC) $(QUERY_SRC)

# Output binaries
INGEST_BIN = ingest
MAIN_BIN = main

# Default target: build both programs
all: $(INGEST_BIN) $(MAIN_BIN)

# Build ingest program
$(INGEST_BIN): $(INGEST_SRC)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $(INGEST_BIN) $(INGEST_SRC)

# Build main program
$(MAIN_BIN): $(MAIN_SRC)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $(MAIN_BIN) $(MAIN_SRC)

# Run ingestion (requires data_dir and gendb_dir arguments)
run-ingest:
	./$(INGEST_BIN) $(DATA_DIR) $(GENDB_DIR)

# Run queries (requires gendb_dir argument)
run-main:
	./$(MAIN_BIN) $(GENDB_DIR)

# Clean build artifacts
clean:
	rm -f $(INGEST_BIN) $(MAIN_BIN)

.PHONY: all clean run-ingest run-main
