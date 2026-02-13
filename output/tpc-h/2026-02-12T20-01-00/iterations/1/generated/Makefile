# GenDB Makefile - Arrow/Parquet-based query engine

# Detect pyarrow installation path
PYARROW_DIR := $(shell python3 -c "import pyarrow; print(pyarrow.__path__[0])")

# Arrow include and library paths
ARROW_INCLUDE := -I$(PYARROW_DIR)/include
ARROW_LIBS := -L$(PYARROW_DIR) -larrow -lparquet -Wl,-rpath,$(PYARROW_DIR)

# Compiler settings
CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -march=native -mtune=native $(ARROW_INCLUDE)
LDFLAGS = $(ARROW_LIBS) -lpthread

# Number of parallel threads (detected at runtime, but useful for build parallelism)
NPROC := $(shell nproc)

# Source files
QUERY_SOURCES := $(wildcard queries/*.cpp)
QUERY_OBJECTS := $(QUERY_SOURCES:.cpp=.o)

# Targets
.PHONY: all clean setup_symlinks

all: main

# Create symlinks for Arrow shared libraries if needed
setup_symlinks:
	@echo "Checking Arrow library symlinks..."
	@cd $(PYARROW_DIR) && \
	for f in *.so.*[0-9][0-9][0-9]; do \
		base=$$(echo $$f | sed 's/\.so\..*/\.so/'); \
		if [ ! -e $$base ]; then \
			echo "Creating symlink: $$base -> $$f"; \
			ln -sf $$f $$base; \
		fi; \
	done
	@echo "Symlink check complete."

# Main executable
main: setup_symlinks main.cpp $(QUERY_SOURCES) arrow_helpers.h operators/scan.h operators/hash_join.h operators/hash_agg.h
	@echo "Compiling main executable..."
	$(CXX) $(CXXFLAGS) -o main main.cpp $(QUERY_SOURCES) $(LDFLAGS)
	@echo "Build complete: ./main"

# Query object files (if building separately)
queries/%.o: queries/%.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean build artifacts
clean:
	rm -f main $(QUERY_OBJECTS)
	@echo "Clean complete."

# Test compilation (optional target for testing)
test_compile: setup_symlinks
	@echo "Testing Arrow/Parquet compilation..."
	@echo '#include <arrow/api.h>' > /tmp/test_arrow.cpp
	@echo '#include <parquet/arrow/reader.h>' >> /tmp/test_arrow.cpp
	@echo 'int main() { return 0; }' >> /tmp/test_arrow.cpp
	$(CXX) $(CXXFLAGS) -o /tmp/test_arrow /tmp/test_arrow.cpp $(LDFLAGS)
	@rm -f /tmp/test_arrow /tmp/test_arrow.cpp
	@echo "Arrow/Parquet compilation test passed."

# Help target
help:
	@echo "GenDB Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all             - Build main executable (default)"
	@echo "  setup_symlinks  - Create Arrow library symlinks"
	@echo "  test_compile    - Test Arrow/Parquet compilation"
	@echo "  clean           - Remove build artifacts"
	@echo "  help            - Show this help message"
	@echo ""
	@echo "Configuration:"
	@echo "  PyArrow:   $(PYARROW_DIR)"
	@echo "  Compiler:  $(CXX)"
	@echo "  Flags:     $(CXXFLAGS)"
	@echo "  CPU cores: $(NPROC)"
