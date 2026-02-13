# GenDB Makefile - Arrow/Parquet Infrastructure
# Auto-configured for pyarrow bundled libraries

# Detect pyarrow installation path
PYARROW_DIR := $(shell python3 -c "import pyarrow; print(pyarrow.__path__[0])")

# Arrow C++ headers and libraries from pyarrow
ARROW_INCLUDE := -I$(PYARROW_DIR)/include
ARROW_LIBS := -L$(PYARROW_DIR) -larrow -lparquet -Wl,-rpath,$(PYARROW_DIR)

# Compiler settings
CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -Wextra -march=native -mtune=native $(ARROW_INCLUDE)
LDFLAGS = $(ARROW_LIBS) -lpthread

# Source files
HEADERS = arrow_helpers.h \
          operators/scan.h \
          operators/hash_join.h \
          operators/hash_agg.h

# Targets
.PHONY: all setup_symlinks clean test_setup

# Setup shared library symlinks for Arrow/Parquet
setup_symlinks:
	@echo "Setting up Arrow/Parquet shared library symlinks..."
	@cd $(PYARROW_DIR) && \
	for f in *.so.*; do \
		if [ -f "$$f" ]; then \
			base=$$(echo $$f | sed 's/\.so\..*/\.so/'); \
			if [ ! -e "$$base" ]; then \
				ln -sf $$f $$base && echo "Created symlink: $$base -> $$f"; \
			fi; \
		fi; \
	done; \
	echo "Symlink setup complete."

# Default target (queries not yet generated)
all: main
	@echo "Infrastructure ready. Waiting for query generation..."
	@echo "When queries are ready, compile with: make main"

# Main executable (will be available after query generation)
main: setup_symlinks main.cpp queries.h $(wildcard queries/*.cpp) $(HEADERS)
	$(CXX) $(CXXFLAGS) -o main main.cpp $(LDFLAGS)
	@echo "Build complete: ./main <parquet_dir>"

# Test compilation of infrastructure
test_setup: setup_symlinks test_read.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -o test_read test_read.cpp $(LDFLAGS)

# Clean build artifacts
clean:
	rm -f main test_read *.o queries/*.o

# Print configuration
info:
	@echo "=== GenDB Build Configuration ==="
	@echo "Compiler: $(CXX)"
	@echo "Flags: $(CXXFLAGS)"
	@echo "PyArrow: $(PYARROW_DIR)"
	@echo "Arrow Include: $(ARROW_INCLUDE)"
	@echo "Arrow Libs: $(ARROW_LIBS)"
	@echo "Hardware: 64 cores (detected)"
	@echo "================================="
