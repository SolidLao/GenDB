PYARROW_DIR := $(shell python3 -c "import pyarrow; print(pyarrow.__path__[0])")
ARROW_INCLUDE := -I$(PYARROW_DIR)/include
ARROW_LIBS := -L$(PYARROW_DIR) -larrow -lparquet -Wl,-rpath,$(PYARROW_DIR)

CXX = g++
CXXFLAGS = -O3 -std=c++17 -Wall -march=native -mavx2 -mfma -funroll-loops -ftree-vectorize -ffast-math $(ARROW_INCLUDE) -I.
LDFLAGS = $(ARROW_LIBS) -lpthread

QUERY_SRCS := $(wildcard queries/*.cpp)
QUERY_OBJS := $(QUERY_SRCS:.cpp=.o)

.PHONY: all clean setup_symlinks

all: main

setup_symlinks:
	@cd $(PYARROW_DIR) && for f in *.so.*[0-9][0-9][0-9]; do base=$$(echo $$f | sed 's/\.so\..*/\.so/'); [ ! -e $$base ] && ln -sf $$f $$base || true; done

main: setup_symlinks main.o $(QUERY_OBJS)
	$(CXX) $(CXXFLAGS) -o main main.o $(QUERY_OBJS) $(LDFLAGS)

main.o: main.cpp queries/queries.h
	$(CXX) $(CXXFLAGS) -c main.cpp -o main.o

queries/%.o: queries/%.cpp parquet_reader.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f main *.o queries/*.o
