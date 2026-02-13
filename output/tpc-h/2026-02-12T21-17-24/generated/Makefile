PYARROW_DIR := $(shell python3 -c "import pyarrow; print(pyarrow.__path__[0])")
ARROW_INCLUDE := -I$(PYARROW_DIR)/include
ARROW_LIBS := -L$(PYARROW_DIR) -larrow -lparquet -Wl,-rpath,$(PYARROW_DIR)

CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall $(ARROW_INCLUDE)
LDFLAGS = $(ARROW_LIBS) -lpthread

.PHONY: all clean setup_symlinks

all: main

setup_symlinks:
	@cd $(PYARROW_DIR) && for f in *.so.*[0-9][0-9][0-9]; do base=$$(echo $$f | sed 's/\.so\..*/\.so/'); [ ! -e $$base ] && ln -sf $$f $$base || true; done

main: setup_symlinks main.cpp $(wildcard queries/*.cpp)
	$(CXX) $(CXXFLAGS) -o main main.cpp $(LDFLAGS)

clean:
	rm -f main *.o queries/*.o
