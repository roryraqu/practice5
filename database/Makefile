SRC = $(wildcard src/*.cpp)
OBJ = $(patsubst src/%.cpp, build/%.o, $(SRC))

all: build interface

build:
	@mkdir -p build

rebuild: clean all

interface: $(OBJ)
	g++ $^ -o $@

build/%.o: src/%.cpp
	g++ -c $< -o $@ -Iinclude -pthread

clean:
	rm -rf build