# QueryKV — plain Makefile build (no CMake/Bazel dependency on purpose).
#
#   make            build both binaries into bin/
#   make test       build and run the unit tests
#   make clean      remove build artifacts

CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -Iinclude -pthread
LDFLAGS  := -pthread

NODE_SRCS   := src/main.cpp src/node.cpp src/config.cpp src/raft.cpp \
               src/gossip.cpp src/wal.cpp src/shard_router.cpp
CLI_SRCS    := client/cli_main.cpp client/client.cpp src/shard_router.cpp
TEST_SRCS   := tests/test_wal.cpp tests/test_protocol.cpp src/wal.cpp src/shard_router.cpp

BIN_DIR := bin

.PHONY: all clean test

all: $(BIN_DIR)/querykv_node $(BIN_DIR)/querykv_cli

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(BIN_DIR)/querykv_node: $(NODE_SRCS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(NODE_SRCS) -o $@ $(LDFLAGS)

$(BIN_DIR)/querykv_cli: $(CLI_SRCS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -Iclient $(CLI_SRCS) -o $@ $(LDFLAGS)

$(BIN_DIR)/test_wal: tests/test_wal.cpp src/wal.cpp | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) tests/test_wal.cpp src/wal.cpp -o $@ $(LDFLAGS)

$(BIN_DIR)/test_protocol: tests/test_protocol.cpp src/shard_router.cpp | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) tests/test_protocol.cpp src/shard_router.cpp -o $@ $(LDFLAGS)

test: $(BIN_DIR)/test_wal $(BIN_DIR)/test_protocol
	$(BIN_DIR)/test_wal
	$(BIN_DIR)/test_protocol

clean:
	rm -rf $(BIN_DIR)
