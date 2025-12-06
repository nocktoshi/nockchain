# Simple Make wrapper for building the prover-gpu project
# Usage:
#   make           # configure and build (Release)
#   make build     # same as default
#   make run       # build and run example with repo PRIME
#   make clean

BUILD_DIR ?= build
CMAKE ?= cmake
NVCC := $(shell command -v nvcc 2>/dev/null || echo /usr/local/cuda/bin/nvcc)
CUDA_ARCHS ?= all
FETCH_SCRIPT := scripts/fetch_blake3.sh

.PHONY: all cmake_build build run clean

all: build

cmake_build:
	@mkdir -p $(BUILD_DIR)
	@echo "==> Configuring (nvcc=$(NVCC), cuda archs=$(CUDA_ARCHS))"
	@$(CMAKE) -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_CUDA_COMPILER="$(NVCC)" -DCMAKE_CUDA_ARCHITECTURES=$(CUDA_ARCHS)
	@echo "==> Building (Release)"
	@$(CMAKE) --build $(BUILD_DIR) --config Release -j$(shell nproc)

build: cmake_build

run: build
	@echo "==> Running example with repo PRIME"
	@$(BUILD_DIR)/prover-gpu 1024 18446744069414584321

clean:
	@echo "==> Cleaning build directory $(BUILD_DIR)"
	@rm -rf $(BUILD_DIR)
