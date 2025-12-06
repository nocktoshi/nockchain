cmake_minimum_required(VERSION 3.18)
project(prover-gpu LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(prover-gpu
  src/main.cpp
  src/kernel.cu
)

# include vendored blake3 implementation for Merkle hashing
if(EXISTS ${CMAKE_SOURCE_DIR}/third_party/blake3/blake3.c AND EXISTS ${CMAKE_SOURCE_DIR}/third_party/blake3/blake3.h)
  target_sources(prover-gpu PRIVATE third_party/blake3/blake3.c)
else()
  message(FATAL_ERROR "BLAKE3 sources not found in third_party/blake3/. Run scripts/fetch_blake3.sh to download official blake3.c/blake3.h before configuring.")
endif()

# Build a shared library providing a simple C ABI so rust can call it directly.
add_library(prover_gpu SHARED
  src/api.cpp
  src/kernel.cu
)

# include blake3 in the shared lib as well so Rust can call hashing if needed
target_sources(prover_gpu PRIVATE third_party/blake3/blake3.c)

set_target_properties(prover_gpu PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Recommended flags; adjust as needed
target_compile_options(prover-gpu PRIVATE
  $<$<COMPILE_LANGUAGE:CUDA>:-lineinfo>
)

# Enable fast math if desired (commented out by default)
# target_compile_options(prover-gpu PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:-use_fast_math>)
