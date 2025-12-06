cmake_minimum_required(VERSION 3.18)
project(prover-gpu LANGUAGES CXX CUDA)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(prover-gpu
  src/main.cpp
  src/kernel.cu
)

# Force certain .cpp sources to be compiled with the CUDA language
set_source_files_properties(src/main.cpp PROPERTIES LANGUAGE CUDA)
set_source_files_properties(src/api.cpp PROPERTIES LANGUAGE CUDA)

# Try to find CUDA toolkit (provides libraries and include paths).
find_package(CUDAToolkit REQUIRED)

# Locate cuda_runtime.h so .cpp files that include it can compile
find_path(CUDA_RUNTIME_INCLUDE_DIR
  NAMES cuda_runtime.h
  HINTS $ENV{CUDA_HOME} /usr/local/cuda
  PATH_SUFFIXES include
)
if(NOT CUDA_RUNTIME_INCLUDE_DIR)
  message(FATAL_ERROR "Could not find 'cuda_runtime.h'. Ensure the CUDA toolkit is installed and CUDA_HOME is set."
    " If CUDA is installed in a nonstandard location, set the CUDA_HOME environment variable or pass -DCMAKE_CUDA_COMPILER.")
endif()

# include vendored blake3 implementation for Merkle hashing
if(EXISTS ${CMAKE_SOURCE_DIR}/lib/blake3/blake3.c AND EXISTS ${CMAKE_SOURCE_DIR}/lib/blake3/blake3.h)
  target_sources(prover-gpu PRIVATE lib/blake3/blake3.c)
  # Ensure the dispatch implementation (platform selection) is also compiled
  if(EXISTS ${CMAKE_SOURCE_DIR}/lib/blake3/blake3_dispatch.c)
    target_sources(prover-gpu PRIVATE lib/blake3/blake3_dispatch.c)
  endif()
else()
  message(FATAL_ERROR "BLAKE3 sources not found in lib/blake3/ ")
endif()

# Build a shared library providing a simple C ABI so rust can call it directly.
add_library(prover_gpu SHARED
  src/api.cpp
  src/kernel.cu
)

# include blake3 in the shared lib as well so Rust can call hashing if needed
target_sources(prover_gpu PRIVATE lib/blake3/blake3.c)
if(EXISTS ${CMAKE_SOURCE_DIR}/lib/blake3/blake3_dispatch.c)
  target_sources(prover_gpu PRIVATE lib/blake3/blake3_dispatch.c)
endif()

set_target_properties(prover_gpu PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Recommended flags; adjust as needed
target_compile_options(prover-gpu PRIVATE
    $<$<COMPILE_LANGUAGE:CUDA>:-lineinfo>
    $<$<COMPILE_LANGUAGE:C>:-Wall -Wextra>
)

target_include_directories(prover-gpu PRIVATE ${CMAKE_SOURCE_DIR})
target_include_directories(prover_gpu PRIVATE ${CMAKE_SOURCE_DIR})

target_include_directories(prover-gpu PRIVATE ${CUDA_RUNTIME_INCLUDE_DIR})
target_include_directories(prover_gpu PRIVATE ${CUDA_RUNTIME_INCLUDE_DIR})

target_link_libraries(prover-gpu PRIVATE CUDA::cudart)
target_link_libraries(prover_gpu PRIVATE CUDA::cudart)

# enforce C for the .c files to avoid name-mangling/linker issues.
set_source_files_properties(${CMAKE_SOURCE_DIR}/lib/blake3/blake3.c PROPERTIES LANGUAGE C)
set_source_files_properties(${CMAKE_SOURCE_DIR}/lib/blake3/blake3_dispatch.c PROPERTIES LANGUAGE C)
