cmake_minimum_required(VERSION 3.18)
project(prover-gpu LANGUAGES C CXX CUDA)

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

# Use an external prebuilt blake3 library instead of compiling sources.
# Set -DUSE_EXTERNAL_BLAKE3=ON (default) and provide
# -DBLAKE3_LIB_PATH=/path/to/libblake3.a or a directory containing it.
option(USE_EXTERNAL_BLAKE3 "Link against an external blake3 library instead of building sources" ON)
set(BLAKE3_LIB_PATH "" CACHE PATH "Path to external blake3 library file or directory")

if(USE_EXTERNAL_BLAKE3)
  if(BLAKE3_LIB_PATH)
    if(IS_DIRECTORY ${BLAKE3_LIB_PATH})
      find_library(BLAKE3_LIB NAMES blake3 PATHS ${BLAKE3_LIB_PATH} NO_DEFAULT_PATH)
    else()
      # Allow passing the full path to the library file or object file
      set(BLAKE3_LIB ${BLAKE3_LIB_PATH})
    endif()
  else()
    find_library(BLAKE3_LIB NAMES blake3)
  endif()

  if(NOT BLAKE3_LIB)
    message(FATAL_ERROR "USE_EXTERNAL_BLAKE3 is ON but external blake3 library not found. Set -DBLAKE3_LIB_PATH=/path/to/libblake3.a or /path/to/blake3.o")
  endif()

  # If the user provided an object file (.o), create an IMPORTED OBJECT
  # target so it can be linked cleanly. If they provided a static/shared
  # library (.a/.so/.dylib) create the appropriate IMPORTED target.
  get_filename_component(BLAKE3_LIB_EXT ${BLAKE3_LIB} EXT)
  string(TOLOWER "${BLAKE3_LIB_EXT}" BLAKE3_LIB_EXT)
  if(BLAKE3_LIB_EXT STREQUAL ".o")
    message(STATUS "Using external blake3 object file: ${BLAKE3_LIB}")
    add_library(blake3_object OBJECT IMPORTED GLOBAL)
    set_property(TARGET blake3_object PROPERTY IMPORTED_OBJECTS "${BLAKE3_LIB}")
    target_link_libraries(prover-gpu PRIVATE blake3_object)
    target_link_libraries(prover_gpu PRIVATE blake3_object)
  elseif(BLAKE3_LIB_EXT STREQUAL ".a")
    message(STATUS "Using external blake3 static library: ${BLAKE3_LIB}")
    add_library(blake3_static STATIC IMPORTED GLOBAL)
    set_target_properties(blake3_static PROPERTIES IMPORTED_LOCATION "${BLAKE3_LIB}")
    target_link_libraries(prover-gpu PRIVATE blake3_static)
    target_link_libraries(prover_gpu PRIVATE blake3_static)
  elseif(BLAKE3_LIB_EXT STREQUAL ".so" OR BLAKE3_LIB_EXT STREQUAL ".dylib" OR BLAKE3_LIB_EXT STREQUAL ".dll")
    message(STATUS "Using external blake3 shared library: ${BLAKE3_LIB}")
    add_library(blake3_shared SHARED IMPORTED GLOBAL)
    set_target_properties(blake3_shared PROPERTIES IMPORTED_LOCATION "${BLAKE3_LIB}")
    target_link_libraries(prover-gpu PRIVATE blake3_shared)
    target_link_libraries(prover_gpu PRIVATE blake3_shared)
  else()
    # Unknown extension — try linking the path directly (may work for some
    # toolchains) and report the choice.
    message(STATUS "Linking external blake3 (unrecognized extension): ${BLAKE3_LIB}")
    target_link_libraries(prover-gpu PRIVATE ${BLAKE3_LIB})
    target_link_libraries(prover_gpu PRIVATE ${BLAKE3_LIB})
  endif()
else()
  message(FATAL_ERROR "Building vendored blake3 sources has been disabled. Set -DUSE_EXTERNAL_BLAKE3=ON and provide -DBLAKE3_LIB_PATH.")
endif()

# Build a shared library providing a simple C ABI so rust can call it directly.
add_library(prover_gpu SHARED
  src/api.cpp
  src/kernel.cu
)

# Link external blake3 into the shared library as well (same library used
# above). The `target_link_libraries` call for `prover-gpu` above also adds
# the link for the shared `prover_gpu` target below when an external library
# is used.
if(USE_EXTERNAL_BLAKE3)
  target_link_libraries(prover_gpu PRIVATE ${BLAKE3_LIB})
endif()

set_target_properties(prover_gpu PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Recommended flags; adjust as needed
target_compile_options(prover-gpu PRIVATE
    $<$<COMPILE_LANGUAGE:CUDA>:-lineinfo>
    $<$<COMPILE_LANGUAGE:C>:-Wall -Wextra>
)

# If optimized SIMD implementations are not present, ensure the dispatcher
# doesn't reference them by disabling those implementations at compile time.
target_compile_definitions(prover-gpu PRIVATE
  BLAKE3_NO_SSE2
  BLAKE3_NO_SSE41
  BLAKE3_NO_AVX2
  BLAKE3_NO_AVX512
)

target_compile_definitions(prover_gpu PRIVATE
  BLAKE3_NO_SSE2
  BLAKE3_NO_SSE41
  BLAKE3_NO_AVX2
  BLAKE3_NO_AVX512
)

target_include_directories(prover-gpu PRIVATE ${CMAKE_SOURCE_DIR})
target_include_directories(prover_gpu PRIVATE ${CMAKE_SOURCE_DIR})

target_include_directories(prover-gpu PRIVATE ${CUDA_RUNTIME_INCLUDE_DIR})
target_include_directories(prover_gpu PRIVATE ${CUDA_RUNTIME_INCLUDE_DIR})

target_link_libraries(prover-gpu PRIVATE CUDA::cudart)
target_link_libraries(prover_gpu PRIVATE CUDA::cudart)

# enforce C for the .c files to avoid name-mangling/linker issues.
# Vendored blake3 sources removed — linking external library instead.
