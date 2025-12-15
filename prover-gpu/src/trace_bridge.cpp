#include "trace_ffi.h"

#include <cuda_runtime.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>

namespace {

static void checkCuda(cudaError_t e, const char* what) {
    if (e != cudaSuccess) {
        std::cerr << "CUDA error (" << what << "): " << cudaGetErrorString(e) << std::endl;
        std::exit(1);
    }
}

}  // namespace

/// Simple bridge: call the Rust FFI to build the v2 trace, then upload the
/// compute/memory Mary buffers to device memory. Returns 0 on success, or the
/// error code from prover_build_trace_v2.
extern "C" int prover_trace_upload_v2(const prover_input_v2_t* input,
                                      prover_device_buffer_t* compute_dev,
                                      prover_device_buffer_t* memory_dev) {
    if (!input || !compute_dev || !memory_dev) {
        return -1;
    }

    prover_trace_bundle_t bundle{};
    int rc = prover_build_trace_v2(input, &bundle);
    if (rc != 0) {
        return rc;
    }

    // Upload compute table
    size_t compute_elems =
        static_cast<size_t>(bundle.compute.len) * static_cast<size_t>(bundle.compute.step);
    if (compute_elems > 0 && bundle.compute.dat) {
        checkCuda(cudaMalloc((void**)&compute_dev->ptr, compute_elems * sizeof(uint64_t)), "cudaMalloc compute");
        checkCuda(cudaMemcpy(compute_dev->ptr, bundle.compute.dat, compute_elems * sizeof(uint64_t), cudaMemcpyHostToDevice),
                  "copy compute");
        compute_dev->len = compute_elems;
        compute_dev->step = bundle.compute.step;
        compute_dev->cols = bundle.compute.num_cols;
    }

    // Upload memory table
    size_t memory_elems =
        static_cast<size_t>(bundle.memory.len) * static_cast<size_t>(bundle.memory.step);
    if (memory_elems > 0 && bundle.memory.dat) {
        checkCuda(cudaMalloc((void**)&memory_dev->ptr, memory_elems * sizeof(uint64_t)), "cudaMalloc memory");
        checkCuda(cudaMemcpy(memory_dev->ptr, bundle.memory.dat, memory_elems * sizeof(uint64_t), cudaMemcpyHostToDevice),
                  "copy memory");
        memory_dev->len = memory_elems;
        memory_dev->step = bundle.memory.step;
        memory_dev->cols = bundle.memory.num_cols;
    }

    return 0;
}

/// Upload precomputed tables (hybrid path) to device. Returns 0 on success.
extern "C" int prover_trace_upload_tables_v2(const prover_tables_v2_t* tables,
                                             prover_device_buffer_t* compute_dev,
                                             prover_device_buffer_t* memory_dev) {
    if (!tables || !compute_dev || !memory_dev) {
        return -1;
    }
    prover_trace_bundle_t bundle{};
    int rc = prover_ingest_tables_v2(tables, &bundle);
    if (rc != 0) {
        return rc;
    }

    size_t compute_elems =
        static_cast<size_t>(bundle.compute.len) * static_cast<size_t>(bundle.compute.step);
    if (compute_elems > 0 && bundle.compute.dat) {
        checkCuda(cudaMalloc((void**)&compute_dev->ptr, compute_elems * sizeof(uint64_t)), "cudaMalloc compute");
        checkCuda(cudaMemcpy(compute_dev->ptr, bundle.compute.dat, compute_elems * sizeof(uint64_t), cudaMemcpyHostToDevice),
                  "copy compute");
        compute_dev->len = compute_elems;
        compute_dev->step = bundle.compute.step;
        compute_dev->cols = bundle.compute.num_cols;
    }

    size_t memory_elems =
        static_cast<size_t>(bundle.memory.len) * static_cast<size_t>(bundle.memory.step);
    if (memory_elems > 0 && bundle.memory.dat) {
        checkCuda(cudaMalloc((void**)&memory_dev->ptr, memory_elems * sizeof(uint64_t)), "cudaMalloc memory");
        checkCuda(cudaMemcpy(memory_dev->ptr, bundle.memory.dat, memory_elems * sizeof(uint64_t), cudaMemcpyHostToDevice),
                  "copy memory");
        memory_dev->len = memory_elems;
        memory_dev->step = bundle.memory.step;
        memory_dev->cols = bundle.memory.num_cols;
    }

    return 0;
}

/// Free a device buffer allocated by the upload helpers (safe on null).
extern "C" void prover_trace_free_device_buffer(prover_device_buffer_t* buf) {
    if (!buf || !buf->ptr) {
        return;
    }
    cudaFree(buf->ptr);
    buf->ptr = nullptr;
    buf->len = 0;
    buf->step = 0;
    buf->cols = 0;
}

