// C definitions that mirror the Rust FFI shims in zkvm-jetpack/src/ffi/trace.rs.
#pragma once
#include <stdint.h>
#include <stddef.h>

typedef struct {
    const uint8_t* ptr;
    size_t len;
} prover_bytes_t;

typedef struct {
    uint32_t step;
    uint32_t len;
    uint32_t num_cols;
    const uint64_t* dat;
} prover_mary_t;

typedef struct {
    prover_bytes_t header;
    prover_bytes_t nonce;
    uint64_t pow_len;
} prover_input_v2_t;

typedef struct {
    prover_mary_t compute;
    prover_mary_t memory;
} prover_tables_v2_t;

typedef struct {
    prover_mary_t compute;
    prover_mary_t memory;
} prover_trace_bundle_t;

// Device buffer descriptor used by GPU upload helpers.
typedef struct {
    uint64_t* ptr;
    size_t len;
    uint32_t step;
    uint32_t cols;
} prover_device_buffer_t;

// Returns 0 on success, non-zero on failure. Currently implemented as a stub.
int prover_build_trace_v2(const prover_input_v2_t* input, prover_trace_bundle_t* out);

// Ingest precomputed tables (hybrid CPU/GPU path). Returns 0 on success.
int prover_ingest_tables_v2(const prover_tables_v2_t* tables, prover_trace_bundle_t* out);

// Build trace on CPU then upload to GPU device buffers. Returns 0 on success.
int prover_trace_upload_v2(const prover_input_v2_t* input,
                           prover_device_buffer_t* compute_dev,
                           prover_device_buffer_t* memory_dev);

// Upload caller-supplied tables to GPU device buffers. Returns 0 on success.
int prover_trace_upload_tables_v2(const prover_tables_v2_t* tables,
                                  prover_device_buffer_t* compute_dev,
                                  prover_device_buffer_t* memory_dev);

// Free a device buffer allocated by the upload helpers (no-op on null).
void prover_trace_free_device_buffer(prover_device_buffer_t* buf);

