# prover-gpu

Prototype GPU prover components for `nockchain`.

a minimal C++/CUDA project that demonstrates batched modular
arithmetic on an NVIDIA GPU (a core building block for STARK provers: field operations,
NTT/FFT, and other parallel primitives).

not a full STARK prover yet. todo:

- implement field arithmetic matching the STARK prime used by `nockchain`
- implement NTT/INTT kernels for polynomial operations
- implement FRI and Merkle hashing (or create CPU/GPU hybrid for hashing)
- implement host-side protocol to accept `prover-input` and output serialized proof

Build (requires CUDA toolkit and CMake):

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release -j
```

Run the sample benchmark (in `build`):

```bash
./prover-gpu 1000000 1000000000000000003
./prover-gpu 1024 18446744069414584321
```


Notes on Montgomery domain handling

- The C API (`prover_ntt_forward`) now performs automatic conversion to and from
  the Montgomery representation. Inputs are converted to Montgomery domain before
  GPU kernels run and outputs are converted back to standard representation
  before being returned to the caller. This reduces mismatch risk between CPU
  reference routines and GPU arithmetic.

Platform caveats

- This project targets NVIDIA CUDA. Building and running the GPU code requires
  the CUDA toolkit and an NVIDIA GPU. macOS machines typically don't have a
  CUDA-capable NVIDIA GPU; run the CUDA builds and benchmarks on Linux with an
  NVIDIA card for best results.
