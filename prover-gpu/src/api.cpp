#include <cstdint>
#include <cstdlib>
#include <cuda_runtime.h>
#include <iostream>

extern "C" {
// Exposed C ABI
int prover_init();
int prover_ntt_forward(const uint64_t* in, uint64_t* out, uint64_t mod, size_t n, uint64_t root);
int prover_ntt_inverse(const uint64_t* in, uint64_t* out, uint64_t mod, size_t n, uint64_t inv_root);
}

extern "C" __global__ void ntt_stage_kernel(uint64_t* __restrict__ a,
                                             const uint64_t* __restrict__ twiddles,
                                             uint64_t mod,
                                             size_t n,
                                             size_t len);
extern "C" __global__ void gen_twiddles_kernel(uint64_t* __restrict__ twiddles,
                                                uint64_t root,
                                                uint64_t mod,
                                                size_t n_half);
extern "C" __global__ void scale_kernel(uint64_t* __restrict__ a,
                                         uint64_t scalar,
                                         uint64_t mod,
                                         size_t n);
extern "C" __global__ void shared_ntt_kernel(uint64_t* __restrict__ a,
                                              uint64_t root,
                                              uint64_t mod,
                                              size_t n,
                                              size_t block_len);
extern "C" __global__ void to_montgomery_kernel(uint64_t* __restrict__ a,
                                                 uint64_t R2,
                                                 uint64_t mod,
                                                 size_t n);
extern "C" __global__ void from_montgomery_kernel(uint64_t* __restrict__ a,
                                                   uint64_t mod,
                                                   size_t n);

static void checkCuda(cudaError_t e, const char* what) {
    if (e != cudaSuccess) {
        std::cerr << "CUDA error (" << what << "): " << cudaGetErrorString(e) << std::endl;
        std::exit(1);
    }
}

int prover_init() {
    // simple device availability check
    int dev = 0;
    cudaError_t e = cudaGetDevice(&dev);
    if (e != cudaSuccess) return -1;
    return 0;
}

// compute modular pow on host (simple, for selecting root inv)
static uint64_t host_modmul(uint64_t a, uint64_t b, uint64_t mod) {
    __uint128_t p = ( __uint128_t)a * b;
    return (uint64_t)(p % (__uint128_t)mod);
}
static uint64_t host_modpow(uint64_t a, uint64_t e, uint64_t mod) {
    uint64_t r = 1 % mod;
    uint64_t base = a % mod;
    while (e) {
        if (e & 1) r = host_modmul(r, base, mod);
        base = host_modmul(base, base, mod);
        e >>= 1;
    }
    return r;
}

int prover_ntt_forward(const uint64_t* in, uint64_t* out, uint64_t mod, size_t n, uint64_t root) {
    if ((n & (n-1)) != 0) return -1;

    size_t n_half = n/2;
    uint64_t *d_poly = nullptr;
    uint64_t *d_tw = nullptr;

    // Compute Montgomery R = 2^64 mod mod and R^2 (used for montify on device)
    unsigned __int128 R128 = (((unsigned __int128)1) << 64) % (unsigned __int128)mod;
    uint64_t R = (uint64_t)R128;
    uint64_t R2 = (uint64_t)(((__uint128_t)R * (__uint128_t)R) % (__uint128_t)mod);

    checkCuda(cudaMalloc((void**)&d_poly, n * sizeof(uint64_t)), "malloc poly");
    checkCuda(cudaMemcpy(d_poly, in, n * sizeof(uint64_t), cudaMemcpyHostToDevice), "copy in");

    // Convert input buffer to Montgomery domain on device
    {
        int tblock = 256;
        int tgrid = (int)((n + tblock - 1) / tblock);
        to_montgomery_kernel<<<tgrid, tblock>>>(d_poly, R2, mod, n);
        checkCuda(cudaDeviceSynchronize(), "to_montgomery");
    }

    // Prefer in-block shared-memory NTT when n is divisible by a reasonable block size
    size_t max_block_threads = 1024; // typical CUDA max
    size_t block_len = (n >= 1024 && (n % 1024) == 0) ? 1024 : ((n >= 256 && (n % 256) == 0) ? 256 : 0);
    if (block_len != 0) {
        size_t grid_blocks = n / block_len;
        size_t shared_bytes = block_len * sizeof(uint64_t);
        // Launch one block per segment, each block performs a full in-block NTT
        // pass montified root so device pow/mul operate in Montgomery domain
        uint64_t root_mont = (uint64_t)(((__uint128_t)root * (__uint128_t)R) % (__uint128_t)mod);
        shared_ntt_kernel<<<(int)grid_blocks, (int)block_len, (int)shared_bytes>>>(d_poly, root_mont, mod, n, block_len);
        checkCuda(cudaDeviceSynchronize(), "shared ntt");
    } else {
        // fallback path: allocate and generate global twiddles
        checkCuda(cudaMalloc((void**)&d_tw, n_half * sizeof(uint64_t)), "malloc tw");
        int tblock = 256;
        int tgrid = (int)((n_half + tblock - 1) / tblock);
        // generate twiddles in Montgomery domain by passing montified root
        uint64_t root_mont = (uint64_t)(((__uint128_t)root * (__uint128_t)R) % (__uint128_t)mod);
        gen_twiddles_kernel<<<tgrid, tblock>>>(d_tw, root_mont, mod, n_half);
        checkCuda(cudaDeviceSynchronize(), "gen twiddles");
        // fallback to global per-stage kernel
        size_t butterflies = n_half;
        int bblock = 256;
        int bgrid = (int)((butterflies + bblock - 1) / bblock);
        for (size_t len = 1; len < n; len <<= 1) {
            ntt_stage_kernel<<<bgrid, bblock>>>(d_poly, d_tw, mod, n, len);
            checkCuda(cudaDeviceSynchronize(), "ntt stage");
        }
    }

    // Convert result out of Montgomery domain on device, then copy to host
    {
        int tblock = 256;
        int tgrid = (int)((n + tblock - 1) / tblock);
        from_montgomery_kernel<<<tgrid, tblock>>>(d_poly, mod, n);
        checkCuda(cudaDeviceSynchronize(), "from_montgomery");
    }
    checkCuda(cudaMemcpy(out, d_poly, n * sizeof(uint64_t), cudaMemcpyDeviceToHost), "copy out");

    // cleanup
    cudaFree(d_poly);
    if (d_tw) cudaFree(d_tw);
    return 0;
}

int prover_ntt_inverse(const uint64_t* in, uint64_t* out, uint64_t mod, size_t n, uint64_t inv_root) {
    if ((n & (n-1)) != 0) return -1;

    size_t n_half = n/2;
    uint64_t *d_poly = nullptr;
    uint64_t *d_tw = nullptr;

    // compute R and R^2 for to_montgomery
    unsigned __int128 R128 = (((unsigned __int128)1) << 64) % (unsigned __int128)mod;
    uint64_t R = (uint64_t)R128;
    uint64_t R2 = (uint64_t)(((__uint128_t)R * (__uint128_t)R) % (__uint128_t)mod);

    checkCuda(cudaMalloc((void**)&d_poly, n * sizeof(uint64_t)), "malloc poly");
    checkCuda(cudaMemcpy(d_poly, in, n * sizeof(uint64_t), cudaMemcpyHostToDevice), "copy in");

    // montify input on device
    {
        int tblock = 256;
        int tgrid = (int)((n + tblock - 1) / tblock);
        to_montgomery_kernel<<<tgrid, tblock>>>(d_poly, R2, mod, n);
        checkCuda(cudaDeviceSynchronize(), "to_montgomery");
    }

    // montify inv_root
    uint64_t inv_root_mont = (uint64_t)(((__uint128_t)inv_root * (__uint128_t)R) % (__uint128_t)mod);

    // Run NTT with inv_root_mont (same branching as forward)
    size_t block_len = (n >= 1024 && (n % 1024) == 0) ? 1024 : ((n >= 256 && (n % 256) == 0) ? 256 : 0);
    if (block_len != 0) {
        size_t grid_blocks = n / block_len;
        size_t shared_bytes = block_len * sizeof(uint64_t);
        shared_ntt_kernel<<<(int)grid_blocks, (int)block_len, (int)shared_bytes>>>(d_poly, inv_root_mont, mod, n, block_len);
        checkCuda(cudaDeviceSynchronize(), "shared intt");
    } else {
        checkCuda(cudaMalloc((void**)&d_tw, n_half * sizeof(uint64_t)), "malloc tw");
        int tblock = 256;
        int tgrid = (int)((n_half + tblock - 1) / tblock);
        gen_twiddles_kernel<<<tgrid, tblock>>>(d_tw, inv_root_mont, mod, n_half);
        checkCuda(cudaDeviceSynchronize(), "gen twiddles");
        size_t butterflies = n_half;
        int bblock = 256;
        int bgrid = (int)((butterflies + bblock - 1) / bblock);
        for (size_t len = 1; len < n; len <<= 1) {
            ntt_stage_kernel<<<bgrid, bblock>>>(d_poly, d_tw, mod, n, len);
            checkCuda(cudaDeviceSynchronize(), "ntt stage");
        }
    }

    // compute inv_n = n^{-1} mod p on host
    auto egcd_inv = [&](uint64_t a, uint64_t m) -> uint64_t {
        __int128 t = 0, newt = 1;
        __int128 r = (__int128)m, newr = (__int128)a;
        while (newr != 0) {
            uint64_t q = (uint64_t)(r / newr);
            __int128 tmp = newt;
            newt = t - (__int128)q * newt;
            t = tmp;
            __int128 tmp_r = newr;
            newr = r - (__int128)q * newr;
            r = tmp_r;
        }
        if (r > 1) return 0;
        if (t < 0) t += m;
        return (uint64_t)t;
    };

    uint64_t inv_n = egcd_inv((uint64_t)(n % (size_t)mod), mod);
    if (inv_n == 0) {
        cudaFree(d_poly);
        if (d_tw) cudaFree(d_tw);
        return -2;
    }

    // montify inv_n (scale kernel expects Montgomery-form scalar)
    uint64_t inv_n_mont = (uint64_t)(((__uint128_t)inv_n * (__uint128_t)R) % (__uint128_t)mod);
    // scale by inv_n in Montgomery domain
    {
        int tblock = 256;
        int tgrid = (int)((n + tblock - 1) / tblock);
        scale_kernel<<<tgrid, tblock>>>(d_poly, inv_n_mont, mod, n);
        checkCuda(cudaDeviceSynchronize(), "scale inv_n");
    }

    // demontify on device
    {
        int tblock = 256;
        int tgrid = (int)((n + tblock - 1) / tblock);
        from_montgomery_kernel<<<tgrid, tblock>>>(d_poly, mod, n);
        checkCuda(cudaDeviceSynchronize(), "from_montgomery");
    }

    checkCuda(cudaMemcpy(out, d_poly, n * sizeof(uint64_t), cudaMemcpyDeviceToHost), "copy out");

    cudaFree(d_poly);
    if (d_tw) cudaFree(d_tw);
    return 0;
}
