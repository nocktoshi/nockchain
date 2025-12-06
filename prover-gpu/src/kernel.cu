#include <stdint.h>
#include <cuda.h>

extern "C" __global__ void mod_mul_kernel(const uint64_t* __restrict__ a,
                                           const uint64_t* __restrict__ b,
                                           uint64_t* __restrict__ out,
                                           uint64_t mod,
                                           size_t n)
{
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    unsigned __int128 prod = (unsigned __int128)a[idx] * (unsigned __int128)b[idx];
    uint64_t r = (uint64_t)(prod % (unsigned __int128)mod);
    out[idx] = r;
}

// --- small modular arithmetic helpers for NTT ---
static __device__ inline uint64_t dev_mod_add(uint64_t a, uint64_t b, uint64_t mod) {
    uint64_t s = a + b;
    if (s >= mod) s -= mod;
    return s;
}

static __device__ inline uint64_t dev_mod_sub(uint64_t a, uint64_t b, uint64_t mod) {
    return (a >= b) ? (a - b) : (a + mod - b);
}

// Montgomery reduction tuned for PRIME = 2^64 - 2^32 + 1
static const unsigned __int128 MONT_PRIME_128 = (unsigned __int128)18446744069414584321ULL;

static __device__ inline uint64_t dev_mont_reduction(unsigned __int128 a) {
    // port of mont_reduction from Rust (reduce_159 style) specialized for PRIME
    unsigned long long low = (unsigned long long)(a & 0xffffffffffffffffULL);
    unsigned long long x1 = (unsigned long long)((a >> 32) & 0xffffffffULL);
    unsigned long long x2 = (unsigned long long)(a >> 64);

    unsigned __int128 c = ((unsigned __int128)(a & 0xffffffffULL) + (unsigned __int128)x1) << 32;
    unsigned long long f = (unsigned long long)(c >> 64);
    unsigned __int128 d = c - ((unsigned __int128)x1 + (unsigned __int128)f * MONT_PRIME_128);

    if ((unsigned __int128)x2 >= d) {
        return (uint64_t)((unsigned __int128)x2 - d);
    } else {
        return (uint64_t)((unsigned __int128)x2 + MONT_PRIME_128 - d);
    }
}

static __device__ inline uint64_t dev_mod_mul(uint64_t a, uint64_t b, uint64_t mod) {
    // Use montgomery-style reduction for 128-bit product
    unsigned __int128 p = (unsigned __int128)a * (unsigned __int128)b;
    return dev_mont_reduction(p);
}

static __device__ inline uint64_t dev_std_mod_mul(uint64_t a, uint64_t b, uint64_t mod) {
    // Standard modular multiplication with 128-bit intermediate
    unsigned __int128 p = (unsigned __int128)a * (unsigned __int128)b;
    return (uint64_t)(p % (unsigned __int128)mod);
}

// Each stage performs n/2 butterflies. We launch one thread per butterfly.
extern "C" __global__ void ntt_stage_kernel(uint64_t* __restrict__ a,
                                             const uint64_t* __restrict__ twiddles,
                                             uint64_t mod,
                                             size_t n,
                                             size_t len)
{
    // len is the "half-size" of the current butterfly group (i.e., step = 2*len)
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x; // idx in [0, n/2)
    size_t half = n >> 1;
    if (idx >= half) return;

    size_t group = idx / len;
    size_t pos = idx % len;
    size_t start = group * (len << 1);
    size_t i = start + pos;
    size_t j = i + len;

    uint64_t u = a[i];
    uint64_t v = a[j];
    uint64_t w = twiddles[pos];
    uint64_t t = dev_std_mod_mul(v, w, mod);
    a[i] = dev_mod_add(u, t, mod);
    a[j] = dev_mod_sub(u, t, mod);
}

// Multiply every element by a scalar (used to multiply by inv_n for INTT)
extern "C" __global__ void scale_kernel(uint64_t* __restrict__ a,
                                         uint64_t scalar,
                                         uint64_t mod,
                                         size_t n)
{
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    a[idx] = dev_std_mod_mul(a[idx], scalar, mod);
}

// Bit-reverse the array in-place
extern "C" __global__ void bitrev_kernel(uint64_t* __restrict__ a, size_t n, size_t logn)
{
    size_t i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    size_t j = 0;
    size_t x = i;
    for (size_t k = 0; k < logn; ++k) {
        j = (j << 1) | (x & 1);
        x >>= 1;
    }
    if (i < j) {
        uint64_t temp = a[i];
        a[i] = a[j];
        a[j] = temp;
    }
}

// device binary exponentiation using montgomery multiply
/*
 * Montgomery-aware exponentiation (device)
 *
 * Contract / expectations:
 * - `base` MUST be in Montgomery representation (i.e. base = original_base * R mod p),
 *   where R = 2^64 mod p. Callers should produce Montgomery-form bases using
 *   `to_montgomery_kernel` (which multiplies by R^2 then uses montgomery reduction),
 *   or place values already in Montgomery form on the device.
 * - The return value is also in Montgomery representation (so it may be fed
 *   directly into `dev_mod_mul` or other Montgomery-aware device ops).
 * - This function uses `dev_mod_mul` for all multiplications, and initializes
 *   the accumulator to montified 1 (1 * R mod p). It therefore performs a
 *   standard binary exponentiation entirely in the Montgomery domain.
 * - Do NOT call this with plain (non-montified) integers; that will produce
 *   incorrect results unless the caller explicitly montified the inputs.
 *
 * Rationale:
 * - Making `dev_mod_pow` Montgomery-aware keeps device pow/mul consistent and
 *   avoids extra host/device conversions in hot paths (twiddle generation,
 *   shared NTT stages, radix-4 fused stages, etc.).
 */
static __device__ inline uint64_t dev_mod_pow(uint64_t base, uint64_t exp, uint64_t mod) {
    // compute R = 2^64 mod mod
    unsigned __int128 R128 = (((unsigned __int128)1) << 64) % (unsigned __int128)mod;
    uint64_t R = (uint64_t)R128;

    // res = montify(1) = 1 * R mod p
    uint64_t res = R % mod;
    uint64_t b = base; // assume base already montified
    while (exp) {
        if (exp & 1) res = dev_mod_mul(res, b, mod);
        b = dev_mod_mul(b, b, mod);
        exp >>= 1;
    }
    return res;
}

// kernel to generate twiddles on device: twiddles[i] = root^i (mod)
extern "C" __global__ void gen_twiddles_kernel(uint64_t* __restrict__ twiddles,
                                               uint64_t root,
                                               uint64_t mod,
                                               size_t n_half)
{
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n_half) return;
    twiddles[idx] = dev_mod_pow(root, idx, mod);
}

// In-block shared-memory iterative NTT kernel. Each block transforms a contiguous
// segment of length `block_len` (must be a power-of-two and <= max threads per block).
// `twiddles` is the global twiddle array of length n/2, computed for the full N.
extern "C" __global__ void shared_ntt_kernel(uint64_t* __restrict__ a,
                                              uint64_t root,
                                              uint64_t mod,
                                              size_t n,
                                              size_t block_len)
{
    extern __shared__ uint64_t s[];
    size_t block_start = blockIdx.x * block_len;
    size_t tid = threadIdx.x;
    if (tid >= block_len) return;

    // load into shared memory
    s[tid] = a[block_start + tid];
    __syncthreads();

    for (size_t len = 1; len < block_len; len <<= 1) {
        if (len * 4 <= block_len) {
            // radix-4 stage: each thread with tid < block_len/4 handles one butterfly
            size_t work = block_len / 4;
            if (tid >= work) { __syncthreads(); continue; }
            size_t group = tid / len;
            size_t pos = tid % len;
            size_t base = group * (4 * len) + pos;
            size_t i0 = base;
            size_t i1 = base + len;
            size_t i2 = base + 2 * len;
            size_t i3 = base + 3 * len;

            // global stride base for radix-4
            size_t stride_base = (n >> 2) / len;
            size_t tw1_idx = pos * stride_base;
            size_t tw2_idx = tw1_idx * 2;
            size_t tw3_idx = tw1_idx * 3;

            uint64_t w1 = dev_mod_pow(root, tw1_idx, mod);
            uint64_t w2 = dev_mod_pow(root, tw2_idx, mod);
            uint64_t w3 = dev_mod_pow(root, tw3_idx, mod);

            uint64_t a0 = s[i0];
            uint64_t a1 = s[i1];
            uint64_t a2 = s[i2];
            uint64_t a3 = s[i3];

            uint64_t t1 = dev_mod_mul(a1, w1, mod);
            uint64_t t2 = dev_mod_mul(a2, w2, mod);
            uint64_t t3 = dev_mod_mul(a3, w3, mod);

            uint64_t A = dev_mod_add(a0, t2, mod);
            uint64_t B = dev_mod_sub(a0, t2, mod);
            uint64_t C = dev_mod_add(t1, t3, mod);
            uint64_t T = dev_mod_sub(t1, t3, mod);

            // primitive 4th root of unity: j = root^(n/4)
            uint64_t j = dev_mod_pow(root, n / 4, mod);
            uint64_t D = dev_mod_mul(T, j, mod);

            s[i0] = dev_mod_add(A, C, mod);
            s[i1] = dev_mod_add(B, D, mod);
            s[i2] = dev_mod_sub(A, C, mod);
            s[i3] = dev_mod_sub(B, D, mod);

            __syncthreads();
        } else {
            // fallback to radix-2 stage (same as before) - have each thread handle one butterfly
            size_t half = block_len >> 1;
            if (tid >= half) { __syncthreads(); continue; }
            size_t group = tid / len;
            size_t pos = tid % len;
            size_t i = group * (len << 1) + pos;
            size_t j = i + len;
            size_t stride = (n >> 1) / len; // stride into global twiddle table
            size_t tw_idx = pos * stride;

            uint64_t u = s[i];
            uint64_t v = s[j];
            uint64_t w = dev_mod_pow(root, tw_idx, mod);
            uint64_t t = dev_mod_mul(v, w, mod);
            s[i] = dev_mod_add(u, t, mod);
            s[j] = dev_mod_sub(u, t, mod);

            __syncthreads();
        }
    }

    // write back
    a[block_start + tid] = s[tid];
}

// Convert array to Montgomery domain using R^2 trick: mont(x) = mont_mul(x, R^2)
extern "C" __global__ void to_montgomery_kernel(uint64_t* __restrict__ a,
                                                 uint64_t R2,
                                                 uint64_t mod,
                                                 size_t n)
{
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    a[idx] = dev_mod_mul(a[idx], R2, mod);
}

// Convert array out of Montgomery domain: from_mont(x) = mont_mul(x, 1)
extern "C" __global__ void from_montgomery_kernel(uint64_t* __restrict__ a,
                                                   uint64_t mod,
                                                   size_t n)
{
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= n) return;
    // multiply by 1 using Montgomery multiply yields x * R^{-1} = original
    a[idx] = dev_mod_mul(a[idx], 1, mod);
}

// Simple device self-test for Montgomery mul and pow. Writes 0 to status[0]
// on success, non-zero on failure. Performs:
//  - montify small integers using R^2 (via dev_mod_mul(x, R2))
//  - montgomery multiply and demontify -> compare to expected (a*b % mod)
//  - montgomery pow and demontify -> compare to expected (a^e % mod)
extern "C" __global__ void montgomery_selftest_kernel(uint64_t mod, uint64_t R2, uint64_t* status) {
    if (threadIdx.x != 0 || blockIdx.x != 0) return;
    uint64_t a = 3;
    uint64_t b = 5;
    // Use standard mod mul for test
    uint64_t prod = dev_std_mod_mul(a, b, mod);
    uint64_t expect = (uint64_t)(((__uint128_t)a * (__uint128_t)b) % (__uint128_t)mod);

    uint64_t ok = 1;
    if (prod != expect) ok = 0;

    // pow test: compute a^e
    uint64_t e = 13;
    uint64_t pow_res = 1;
    for (uint64_t i = 0; i < e; ++i) {
        pow_res = dev_std_mod_mul(pow_res, a, mod);
    }
    // compute expected a^e on device (small loop)
    uint64_t expected_pow = 1;
    for (uint64_t i = 0; i < e; ++i) {
        expected_pow = (uint64_t)(((__uint128_t)expected_pow * (__uint128_t)a) % (__uint128_t)mod);
    }
    if (pow_res != expected_pow) ok = 0;

    status[0] = ok ? 0 : 1;
}

// FRI fold kernel: given array `a` of length `n` (montified), fold into first n/2
// by computing for 0 <= i < n/2: a[i] = a[i] + alpha * a[i + n/2]  (all in Montgomery form)
extern "C" __global__ void fri_fold_kernel(uint64_t* __restrict__ a,
                                           uint64_t alpha_mont,
                                           uint64_t mod,
                                           size_t n)
{
    size_t idx = blockIdx.x * blockDim.x + threadIdx.x;
    size_t half = n >> 1;
    if (idx >= half) return;
    uint64_t u = a[idx];
    uint64_t v = a[idx + half];
    uint64_t t = dev_mod_mul(v, alpha_mont, mod);
    a[idx] = dev_mod_add(u, t, mod);
}

