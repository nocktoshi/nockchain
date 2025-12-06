#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <array>
#include <sstream>
#include <iomanip>
#include "lib/blake3/blake3.h"
#include <cuda_runtime.h>

extern "C" __global__ void mod_mul_kernel(const uint64_t* __restrict__ a,
                                           const uint64_t* __restrict__ b,
                                           uint64_t* __restrict__ out,
                                           uint64_t mod,
                                           size_t n);

extern "C" __global__ void ntt_stage_kernel(uint64_t* __restrict__ a,
                                             const uint64_t* __restrict__ twiddles,
                                             uint64_t mod,
                                             size_t n,
                                             size_t len);

extern "C" __global__ void scale_kernel(uint64_t* __restrict__ a,
                                         uint64_t scalar,
                                         uint64_t mod,
                                         size_t n);
extern "C" __global__ void montgomery_selftest_kernel(uint64_t mod, uint64_t R2, uint64_t* status);
extern "C" __global__ void to_montgomery_kernel(uint64_t* __restrict__ a,
                                                 uint64_t R2,
                                                 uint64_t mod,
                                                 size_t n);
extern "C" __global__ void from_montgomery_kernel(uint64_t* __restrict__ a,
                                                   uint64_t mod,
                                                   size_t n);
extern "C" __global__ void fri_fold_kernel(uint64_t* __restrict__ a,
                                           uint64_t alpha_mont,
                                           uint64_t mod,
                                           size_t n);

static void checkCuda(cudaError_t e, const char* what) {
    if (e != cudaSuccess) {
        std::cerr << "CUDA error (" << what << "): " << cudaGetErrorString(e) << std::endl;
        std::exit(1);
    }
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <elements> <modulus>\n";
        return 1;
    }
    size_t n = std::stoull(argv[1]);
    uint64_t mod = std::stoull(argv[2]);

    // n must be power-of-two for this prototype NTT
    auto is_pow2 = [](size_t x){ return x && !(x & (x-1)); };
    if (!is_pow2(n)) {
        std::cerr << "NTT length must be a power of two\n";
        return 2;
    }

    std::vector<uint64_t> a(n), b(n), out(n);
    std::mt19937_64 rng(12345);
    for (size_t i = 0; i < n; ++i) {
        a[i] = rng() % mod;
        b[i] = rng() % mod;
    }

    uint64_t *d_a, *d_b, *d_out;
    checkCuda(cudaMalloc((void**)&d_a, n * sizeof(uint64_t)), "cudaMalloc a");
    checkCuda(cudaMalloc((void**)&d_b, n * sizeof(uint64_t)), "cudaMalloc b");
    checkCuda(cudaMalloc((void**)&d_out, n * sizeof(uint64_t)), "cudaMalloc out");

    checkCuda(cudaMemcpy(d_a, a.data(), n * sizeof(uint64_t), cudaMemcpyHostToDevice), "copy a");
    checkCuda(cudaMemcpy(d_b, b.data(), n * sizeof(uint64_t), cudaMemcpyHostToDevice), "copy b");

    int block = 256;
    int grid = (int)((n + block - 1) / block);

    // warmup
    mod_mul_kernel<<<grid, block>>>(d_a, d_b, d_out, mod, n);
    checkCuda(cudaDeviceSynchronize(), "warmup sync");

    // --- run montgomery self-test on device ---
    uint64_t *d_status = nullptr;
    checkCuda(cudaMalloc((void**)&d_status, sizeof(uint64_t)), "malloc status");
    // compute R^2 = (2^64 mod mod)^2 mod mod on host
    unsigned __int128 R128 = (((unsigned __int128)1) << 64) % (unsigned __int128)mod;
    uint64_t R = (uint64_t)R128;
    uint64_t R2 = (uint64_t)(((__uint128_t)R * (__uint128_t)R) % (__uint128_t)mod);
    montgomery_selftest_kernel<<<1,1>>>(mod, R2, d_status);
    checkCuda(cudaDeviceSynchronize(), "montgomery selftest");
    uint64_t host_status = 1;
    checkCuda(cudaMemcpy(&host_status, d_status, sizeof(uint64_t), cudaMemcpyDeviceToHost), "copy status");
    if (host_status != 0) {
        std::cerr << "Montgomery self-test FAILED on device (status=" << host_status << ")\n";
    } else {
        std::cout << "Montgomery self-test PASSED on device\n";
    }
    cudaFree(d_status);

    auto t0 = std::chrono::high_resolution_clock::now();
    mod_mul_kernel<<<grid, block>>>(d_a, d_b, d_out, mod, n);
    checkCuda(cudaDeviceSynchronize(), "kernel sync");
    auto t1 = std::chrono::high_resolution_clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    std::cout << "Elements: " << n << "  Time (ms): " << ms << "  Throughput (ops/sec): " << (double)n / (ms/1000.0) << "\n";

    checkCuda(cudaMemcpy(out.data(), d_out, n * sizeof(uint64_t), cudaMemcpyDeviceToHost), "copy out");

    // one small sanity check
    for (size_t i = 0; i < std::min<size_t>(10, n); ++i) {
        unsigned __int128 prod = (unsigned __int128)a[i] * (unsigned __int128)b[i];
        uint64_t expect = (uint64_t)(prod % (unsigned __int128)mod);
        if (out[i] != expect) {
            std::cerr << "Mismatch at " << i << " expected=" << expect << " got=" << out[i] << "\n";
            return 2;
        }
    }

    cudaFree(d_a);
    cudaFree(d_b);
    cudaFree(d_out);

    // --- Now run a simple forward+inverse NTT test on GPU ---
    // For integration with nockchain, use the repo's base field prime and
    // precomputed ordered roots. If n is too large for the available table,
    // the user can still pass a custom root as an extra arg.
    const uint64_t REPO_PRIME = 18446744069414584321ULL;
    static const uint64_t REPO_ROOTS[] = {
        0x0000000000000001ULL, 0xffffffff00000000ULL, 0x0001000000000000ULL, 0xfffffffeff000001ULL,
        0xefffffff00000001ULL, 0x00003fffffffc000ULL, 0x0000008000000000ULL, 0xf80007ff08000001ULL,
        0xbf79143ce60ca966ULL, 0x1905d02a5c411f4eULL, 0x9d8f2ad78bfed972ULL, 0x0653b4801da1c8cfULL,
        0xf2c35199959dfcb6ULL, 0x1544ef2335d17997ULL, 0xe0ee099310bba1e2ULL, 0xf6b2cffe2306baacULL,
        0x54df9630bf79450eULL, 0xabd0a6e8aa3d8a0eULL, 0x81281a7b05f9beacULL, 0xfbd41c6b8caa3302ULL,
        0x30ba2ecd5e93e76dULL, 0xf502aef532322654ULL, 0x4b2a18ade67246b5ULL, 0xea9d5a1336fbc98bULL,
        0x86cdcc31c307e171ULL, 0x4bbaf5976ecfefd8ULL, 0xed41d05b78d6e286ULL, 0x10d78dd8915a171dULL,
        0x59049500004a4485ULL, 0xdfa8c93ba46d2666ULL, 0x7e9bd009b86a0845ULL, 0x400a7f755588e659ULL,
        0x185629dcda58878cULL,
    };

    if (mod != REPO_PRIME) {
        std::cerr << "Warning: provided modulus does not match repo PRIME; using provided modulus for arithmetic.\n";
    }

    // auto-select root from table if possible
    size_t logn = 0; while ((size_t)1<<logn < n) ++logn;
    uint64_t root = 0;
    if (logn < (sizeof(REPO_ROOTS)/sizeof(REPO_ROOTS[0]))) {
        root = REPO_ROOTS[logn];
    } else if (argc >= 5) {
        root = std::stoull(argv[3]);
    } else {
        std::cerr << "NTT root not available for this n; pass root as 3rd arg.\n";
        return 3;
    }

    // compute modular inverse of root using Fermat (assumes prime modulus)
    auto modmul = [&](uint64_t a, uint64_t b)->uint64_t{ __uint128_t p=(__uint128_t)a*b; return (uint64_t)(p%mod); };
    auto modpow = [&](uint64_t a, uint64_t e){ uint64_t r=1; uint64_t base=a%mod; uint64_t ee=e; while(ee){ if(ee&1) r=modmul(r,base); base=modmul(base,base); ee>>=1;} return r; };
    uint64_t inv_root = modpow(root, mod-2);

    // build sample polynomial
    std::vector<uint64_t> poly(n);
    for (size_t i = 0; i < n; ++i) poly[i] = (uint64_t)(i % 1000);

    // host bit-reversal reorder
    size_t logn = 0; while ((size_t)1<<logn < n) ++logn;
    auto bitrev = [&](size_t x){ size_t y=0; for(size_t i=0;i<logn;++i){ y=(y<<1)|(x&1); x>>=1;} return y; };
    std::vector<uint64_t> reordered(n);
    for (size_t i = 0; i < n; ++i) reordered[bitrev(i)] = poly[i];

    // compute twiddles (size n/2)
    std::vector<uint64_t> twiddles(n/2);
    for (size_t i = 0; i < n/2; ++i) twiddles[i] = modpow(root, i);

    uint64_t *d_poly, *d_tw;
    checkCuda(cudaMalloc((void**)&d_poly, n * sizeof(uint64_t)), "cudaMalloc poly");
    checkCuda(cudaMalloc((void**)&d_tw, (n/2) * sizeof(uint64_t)), "cudaMalloc tw");

    checkCuda(cudaMemcpy(d_poly, reordered.data(), n * sizeof(uint64_t), cudaMemcpyHostToDevice), "copy poly");
    checkCuda(cudaMemcpy(d_tw, twiddles.data(), (n/2) * sizeof(uint64_t), cudaMemcpyHostToDevice), "copy tw");

    // iterative stages: len = 1,2,4,...,n/2. Each stage launches n/2 threads.
    size_t butterflies = n/2;
    int bblock = 256;
    int bgrid = (int)((butterflies + bblock - 1) / bblock);
    for (size_t len = 1; len < n; len <<= 1) {
        ntt_stage_kernel<<<bgrid, bblock>>>(d_poly, d_tw, mod, n, len);
        checkCuda(cudaDeviceSynchronize(), "ntt stage sync");
    }

    // Inverse transform: use inv_root twiddles and same stages, then multiply by inv_n
    std::vector<uint64_t> inv_tw(n/2);
    for (size_t i = 0; i < n/2; ++i) inv_tw[i] = modpow(inv_root, i);
    checkCuda(cudaMemcpy(d_tw, inv_tw.data(), (n/2) * sizeof(uint64_t), cudaMemcpyHostToDevice), "copy inv tw");

    for (size_t len = 1; len < n; len <<= 1) {
        ntt_stage_kernel<<<bgrid, bblock>>>(d_poly, d_tw, mod, n, len);
        checkCuda(cudaDeviceSynchronize(), "intt stage sync");
    }

    // multiply by inv_n
    // compute inv_n mod `mod` on host (mod is not necessarily prime; caller must ensure field)
    // Here we assume mod is prime and compute inv_n = n^(mod-2)
    uint64_t inv_n = modpow(n % mod, mod-2);
    scale_kernel<<<(int)((n + 255)/256), 256>>>(d_poly, inv_n, mod, n);
    checkCuda(cudaDeviceSynchronize(), "scale sync");

    std::vector<uint64_t> final(n);
    checkCuda(cudaMemcpy(final.data(), d_poly, n * sizeof(uint64_t), cudaMemcpyDeviceToHost), "copy final");

    // undo bitrev to compare to original
    std::vector<uint64_t> recovered(n);
    for (size_t i = 0; i < n; ++i) recovered[i] = final[bitrev(i)];

    bool ok = true;
    for (size_t i = 0; i < n; ++i) {
        if (recovered[i] != (poly[i] % mod)) { ok = false; break; }
    }
    std::cout << "NTT roundtrip " << (ok?"OK":"FAIL") << "\n";

    cudaFree(d_poly);
    cudaFree(d_tw);

    // --- FRI folding + Merkle hashing prototype ---
    // We'll perform repeated FRI folds on `recovered` (host standard representation),
    // montify the buffer on device, run device `fri_fold_kernel` rounds, and at each
    // round copy the (montified) layer to host, demontify using R_inv, and compute a
    // SHA-256 Merkle root for that layer. This is a prototype/hash-demo only.

    // small SHA-256 implementation (public-domain, minimal)
    // BLAKE3 Merkle helpers (keyed mode)
    auto blake3_hash = [&](const std::vector<uint8_t>& key, const std::vector<uint8_t>& msg) {
        blake3_hasher h;
        if (key.size() == 32) {
            blake3_hasher_init_keyed(&h, key.data());
        } else {
            blake3_hasher_init(&h);
        }
        if (!msg.empty()) blake3_hasher_update(&h, msg.data(), msg.size());
        std::vector<uint8_t> out(32);
        blake3_hasher_finalize(&h, out.data(), out.size());
        return out;
    };
    auto hex = [&](const std::vector<uint8_t>& v){ std::ostringstream ss; ss<<std::hex<<std::setfill('0'); for(auto x:v) ss<<std::setw(2)<<(int)x; return ss.str(); };

    // Prepare device buffer for FRI folding
    uint64_t* d_fri = nullptr;
    checkCuda(cudaMalloc((void**)&d_fri, n * sizeof(uint64_t)), "malloc d_fri");
    checkCuda(cudaMemcpy(d_fri, recovered.data(), n * sizeof(uint64_t), cudaMemcpyHostToDevice), "copy recovered");

    // montify buffer on device using R^2
    int tblock = 256; int tgrid = (int)((n + tblock - 1) / tblock);
    to_montgomery_kernel<<<tgrid, tblock>>>(d_fri, R2, mod, n);
    checkCuda(cudaDeviceSynchronize(), "to_montgomery fri");

    // compute R_inv on host for demontify
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
    uint64_t R_inv = egcd_inv(R, mod);
    if (R_inv == 0) std::cerr << "Warning: failed to compute R_inv for demontify\n";

    std::mt19937_64 fri_rng(424242);
    size_t cur_n = n;
    std::vector<std::string> merkle_roots;
    while (cur_n > 16) {
        // choose alpha in field (non-zero)
        uint64_t alpha = (uint64_t)(fri_rng() % (mod-1) + 1);
        uint64_t alpha_mont = (uint64_t)(((__uint128_t)alpha * (__uint128_t)R) % (__uint128_t)mod);

        int g = (int)(((cur_n>>1) + tblock - 1) / tblock);
        fri_fold_kernel<<<g, tblock>>>(d_fri, alpha_mont, mod, cur_n);
        checkCuda(cudaDeviceSynchronize(), "fri fold");

        // copy first cur_n/2 montified values back and demontify on host
        size_t layer_n = cur_n >> 1;
        std::vector<uint64_t> layer_mont(layer_n);
        checkCuda(cudaMemcpy(layer_mont.data(), d_fri, layer_n * sizeof(uint64_t), cudaMemcpyDeviceToHost), "copy layer mont");
        std::vector<uint64_t> layer(layer_n);
        for (size_t i = 0; i < layer_n; ++i) {
            __uint128_t p = ( __uint128_t ) layer_mont[i] * ( __uint128_t ) R_inv;
            layer[i] = (uint64_t)(p % (__uint128_t)mod);
        }

        // compute leaf hashes
        // keys for domain separation (32 bytes each). For simplicity we use
        // ascii prefixes repeated to fill 32 bytes. In production use secure
        // keys or context strings.
        std::vector<uint8_t> leaf_key(32, 0), node_key(32, 0);
        const char* LK = "BLAKE3_LEAF_KEY_v1_____";
        const char* NK = "BLAKE3_NODE_KEY_v1_____";
        memcpy(leaf_key.data(), LK, std::min<size_t>(strlen(LK), 32));
        memcpy(node_key.data(), NK, std::min<size_t>(strlen(NK), 32));

        std::vector<std::vector<uint8_t>> hashes(layer_n);
        for (size_t i = 0; i < layer_n; ++i) {
            uint64_t v = layer[i];
            std::vector<uint8_t> bytes(8);
            for (int k = 0; k < 8; ++k) bytes[7-k] = (v >> (k*8)) & 0xff;
            hashes[i] = blake3_hash(leaf_key, bytes);
        }
        // build merkle tree
        while (hashes.size() > 1) {
            std::vector<std::vector<uint8_t>> next;
            for (size_t i = 0; i < hashes.size(); i += 2) {
                std::vector<uint8_t> concat = hashes[i];
                if (i + 1 < hashes.size()) concat.insert(concat.end(), hashes[i+1].begin(), hashes[i+1].end());
                else concat.insert(concat.end(), hashes[i].begin(), hashes[i].end());
                next.push_back(blake3_hash(node_key, concat));
            }
            hashes.swap(next);
        }
        merkle_roots.push_back(hex(hashes[0]));

        cur_n = layer_n;
        // continue folding in-place on device (d_fri now has folded data in first cur_n)
    }

    std::cout << "FRI Merkle roots per layer (from top):\n";
    for (size_t i = 0; i < merkle_roots.size(); ++i) std::cout << "  layer " << i << ": " << merkle_roots[i] << "\n";

    cudaFree(d_fri);

    return 0;
}
