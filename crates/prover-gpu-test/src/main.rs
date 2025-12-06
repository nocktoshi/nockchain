use std::path::PathBuf;

use libloading::Library;
use nockchain_math::belt::{badd, bmul, bpow, bsub, Belt, PRIME};

fn bitrev(n: usize, mut x: usize) -> usize {
    let mut y = 0usize;
    let mut bits = 0usize;
    while (1usize << bits) < n {
        bits += 1;
    }
    for _ in 0..bits {
        y = (y << 1) | (x & 1);
        x >>= 1;
    }
    y
}

fn cpu_ntt(a: &mut [u64], root: u64) {
    let n = a.len();
    // bitrev reorder
    for i in 0..n {
        let j = bitrev(n, i);
        if i < j {
            a.swap(i, j);
        }
    }
    let mut len = 2;
    while len <= n {
        // wlen = root^(n/len)
        let wlen = bpow(root, (n / len) as u64);
        for i in (0..n).step_by(len) {
            let mut w = 1u64;
            let half = len / 2;
            for j in 0..half {
                let u = a[i + j];
                let v = bmul(a[i + j + half], w);
                a[i + j] = badd(u, v);
                a[i + j + half] = bsub(u, v);
                w = bmul(w, wlen);
            }
        }
        len <<= 1;
    }
}

fn main() {
    // library location: allow override with PROVER_GPU_LIB env var
    let lib_path =
        std::env::var("PROVER_GPU_LIB").unwrap_or_else(|_| "libprover_gpu.so".to_string());
    let lib = match unsafe { Library::new(&lib_path) } {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to load {}: {}", lib_path, e);
            eprintln!("Build the GPU library with CMake and set PROVER_GPU_LIB to the .so path before running tests.");
            return;
        }
    };

    unsafe {
        type FwdSig = unsafe extern "C" fn(*const u64, *mut u64, u64, usize, u64) -> i32;
        type InvSig = unsafe extern "C" fn(*const u64, *mut u64, u64, usize, u64) -> i32;

        let fwd: libloading::Symbol<FwdSig> = match lib.get(b"prover_ntt_forward") {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Missing symbol prover_ntt_forward: {}", e);
                return;
            }
        };
        let inv: libloading::Symbol<InvSig> = match lib.get(b"prover_ntt_inverse") {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Missing symbol prover_ntt_inverse: {}", e);
                return;
            }
        };

        let sizes = [256usize, 1024usize, 4096usize];
        for &n in &sizes {
            println!("Round-trip Testing N = {}", n);
            // prepare input
            let mut poly: Vec<u64> = (0..n).map(|i| ((i * 13 + 7) % 1000) as u64).collect();
            let original = poly.clone();

            // select primitive root using repo method
            let order_belt = Belt(n as u64);
            let root_belt = match order_belt.ordered_root() {
                Ok(r) => r,
                Err(_) => {
                    eprintln!("No precomputed root for n={}", n);
                    continue;
                }
            };
            let root = root_belt.0 as u64;

            // compute inv_root via extended gcd
            fn egcd_inv(a: u64, m: u64) -> Option<u64> {
                let mut t: i128 = 0;
                let mut newt: i128 = 1;
                let mut r: i128 = m as i128;
                let mut newr: i128 = a as i128;
                while newr != 0 {
                    let q = (r / newr) as i128;
                    let tmp_t = newt;
                    newt = t - q * newt;
                    t = tmp_t;
                    let tmp_r = newr;
                    newr = r - q * newr;
                    r = tmp_r;
                }
                if r > 1 {
                    return None;
                }
                if t < 0 {
                    t += m as i128;
                }
                Some(t as u64)
            }

            let inv_root = match egcd_inv(root, PRIME) {
                Some(x) => x,
                None => {
                    eprintln!("failed to invert root");
                    continue;
                }
            };

            // Call GPU forward
            let mut tmp = vec![0u64; n];
            let ret = fwd(poly.as_ptr(), tmp.as_mut_ptr(), PRIME, n, root);
            if ret != 0 {
                eprintln!("GPU forward error {}", ret);
                continue;
            }

            // Call GPU inverse
            let mut round = vec![0u64; n];
            let ret2 = inv(tmp.as_ptr(), round.as_mut_ptr(), PRIME, n, inv_root);
            if ret2 != 0 {
                eprintln!("GPU inverse error {}", ret2);
                continue;
            }

            // compare round to original
            let mut ok = true;
            for i in 0..n {
                if original[i] != round[i] {
                    eprintln!(
                        "Roundtrip mismatch at {}: orig={} round={}",
                        i, original[i], round[i]
                    );
                    ok = false;
                    break;
                }
            }
            println!("N = {} roundtrip: {}", n, if ok { "OK" } else { "FAIL" });
        }
    }
}
