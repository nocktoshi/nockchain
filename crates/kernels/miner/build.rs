use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.ancestors().nth(3).expect("repo root");
    let jam_path = repo_root.join("assets/miner.jam");

    println!("cargo:rerun-if-env-changed=KERNEL_JAM_PATH");
    println!("cargo:rerun-if-changed={}", jam_path.display());

    let jam_path = if let Some(ref p) = env::var_os("KERNEL_JAM_PATH") {
        PathBuf::from(p)
    } else {
        if !jam_path.exists() {
            panic!(
                "assets/miner.jam not found at {}. Build it with: make assets/miner.jam",
                jam_path.display()
            );
        }
        jam_path
    };

    println!("cargo:rustc-env=KERNEL_JAM_PATH={}", jam_path.display());
}
