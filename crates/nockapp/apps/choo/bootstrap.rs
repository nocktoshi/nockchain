// Bazel-specific bootstrap file for choo
// This is used to provide the correct paths for include_bytes! in the Bazel build

#[cfg(bazel_build)]
pub const KERNEL_JAM: &[u8] = include_bytes!("bootstrap/choo.jam");
#[cfg(bazel_build)]
pub const HOON_TXT: &[u8] = include_bytes!("../hoon/hoon-138.hoon");