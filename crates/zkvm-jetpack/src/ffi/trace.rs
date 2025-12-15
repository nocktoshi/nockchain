//! C ABI shims for the prover trace builder (version 2).
//! This exposes a stable ABI for C/C++/CUDA callers. The actual trace builder
//! is still being wired up; for now the call returns a structured error so the
//! GPU side can handle it gracefully.

use std::ffi::c_int;

/// Borrowed byte slice view for FFI.
#[repr(C)]
pub struct FfiBytes {
    pub ptr: *const u8,
    pub len: usize,
}

/// View of a Mary (row-major table) for FFI consumers.
#[repr(C)]
pub struct FfiMary {
    /// number of u64 words per row (Mary::step)
    pub step: u32,
    /// number of rows
    pub len: u32,
    /// optional column count metadata (may be 0; not used for sizing)
    pub num_cols: u32,
    /// pointer to the contiguous data buffer (len * step u64s)
    pub dat: *const u64,
}

/// Owned Mary buffer (kept on the Rust side so the FFI view stays valid).
pub struct OwnedMary {
    pub step: u32,
    pub len: u32,
    pub num_cols: u32,
    pub dat: Vec<u64>,
}

impl OwnedMary {
    fn empty() -> Self {
        Self {
            step: 0,
            len: 0,
            num_cols: 0,
            dat: Vec::new(),
        }
    }
    fn as_ffi(&self) -> FfiMary {
        FfiMary {
            step: self.step,
            len: self.len,
            num_cols: self.num_cols,
            dat: self.dat.as_ptr(),
        }
    }
}

/// Prover input for version 2. Header/nonce are provided as raw bytes for now.
#[repr(C)]
pub struct ProverInputV2 {
    pub header: FfiBytes,
    pub nonce: FfiBytes,
    pub pow_len: u64,
}

/// Precomputed tables supplied by the caller (hybrid CPU/GPU flow).
#[repr(C)]
pub struct ProverTablesV2 {
    pub compute: FfiMary,
    pub memory: FfiMary,
}

/// Bundle of compute/memory tables for the prover trace.
#[repr(C)]
pub struct TraceBundle {
    pub compute: FfiMary,
    pub memory: FfiMary,
}

/// Owned bundle that keeps backing storage alive for the FFI views.
pub struct TraceBundleOwned {
    pub compute: OwnedMary,
    pub memory: OwnedMary,
}

/// Result codes surfaced across the FFI boundary.
#[repr(C)]
pub enum TraceBuildErr {
    NullPtr = -1,
    Unimplemented = -2,
    BuildFailed = -3,
    InvalidInput = -4,
}

/// Entry point callable from Rust code (non-FFI) to build the v2 trace.
/// TODO: wire this to the actual v2 trace builder (compute + memory tables).
pub fn build_trace_v2(_input: &ProverInputV2) -> Result<TraceBundleOwned, TraceBuildErr> {
    // TODO: Factor the jet logic (compute_table_jets_v2, memory_table_jets_v2) into a
    // pure-Rust builder that takes prover input and returns Mary buffers.
    Err(TraceBuildErr::Unimplemented)
}

/// Build a trace bundle from caller-supplied tables (hybrid CPU/GPU flow).
pub fn build_trace_v2_from_tables(tables: &ProverTablesV2) -> Result<TraceBundleOwned, TraceBuildErr> {
    // Defensive: verify pointers are non-null when lengths are non-zero.
    let copy_mary = |m: &FfiMary| -> Result<OwnedMary, TraceBuildErr> {
        let elems = (m.len as usize)
            .checked_mul(m.step as usize)
            .ok_or(TraceBuildErr::InvalidInput)?;
        if elems > 0 && m.dat.is_null() {
            return Err(TraceBuildErr::InvalidInput);
        }
        let mut out = OwnedMary {
            step: m.step,
            len: m.len,
            num_cols: m.num_cols,
            dat: Vec::with_capacity(elems),
        };
        if elems > 0 {
            // Safety: caller guarantees `dat` points to `elems` u64s.
            let slice = unsafe { std::slice::from_raw_parts(m.dat, elems) };
            out.dat.extend_from_slice(slice);
        }
        Ok(out)
    };

    Ok(TraceBundleOwned {
        compute: copy_mary(&tables.compute)?,
        memory: copy_mary(&tables.memory)?,
    })
}

/// Build the prover trace (version 2) from the input header/nonce/pow_len.
///
/// Returns 0 on success, negative error code on failure.
#[no_mangle]
pub extern "C" fn prover_build_trace_v2(input: *const ProverInputV2, out: *mut TraceBundle) -> c_int {
    if input.is_null() || out.is_null() {
        return TraceBuildErr::NullPtr as c_int;
    }
    // Safety: caller promises a valid pointer.
    let input_ref = unsafe { &*input };
    match build_trace_v2(input_ref) {
        Ok(bundle_owned) => {
            // Keep backing storage alive by leaking it; caller is expected to
            // consume the data before process exit. A more refined API would
            // add an explicit free function.
            let leaked = Box::new(bundle_owned);
            unsafe {
                (*out).compute = leaked.compute.as_ffi();
                (*out).memory = leaked.memory.as_ffi();
            }
            Box::leak(leaked);
            0
        }
        Err(e) => e as c_int,
    }
}

/// Ingest caller-supplied precomputed tables into a TraceBundle for GPU upload.
#[no_mangle]
pub extern "C" fn prover_ingest_tables_v2(tables: *const ProverTablesV2, out: *mut TraceBundle) -> c_int {
    if tables.is_null() || out.is_null() {
        return TraceBuildErr::NullPtr as c_int;
    }
    let tables_ref = unsafe { &*tables };
    match build_trace_v2_from_tables(tables_ref) {
        Ok(bundle_owned) => {
            let leaked = Box::new(bundle_owned);
            unsafe {
                (*out).compute = leaked.compute.as_ffi();
                (*out).memory = leaked.memory.as_ffi();
            }
            Box::leak(leaked);
            0
        }
        Err(e) => e as c_int,
    }
}

