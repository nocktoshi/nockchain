use std::ffi::OsString;
use std::path::{Path, PathBuf};

use libloading::{Library, Symbol};
use nockchain_math::mary::Mary;
use thiserror::Error;

use crate::ffi::trace::{FfiMary, ProverTablesV2};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DeviceBuffer {
    pub ptr: *mut u64,
    pub len: usize,
    pub step: u32,
    pub cols: u32,
}

type UploadTablesFn =
    unsafe extern "C" fn(*const ProverTablesV2, *mut DeviceBuffer, *mut DeviceBuffer) -> i32;
type FreeBufferFn = unsafe extern "C" fn(*mut DeviceBuffer);

#[derive(Debug, Error)]
pub enum GpuUploadError {
    #[error("failed to load prover GPU library: {0}")]
    Load(libloading::Error),
    #[error("GPU upload returned error code {0}")]
    Ffi(i32),
    #[error("missing symbol {0} in prover GPU library")]
    MissingSymbol(&'static str),
}

/// Handle that keeps the CUDA buffers (and the loaded GPU library) alive.
pub struct GpuTraceUpload {
    _lib: Library,
    pub compute: DeviceBuffer,
    pub memory: DeviceBuffer,
    free_fn: FreeBufferFn,
}

impl Drop for GpuTraceUpload {
    fn drop(&mut self) {
        unsafe {
            (self.free_fn)(&mut self.compute as *mut _);
            (self.free_fn)(&mut self.memory as *mut _);
        }
    }
}

fn ffi_mary_from_mary(m: &Mary) -> FfiMary {
    FfiMary {
        step: m.step,
        len: m.len,
        num_cols: 0, // optional metadata; not used for sizing
        dat: m.dat.as_ptr(),
    }
}

fn default_gpu_lib_path() -> PathBuf {
    std::env::var_os("PROVER_GPU_LIB")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("libprover_gpu.so"))
}

/// Upload precomputed v2 tables (compute + memory) to the GPU. The caller
/// owns the Mary buffers and is responsible for keeping them alive while the
/// GPU buffers are in use.
pub fn upload_tables_v2_to_gpu(
    compute: &Mary,
    memory: &Mary,
    lib_path: Option<&Path>,
) -> Result<GpuTraceUpload, GpuUploadError> {
    let path: PathBuf = lib_path
        .map(|p| p.to_path_buf())
        .or_else(|| Some(default_gpu_lib_path()))
        .unwrap_or_else(|| PathBuf::from(OsString::new()));

    // Load GPU shared library and resolve symbols.
    // Safety: loading a shared library is unsafe; caller controls path via env/arg.
    // This is confined to this function and errors are surfaced as Result.
    let lib = unsafe { Library::new(&path) }.map_err(GpuUploadError::Load)?;
    unsafe {
        let upload_sym: Symbol<UploadTablesFn> =
            lib.get(b"prover_trace_upload_tables_v2").map_err(|_| GpuUploadError::MissingSymbol("prover_trace_upload_tables_v2"))?;
        let free_sym: Symbol<FreeBufferFn> =
            lib.get(b"prover_trace_free_device_buffer").map_err(|_| GpuUploadError::MissingSymbol("prover_trace_free_device_buffer"))?;

        let upload_fn: UploadTablesFn = *upload_sym;
        let free_fn: FreeBufferFn = *free_sym;

        let tables = ProverTablesV2 {
            compute: ffi_mary_from_mary(compute),
            memory: ffi_mary_from_mary(memory),
        };

        let mut compute_dev = DeviceBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
            step: 0,
            cols: 0,
        };
        let mut memory_dev = DeviceBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
            step: 0,
            cols: 0,
        };

        let rc = upload_fn(&tables as *const ProverTablesV2, &mut compute_dev, &mut memory_dev);
        if rc != 0 {
            return Err(GpuUploadError::Ffi(rc));
        }

        Ok(GpuTraceUpload {
            _lib: lib,
            compute: compute_dev,
            memory: memory_dev,
            free_fn,
        })
    }
}


