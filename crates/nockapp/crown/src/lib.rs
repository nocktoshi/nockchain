//! # Crown
//!
//! The Crown library provides a set of modules and utilities for working with
//! the Sword runtime. It includes functionality for handling jammed nouns, kernels (as jammed nouns),
//! and various types and utilities that make sword easier to use.
//!
//! ## Modules
//!
//! - `kernel`: Sword runtime interface.
//! - `noun`: Extensions and utilities for working with Urbit nouns.
//! - `utils`: Errors, misc functions and extensions.
//!
pub mod drivers;
pub mod kernel;
pub mod nockapp;
pub mod noun;
pub mod observability;
pub mod utils;

pub use bytes::*;
pub use nockapp::NockApp;
pub use noun::{AtomExt, JammedNoun, NounExt};
pub use sword::noun::Noun;
pub use utils::bytes::{ToBytes, ToBytesExt};
pub use utils::error::{CrownError, Result};

pub use drivers::*;

use std::path::PathBuf;

/// Returns the default directory where kernel data is stored.
///
/// # Arguments
///
/// * `dir` - A string slice that holds the kernel identifier.
///
/// # Example
///
/// ```
///
/// use std::path::PathBuf;
/// use crown::default_data_dir;
/// let dir = default_data_dir("crown");
/// assert_eq!(dir, PathBuf::from("./.data.crown"));
/// ```
pub fn default_data_dir(dir_name: &str) -> PathBuf {
    PathBuf::from(format!("./.data.{}", dir_name))
}

pub fn system_data_dir() -> PathBuf {
    let home_dir = dirs::home_dir().expect("Failed to get home directory");
    home_dir.join(".nockapp")
}

/// Default size for the Nock stack (1 GB)
pub const DEFAULT_NOCK_STACK_SIZE: usize = 1 << 27;
