//! Exit-effect handling for interactive sessions that run the kernel multiple times on one serf.

use nockvm::noun::NounAllocator;
use tracing::{debug, error};

use crate::nockapp::driver::{make_driver, IODriverFn};
use crate::nockapp::EXIT_OK;

/// Like [`super::exit::exit`], but on exit code 0 completes the current [`crate::NockApp::run`]
/// without shutting down the serf, so another `run` can be started.
///
/// Non-zero exits still go through the normal exit path (save + shutdown).
pub fn complete_run_on_exit() -> IODriverFn {
    make_driver(|handle| async move {
        debug!("complete_run_on_exit: waiting for effects");
        loop {
            match handle.next_effect().await {
                Ok(eff) => {
                    let exit_code: Option<usize> = unsafe {
                        let noun = eff.root();
                        if let Ok(cell) = noun.as_cell() {
                            let space = eff.noun_space();
                            let cell = cell.in_space(&space);
                            if cell.head().eq_bytes(b"exit") && cell.tail().is_atom() {
                                Some(
                                    cell.tail()
                                        .as_atom()
                                        .and_then(|atom| atom.as_u64())
                                        .map(|u| u as usize)
                                        .unwrap_or(1),
                                )
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };
                    if let Some(code) = exit_code {
                        if code == EXIT_OK {
                            handle.exit.complete_run().await?;
                        } else {
                            handle.exit.exit(code).await?;
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving effect: {:?}", e);
                }
            }
        }
    })
}
